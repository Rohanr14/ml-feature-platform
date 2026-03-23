"""FastAPI model serving endpoint.

Loads a trained model from MLflow, fetches online features from Feast,
and returns anomaly predictions for incoming transactions.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter

import mlflow.pyfunc
import pandas as pd
from feast import FeatureStore
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.rag_agent.agent import PlatformQueryAgent
from prometheus_client import Counter, Histogram, make_asgi_app

MODEL_FEATURE_COLUMNS = [
    "daily_txn_count",
    "daily_txn_sum",
    "daily_txn_avg",
    "daily_txn_max",
    "daily_unique_categories",
    "daily_unique_merchants",
    "daily_unique_devices",
    "first_txn_hour",
    "last_txn_hour",
    "rolling_7d_txn_count",
    "rolling_7d_avg_amount",
    "rolling_30d_txn_count",
    "rolling_30d_avg_amount",
]
MODEL_FEATURE_SOURCE_MAP = {
    **{feature_name: f"user_daily_features:{feature_name}" for feature_name in MODEL_FEATURE_COLUMNS[:9]},
    **{feature_name: f"user_rolling_features:{feature_name}" for feature_name in MODEL_FEATURE_COLUMNS[9:]},
}
ONLINE_FEATURE_REFERENCES = [
    *(f"user_daily_features:{feature_name}" for feature_name in MODEL_FEATURE_COLUMNS[:9]),
    *(f"user_rolling_features:{feature_name}" for feature_name in MODEL_FEATURE_COLUMNS[9:]),
    "user_realtime_5m_features:txn_count",
    "user_realtime_5m_features:max_amount_ratio",
    "user_transaction_risk_context:amount_over_daily_avg",
    "user_transaction_risk_context:amount_over_30d_avg",
    "user_transaction_risk_context:after_hours_txn",
    "user_transaction_risk_context:rapid_repeat_txn",
]


class ServingSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SERVING_", extra="ignore")

    feature_repo_path: str = "src/feature_store"
    mlflow_model_uri: str = "models:/TransactionAnomalyDetector/Production"
    anomaly_threshold: float = 0.5


class PredictionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: str
    transaction_id: str
    amount: float
    category: str
    merchant_id: str
    device_type: str
    event_timestamp: datetime | None = None
    ms_since_last_txn: int | None = None


class PredictionResponse(BaseModel):
    transaction_id: str
    anomaly_score: float
    is_anomaly: bool
    model_version: str


class ModelInfoResponse(BaseModel):
    model_version: str
    anomaly_threshold: float
    required_request_fields: list[str]
    model_feature_columns: list[str]
    online_feature_references: list[str]


class AgentQueryRequest(BaseModel):
    question: str
    top_k: int = 3


class AgentQueryResponse(BaseModel):
    answer: str
    citations: list[str]
    matched_sections: list[str]


@dataclass
class ServingArtifacts:
    feature_store: FeatureStore
    model: mlflow.pyfunc.PyFuncModel
    model_version: str
    query_agent: PlatformQueryAgent


# ── Metrics ──
PREDICTION_COUNT = Counter("predictions_total", "Total predictions served", ["result"])
PREDICTION_LATENCY = Histogram("prediction_latency_seconds", "Prediction latency")


def build_online_entity_row(request: PredictionRequest) -> dict[str, object]:
    event_timestamp = request.event_timestamp or datetime.now(timezone.utc)
    return {
        "user_id": request.user_id,
        "request_amount": request.amount,
        "request_hour": event_timestamp.hour,
        "request_ms_since_last_txn": request.ms_since_last_txn if request.ms_since_last_txn is not None else -1,
    }


def flatten_online_features(online_response: dict[str, list[object]]) -> dict[str, object]:
    flattened: dict[str, object] = {}
    for key, values in online_response.items():
        if key == "user_id":
            continue
        flattened[key.replace("__", ":")] = values[0] if values else None
    return flattened


def build_model_feature_values(flattened_features: dict[str, object]) -> dict[str, float]:
    model_feature_values = {
        feature_name: flattened_features.get(feature_reference)
        for feature_name, feature_reference in MODEL_FEATURE_SOURCE_MAP.items()
    }
    missing_features = [
        feature_name for feature_name, value in model_feature_values.items() if value is None
    ]
    if missing_features:
        missing_str = ", ".join(missing_features)
        raise ValueError(f"Missing online features required for inference: {missing_str}")
    return {feature_name: float(value) for feature_name, value in model_feature_values.items()}


def feature_dict_to_model_input(feature_values: dict[str, object]) -> pd.DataFrame:
    row = {feature_name: float(feature_values[feature_name]) for feature_name in MODEL_FEATURE_COLUMNS}
    return pd.DataFrame([row])


def load_serving_artifacts(settings: ServingSettings) -> ServingArtifacts:
    feature_store = FeatureStore(repo_path=settings.feature_repo_path)
    model = mlflow.pyfunc.load_model(model_uri=settings.mlflow_model_uri)
    return ServingArtifacts(
        feature_store=feature_store,
        model=model,
        model_version=settings.mlflow_model_uri,
        query_agent=PlatformQueryAgent.from_repo(),
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = ServingSettings()
    app.state.settings = settings
    app.state.artifacts = None
    app.state.startup_error = None

    try:
        app.state.artifacts = load_serving_artifacts(settings)
    except Exception as exc:
        app.state.startup_error = str(exc)

    yield


app = FastAPI(title="ML Feature Platform - Anomaly Detection API", version="0.1.0", lifespan=lifespan)

# Mount Prometheus metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.get("/health")
async def health():
    if app.state.startup_error:
        return {"status": "degraded", "detail": app.state.startup_error}
    return {"status": "healthy", "model_version": app.state.artifacts.model_version}


@app.get("/model-info", response_model=ModelInfoResponse)
async def model_info():
    if app.state.startup_error or app.state.artifacts is None:
        raise HTTPException(status_code=503, detail="Serving artifacts are not available")

    return ModelInfoResponse(
        model_version=app.state.artifacts.model_version,
        anomaly_threshold=app.state.settings.anomaly_threshold,
        required_request_fields=list(PredictionRequest.model_fields.keys()),
        model_feature_columns=MODEL_FEATURE_COLUMNS,
        online_feature_references=ONLINE_FEATURE_REFERENCES,
    )


@app.post("/agent/query", response_model=AgentQueryResponse)
async def agent_query(request: AgentQueryRequest):
    if app.state.startup_error or app.state.artifacts is None:
        raise HTTPException(status_code=503, detail="Serving artifacts are not available")

    answer = app.state.artifacts.query_agent.answer(request.question, top_k=request.top_k)
    return AgentQueryResponse(
        answer=answer.answer,
        citations=answer.citations,
        matched_sections=answer.matched_sections,
    )


@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest):
    """Score a transaction for anomaly probability using Feast online features."""
    if app.state.startup_error or app.state.artifacts is None:
        raise HTTPException(status_code=503, detail="Serving artifacts are not available")

    artifacts: ServingArtifacts = app.state.artifacts
    entity_row = build_online_entity_row(request)

    start = perf_counter()
    with PREDICTION_LATENCY.time():
        try:
            online_response = artifacts.feature_store.get_online_features(
                features=ONLINE_FEATURE_REFERENCES,
                entity_rows=[entity_row],
            ).to_dict()
            flattened_features = flatten_online_features(online_response)
            model_feature_values = build_model_feature_values(flattened_features)
            model_input = feature_dict_to_model_input(model_feature_values)
            prediction_output = artifacts.model.predict(model_input)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Prediction failed: {exc}") from exc

    if isinstance(prediction_output, pd.DataFrame):
        anomaly_score = float(prediction_output.iloc[0, 0])
    elif isinstance(prediction_output, pd.Series):
        anomaly_score = float(prediction_output.iloc[0])
    else:
        anomaly_score = float(prediction_output[0])

    is_anomaly = anomaly_score >= app.state.settings.anomaly_threshold
    PREDICTION_COUNT.labels(result="anomaly" if is_anomaly else "normal").inc()

    _ = perf_counter() - start
    return PredictionResponse(
        transaction_id=request.transaction_id,
        anomaly_score=anomaly_score,
        is_anomaly=is_anomaly,
        model_version=artifacts.model_version,
    )
