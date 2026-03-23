"""
FastAPI model serving endpoint.

Loads model from MLflow registry, fetches real-time features
from Feast online store, returns anomaly predictions.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, make_asgi_app

# TODO Phase 3: Import model loader, Feast online client


# ── Metrics ──
PREDICTION_COUNT = Counter("predictions_total", "Total predictions served", ["result"])
PREDICTION_LATENCY = Histogram("prediction_latency_seconds", "Prediction latency")


# ── Request/Response Schemas ──
class PredictionRequest(BaseModel):
    user_id: str
    transaction_id: str
    amount: float
    category: str
    merchant_id: str
    device_type: str


class PredictionResponse(BaseModel):
    transaction_id: str
    anomaly_score: float
    is_anomaly: bool
    model_version: str


# ── App ──
@asynccontextmanager
async def lifespan(app: FastAPI):
    # TODO Phase 3: Load model from MLflow, init Feast online store
    yield
    # Cleanup


app = FastAPI(title="ML Feature Platform - Anomaly Detection API", version="0.1.0", lifespan=lifespan)

# Mount Prometheus metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest):
    """
    Score a transaction for anomaly probability.

    1. Fetch user's real-time features from Feast online store
    2. Combine with request features
    3. Run model inference
    4. Return anomaly score + binary decision
    """
    # TODO Phase 3: Implement
    raise HTTPException(status_code=501, detail="Not yet implemented")
