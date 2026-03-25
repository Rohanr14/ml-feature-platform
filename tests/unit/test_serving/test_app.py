"""Unit tests for the FastAPI serving helpers and prediction flow."""

from __future__ import annotations

import asyncio
import importlib
import sys
import types
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace


class FakeBaseModel:
    model_fields = {}

    def __init_subclass__(cls, **kwargs):
        annotations = getattr(cls, "__annotations__", {})
        cls.model_fields = {name: None for name in annotations}

    def __init__(self, **data):
        for key, value in data.items():
            setattr(self, key, value)


class FakeDataFrame:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.columns = list(self.rows[0].keys()) if self.rows else []
        self.iloc = _FakeILoc(self)


class _FakeILoc:
    def __init__(self, frame):
        self.frame = frame

    def __getitem__(self, key):
        row_idx, col_idx = key
        column_name = self.frame.columns[col_idx]
        return self.frame.rows[row_idx][column_name]


class FakeSeries(list):
    @property
    def iloc(self):
        return self

    def __getitem__(self, index):
        return super().__getitem__(index)


class FakeHTTPException(Exception):
    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class FakeFastAPI:
    def __init__(self, *args, **kwargs):
        self.state = SimpleNamespace()

    def mount(self, *args, **kwargs):
        return None

    def get(self, *args, **kwargs):
        def decorator(func):
            return func
        return decorator

    def post(self, *args, **kwargs):
        def decorator(func):
            return func
        return decorator


class FakeCounter:
    def __init__(self, *args, **kwargs):
        self.count = 0

    def labels(self, **kwargs):
        return self

    def inc(self):
        self.count += 1


class _FakeTimer:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeHistogram:
    def __init__(self, *args, **kwargs):
        pass

    def time(self):
        return _FakeTimer()


def install_test_stubs() -> None:
    mlflow_module = types.ModuleType("mlflow")
    pyfunc_module = types.ModuleType("mlflow.pyfunc")

    class PyFuncModel:
        def predict(self, model_input):
            return [0.42]

    pyfunc_module.PyFuncModel = PyFuncModel
    pyfunc_module.load_model = lambda model_uri: PyFuncModel()
    mlflow_module.pyfunc = pyfunc_module
    sys.modules["mlflow"] = mlflow_module
    sys.modules["mlflow.pyfunc"] = pyfunc_module

    pandas_module = types.ModuleType("pandas")
    pandas_module.DataFrame = FakeDataFrame
    pandas_module.Series = FakeSeries
    sys.modules["pandas"] = pandas_module

    feast_module = types.ModuleType("feast")

    class FeatureStore:
        def __init__(self, repo_path: str):
            self.repo_path = repo_path

        def get_online_features(self, *, features, entity_rows):
            response = {"user_id": [entity_rows[0]["user_id"]]}
            for feature in features:
                response[feature.replace(":", "__")] = [1.0]
            return SimpleNamespace(to_dict=lambda: response)

    feast_module.FeatureStore = FeatureStore
    sys.modules["feast"] = feast_module

    fastapi_module = types.ModuleType("fastapi")
    fastapi_module.FastAPI = FakeFastAPI
    fastapi_module.HTTPException = FakeHTTPException
    sys.modules["fastapi"] = fastapi_module

    pydantic_module = types.ModuleType("pydantic")
    pydantic_module.BaseModel = FakeBaseModel
    pydantic_module.ConfigDict = dict
    sys.modules["pydantic"] = pydantic_module

    pydantic_settings_module = types.ModuleType("pydantic_settings")
    pydantic_settings_module.BaseSettings = FakeBaseModel
    pydantic_settings_module.SettingsConfigDict = dict
    sys.modules["pydantic_settings"] = pydantic_settings_module

    prometheus_module = types.ModuleType("prometheus_client")
    prometheus_module.Counter = FakeCounter
    prometheus_module.Histogram = FakeHistogram
    prometheus_module.make_asgi_app = lambda: object()
    sys.modules["prometheus_client"] = prometheus_module


install_test_stubs()
serving_app = importlib.import_module("src.serving.app")


class PredictFlowTests(unittest.TestCase):
    def test_build_online_entity_row_uses_request_timestamp(self):
        request = serving_app.PredictionRequest(
            user_id="user_123",
            transaction_id="txn_123",
            amount=75.0,
            category="grocery",
            merchant_id="merchant_1",
            device_type="ios",
            event_timestamp=datetime(2026, 3, 23, 14, 30, tzinfo=timezone.utc),
            ms_since_last_txn=500,
        )

        entity_row = serving_app.build_online_entity_row(request)

        self.assertEqual(entity_row["request_hour"], 14)
        self.assertEqual(entity_row["request_ms_since_last_txn"], 500)

    def test_flatten_online_features_normalizes_feast_keys(self):
        flattened = serving_app.flatten_online_features(
            {
                "user_id": ["user_1"],
                "user_daily_features__daily_txn_count": [3],
                "user_rolling_features__rolling_7d_txn_count": [12],
            }
        )

        self.assertEqual(flattened["user_daily_features:daily_txn_count"], 3)
        self.assertEqual(flattened["user_rolling_features:rolling_7d_txn_count"], 12)

    def test_build_model_feature_values_requires_all_expected_sources(self):
        with self.assertRaises(ValueError):
            serving_app.build_model_feature_values({"user_daily_features:daily_txn_count": 1.0})

    def test_model_info_reports_contract(self):
        serving_app.app.state.startup_error = None
        serving_app.app.state.settings = SimpleNamespace(anomaly_threshold=0.7)
        serving_app.app.state.artifacts = SimpleNamespace(model_version="models:/TransactionAnomalyDetector/Production")

        response = asyncio.run(serving_app.model_info())

        self.assertEqual(response.model_version, "models:/TransactionAnomalyDetector/Production")
        self.assertEqual(response.anomaly_threshold, 0.7)
        self.assertIn("user_id", response.required_request_fields)
        self.assertEqual(response.model_feature_columns, serving_app.MODEL_FEATURE_COLUMNS)

    def test_root_endpoint_returns_service_index(self):
        response = asyncio.run(serving_app.root())
        self.assertEqual(response["status_endpoint"], "/health")
        self.assertEqual(response["prediction_endpoint"], "/predict")

    def test_favicon_endpoint_returns_empty_payload(self):
        response = asyncio.run(serving_app.favicon())
        self.assertEqual(response, {})


    def test_agent_query_returns_structured_answer(self):
        serving_app.app.state.startup_error = None
        serving_app.app.state.artifacts = SimpleNamespace(
            query_agent=SimpleNamespace(
                answer=lambda question, top_k=3: SimpleNamespace(
                    answer=f"reply: {question}",
                    citations=["docs/PHASE_3_GUIDE.md"],
                    matched_sections=["Local Development"],
                )
            )
        )

        request = serving_app.AgentQueryRequest(question="How do I smoke test the API?", top_k=2)
        response = asyncio.run(serving_app.agent_query(request))

        self.assertEqual(response.citations, ["docs/PHASE_3_GUIDE.md"])
        self.assertIn("reply:", response.answer)
        self.assertEqual(response.matched_sections, ["Local Development"])

    def test_predict_returns_prediction_response(self):
        class DummyFeatureStore:
            def get_online_features(self, *, features, entity_rows):
                response = {"user_id": [entity_rows[0]["user_id"]]}
                for feature_name in serving_app.MODEL_FEATURE_COLUMNS[:9]:
                    response[f"user_daily_features__{feature_name}"] = [2.0]
                for feature_name in serving_app.MODEL_FEATURE_COLUMNS[9:]:
                    response[f"user_rolling_features__{feature_name}"] = [4.0]
                response["user_realtime_5m_features__txn_count"] = [5.0]
                response["user_realtime_5m_features__max_amount_ratio"] = [1.2]
                response["user_transaction_risk_context__amount_over_daily_avg"] = [1.1]
                response["user_transaction_risk_context__amount_over_30d_avg"] = [1.3]
                response["user_transaction_risk_context__after_hours_txn"] = [0]
                response["user_transaction_risk_context__rapid_repeat_txn"] = [1]
                return SimpleNamespace(to_dict=lambda: response)

        class DummyModel:
            def predict(self, model_input):
                self.last_columns = list(model_input.columns)
                return [0.91]

        dummy_model = DummyModel()
        serving_app.app.state.startup_error = None
        serving_app.app.state.settings = SimpleNamespace(anomaly_threshold=0.5)
        serving_app.app.state.artifacts = serving_app.ServingArtifacts(
            feature_store=DummyFeatureStore(),
            model=dummy_model,
            model_version="models:/TransactionAnomalyDetector/Production",
            query_agent=SimpleNamespace(answer=lambda question, top_k=3: SimpleNamespace(answer="ok", citations=[], matched_sections=[])),
        )

        request = serving_app.PredictionRequest(
            user_id="user_456",
            transaction_id="txn_456",
            amount=130.0,
            category="electronics",
            merchant_id="merchant_9",
            device_type="android",
        )
        response = asyncio.run(serving_app.predict(request))

        self.assertEqual(response.transaction_id, "txn_456")
        self.assertTrue(response.is_anomaly)
        self.assertAlmostEqual(response.anomaly_score, 0.91)
        self.assertEqual(dummy_model.last_columns, serving_app.MODEL_FEATURE_COLUMNS)


if __name__ == "__main__":
    unittest.main()
