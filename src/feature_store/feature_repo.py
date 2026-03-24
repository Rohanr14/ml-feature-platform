"""
Feast feature store definitions.

Registers both batch (dbt) and streaming (Flink) feature sources,
enabling point-in-time correct feature retrieval for training
and low-latency online serving.
"""

from datetime import timedelta
from pathlib import Path

import pandas as pd
from feast import Entity, FeatureView, Field, FileSource, KafkaSource, RequestSource
from feast.data_format import JsonFormat
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Int64, String

# ── Resolve project root for local data paths ──
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_FEAST_DATA = _PROJECT_ROOT / "data" / "feast"

# ── Entities ──
user = Entity(
    name="user_id",
    description="Unique user identifier",
)

# ── Batch Sources (from dbt → local Parquet, mirrored to MinIO) ──
user_daily_source = FileSource(
    name="user_daily_features_source",
    path=str(_FEAST_DATA / "user_daily_features.parquet"),
    timestamp_field="event_date",
)

user_rolling_source = FileSource(
    name="user_rolling_features_source",
    path=str(_FEAST_DATA / "user_rolling_features.parquet"),
    timestamp_field="event_date",
)

user_realtime_5m_batch_source = FileSource(
    name="user_realtime_5m_batch_source",
    path=str(_FEAST_DATA / "features_5m.parquet"),
    timestamp_field="window_end",
)

# ── Stream Source (from Flink → Kafka) ──
# Flink emits the 5-minute feature windows as JSON to the `features-5m` topic.
# The corresponding batch source is where the same records should be archived for
# historical training/backfills to keep Feast's online/offline semantics aligned.
user_realtime_5m_stream_source = KafkaSource(
    name="user_realtime_5m_stream_source",
    kafka_bootstrap_servers="localhost:9092",
    topic="features-5m",
    timestamp_field="window_end",
    batch_source=user_realtime_5m_batch_source,
    message_format=JsonFormat(
        schema_json=(
            "user_id string, window_start bigint, window_end bigint, window_label string, "
            "txn_count bigint, txn_sum double, txn_avg double, txn_max double, txn_min double, "
            "unique_categories int, unique_merchants int, unique_devices int, avg_inter_txn_ms double, "
            "min_inter_txn_ms double, anomaly_count bigint, max_amount_ratio double, high_amount_count bigint, "
            "computed_at bigint"
        )
    ),
    watermark_delay_threshold=timedelta(minutes=5),
)

# ── Request-Time Sources ──
transaction_request = RequestSource(
    name="transaction_request",
    schema=[
        Field(name="request_amount", dtype=Float32),
        Field(name="request_hour", dtype=Int64),
        Field(name="request_ms_since_last_txn", dtype=Int64),
    ],
)

# ── Feature Views ──
user_daily_fv = FeatureView(
    name="user_daily_features",
    entities=[user],
    ttl=timedelta(days=90),
    schema=[
        Field(name="daily_txn_count", dtype=Int64),
        Field(name="daily_txn_sum", dtype=Float32),
        Field(name="daily_txn_avg", dtype=Float32),
        Field(name="daily_txn_max", dtype=Float32),
        Field(name="daily_unique_categories", dtype=Int64),
        Field(name="daily_unique_merchants", dtype=Int64),
        Field(name="daily_unique_devices", dtype=Int64),
        Field(name="first_txn_hour", dtype=Int64),
        Field(name="last_txn_hour", dtype=Int64),
        Field(name="has_anomaly", dtype=Int64),
    ],
    source=user_daily_source,
    online=True,
)

user_rolling_fv = FeatureView(
    name="user_rolling_features",
    entities=[user],
    ttl=timedelta(days=365),
    schema=[
        Field(name="rolling_7d_txn_count", dtype=Int64),
        Field(name="rolling_7d_avg_amount", dtype=Float32),
        Field(name="rolling_30d_txn_count", dtype=Int64),
        Field(name="rolling_30d_avg_amount", dtype=Float32),
    ],
    source=user_rolling_source,
    online=True,
)

user_realtime_5m_fv = FeatureView(
    name="user_realtime_5m_features",
    entities=[user],
    ttl=timedelta(hours=6),
    schema=[
        Field(name="window_label", dtype=String),
        Field(name="txn_count", dtype=Int64),
        Field(name="txn_sum", dtype=Float32),
        Field(name="txn_avg", dtype=Float32),
        Field(name="txn_max", dtype=Float32),
        Field(name="txn_min", dtype=Float32),
        Field(name="unique_categories", dtype=Int64),
        Field(name="unique_merchants", dtype=Int64),
        Field(name="unique_devices", dtype=Int64),
        Field(name="avg_inter_txn_ms", dtype=Float32),
        Field(name="min_inter_txn_ms", dtype=Float32),
        Field(name="anomaly_count", dtype=Int64),
        Field(name="max_amount_ratio", dtype=Float32),
        Field(name="high_amount_count", dtype=Int64),
    ],
    source=user_realtime_5m_stream_source,
    online=True,
)

@on_demand_feature_view(
    sources=[user_daily_fv, user_rolling_fv, user_realtime_5m_fv, transaction_request],
    schema=[
        Field(name="amount_over_daily_avg", dtype=Float32),
        Field(name="amount_over_30d_avg", dtype=Float32),
        Field(name="after_hours_txn", dtype=Int64),
        Field(name="rapid_repeat_txn", dtype=Int64),
    ],
)
def user_transaction_risk_context(features_df: pd.DataFrame) -> pd.DataFrame:
    daily_avg = features_df["daily_txn_avg"].clip(lower=1.0)
    rolling_avg = features_df["rolling_30d_avg_amount"].clip(lower=1.0)

    after_hours = (
        (features_df["request_hour"] < features_df["first_txn_hour"])
        | (features_df["request_hour"] > features_df["last_txn_hour"])
    ).astype("int64")
    rapid_repeat = (features_df["request_ms_since_last_txn"] >= 0).astype("int64")
    rapid_repeat &= (features_df["request_ms_since_last_txn"] < 60_000).astype("int64")

    transformed_df = pd.DataFrame()
    transformed_df["amount_over_daily_avg"] = (features_df["request_amount"] / daily_avg).astype("float32")
    transformed_df["amount_over_30d_avg"] = (features_df["request_amount"] / rolling_avg).astype("float32")
    transformed_df["after_hours_txn"] = after_hours
    transformed_df["rapid_repeat_txn"] = rapid_repeat.astype("int64")
    return transformed_df
