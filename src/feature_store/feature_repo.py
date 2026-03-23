"""
Feast feature store definitions.

Registers both batch (dbt) and streaming (Flink) feature sources,
enabling point-in-time correct feature retrieval for training
and low-latency online serving.
"""

from datetime import timedelta

from feast import Entity, Feature, FeatureView, Field, FileSource
from feast.types import Float32, Int64, String

# ── Entities ──
user = Entity(
    name="user_id",
    description="Unique user identifier",
)

# ── Batch Source (from dbt → Delta Lake) ──
user_daily_source = FileSource(
    name="user_daily_features_source",
    path="s3://ml-feature-platform/delta/user_daily_features/",
    timestamp_field="event_date",
    # In production: use DeltaSource or RedshiftSource
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
        Field(name="has_anomaly", dtype=Int64),
    ],
    source=user_daily_source,
    online=True,
)

# TODO Phase 2:
# - Add StreamSource from Flink real-time features
# - Add user_rolling_features FeatureView
# - Add on-demand feature transforms
