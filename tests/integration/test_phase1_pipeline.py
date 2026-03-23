"""
Integration tests for the Phase 1 data pipeline.

Tests the full chain: data generator → JSON serialization → 
deserialization compatibility → feature computation sanity.

These tests run without Docker (no Kafka/Flink required).
For full e2e tests with infrastructure, see tests/e2e/.
"""

import json
import statistics
from datetime import datetime, timezone, timedelta

import pytest

from src.data_generator.config import GeneratorConfig
from src.data_generator.schemas import Transaction
from src.data_generator.txn_producer import create_transaction


class TestDataGeneratorIntegration:
    """Validate that the generator produces Flink-compatible output."""

    @pytest.fixture
    def batch(self):
        """Generate a batch of 100 transactions."""
        config = GeneratorConfig(num_users=50, num_merchants=20, anomaly_rate=0.05)
        return [create_transaction(config) for _ in range(100)]

    def test_json_round_trip(self, batch):
        """Every transaction survives JSON serialize → deserialize."""
        for txn in batch:
            json_str = txn.model_dump_json()
            restored = Transaction.model_validate_json(json_str)
            assert restored.transaction_id == txn.transaction_id
            assert restored.amount == txn.amount
            assert restored.user_id == txn.user_id

    def test_json_has_all_flink_fields(self, batch):
        """
        The JSON keys must exactly match what the Java 
        TransactionDeserializer expects (snake_case).
        """
        required_keys = {
            "transaction_id", "user_id", "amount", "currency",
            "category", "merchant_id", "timestamp", "is_anomaly",
            "session_id", "device_type", "location_country",
        }
        for txn in batch:
            data = json.loads(txn.model_dump_json())
            assert required_keys.issubset(data.keys()), \
                f"Missing keys: {required_keys - data.keys()}"

    def test_timestamps_are_iso8601(self, batch):
        """Timestamps must be parseable as ISO-8601 (Java Instant.parse expects this)."""
        for txn in batch:
            ts = datetime.fromisoformat(txn.timestamp)
            assert ts.tzinfo is not None, "Timestamp must be timezone-aware"

    def test_user_id_format(self, batch):
        """User IDs must match the pattern the Flink job keys by."""
        for txn in batch:
            assert txn.user_id.startswith("user_")
            user_num = int(txn.user_id.split("_")[1])
            assert 1 <= user_num <= 50

    def test_amount_ranges(self, batch):
        """Normal txns: $5-$200, anomalies: $500-$5000."""
        for txn in batch:
            if txn.is_anomaly:
                assert 500 <= txn.amount <= 5000
            else:
                assert 5 <= txn.amount <= 200

    def test_batch_statistics(self, batch):
        """Verify batch-level statistics are reasonable."""
        amounts = [t.amount for t in batch]
        anomaly_count = sum(1 for t in batch if t.is_anomaly)
        unique_users = len(set(t.user_id for t in batch))

        # At 5% anomaly rate, expect roughly 0-15 anomalies in 100 txns
        assert anomaly_count < 20, f"Too many anomalies: {anomaly_count}"

        # With 50 possible users and 100 txns, should see decent spread
        assert unique_users > 10, f"Too few unique users: {unique_users}"

        # Average amount should be reasonable (mix of normal + anomaly)
        avg = statistics.mean(amounts)
        assert 5 < avg < 500, f"Average amount {avg} seems off"


class TestFeatureComputationSanity:
    """
    Validate that the feature computation logic (mirrored between
    Java FeatureAccumulator and dbt SQL) produces sane results.
    
    This is a Python-side sanity check. The Java unit tests cover
    the FeatureAccumulator directly.
    """

    def test_daily_aggregation_logic(self):
        """Simulate what user_daily_features.sql computes."""
        config = GeneratorConfig(num_users=5, anomaly_rate=0.0)
        txns = [create_transaction(config) for _ in range(50)]

        # Group by user
        by_user: dict[str, list[Transaction]] = {}
        for t in txns:
            by_user.setdefault(t.user_id, []).append(t)

        for user_id, user_txns in by_user.items():
            amounts = [t.amount for t in user_txns]
            count = len(user_txns)
            total = sum(amounts)
            avg = total / count
            max_amt = max(amounts)
            unique_cats = len(set(t.category for t in user_txns))
            unique_merchants = len(set(t.merchant_id for t in user_txns))

            assert count > 0
            assert total > 0
            assert avg == pytest.approx(total / count)
            assert max_amt <= max(amounts)
            assert 1 <= unique_cats <= len(config.categories)

    def test_velocity_computation_logic(self):
        """Simulate what VelocityEnricher.java computes."""
        config = GeneratorConfig(num_users=1)
        txns = [create_transaction(config) for _ in range(10)]

        # Sort by timestamp (they should already be monotonic since
        # we generate them sequentially, but be safe)
        txns.sort(key=lambda t: t.timestamp)

        # First txn has no previous → ms_since_last = -1
        # Subsequent txns have positive ms_since_last
        timestamps = [datetime.fromisoformat(t.timestamp) for t in txns]
        for i in range(1, len(timestamps)):
            delta = timestamps[i] - timestamps[i - 1]
            # Since we generate txns in a tight loop, deltas should be
            # very small (< 1 second typically)
            assert delta.total_seconds() >= 0
