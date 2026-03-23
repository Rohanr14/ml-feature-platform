"""Tests for the synthetic transaction generator."""

from src.data_generator.config import GeneratorConfig
from src.data_generator.txn_producer import create_transaction


class TestCreateTransaction:
    def test_returns_valid_transaction(self):
        config = GeneratorConfig()
        txn = create_transaction(config)
        assert txn.transaction_id is not None
        assert txn.user_id.startswith("user_")
        assert txn.amount > 0
        assert txn.category in config.categories + ["electronics", "jewelry", "gift_cards"]

    def test_anomaly_rate(self):
        """Over many samples, anomaly rate should approximate config."""
        config = GeneratorConfig(anomaly_rate=0.5)
        txns = [create_transaction(config) for _ in range(1000)]
        anomaly_pct = sum(1 for t in txns if t.is_anomaly) / len(txns)
        assert 0.35 < anomaly_pct < 0.65, f"Anomaly rate {anomaly_pct} too far from 0.5"

    def test_anomaly_amounts_are_high(self):
        config = GeneratorConfig(anomaly_rate=1.0)
        txn = create_transaction(config)
        assert txn.amount >= 500
