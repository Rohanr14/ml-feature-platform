"""Shared test fixtures."""

import pytest

from src.data_generator.config import GeneratorConfig


@pytest.fixture
def generator_config():
    return GeneratorConfig(
        kafka_bootstrap_servers="localhost:9092",
        events_per_second=1.0,
        num_users=10,
        num_merchants=5,
    )
