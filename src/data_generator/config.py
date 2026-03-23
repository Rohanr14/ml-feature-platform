"""Configuration for the transaction generator."""

from pydantic_settings import BaseSettings


class GeneratorConfig(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "raw-transactions"
    events_per_second: float = 10.0
    num_users: int = 1000
    num_merchants: int = 200
    anomaly_rate: float = 0.02
    categories: list[str] = [
        "electronics",
        "clothing",
        "groceries",
        "home_garden",
        "sports",
        "books",
        "beauty",
        "automotive",
    ]

    class Config:
        env_prefix = "GENERATOR_"
