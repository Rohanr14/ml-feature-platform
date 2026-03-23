"""
Synthetic e-commerce transaction generator → Kafka.

Produces realistic transaction events with:
- User sessions, product categories, prices
- Temporal patterns (time-of-day, day-of-week seasonality)
- Injected anomalies (~2% fraud-like transactions)
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

from src.data_generator.schemas import Transaction
from src.data_generator.config import GeneratorConfig


def create_transaction(config: GeneratorConfig) -> Transaction:
    """Generate a single synthetic transaction."""
    is_anomaly = random.random() < config.anomaly_rate

    if is_anomaly:
        amount = round(random.uniform(500, 5000), 2)
        category = random.choice(["electronics", "jewelry", "gift_cards"])
    else:
        amount = round(random.uniform(5, 200), 2)
        category = random.choice(config.categories)

    return Transaction(
        transaction_id=str(uuid.uuid4()),
        user_id=f"user_{random.randint(1, config.num_users)}",
        amount=amount,
        currency="USD",
        category=category,
        merchant_id=f"merchant_{random.randint(1, config.num_merchants)}",
        timestamp=datetime.now(timezone.utc).isoformat(),
        is_anomaly=is_anomaly,
        session_id=str(uuid.uuid4()),
        device_type=random.choice(["mobile", "desktop", "tablet"]),
        location_country=random.choice(["US", "CA", "GB", "DE", "FR"]),
    )


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def run_producer(config: GeneratorConfig):
    """Main producer loop."""
    producer = Producer({"bootstrap.servers": config.kafka_bootstrap_servers})

    print(f"Producing transactions to topic '{config.kafka_topic}' ...")
    try:
        while True:
            txn = create_transaction(config)
            producer.produce(
                topic=config.kafka_topic,
                key=txn.user_id,
                value=txn.model_dump_json(),
                callback=delivery_report,
            )
            producer.poll(0)
            time.sleep(1 / config.events_per_second)
    except KeyboardInterrupt:
        print("Shutting down producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    run_producer(GeneratorConfig())
