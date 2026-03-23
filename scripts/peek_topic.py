"""
Kafka topic inspector — peek at messages flowing through the pipeline.

Usage:
    python -m scripts.peek_topic raw-transactions
    python -m scripts.peek_topic features-5m --count 5
    python -m scripts.peek_topic enriched-transactions --from-beginning
"""

import argparse
import json
import sys

from confluent_kafka import Consumer, KafkaError


def peek(topic: str, bootstrap_servers: str, count: int, from_beginning: bool):
    config = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"peek-{topic}-debug",
        "auto.offset.reset": "earliest" if from_beginning else "latest",
        "enable.auto.commit": False,
    }

    consumer = Consumer(config)
    consumer.subscribe([topic])

    print(f"\n📡 Listening on '{topic}' ({'from beginning' if from_beginning else 'latest'})...\n")

    seen = 0
    try:
        while seen < count:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                print("  ⏳ waiting for messages...")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"  ❌ Error: {msg.error()}")
                break

            try:
                value = json.loads(msg.value().decode("utf-8"))
                print(f"  [{msg.partition()}:{msg.offset()}] {json.dumps(value, indent=2)}")
            except (json.JSONDecodeError, UnicodeDecodeError):
                print(f"  [{msg.partition()}:{msg.offset()}] (raw) {msg.value()[:200]}")

            seen += 1
            print()

    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        consumer.close()

    print(f"Saw {seen} messages.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Peek at Kafka topic messages")
    parser.add_argument("topic", help="Topic name to consume from")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--count", type=int, default=10, help="Number of messages to read")
    parser.add_argument("--from-beginning", action="store_true")
    args = parser.parse_args()

    peek(args.topic, args.bootstrap_servers, args.count, args.from_beginning)
