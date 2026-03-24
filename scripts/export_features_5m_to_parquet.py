"""
Kafka features-5m → local Parquet for Feast.

Reads all messages from the beginning of the 'features-5m' topic,
waits until no new messages arrive for --idle-timeout seconds, then
writes data/feast/features_5m.parquet so that `feast apply` can
infer the schema from the batch source.

Usage:
    python scripts/export_features_5m_to_parquet.py
    python scripts/export_features_5m_to_parquet.py --idle-timeout 10
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError

SCHEMA = pa.schema([
    ("user_id", pa.string()),
    ("window_start", pa.int64()),
    ("window_end", pa.int64()),
    ("window_label", pa.string()),
    ("txn_count", pa.int64()),
    ("txn_sum", pa.float64()),
    ("txn_avg", pa.float64()),
    ("txn_max", pa.float64()),
    ("txn_min", pa.float64()),
    ("unique_categories", pa.int64()),
    ("unique_merchants", pa.int64()),
    ("unique_devices", pa.int64()),
    ("avg_inter_txn_ms", pa.float64()),
    ("min_inter_txn_ms", pa.float64()),
    ("anomaly_count", pa.int64()),
    ("max_amount_ratio", pa.float64()),
    ("high_amount_count", pa.int64()),
    ("computed_at", pa.int64()),
])

OUT_PATH = Path(__file__).resolve().parent.parent / "data" / "feast" / "features_5m.parquet"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--topic", default="features-5m")
    p.add_argument("--idle-timeout", type=int, default=15,
                   help="Stop after this many seconds with no new messages")
    return p.parse_args()


def main():
    args = parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": "features-5m-parquet-exporter",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([args.topic])

    print(f"Reading '{args.topic}' from beginning (idle-timeout={args.idle_timeout}s)...")

    records: list[dict] = []
    last_message_at = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                if time.time() - last_message_at >= args.idle_timeout:
                    break
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"  Error: {msg.error()}")
                continue

            try:
                records.append(json.loads(msg.value().decode("utf-8")))
                last_message_at = time.time()
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass
    finally:
        consumer.close()

    if not records:
        print("No messages found. Is the producer and Flink job running?")
        return

    # Build typed arrays from collected records
    arrays = {}
    for field in SCHEMA:
        col = [r.get(field.name) for r in records]
        arrays[field.name] = pa.array(col, type=field.type)

    table = pa.table(arrays, schema=SCHEMA)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, OUT_PATH)
    print(f"Wrote {len(records)} rows → {OUT_PATH}")


if __name__ == "__main__":
    main()
