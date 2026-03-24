"""
Kafka → MinIO Delta Lake writer for raw transactions.

Consumes from the 'raw-transactions' topic, batches records, and writes
Delta Lake format to MinIO at s3://ml-feature-platform/delta/raw-transactions/.
This bridges the streaming layer to the batch layer so dbt can read the data
via DuckDB's delta_scan().

Usage:
    python scripts/kafka_to_minio.py
    python scripts/kafka_to_minio.py --batch-size 500 --flush-interval 30
"""

from __future__ import annotations

import argparse
import json
import signal
import time

import pyarrow as pa
from confluent_kafka import Consumer, KafkaError
from deltalake import DeltaTable, write_deltalake

SCHEMA = pa.schema([
    ("transaction_id", pa.string()),
    ("user_id", pa.string()),
    ("amount", pa.float64()),
    ("currency", pa.string()),
    ("category", pa.string()),
    ("merchant_id", pa.string()),
    ("timestamp", pa.string()),
    ("is_anomaly", pa.bool_()),
    ("session_id", pa.string()),
    ("device_type", pa.string()),
    ("location_country", pa.string()),
])

TABLE_URI = "s3://ml-feature-platform/delta/raw-transactions"
STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": "http://localhost:9000",
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin",
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write raw transactions from Kafka to MinIO as Delta Lake")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="raw-transactions")
    parser.add_argument("--batch-size", type=int, default=200, help="Records per flush")
    parser.add_argument("--flush-interval", type=int, default=15, help="Max seconds between flushes")
    parser.add_argument("--from-beginning", action="store_true", help="Read from beginning of topic")
    return parser.parse_args()


def flush_batch(records: list[dict], flush_count: int) -> int:
    if not records:
        return flush_count

    arrays = {field.name: [] for field in SCHEMA}
    for record in records:
        for field in SCHEMA:
            arrays[field.name].append(record.get(field.name))

    table = pa.table(arrays, schema=SCHEMA)

    mode = "append" if flush_count > 0 or delta_table_exists() else "overwrite"
    write_deltalake(
        TABLE_URI,
        table,
        mode=mode,
        schema_mode="merge",
        storage_options=STORAGE_OPTIONS,
    )
    print(f"  Flushed {len(records)} records → {TABLE_URI} (mode={mode})")
    return flush_count + 1


def delta_table_exists() -> bool:
    try:
        DeltaTable(TABLE_URI, storage_options=STORAGE_OPTIONS)
        return True
    except Exception:
        return False


def main():
    args = parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": "kafka-to-minio-writer",
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([args.topic])

    shutdown = False

    def handle_signal(signum, frame):
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    print(f"Consuming '{args.topic}' → {TABLE_URI}")
    print(f"  batch_size={args.batch_size}, flush_interval={args.flush_interval}s")
    print(f"  offset_reset={'earliest' if args.from_beginning else 'latest'}")
    print()

    batch: list[dict] = []
    flush_count = 0
    last_flush = time.time()
    total_written = 0

    try:
        while not shutdown:
            msg = consumer.poll(timeout=1.0)

            if msg is not None and not msg.error():
                try:
                    record = json.loads(msg.value().decode("utf-8"))
                    batch.append(record)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    pass
            elif msg is not None and msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"  Error: {msg.error()}")

            elapsed = time.time() - last_flush
            if len(batch) >= args.batch_size or (batch and elapsed >= args.flush_interval):
                flush_count = flush_batch(batch, flush_count)
                total_written += len(batch)
                batch = []
                last_flush = time.time()

    finally:
        if batch:
            flush_count = flush_batch(batch, flush_count)
            total_written += len(batch)
        consumer.close()
        print(f"\nDone. Wrote {total_written} total records in {flush_count} flushes.")


if __name__ == "__main__":
    main()
