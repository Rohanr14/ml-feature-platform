"""
Kafka → MinIO Parquet writer for raw transactions.

Consumes from the 'raw-transactions' topic, batches records, and writes
Parquet files to MinIO at s3://ml-feature-platform/delta/raw-transactions/.
This bridges the streaming layer to the batch layer so dbt can read the data.

Usage:
    python scripts/kafka_to_minio.py
    python scripts/kafka_to_minio.py --batch-size 500 --flush-interval 30
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from io import BytesIO

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError

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

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET = "ml-feature-platform"
PREFIX = "delta/raw-transactions"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write raw transactions from Kafka to MinIO as Parquet")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="raw-transactions")
    parser.add_argument("--batch-size", type=int, default=200, help="Records per Parquet file")
    parser.add_argument("--flush-interval", type=int, default=15, help="Max seconds between flushes")
    parser.add_argument("--from-beginning", action="store_true", help="Read from beginning of topic")
    return parser.parse_args()


def create_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def flush_batch(s3_client, records: list[dict], file_counter: int) -> int:
    if not records:
        return file_counter

    arrays = {field.name: [] for field in SCHEMA}
    for record in records:
        for field in SCHEMA:
            arrays[field.name].append(record.get(field.name))

    table = pa.table(arrays, schema=SCHEMA)

    buf = BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    key = f"{PREFIX}/part-{file_counter:06d}-{int(time.time())}.parquet"
    s3_client.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    print(f"  Wrote {len(records)} records → s3://{BUCKET}/{key}")

    return file_counter + 1


def main():
    args = parse_args()
    s3_client = create_s3_client()

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

    print(f"Consuming '{args.topic}' → MinIO s3://{BUCKET}/{PREFIX}/")
    print(f"  batch_size={args.batch_size}, flush_interval={args.flush_interval}s")
    print(f"  offset_reset={'earliest' if args.from_beginning else 'latest'}")
    print()

    batch: list[dict] = []
    file_counter = 0
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
                file_counter = flush_batch(s3_client, batch, file_counter)
                total_written += len(batch)
                batch = []
                last_flush = time.time()

    finally:
        if batch:
            file_counter = flush_batch(s3_client, batch, file_counter)
            total_written += len(batch)
        consumer.close()
        print(f"\nDone. Wrote {total_written} total records in {file_counter} files.")


if __name__ == "__main__":
    main()
