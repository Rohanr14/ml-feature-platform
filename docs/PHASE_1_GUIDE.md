# Phase 1: Streaming Ingestion → Real-Time Features → Delta Lake

## What This Phase Builds

A real-time feature engineering pipeline:

```
Python txn_producer → Kafka → Flink (3 window sizes + velocity) → Kafka output topics
                                                                 → Filesystem (for Delta Lake)
```

**Features computed in real-time per user:**

| Feature Group | Features | Window |
|---|---|---|
| Volume | txn count, sum, avg, min, max | 5m, 15m, 1h |
| Diversity | unique categories, merchants, devices | 5m, 15m, 1h |
| Velocity (in-window) | avg inter-txn time, min inter-txn time | 5m, 15m, 1h |
| Velocity (cross-window) | ms since user's last transaction | per event |
| Risk signals | anomaly count, max/avg ratio, high-amount count | 5m, 15m, 1h |

**Batch features computed via dbt:**

| Feature | Granularity |
|---|---|
| Daily aggregations (count, sum, avg, max, diversity) | Per user per day |
| Rolling 7-day and 30-day windows | Per user per day |

## Prerequisites

- **Docker and Docker Compose** (for Kafka, Flink, MinIO, Postgres)
- **Java 17+** and **Maven** (for building the Flink JAR)
- **Python 3.11+** (for the data generator and dbt)
- ~4 GB free RAM for the Docker containers

## Setup

### 1. Install Python dependencies

```bash
pip install -e ".[dev]"
pip install dbt-duckdb
```

### 2. Start infrastructure

```bash
make infra-up
```

This starts: Kafka (KRaft), MinIO, Postgres + pgvector, MLflow, Flink (JobManager + TaskManager), Prometheus, Grafana.

### 3. Initialize Kafka topics and MinIO bucket

```bash
make init
```

Creates 5 Kafka topics and the `ml-feature-platform` S3 bucket in MinIO.

### 4. Build the Flink JAR

```bash
make flink-build
```

Produces `src/flink_jobs/target/flink-feature-jobs-0.1.0.jar` (fat JAR with all dependencies).

### 5. Run Java tests

```bash
cd src/flink_jobs && mvn test
```

Validates: Transaction deserialization, FeatureAccumulator math, merge correctness.

## Running the Pipeline

Open 3 terminal tabs:

**Tab 1 — Start producing transactions:**
```bash
make produce
# Streams ~10 txn/sec to Kafka topic "raw-transactions"
```

**Tab 2 — Submit the Flink job:**
```bash
make flink-submit
# Deploys the feature pipeline to the Flink cluster
```

**Tab 3 — Verify output:**
```bash
# Peek at windowed features
make peek TOPIC=features-5m

# Peek at velocity-enriched transactions  
make peek TOPIC=enriched-transactions

# Check all topics
for topic in features-5m features-15m features-1h enriched-transactions; do
    echo "=== $topic ==="
    make peek TOPIC=$topic
done
```

### Monitor

- **Flink Dashboard:** http://localhost:8081 — see running jobs, throughput, backpressure
- **MinIO Console:** http://localhost:9001 — browse Delta Lake files (minioadmin/minioadmin)

## Running dbt Batch Features

After the streaming pipeline has written some data:

```bash
cd src/dbt_features

# Run all models
dbt run --profiles-dir .

# Run tests
dbt test --profiles-dir .

# Generate docs
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

## What You'll Have After Phase 1

- **Kafka** receiving ~10 txn/sec with 6-partition parallelism
- **Flink** computing 15+ features across 3 sliding windows in real-time
- **Stateful velocity tracking** per user (cross-window, using Flink keyed state)
- **4 output Kafka topics** (features-5m, features-15m, features-1h, enriched-transactions)
- **Filesystem sink** for batch processing / Delta Lake ingestion
- **dbt models** producing daily + rolling user features
- **Exactly-once semantics** via Flink checkpointing
- **Java unit tests** validating aggregation math and serialization
- **Python integration tests** validating the data generator → pipeline compatibility

## Architecture Decisions

**Why sliding windows instead of tumbling?**
Sliding windows (e.g., 5m window, 1m slide) produce a new feature vector every 1 minute using the last 5 minutes of data. This gives the downstream model fresh features without waiting for a full window to close. The tradeoff is higher compute cost (5x more windows), but for 1000 users at 10 txn/sec this is trivial.

**Why a separate VelocityEnricher (stateful) instead of just windowed velocity?**
Windowed velocity only captures inter-arrival times *within* a window. But a user transacting 3 seconds after a 2-hour gap is a critical fraud signal that no single window captures. The `VelocityEnricher` uses Flink `ValueState` to track the absolute last transaction time per user, giving us this cross-window signal.

**Why DuckDB for dbt instead of Spark?**
For local development, DuckDB is zero-config and reads Delta/Parquet natively. It's ~100x faster to start than a Spark session. The SQL is ANSI-compatible, so migrating to Spark/Trino for production is a find-and-replace.

## Next: Phase 2

Phase 2 wires the features into Feast (offline + online stores), builds the PyTorch training pipeline, and adds MLflow experiment tracking.
