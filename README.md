# Real-Time ML Feature Platform

A production-grade, end-to-end machine learning platform that ingests streaming e-commerce transaction data, computes real-time and batch features, trains an anomaly-detection model, serves predictions via API, and exposes a natural-language query agent — all running locally with Docker.

## Architecture

```
  Synthetic Transaction Generator
              │
              ▼
         ┌─────────┐   raw-transactions
         │  Kafka   │ ─────────────────────────────────────────┐
         └─────────┘                                           │
              │                                                │
              │ enriched-transactions                          │
              ▼                                                ▼
         ┌─────────┐  features-5m     ┌──────────┐  Delta Lake (MinIO)
         │  Flink   │ ────────────────►│          │◄───────────────────
         └─────────┘                  │  Feast   │
                                      │ Feature  │◄── dbt (batch features)
                                      │  Store   │         │
                                      └────┬─────┘         │
                                           │           DuckDB / Delta Lake
                              ┌────────────┼────────────┐
                              ▼                         ▼
                        ┌──────────┐           ┌──────────────┐
                        │ PyTorch  │           │   FastAPI    │
                        │ Training │           │   Serving    │
                        └────┬─────┘           └──────┬───────┘
                             │                        │
                             ▼                        │
                        ┌──────────┐                  │
                        │  MLflow  │◄─────────────────┘
                        │ Registry │
                        └──────────┘

  ┌──────────────────┐     ┌───────────────────────┐
  │  RAG Agent       │     │  Prometheus + Grafana  │
  │ (LangChain +     │     │  (Observability)       │
  │  pgvector)       │     └───────────────────────┘
  └──────────────────┘
```

## Tech Stack

| Layer            | Technology                                    |
|------------------|-----------------------------------------------|
| Streaming ingest | Apache Kafka (KRaft mode)                     |
| Stream features  | Apache Flink (Java, 5-minute tumbling windows)|
| Batch features   | dbt + DuckDB (`delta_scan`)                   |
| Storage          | Delta Lake on MinIO (S3-compatible)           |
| Feature store    | Feast (offline file store + SQLite online)    |
| Training         | PyTorch (transformer-style anomaly detector)  |
| Experiment track | MLflow (PostgreSQL backend, MinIO artifacts)  |
| Serving          | FastAPI + Prometheus metrics                  |
| LLM agent        | LangChain + pgvector (RAG over feature meta)  |
| Infrastructure   | Docker Compose                                |
| Observability    | Prometheus, Grafana                           |

## Prerequisites

- Docker Desktop (with enough RAM — recommend ≥ 8 GB allocated)
- Python 3.12 (Anaconda or venv)
- Java 17+ and Maven (for Flink job compilation only)

Install Python dependencies:

```bash
pip install -r requirements.txt
```

> **Note:** `numpy<2` is pinned in `requirements.txt` because `deltalake`, `pandas`, and `feast` require NumPy 1.x ABI. `boto3` is required for MLflow artifact upload to MinIO.

## Infrastructure

| Service    | Port  | URL                        | Credentials           |
|------------|-------|----------------------------|-----------------------|
| Kafka      | 9092  | localhost:9092             | —                     |
| MinIO      | 9000  | http://localhost:9000      | minioadmin / minioadmin|
| MinIO UI   | 9001  | http://localhost:9001      | minioadmin / minioadmin|
| MLflow     | 5001  | http://localhost:5001      | —                     |
| Flink UI   | 8081  | http://localhost:8081      | —                     |
| Grafana    | 3000  | http://localhost:3000      | admin / admin         |
| Prometheus | 9090  | http://localhost:9090      | —                     |
| Serving    | 8000  | http://localhost:8000      | —                     |

## Full Pipeline Walkthrough

### Phase 0 — Reset (optional)

Start completely fresh:

```bash
make reset    # tears down Docker volumes + wipes local data files
```

### Phase 1 — Streaming Infrastructure

```bash
# 1. Start all Docker services
make infra-up

# 2. Create Kafka topics + MinIO bucket (run once)
make init

# 3. Build and submit the Flink feature job
make flink-submit

# 4. Start the transaction producer (leave running in a terminal)
make produce

# 5. Verify data is flowing through all three topics
make peek TOPIC=raw-transactions
make peek TOPIC=enriched-transactions
make peek TOPIC=features-5m
```

The Flink job computes 5-minute tumbling-window features per user:
`txn_count`, `txn_sum/avg/max/min`, `unique_categories/merchants/devices`,
`avg_inter_txn_ms`, `anomaly_count`, `max_amount_ratio`, `high_amount_count`.

### Phase 2 — Batch Features + Training

Run these in order (producer must be running to generate data):

```bash
# Consume raw-transactions topic → Delta Lake in MinIO
make kafka-to-minio        # Ctrl+C when done (exits cleanly, prints total)

# Transform raw Delta Lake data into feature tables via dbt + DuckDB
make dbt-run
make dbt-test              # 14 data quality tests

# Export feature tables to local Parquet for Feast + mirror to MinIO
make dbt-export            # exports dbt tables + features-5m from Kafka

# Register feature views and entity definitions with Feast
make feast-apply

# Generate (user_id, event_timestamp, has_anomaly) rows for training
make generate-entity-rows

# Train the PyTorch anomaly detector; logs to MLflow
make train
```

After `make train` you should see metrics like:
```
test_roc_auc: ~0.987
val_roc_auc:  ~0.999
test_accuracy: ~0.958
```

### Phase 3 — Promotion + Serving

```bash
# Promote latest MLflow model version to Production alias
make promote-model

# Materialize Feast features to the SQLite online store
make feast-materialize

# Start the FastAPI serving endpoint
make serve

# Smoke test: send a sample request and verify a prediction
make serve-smoke
```

The serving endpoint at `http://localhost:8000/predict` accepts a transaction
payload and returns an anomaly probability using online Feast features + the
Production MLflow model.

### Phase 4 — RAG Agent + Observability

```bash
# Index feature metadata into pgvector
make rag-index

# Query the metadata agent in natural language
make rag-query QUESTION="Which features capture velocity risk?"
```

Grafana dashboards are available at `http://localhost:3000` (admin/admin).
The serving app exposes Prometheus metrics at `http://localhost:8000/metrics`.

## Project Structure

```
ml-feature-platform/
├── docker-compose.yml            # All infrastructure services
├── Makefile                      # Pipeline entrypoints (make help)
├── requirements.txt              # Python dependencies (numpy<2 pinned)
│
├── scripts/
│   ├── init-infra.sh             # Create Kafka topics + MinIO bucket
│   ├── kafka_to_minio.py         # Stream raw-transactions → Delta Lake
│   ├── export_dbt_to_minio.py    # dbt tables → Parquet (Feast + MinIO)
│   ├── export_features_5m_to_parquet.py  # Kafka features-5m → Parquet
│   ├── generate_entity_rows.py   # Build training entity rows from dbt
│   ├── promote_model.py          # Set MLflow Production alias
│   ├── smoke_test_serving.py     # End-to-end serving smoke test
│   ├── build_rag_pgvector_index.py  # Index feature metadata → pgvector
│   ├── run_rag_query.py          # CLI for the RAG agent
│   └── peek_topic.py             # Inspect Kafka topic messages
│
├── src/
│   ├── data_generator/           # Synthetic transaction producer
│   ├── dbt_features/             # dbt models (staging + feature tables)
│   │   └── models/
│   │       ├── staging/          # stg_transactions (Delta Lake source)
│   │       └── features/         # user_daily_features, user_rolling_features
│   ├── feature_store/            # Feast feature repo + feature_store.yaml
│   ├── flink_jobs/               # Java/Flink streaming feature computation
│   ├── training/
│   │   ├── configs/default.yaml  # Model + training hyperparameters
│   │   ├── models/               # TransactionAnomalyDetector (PyTorch)
│   │   └── scripts/train.py      # Training entrypoint
│   ├── serving/app.py            # FastAPI prediction endpoint
│   └── rag_agent/                # LangChain RAG over feature metadata
│
├── infra/
│   ├── docker/Dockerfile.serving # Serving container
│   ├── k8s/                      # Kubernetes manifests (kustomize)
│   └── monitoring/               # Prometheus + Grafana config
│
├── tests/
│   ├── unit/                     # Unit tests (data generator, serving, RAG)
│   └── integration/              # Phase 1 pipeline integration tests
│
└── data/                         # Local data files (git-ignored)
    ├── dbt_dev.duckdb            # DuckDB database (written by dbt)
    ├── entity_rows.parquet       # Training entity rows
    └── feast/                    # Local Parquet files for Feast offline store
```

## Feature Views

| Feature View                  | Source       | TTL    | Key Features                                              |
|-------------------------------|--------------|--------|-----------------------------------------------------------|
| `user_daily_features`         | dbt / Delta  | 90d    | daily txn count/sum/avg/max, unique categories/merchants  |
| `user_rolling_features`       | dbt / Delta  | 365d   | 7d + 30d rolling txn count and avg amount                 |
| `user_realtime_5m_features`   | Flink/Kafka  | 6h     | 5-min window: velocity, inter-txn timing, anomaly count   |
| `user_transaction_risk_context` | On-demand  | —      | amount ratios, after-hours flag, rapid-repeat flag        |

## Model

**TransactionAnomalyDetector** — a PyTorch attention-based binary classifier trained on 13 features from the Feast offline store. Tracks training in MLflow, stores artifacts in MinIO.

Config: `src/training/configs/default.yaml`
- Input dim: 13 features
- Hidden dim: 64, attention heads: 4, dropout: 0.1
- 50 epochs, early stopping patience 5, AdamW lr=0.001

## Common Commands

```bash
make help             # List all available targets
make reset            # Wipe all data and Docker volumes
make phase1           # infra-up + init in one step
make peek TOPIC=X     # Inspect a Kafka topic (raw-transactions | enriched-transactions | features-5m)
make dbt-export       # Export all Parquet files for Feast (dbt + Kafka)
make train            # Train and log to MLflow
make promote-model    # Mark latest model version as Production
make serve            # Start FastAPI serving endpoint
make rag-query QUESTION="..."  # Ask the metadata agent
```

## Troubleshooting

**MLflow UI not loading (`localhost:5001`)**
MLflow depends on postgres being ready. Check container status:
```bash
docker compose ps
docker compose logs mlflow
docker compose restart mlflow   # usually fixes it after postgres is healthy
```

**`feast apply` fails with FileNotFoundError on features_5m.parquet**
Run `make dbt-export` which now also dumps the Kafka topic to Parquet.

**`make train` fails with `n_samples=0`**
Re-run `make dbt-export && make generate-entity-rows` — the Parquet files need to be regenerated with UTC-correct timestamps before training.

**NumPy 2.x crash on import**
```bash
pip install "numpy<2"
```

## License

MIT
