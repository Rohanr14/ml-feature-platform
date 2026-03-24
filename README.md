# ⚡ Real-Time ML Feature Platform

A production-grade, end-to-end machine learning platform that ingests streaming e-commerce transaction data, computes real-time and batch features, trains and serves ML models, and exposes predictions via API — with full observability and an LLM-powered query agent.

## Architecture

```
                          ┌─────────────┐
                          │  Data Source │
                          │ (Synthetic   │
                          │  Txn Gen)    │
                          └──────┬───────┘
                                 │
                                 ▼
                          ┌─────────────┐
                          │    Kafka     │
                          │  (Streaming  │
                          │   Ingest)    │
                          └──────┬───────┘
                                 │
                    ┌────────────┼────────────┐
                    ▼                         ▼
             ┌─────────────┐          ┌─────────────┐
             │    Flink     │          │     dbt      │
             │ (Real-Time   │          │  (Batch      │
             │  Features)   │          │   Features)  │
             └──────┬───────┘          └──────┬───────┘
                    │                         │
                    └────────────┬────────────┘
                                 ▼
                          ┌─────────────┐
                          │  Delta Lake  │
                          │  (MinIO/S3)  │
                          └──────┬───────┘
                                 │
                                 ▼
                          ┌─────────────┐
                          │    Feast     │
                          │ (Feature     │
                          │   Store)     │
                          └──────┬───────┘
                                 │
                    ┌────────────┼────────────┐
                    ▼                         ▼
             ┌─────────────┐          ┌─────────────┐
             │   PyTorch    │          │   FastAPI    │
             │  (Training)  │          │  (Serving)   │
             └──────┬───────┘          └─────────────┘
                    │                         ▲
                    ▼                         │
             ┌─────────────┐                  │
             │   MLflow     │─────────────────┘
             │ (Registry)   │
             └─────────────┘

             ┌─────────────┐          ┌─────────────┐
             │  RAG Agent   │          │  Grafana +   │
             │ (LangChain)  │          │  Prometheus  │
             └─────────────┘          └─────────────┘
```

## Tech Stack

| Layer              | Technology                          |
|--------------------|-------------------------------------|
| Streaming          | Apache Kafka, Apache Flink          |
| Batch Transforms   | dbt                                 |
| Storage            | Delta Lake on MinIO (S3-compatible) |
| Feature Store      | Feast                               |
| Training           | PyTorch, MLflow                     |
| Serving            | FastAPI                             |
| LLM Agent          | LangChain + pgvector                |
| Orchestration      | Dagster                             |
| Infrastructure     | Docker, Kubernetes, Terraform       |
| Observability      | Prometheus, Grafana                 |

## Project Phases

- **Phase 1:** Streaming ingestion (Kafka + Flink) → Delta Lake. Batch features with dbt.
- **Phase 2:** Feast feature store + PyTorch model training + MLflow experiment tracking.
- **Phase 3:** FastAPI model serving + Docker/K8s deployment.
- **Phase 4:** RAG agent + Grafana monitoring + polish.

## Quick Start

```bash
# Clone
git clone https://github.com/yourusername/ml-feature-platform.git
cd ml-feature-platform

# 1) Start infrastructure
make infra-up

# 2) Initialize Kafka topics + MinIO paths (run once after infra-up)
make init

# 3) Submit Flink feature job (run in its own terminal; keeps running)
make flink-submit

# 4) Start transaction producer (run in another terminal)
make produce

# 5) Verify features are being written
make peek TOPIC=features-5m

# (See docs/PHASE_1_GUIDE.md, docs/PHASE_3_GUIDE.md, and docs/PHASE_4_GUIDE.md for detailed walkthroughs)
```

## Project Structure

```
See tree below or browse the repo.
```

## License

MIT
