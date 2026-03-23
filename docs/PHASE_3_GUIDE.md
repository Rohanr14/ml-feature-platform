# Phase 3: Model Serving → Docker/Kubernetes Deployment

## What This Phase Builds

A deployable inference surface around the trained anomaly model:

```
MLflow Registry ──► FastAPI Serving App ──► /predict
        ▲                    │
        │                    ├──► Feast online features
        │                    ├──► /health
        │                    ├──► /model-info
        │                    └──► /metrics
        │
 Docker Compose / Kubernetes
```

**What you get in Phase 3:**

- A FastAPI app that loads the anomaly detector from MLflow
- Feast-powered online feature retrieval for inference
- Prometheus-friendly metrics at `/metrics`
- Local container execution via Docker Compose
- Kubernetes deployment manifests for the serving API
- A smoke-test workflow for `/health`, `/model-info`, and `/predict`

## Prerequisites

- Phase 2 artifacts available enough to load a model from MLflow
- Docker and Docker Compose for local container runs
- Kubernetes access if you want to use the provided manifests
- Python 3.11+ for local development commands

## Local Development

### 1. Start the backing infrastructure

```bash
make infra-up
```

This brings up MLflow, MinIO, Postgres, Kafka, Flink, Prometheus, and Grafana from `docker-compose.yml`.

### 2. Run the serving API directly

```bash
make serve
```

This starts Uvicorn against `src.serving.app:app` on port `8000`.

### 3. Inspect the API contract

```bash
curl http://localhost:8000/health
curl http://localhost:8000/model-info
```

- `/health` reports whether Feast + MLflow artifacts loaded successfully
- `/model-info` exposes the model version, threshold, request fields, and online feature references

### 4. Run a smoke test against the serving app

```bash
make serve-smoke
```

This calls:

- `GET /health`
- `GET /model-info`
- `POST /predict`

using `scripts/smoke_test_serving.py`.

## Docker Compose Serving

The repo includes a `serving` service definition that points at:

- `infra/docker/Dockerfile.serving`
- `SERVING_FEATURE_REPO_PATH=/app/src/feature_store`
- `SERVING_MLFLOW_MODEL_URI=models:/TransactionAnomalyDetector/Production`

Start it with:

```bash
docker compose up -d serving
```

Then verify it:

```bash
python scripts/smoke_test_serving.py --base-url http://localhost:8000
```

## Kubernetes Deployment

The Kubernetes base manifests live in `infra/k8s/base/` and configure:

- container image `ml-feature-platform/serving:latest`
- port `8000`
- `/health` liveness and readiness probes
- `SERVING_*` environment variables used by the FastAPI app

Render/apply them with your normal Kustomize workflow, for example:

```bash
kubectl apply -k infra/k8s/base
```

## Operational Notes

- The serving app reads MLflow registry info from `MLFLOW_TRACKING_URI` and model selection from `SERVING_MLFLOW_MODEL_URI`
- Feature lookup depends on the Feast repo under `src/feature_store`
- `/metrics` is mounted for Prometheus scraping
- `/model-info` is useful for deployment verification because it shows the contract the model expects

## What “Done” Looks Like For Phase 3

You should be able to:

- start the serving API locally
- load a model from MLflow successfully
- retrieve online features from Feast successfully
- get a valid `/predict` response
- run the smoke-test script successfully
- deploy the same serving app configuration via Docker Compose or Kubernetes

## Next: Phase 4

Phase 4 adds the RAG agent, richer observability polish, and the remaining user-facing platform integration work.
