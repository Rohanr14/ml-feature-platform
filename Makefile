.PHONY: help infra-up infra-down mlflow-restart init produce kafka-to-minio test lint fmt dbt-run dbt-test dbt-export feast-export-5m feast-apply feast-materialize generate-entity-rows train promote-model serve serve-smoke rag-index rag-query reset install

FLINK_JAR_LOCAL := src/flink_jobs/target/flink-feature-jobs-0.1.0.jar
FLINK_JAR_CONTAINER := /tmp/flink-feature-jobs-0.1.0.jar

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Infrastructure ──

infra-up: ## Start all infrastructure services
	docker compose up -d

infra-down: ## Stop all infrastructure services
	docker compose down

mlflow-restart: ## Recreate MLflow container (required after upgrading image version)
	docker compose up -d --force-recreate mlflow
	@echo "Waiting 30s for MLflow to install deps and start..."
	@sleep 30
	@echo "MLflow ready at http://localhost:5001"

init: ## Create Kafka topics + MinIO bucket (run once after infra-up)
	bash scripts/init-infra.sh

# ── Data Pipeline ──

produce: ## Run the transaction producer (streams synthetic txns to Kafka)
	python -m src.data_generator.txn_producer

flink-build: ## Build Flink job fat JAR
	cd src/flink_jobs && mvn -DskipTests clean package -q
	@echo "JAR: src/flink_jobs/target/flink-feature-jobs-0.1.0.jar"

flink-submit: flink-build ## Build and submit Flink job to local cluster
	docker compose cp $(FLINK_JAR_LOCAL) flink-jobmanager:$(FLINK_JAR_CONTAINER)
	docker compose exec flink-jobmanager flink run \
		$(FLINK_JAR_CONTAINER) \
		--kafka.bootstrap-servers kafka:29092

kafka-to-minio: ## Stream raw transactions from Kafka to MinIO as Parquet
	python scripts/kafka_to_minio.py --from-beginning

peek: ## Peek at a Kafka topic (usage: make peek TOPIC=features-5m)
	python scripts/peek_topic.py $(or $(TOPIC),raw-transactions) --count 5

# ── Batch Features ──

dbt-run: ## Run dbt models (daily + rolling features from Delta Lake)
	cd src/dbt_features && dbt run --profiles-dir .

dbt-test: ## Run dbt tests
	cd src/dbt_features && dbt test --profiles-dir .

dbt-export: ## Export dbt feature tables + Kafka features-5m to local Parquet for Feast
	python scripts/export_dbt_to_minio.py
	python scripts/export_features_5m_to_parquet.py

# ── Feature Store ──

feast-export-5m: ## Dump features-5m Kafka topic → data/feast/features_5m.parquet
	python scripts/export_features_5m_to_parquet.py

feast-apply: ## Register Feast feature views
	cd src/feature_store && feast apply

feast-materialize: ## Materialize features to online store for serving
	cd src/feature_store && feast materialize-incremental $$(date -u +"%Y-%m-%dT%H:%M:%S")

# ── ML Training ──

generate-entity-rows: ## Generate entity rows for training from dbt output
	python scripts/generate_entity_rows.py

train: ## Train the anomaly detection model and log to MLflow
	python -m src.training.scripts.train \
		--entity-rows-path data/entity_rows.parquet \
		--feature-repo src/feature_store \
		--tracking-uri http://localhost:5001 \
		--experiment-name transaction-anomaly-detector \
		--run-name demo-run

promote-model: ## Promote latest model version to Production in MLflow
	python scripts/promote_model.py

# ── ML Serving ──

serve: ## Start the FastAPI serving endpoint
	uvicorn src.serving.app:app --host 0.0.0.0 --port 8000 --reload

serve-smoke: ## Smoke test a running serving endpoint
	python scripts/smoke_test_serving.py

rag-index: ## Build and persist the Phase 4 metadata index into pgvector
	python scripts/build_rag_pgvector_index.py

rag-query: ## Ask the Phase 4 metadata agent from the CLI (usage: make rag-query QUESTION="...")
	python scripts/run_rag_query.py "$(QUESTION)"

# ── Quality ──

install: ## Install Python dependencies (pins numpy<2 for binary compatibility)
	pip install -r requirements.txt

test: ## Run all tests
	pytest tests/ -v

test-unit: ## Run unit tests only
	pytest tests/unit/ -v

lint: ## Lint with ruff
	ruff check src/ tests/

fmt: ## Format with ruff
	ruff format src/ tests/

# ── Convenience ──

reset: ## Tear down infra (deletes all volumes) and wipe local data artifacts
	docker compose down -v
	rm -rf data/feast/ data/entity_rows.parquet data/dbt_dev.duckdb
	rm -rf src/feature_store/data/registry.db src/feature_store/data/online_store.db
	rm -rf src/dbt_features/target/ src/dbt_features/logs/
	@echo "All data cleared. Run 'make phase1' to start fresh."

phase1: infra-up init ## Full Phase 1 setup (start infra + init topics/bucket)
	@echo ""
	@echo "Infrastructure ready! Next steps:"
	@echo "  1. make flink-submit (start the feature pipeline)"
	@echo "  2. make produce      (start generating transactions)"
	@echo "  3. make peek TOPIC=features-5m (verify output)"
