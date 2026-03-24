.PHONY: help infra-up infra-down init produce test lint fmt serve serve-smoke rag-index rag-query

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

# ── ML ──

serve: ## Start the FastAPI serving endpoint
	uvicorn src.serving.app:app --host 0.0.0.0 --port 8000 --reload

serve-smoke: ## Smoke test a running serving endpoint
	python scripts/smoke_test_serving.py

rag-index: ## Build and persist the Phase 4 metadata index into pgvector
	python scripts/build_rag_pgvector_index.py

rag-query: ## Ask the Phase 4 metadata agent from the CLI (usage: make rag-query QUESTION="...")
	python scripts/run_rag_query.py "$(QUESTION)"

# ── Quality ──

test: ## Run all tests
	pytest tests/ -v

test-unit: ## Run unit tests only
	pytest tests/unit/ -v

lint: ## Lint with ruff
	ruff check src/ tests/

fmt: ## Format with ruff
	ruff format src/ tests/

# ── Convenience ──

phase1: infra-up init ## Full Phase 1 setup (start infra + init topics/bucket)
	@echo ""
	@echo "Infrastructure ready! Next steps:"
	@echo "  1. make flink-submit (start the feature pipeline)"
	@echo "  2. make produce      (start generating transactions)"
	@echo "  3. make peek TOPIC=features-5m (verify output)"
