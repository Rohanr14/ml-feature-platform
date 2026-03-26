#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────
# init-infra.sh — One-time setup for Kafka topics + MinIO bucket
#
# Run after `docker compose up -d`:
#   ./scripts/init-infra.sh
# ──────────────────────────────────────────────────────────

set -euo pipefail

KAFKA_CONTAINER="ml-feature-platform-kafka-1"
MINIO_ALIAS="local"
MINIO_ENDPOINT="http://localhost:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
BUCKET="ml-feature-platform"

echo "════════════════════════════════════════"
echo "  Initializing infrastructure"
echo "════════════════════════════════════════"

# ── Wait for Kafka ──
echo ""
echo "⏳ Waiting for Kafka to be ready..."
for i in $(seq 1 30); do
    if docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list &>/dev/null; then
        echo "✅ Kafka is ready"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "❌ Kafka did not start in time"
        exit 1
    fi
    sleep 2
done

# ── Create Kafka topics ──
TOPICS=(
    "raw-transactions:6:1"       # 6 partitions for parallelism
    "features-5m:3:1"
    "features-15m:3:1"
    "features-1h:3:1"
    "enriched-transactions:6:1"
)

echo ""
echo "📋 Creating Kafka topics..."
for topic_spec in "${TOPICS[@]}"; do
    IFS=":" read -r topic partitions replication <<< "$topic_spec"

    if docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --describe --topic "$topic" &>/dev/null; then
        echo "   ⏭  Topic '$topic' already exists"
    else
        docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server localhost:9092 \
            --create \
            --topic "$topic" \
            --partitions "$partitions" \
            --replication-factor "$replication"
        echo "   ✅ Created topic '$topic' (partitions=$partitions)"
    fi
done

# ── Create MinIO bucket ──
echo ""
echo "🪣 Setting up MinIO bucket..."

# Install mc (MinIO client) if not present
if ! command -v mc &>/dev/null; then
    echo "   📥 Installing MinIO client (mc)..."
    MC_DIR="$HOME/.local/bin"
    mkdir -p "$MC_DIR"
    if [[ "$(uname)" == "Darwin" ]]; then
        curl -sSL https://dl.min.io/client/mc/release/darwin-amd64/mc -o "$MC_DIR/mc"
    else
        curl -sSL https://dl.min.io/client/mc/release/linux-amd64/mc -o "$MC_DIR/mc"
    fi
    chmod +x "$MC_DIR/mc"
    export PATH="$MC_DIR:$PATH"
fi

mc alias set "$MINIO_ALIAS" "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" --api S3v4 2>/dev/null

if mc ls "$MINIO_ALIAS/$BUCKET" &>/dev/null; then
    echo "   ⏭  Bucket '$BUCKET' already exists"
else
    mc mb "$MINIO_ALIAS/$BUCKET"
    echo "   ✅ Created bucket '$BUCKET'"
fi

# Create sub-paths (MinIO doesn't need real directories, but helps with ls)
for prefix in delta streaming mlflow; do
    mc cp /dev/null "$MINIO_ALIAS/$BUCKET/$prefix/.keep" 2>/dev/null || true
done
echo "   ✅ Created sub-paths: delta/, streaming/, mlflow/"

echo ""
echo "════════════════════════════════════════"
echo "  ✅ Infrastructure initialized!"
echo ""
echo "  Kafka UI:     http://localhost:9092"
echo "  MinIO UI:     http://localhost:9001"
echo "  Flink UI:     http://localhost:8081"
echo "  MLflow UI:    http://localhost:5001"
echo "  Prometheus:   http://localhost:9090"
echo "  Grafana:      http://localhost:3000"
echo "════════════════════════════════════════"
