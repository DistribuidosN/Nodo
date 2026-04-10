#!/bin/bash
# Single worker production deployment helper
# Usage:
#   ./scripts/deploy-worker.sh [worker-id] [java-backend-host:port]

set -e

WORKER_ID="${1:-worker-prod-$(hostname)}"
JAVA_BACKEND="${2:-}"

echo "=== Deploying single image processing worker ==="
echo "Worker ID: $WORKER_ID"
echo "Java Backend Callback: ${JAVA_BACKEND:-(disabled)}"
echo ""

echo "Building Docker image..."
docker build -t image-worker:latest .

docker network create worker-net 2>/dev/null || true

echo "Cleaning up previous instance..."
docker stop "image-worker-$WORKER_ID" 2>/dev/null || true
docker rm "image-worker-$WORKER_ID" 2>/dev/null || true

cat > ".env.worker-$WORKER_ID" << EOF
WORKER_NODE_ID=$WORKER_ID
WORKER_BIND_HOST=0.0.0.0
WORKER_BIND_PORT=50051
WORKER_COORDINATOR_TARGET=$JAVA_BACKEND
WORKER_METRICS_HOST=0.0.0.0
WORKER_METRICS_PORT=9100
WORKER_HEALTH_HOST=0.0.0.0
WORKER_HEALTH_PORT=8081
WORKER_INPUT_DIR=/app/data/input
WORKER_OUTPUT_DIR=/app/data/output
WORKER_STATE_DIR=/app/data/state
WORKER_LOG_LEVEL=INFO
EOF

echo "Starting worker container..."
docker run -d \
  --name "image-worker-$WORKER_ID" \
  --network worker-net \
  --restart unless-stopped \
  --security-opt no-new-privileges:true \
  --env-file ".env.worker-$WORKER_ID" \
  -p 50051:50051 \
  -p 8081:8081 \
  -p 9100:9100 \
  -v "worker-data-$WORKER_ID:/app/data" \
  image-worker:latest

echo ""
echo "Worker deployed successfully."
echo ""
echo "Worker endpoints:"
echo "  gRPC:    localhost:50051"
echo "  Health:  http://localhost:8081/readyz"
echo "  Metrics: http://localhost:9100/metrics"
echo ""
echo "Logs: docker logs -f image-worker-$WORKER_ID"
echo "Stop: docker stop image-worker-$WORKER_ID"
