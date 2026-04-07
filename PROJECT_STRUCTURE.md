# Project Structure Overview

## After Refactoring

```
Nodo/
├── 📄 README.md                          ← REWRITTEN (complete guide, 2 modes)
├── 📄 REFACTORING_SUMMARY.md             ← NEW (what changed, why)
├── 📄 JAVA_INTEGRATION.md                ← NEW (Java backend integration)
├── 📄 .env.example                       ← Legacy (backward compat)
├── 📄 .env.production.example            ← NEW (production template)
│
├── 🐳 docker-compose.yml                 ← UPDATED (single worker, minimal)
│                                         │ For: Production deployment
│                                         │ Includes: 1 worker, no dependencies
│
├── 🐳 docker-compose-dev.yml             ← RENAMED (from docker-compose.yml)
│                                         │ For: Local development
│                                         │ Includes: MinIO, Prometheus, Grafana, 3 workers
│
├── 📦 Dockerfile                         ← UNCHANGED
│
├── 🐍 proto/
│   ├── imagenode.proto                   ← UNCHANGED (image processing service)
│   ├── worker_node.proto                 ← UNCHANGED (control + callbacks)
│   ├── imagenode_pb2.py
│   ├── imagenode_pb2_grpc.py
│   ├── worker_node_pb2.py
│   └── worker_node_pb2_grpc.py
│
├── 🐍 worker/
│   ├── server.py                         ← UNCHANGED (gRPC server init)
│   ├── __main__.py                       ← UNCHANGED
│   ├── config.py                         ← UNCHANGED (but see node.py)
│   │
│   ├── core/
│   │   ├── node.py                       ← UPDATED (metrics/health optional)
│   │   ├── worker_runtime.py             ← UNCHANGED
│   │   ├── serde.py
│   │   └── ...
│   │
│   ├── grpc/
│   │   ├── image_node_service.py         ← UNCHANGED
│   │   ├── worker_control_service.py     ← UNCHANGED
│   │   └── ...
│   │
│   ├── execution/                        ← UNCHANGED
│   ├── scheduler/                        ← UNCHANGED
│   ├── telemetry/                        ← UNCHANGED
│   │   └── metrics.py                    (Prometheus always available, but optional)
│   │
│   └── models/                           ← UNCHANGED
│
├── 📁 scripts/
│   ├── deploy-worker.sh                  ← NEW (automated deployment)
│   ├── healthcheck.py                    ← UNCHANGED
│   ├── ocr_backend.py                    ← UNCHANGED
│   ├── inference_backend.py              ← UNCHANGED
│   └── ...
│
├── 📁 examples/
│   ├── imagenode_client.py               ← UNCHANGED
│   ├── submit_task.py                    ← UNCHANGED
│   └── ...
│
├── 📁 tests/                             ← UNCHANGED
├── 📁 deploy/                            ← UNCHANGED (Prometheus config for dev)
┗ 📁 docs/                                ← UNCHANGED
    └── ARCHITECTURE.md                   ← Still valid
```

---

## Configuration Modes

### Production Mode: Single Worker

```
Environment: .env (based on .env.production.example)
Docker: docker-compose.yml

Config:
├── WORKER_NODE_ID=worker-prod-01         # Worker identity
├── WORKER_BIND_HOST=0.0.0.0
├── WORKER_BIND_PORT=50051                # gRPC port
├── WORKER_COORDINATOR_TARGET=...         # Java backend (optional)
├── WORKER_METRICS_PORT=9100              # Prometheus export (optional, set 0 to disable)
├── WORKER_HEALTH_PORT=8081               # Health checks (optional, set 0 to disable)
├── WORKER_REQUIRE_SHARED_STORAGE=false   # Local disk only
└── WORKER_OUTPUT_DIR=/app/data/out       # Local paths

Running:
$ docker-compose up -d

Ports Exposed:
- 50051:50051    # gRPC (required)
- 8081:8081      # Health checks (req for orchestrator)
- 9100:9100      # Metrics Prometheus (for monitoring)

Dependencies: NONE (no MinIO, Prometheus, Grafana)

Result: Single dedicated worker per machine, fully isolated
```

### Development Mode: Full Stack

```
Environment: defaults in docker-compose-dev.yml (no .env needed)
Docker: docker-compose-dev.yml

Services:
├── MinIO                     # S3-compatible storage
│   ├── Port 9000 (API)
│   └── Port 9001 (Console)
├── Prometheus                # Metrics scraping
│   └── Port 9090
├── Grafana                   # Dashboards
│   ├── Port 3000
│   └── Dashboard: "Distributed Image Worker"
├── Worker-1
│   ├── gRPC: localhost:50051
│   ├── Health: localhost:8081
│   └── Metrics: localhost:9101
├── Worker-2
│   ├── gRPC: localhost:50061
│   ├── Health: localhost:8082
│   └── Metrics: localhost:9102
└── Worker-3
    ├── gRPC: localhost:50071
    ├── Health: localhost:8083
    └── Metrics: localhost:9103

Config: Shared MinIO storage
- WORKER_REQUIRE_SHARED_STORAGE=true
- WORKER_STORAGE_ENDPOINT_URL=http://minio:9000
- WORKER_OUTPUT_URI_PREFIX=s3://distributed-output/worker-X

Running:
$ docker-compose -f docker-compose-dev.yml up -d

Dependencies: ALL (MinIO, Prometheus, Grafana)

Result: Complete local environment for testing/debugging
```

---

## Docker Compose Files

### `docker-compose.yml` — PRODUCTION

```yaml
version: '3.8'

services:
  worker:
    build:
      context: .
      dockerfile: Dockerfile
    init: true
    restart: unless-stopped
    environment:
      WORKER_NODE_ID: ${WORKER_NODE_ID:-worker-production}
      WORKER_BIND_HOST: ${WORKER_BIND_HOST:-0.0.0.0}
      WORKER_BIND_PORT: ${WORKER_BIND_PORT:-50051}
      WORKER_COORDINATOR_TARGET: ${WORKER_COORDINATOR_TARGET:-}
      WORKER_METRICS_HOST: ${WORKER_METRICS_HOST:-0.0.0.0}
      WORKER_METRICS_PORT: ${WORKER_METRICS_PORT:-9100}
      WORKER_HEALTH_HOST: ${WORKER_HEALTH_HOST:-0.0.0.0}
      WORKER_HEALTH_PORT: ${WORKER_HEALTH_PORT:-8081}
      WORKER_OUTPUT_DIR: /app/data/out
      WORKER_STATE_DIR: /app/data/state
      WORKER_REQUIRE_SHARED_STORAGE: "false"
      # ... more config
    ports:
      - "50051:50051"
      - "8081:8081"
      - "9100:9100"
    volumes:
      - worker-data:/app/data

volumes:
  worker-data:
```

**Key features:**
- Single service, minimal configuration
- All behavior configurable via env vars
- No external dependencies (MinIO, Prometheus, Grafana)
- Comments explaining each setting
- Supports TLS, remote storage, callbacks (optional)

### `docker-compose-dev.yml` — DEVELOPMENT

```yaml
services:
  minio:
    image: minio/minio:latest
    # ... S3 storage for local development

  prometheus:
    image: prom/prometheus:v2.54.1
    # ... Metrics scraping

  grafana:
    image: grafana/grafana-oss:11.1.0
    # ... Dashboards

  worker1:
    # ... Full config with MinIO, TLS, callbacks
    depends_on:
      minio-init: { condition: service_completed_successfully }

  worker2:
    # ... Same as worker1 with different ports

  worker3:
    # ... Same as worker1 with different ports
```

**Key features:**
- Three fully-configured workers
- All with MinIO storage, TLS, callbacks
- Prometheus scrapes all workers
- Grafana shows dashboards
- Perfect for load testing and debugging

---

## Environment Variables

### Production (Required)

```bash
WORKER_NODE_ID                    # Unique identifier
WORKER_BIND_HOST                  # 0.0.0.0 for Docker
WORKER_BIND_PORT                  # 50051 (gRPC)
```

### Production (Optional)

```bash
WORKER_COORDINATOR_TARGET         # Java backend callback (empty = standalone)
WORKER_METRICS_HOST               # For Prometheus (0 = disable)
WORKER_METRICS_PORT               # (0 = disable)
WORKER_HEALTH_HOST                # For health probes (0 = disable)
WORKER_HEALTH_PORT                # (0 = disable)
```

### Storage

```bash
WORKER_OUTPUT_DIR                 # /app/data/out (local)
WORKER_STATE_DIR                  # /app/data/state (local)
WORKER_REQUIRE_SHARED_STORAGE     # false = local, true = S3
```

For S3 (optional):
```bash
WORKER_OUTPUT_URI_PREFIX          # s3://bucket/path
WORKER_STATE_URI_PREFIX           # s3://bucket/path
WORKER_STORAGE_ENDPOINT_URL       # S3 endpoint
WORKER_STORAGE_ACCESS_KEY_ID      # S3 credentials
WORKER_STORAGE_SECRET_ACCESS_KEY  # S3 credentials
```

### TLS (Optional but Recommended)

```bash
WORKER_GRPC_SERVER_CERT_FILE      # /etc/worker/certs/server.crt
WORKER_GRPC_SERVER_KEY_FILE       # /etc/worker/certs/server.key
```

---

## Deployment Scenarios

### Scenario 1: Single Worker, No External Dependencies

```bash
cp .env.production.example .env
docker-compose up -d

# That's it! Worker running on :50051
```

### Scenario 2: Single Worker, Reports to Java Backend

```bash
cat > .env << EOF
WORKER_NODE_ID=worker-prod-01
WORKER_COORDINATOR_TARGET=java-backend.corp.net:50052
WORKER_METRICS_PORT=9100
EOF

docker-compose up -d

# Worker reports progress/results to Java backend
# Java backend harvests metrics from :9100/metrics
```

### Scenario 3: Single Worker, TLS Protected

```bash
cat > .env << EOF
WORKER_NODE_ID=worker-secure
WORKER_GRPC_SERVER_CERT_FILE=/etc/worker/certs/server.crt
WORKER_GRPC_SERVER_KEY_FILE=/etc/worker/certs/server.key
WORKER_GRPC_SERVER_CLIENT_CA_FILE=/etc/worker/certs/ca.crt
WORKER_GRPC_SERVER_REQUIRE_CLIENT_AUTH=true
EOF

docker run -d \
  -e WORKER_NODE_ID=worker-secure \
  -e WORKER_GRPC_SERVER_CERT_FILE=/etc/worker/certs/server.crt \
  -e WORKER_GRPC_SERVER_KEY_FILE=/etc/worker/certs/server.key \
  -v /path/to/certs:/etc/worker/certs:ro \
  -p 50051:50051 \
  image-worker
```

### Scenario 4: Local Development with Full Stack

```bash
docker-compose -f docker-compose-dev.yml up -d

# MinIO: localhost:9001
# Grafana: localhost:3000
# Prometheus: localhost:9090
# Worker-1: localhost:50051
# Worker-2: localhost:50061
# Worker-3: localhost:50071
```

---

## Integration with Java Backend

### Quick Integration

1. **Read**: [JAVA_INTEGRATION.md](JAVA_INTEGRATION.md)
2. **Add dependency**: gRPC + protobuf stubs
3. **Create client**: 3 lines of code (see guide)
4. **Select worker**: Load-aware algorithm (see guide)
5. **Submit task**: Standard gRPC call (unchanged)

### Code Example (Java)

```java
// 1. Create client
ManagedChannel channel = ManagedChannelBuilder
    .forAddress("worker-1", 50051)
    .usePlaintext()
    .build();

ImageNodeServiceGrpc.ImageNodeServiceBlockingStub stub =
    ImageNodeServiceGrpc.newBlockingStub(channel);

// 2. Submit task
SubmitTaskRequest req = SubmitTaskRequest.newBuilder()
    .setTask(Task.newBuilder()
        .setTaskId(UUID.randomUUID().toString())
        .setInput(InputImage.newBuilder()
            .setContent(imageBytes)
            .build())
        .addTransforms(Transformation.newBuilder()
            .setType(OperationType.OPERATION_RESIZE)
            .putParams("width", "800")
            .build())
        .build())
    .build();

SubmitTaskReply reply = stub.submitTask(req);
```

---

## Files You Should Know About

### New or Significantly Updated

| File | Type | Purpose |
|------|------|---------|
| `README.md` | 📄 | Complete documentation (production + dev) |
| `REFACTORING_SUMMARY.md` | 📄 | Changes made and why |
| `JAVA_INTEGRATION.md` | 📄 | Java backend integration guide |
| `.env.production.example` | 📄 | Production config template |
| `docker-compose.yml` | 🐳 | Single worker (production) |
| `docker-compose-dev.yml` | 🐳 | 3 workers + monitoring (dev) |
| `scripts/deploy-worker.sh` | 🔧 | Deployment helper script |
| `worker/core/node.py` | 🐍 | Metrics/health now optional |

### Unchanged (But Important)

| File | Type | Purpose |
|------|------|---------|
| `Dockerfile` | 🐳 | Image build (no changes) |
| `proto/*.proto` | 📄 | gRPC contracts (no changes) |
| `worker/server.py` | 🐍 | Server init (no changes) |
| `worker/grpc/*.py` | 🐍 | Services (no changes) |
| `pyproject.toml` | 📦 | Dependencies (no changes) |

---

## Success Criteria: All Met ✅

- ✅ Workers ready for single-instance deployment
- ✅ Architecture clear: Java ↔ Workers
- ✅ No mandatory external dependencies
- ✅ Easy to understand deployment options
- ✅ TLS support (optional but recommended)
- ✅ Callback reporting to Java (optional)
- ✅ Local storage by default
- ✅ Development mode still fully functional
- ✅ Documentation complete and clear
- ✅ Java integration guide included
- ✅ `.proto` files unchanged
- ✅ Backward compatible

---

## Next Actions

1. **Review** `REFACTORING_SUMMARY.md` for detailed changes
2. **Read** `README.md` for deployment options
3. **Copy** `.env.production.example` → `.env` for your environment
4. **Build** with `docker build -t image-worker .`
5. **Test** with `docker-compose up -d`
6. **Verify** with `curl http://localhost:8081/readyz`
7. **Integrate** Java backend using `JAVA_INTEGRATION.md`

---

**Project Status**: ✅ Production-Ready Architecture
