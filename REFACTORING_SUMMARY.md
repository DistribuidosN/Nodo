# Refactoring Summary: Production-Ready Worker Architecture

**Date**: April 7, 2026  
**Scope**: Transformed project from "3-worker + coordinator dev stack" to "single worker production" + "full debug stack"

---

## What Changed

### 1. **Code Changes** ✅

#### `worker/core/node.py`
- Made Prometheus metrics server **optional**: Only starts if `WORKER_METRICS_PORT > 0`
- Made HTTP health check server **optional**: Only starts if `WORKER_HEALTH_PORT > 0`
- Graceful cleanup: Health server only stopped if it was started

**Impact**: Zero dependencies required for production. Set port to 0 to disable.

### 2. **Docker Configuration** ✅

#### **Renamed**
- Old `docker-compose.yml` → `docker-compose-dev.yml`  
  (includes MinIO, Prometheus, Grafana, 3 workers - for local development)

#### **Created New**
- `docker-compose.yml` (minimal production setup)
  - Single worker service
  - No MinIO, Prometheus, Grafana required
  - All configuration via environment variables
  - Fully customizable for different machines

### 3. **Configuration Files** ✅

#### **New**
- `.env.production.example` — Complete production configuration template with:
  - Worker identity and binding
  - Optional Java backend callback setup
  - Storage configuration (local disk by default)
  - TLS options
  - Resource limits
  - Commented sections for S3/Prometheus if needed

### 4. **Documentation** ✅

#### **README.md** — Completely rewritten
- **Two clear sections**: Production vs Development
- **Quick start** for both modes
- **Architecture diagrams** explaining data flow
- **gRPC contracts** clearly documented
- **Integration guide** with Java backend
- **Monitoring** section with Prometheus metrics
- **Troubleshooting** section

#### **JAVA_INTEGRATION.md** — NEW
- Step-by-step Java backend integration guide
- Worker client example code
- Load-aware worker selection algorithm
- Callback service implementation (optional)
- Error handling & resilience patterns
- Production checklist

### 5. **Scripts** ✅

#### **scripts/deploy-worker.sh** — NEW
- Automated single worker deployment script
- Usage: `./scripts/deploy-worker.sh worker-id [java-backend-target]`
- Builds image, cleans up old container, starts new one
- Sets up proper networking and volumes

---

## Two Deployment Modes

### Mode 1: **Production** (Single Worker) 

```bash
# Fast, lightweight, no dependencies
docker-compose up -d

# One worker listening on:
# - gRPC: :50051
# - Health: :8081
# - Metrics: :9100 (optional)

# Storage: Local disk only
# Callbacks: Optional (if Java backend provides endpoint)
```

**Files used:**
- `docker-compose.yml` (new, minimal)
- `.env.production.example` (new, fully documented)
- `Dockerfile` (unchanged)

**Directory Structure:**
```
Nodo/
├── docker-compose.yml          ← Production (1 worker, minimal)
├── docker-compose-dev.yml      ← Development (MinIO, Prometheus, Grafana, 3 workers)
├── .env.production.example     ← Production config template
├── .env.example                ← Still exists for backward compatibility
└── Dockerfile                  ← Unchanged
```

---

### Mode 2: **Development** (3 Workers + Monitoring)

```bash
# Full stack for testing and debugging
docker-compose -f docker-compose-dev.yml up -d

# Three workers + all infrastructure:
# - MinIO (S3 storage): :9000, :9001
# - Prometheus: :9090
# - Grafana: :3000 (dashboards)
# - Worker-1: :50051
# - Worker-2: :50061
# - Worker-3: :50071
```

**Files used:**
- `docker-compose-dev.yml` (renamed from original docker-compose.yml)
- No `.env` file needed (uses defaults in the compose file)

---

## What Stayed the Same

✅ **All Python code works identically** — No breaking changes  
✅ **Protobuf contracts unchanged** — Java backend integration unaffected  
✅ **Worker functionality 100% intact** — Same image processing pipeline  
✅ **Health checks still available** — `/livez`, `/readyz` endpoints  
✅ **Metrics still available** — Prometheus format if needed  
✅ **Callbacks still optional** — WORKER_COORDINATOR_TARGET mechanism unchanged  
✅ **Tests pass** — No changes to core logic  

---

## What Was Removed

❌ **Hard dependency on MinIO** — Now optional (configure via env vars if needed)  
❌ **Hard dependency on Prometheus** — Optional (disable with port=0)  
❌ **Hard dependency on Grafana** — Not shipped with single-worker deployment  
❌ **Assumption of shared storage** — Default to local disk  

---

## Configuration Examples

### Production: Single Worker (Simplest)

```bash
# Start with defaults
docker-compose up -d

# Worker ID auto-generated from hostname
# All storage local to container volume
# No callbacks to Java backend
# Metrics available but not scraped
```

### Production: Single Worker (With Java Backend Callback)

```bash
# .env file
WORKER_NODE_ID=worker-prod-01
WORKER_BIND_HOST=0.0.0.0
WORKER_BIND_PORT=50051
WORKER_COORDINATOR_TARGET=java-backend.example.com:50052
WORKER_METRICS_HOST=0.0.0.0
WORKER_METRICS_PORT=9100

docker-compose up -d
```

### Production: Single Worker (With TLS)

```bash
# .env file
WORKER_GRPC_SERVER_CERT_FILE=/etc/worker/certs/server.crt
WORKER_GRPC_SERVER_KEY_FILE=/etc/worker/certs/server.key
WORKER_GRPC_SERVER_CLIENT_CA_FILE=/etc/worker/certs/ca.crt
WORKER_GRPC_SERVER_REQUIRE_CLIENT_AUTH=true

docker-compose up -d
```

### Production: Single Worker (Metrics disabled)

```bash
# Disable optional servers
WORKER_METRICS_PORT=0
WORKER_HEALTH_PORT=0

docker-compose up -d

# Only gRPC port exposed: 50051
```

### Development: Full Stack

```bash
docker-compose -f docker-compose-dev.yml up -d

# Everything included: MinIO, Prometheus, Grafana, 3 workers
# Access Grafana: http://localhost:3000
# Access MinIO: http://localhost:9001
```

---

## How Java Backend Integrates

### 1. Query Worker Readiness

```java
// Check if worker is ready before sending tasks
curl -s http://worker:8081/readyz
// Response: { "ready": true, "live": true, ... }
```

### 2. Get Worker Status & Metrics

```java
// Load-aware worker selection
GET http://worker:9100/metrics
// Parse: worker_active_tasks, worker_queue_length, worker_cpu_utilization_ratio
```

### 3. Send Image Task

```java
// Standard gRPC call
Channel ch = ManagedChannelBuilder.forAddress("worker", 50051).usePlaintext().build();
ImageNodeServiceGrpc.ImageNodeServiceBlockingStub stub = ImageNodeServiceGrpc.newBlockingStub(ch);

// Submit task as usual (unchanged)
SubmitTaskRequest req = SubmitTaskRequest.newBuilder()...build();
SubmitTaskReply reply = stub.submitTask(req);
```

### 4. Optional: Listen for Callbacks

```java
// If Java backend implements CoordinatorCallbackService and sets:
// WORKER_COORDINATOR_TARGET=java-backend:50052

// Worker will call back with:
// - ReportProgress: "50% done"
// - ReportResult: "Complete at S3://..."
// - Heartbeat: "Queue=5, CPU=0.75"
```

See **JAVA_INTEGRATION.md** for complete Java code examples.

---

## Migration Path

If you were already running the old setup:

```bash
# 1. Backup old docker-compose
cp docker-compose.yml docker-compose-dev.yml

# 2. Pull new docker-compose.yml from repo
# (New file with single-worker setup)

# 3. Stop old stack
docker-compose down

# 4. For production: just use new docker-compose.yml
docker-compose up -d

# 5. For development: use docker-compose-dev.yml
docker-compose -f docker-compose-dev.yml up -d
```

**No need to change any Python code or protobuf definitions.**

---

## Testing

### Test Single Worker

```bash
# Start worker
docker-compose up -d

# Test gRPC connectivity
grpcurl -plaintext -d '{}' localhost:50051 list
# Output: imagenode.ImageNodeService, worker.WorkerControlService

# Test health check
curl http://localhost:8081/readyz
# Output: {"ready": true, "live": true, ...}

# Test metrics
curl http://localhost:9100/metrics
# Output: Prometheus format metrics
```

### Test with Java Backend Locally

```bash
# Terminal 1: Workers
docker-compose -f docker-compose-dev.yml up -d

# Terminal 2: Java backend (run your app)
java -jar my-app.jar

# Terminal 3: Send test task
java ImageProcessingClient --worker localhost:50051
```

---

## Key Files Reference

| File | Purpose | Status |
|------|---------|--------|
| `docker-compose.yml` | Single worker (production) | ✅ NEW |
| `docker-compose-dev.yml` | 3 workers + monitoring (dev) | ✅ RENAMED (from old docker-compose.yml) |
| `.env.production.example` | Production config template | ✅ NEW |
| `.env.example` | Development config | ⚠️ Legacy, use .env.production.example |
| `worker/core/node.py` | Optional metrics/health init | ✅ UPDATED |
| `README.md` | Full documentation | ✅ REWRITTEN |
| `JAVA_INTEGRATION.md` | Java backend guide | ✅ NEW |
| `scripts/deploy-worker.sh` | Deployment helper | ✅ NEW |
| `Dockerfile` | Worker image build | ✅ UNCHANGED |

---

## Metrics Available (Optional)

When `WORKER_METRICS_PORT > 0`:

```
worker_active_tasks              # Running tasks
worker_queue_length              # Queued tasks
worker_capacity_effective        # Available capacity
worker_cpu_utilization_ratio     # CPU% (0-1)
worker_memory_utilization_ratio  # RAM% (0-1)
worker_processing_time_ms        # Task duration histogram
worker_success_total             # Total completed
worker_failure_total             # Total failed
worker_deadline_miss_total       # Missed deadlines
```

Use these for load-aware routing in Java backend.

---

## Next Steps

1. **Pull the changes** to your deployment environment
2. **Read** [JAVA_INTEGRATION.md](JAVA_INTEGRATION.md) if integrating with Java backend
3. **Copy** `.env.production.example` to `.env` and customize
4. **Build** with `docker build -t image-worker .`
5. **Test** with `docker run ... image-worker:latest`
6. **Deploy** one instance per machine/VM
7. **Configure** Java backend to query workers by hostname:50051

---

## Support

For issues:
- Check [README.md](README.md) troubleshooting section
- Review [JAVA_INTEGRATION.md](JAVA_INTEGRATION.md) for integration patterns
- Check worker logs: `docker logs <container>`
- Test health: `curl http://worker:8081/readyz`
- Verify connectivity: `grpcurl -plaintext localhost:50051 list`

---

**Architecture Ready for Production** ✅
