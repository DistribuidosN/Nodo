# Single Worker Deployment Guide

**Quick commands to run a single image processing worker in production.**

---

## Option 1: Docker Compose (Recommended)

### Minimal Setup (2 minutes)

```bash
# 1. Copy production config
cp .env.production.example .env

# 2. Edit if needed (or just use defaults)
cat .env
# WORKER_NODE_ID=worker-production
# WORKER_BIND_HOST=0.0.0.0
# WORKER_BIND_PORT=50051
# ... (all other settings have sensible defaults)

# 3. Build and start
docker-compose up -d

# 4. Verify
curl http://localhost:8081/readyz
# {"ready": true, "live": true, ...}

# 5. Done!
grpcurl -plaintext -d '{}' localhost:50051 list
# imagenode.ImageNodeService
# worker.WorkerControlService
```

### With Java Backend Callback

```bash
cat > .env << 'EOF'
WORKER_NODE_ID=worker-prod-01
WORKER_BIND_HOST=0.0.0.0
WORKER_BIND_PORT=50051
WORKER_COORDINATOR_TARGET=your-java-backend.com:50052
WORKER_METRICS_HOST=0.0.0.0
WORKER_METRICS_PORT=9100
WORKER_HEALTH_HOST=0.0.0.0
WORKER_HEALTH_PORT=8081
EOF

docker-compose up -d

# Worker will now call back to your Java backend with:
# - ReportProgress (during processing)
# - ReportResult (when done)
# - Heartbeat (every 5 seconds)
```

### With TLS (Secure)

```bash
cat > .env << 'EOF'
WORKER_NODE_ID=worker-secure-01
WORKER_BIND_HOST=0.0.0.0
WORKER_BIND_PORT=50051
WORKER_GRPC_SERVER_CERT_FILE=/etc/worker/certs/server.crt
WORKER_GRPC_SERVER_KEY_FILE=/etc/worker/certs/server.key
WORKER_GRPC_SERVER_CLIENT_CA_FILE=/etc/worker/certs/ca.crt
WORKER_GRPC_SERVER_REQUIRE_CLIENT_AUTH=true
EOF

# Mount your certificate files:
docker-compose -f docker-compose.override.yml up -d
```

**docker-compose.override.yml:**
```yaml
services:
  worker:
    volumes:
      - /path/to/certs:/etc/worker/certs:ro
```

### Monitor

```bash
# Check status
docker-compose ps

# View logs
docker-compose logs -f

# Stop
docker-compose down

# Restart
docker-compose restart
```

---

## Option 2: Direct Docker Run

### Simplest

```bash
docker build -t image-worker .

docker run -d \
  --name worker-1 \
  -p 50051:50051 \
  -p 8081:8081 \
  -p 9100:9100 \
  -v worker-data:/app/data \
  image-worker
```

### With Custom Configuration

```bash
docker run -d \
  --name worker-prod-01 \
  -e WORKER_NODE_ID=worker-prod-01 \
  -e WORKER_BIND_HOST=0.0.0.0 \
  -e WORKER_BIND_PORT=50051 \
  -e WORKER_COORDINATOR_TARGET=java-backend:50052 \
  -e WORKER_METRICS_HOST=0.0.0.0 \
  -e WORKER_METRICS_PORT=9100 \
  -p 50051:50051 \
  -p 8081:8081 \
  -p 9100:9100 \
  -v worker-prod-data:/app/data \
  --restart unless-stopped \
  image-worker:latest
```

### Verify

```bash
# Check container
docker ps | grep worker

# Check health
curl http://localhost:8081/readyz

# Check logs
docker logs -f worker-prod-01

# Connect with grpcurl
grpcurl -plaintext localhost:50051 list
```

---

## Option 3: Kubernetes Deployment

### ConfigMap (Configuration)

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: worker-config
  namespace: image-workers
data:
  WORKER_NODE_ID: "worker-k8s-01"
  WORKER_BIND_HOST: "0.0.0.0"
  WORKER_BIND_PORT: "50051"
  WORKER_COORDINATOR_TARGET: "java-backend.default:50052"
  WORKER_METRICS_HOST: "0.0.0.0"
  WORKER_METRICS_PORT: "9100"
```

### StatefulSet (Deployment)

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: image-worker
  namespace: image-workers
spec:
  serviceName: image-workers
  replicas: 3
  selector:
    matchLabels:
      app: image-worker
  template:
    metadata:
      labels:
        app: image-worker
    spec:
      containers:
      - name: worker
        image: myregistry.com/image-worker:latest
        ports:
        - containerPort: 50051
          name: grpc
        - containerPort: 8081
          name: health
        - containerPort: 9100
          name: metrics
        envFrom:
        - configMapRef:
            name: worker-config
        env:
        - name: WORKER_NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name  # Pod name as node ID
        resources:
          requests:
            cpu: "2"
            memory: "4Gi"
          limits:
            cpu: "4"
            memory: "8Gi"
        livenessProbe:
          httpGet:
            path: /livez
            port: 8081
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /readyz
            port: 8081
          initialDelaySeconds: 10
          periodSeconds: 5
        volumeMounts:
        - name: data
          mountPath: /app/data
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: [ "ReadWriteOnce" ]
      resources:
        requests:
          storage: 50Gi
---
apiVersion: v1
kind: Service
metadata:
  name: image-workers
  namespace: image-workers
spec:
  clusterIP: None
  selector:
    app: image-worker
  ports:
  - port: 50051
    name: grpc
  - port: 8081
    name: health
  - port: 9100
    name: metrics
```

Deploy:
```bash
kubectl create namespace image-workers
kubectl apply -f worker-statefulset.yaml

# Check
kubectl get pods -n image-workers
kubectl logs -f image-worker-0 -n image-workers
```

Access from Java backend:
```java
// Each pod: image-worker-0.image-workers:50051, etc.
String[] workerHosts = {
    "image-worker-0.image-workers:50051",
    "image-worker-1.image-workers:50051",
    "image-worker-2.image-workers:50051"
};
```

---

## Ports Reference

For **one** worker, these ports are used:

| Port | Protocol | Purpose | Disable With |
|------|----------|---------|--------------|
| 50051 | gRPC | Image processing (required) | (can't disable) |
| 8081 | HTTP | Health checks `/livez`, `/readyz` | `WORKER_HEALTH_PORT=0` |
| 9100 | HTTP | Prometheus metrics `/metrics` | `WORKER_METRICS_PORT=0` |

**For multiple workers** on different machines: use the same ports (they're on different hosts)

**For multiple workers** in containers on same machine:

```bash
# Worker 1
docker run ... -p 50051:50051 -p 8081:8081 -p 9100:9100 ... worker1

# Worker 2
docker run ... -p 50052:50051 -p 8082:8081 -p 9101:9100 ... worker2

# Worker 3
docker run ... -p 50053:50051 -p 8083:8081 -p 9102:9100 ... worker3
```

---

## Testing the Deployment

### 1. Health Check

```bash
curl -i http://localhost:8081/livez
# HTTP/1.1 200 OK
# {"live": true, "ready": true, "state": "READY", ...}
```

### 2. Metrics

```bash
curl http://localhost:9100/metrics

# Output:
# worker_active_tasks 0
# worker_queue_length 0
# worker_cpu_utilization_ratio 0.1
# ...
```

### 3. gRPC Connectivity

```bash
grpcurl -plaintext localhost:50051 list
# imagenode.ImageNodeService
# worker.WorkerControlService

grpcurl -plaintext -d '{}' localhost:50051 \
  imagenode.ImageNodeService.HealthCheck
# { "ready": true, "message" : "worker ready" }
```

### 4. Submit a Test Task

Python client (included in repo):

```bash
python examples/submit_task.py \
  --target localhost:50051 \
  --image /path/to/test.png \
  --filter resize --width 800 --height 600
```

Java client (see [JAVA_INTEGRATION.md](JAVA_INTEGRATION.md)):

```java
// See JAVA_INTEGRATION.md for full example
SubmitTaskRequest req = SubmitTaskRequest.newBuilder()
    .setTask(Task.newBuilder()
        .setTaskId(UUID.randomUUID().toString())
        .setInput(InputImage.newBuilder()
            .setContent(ByteString.copyFrom(imageBytes))
            .build())
        .build())
    .build();

SubmitTaskReply reply = stub.submitTask(req);
System.out.println("Task submitted: " + reply.getTaskId());
```

---

## Troubleshooting

### Worker won't start

```bash
docker-compose logs
# or
docker logs worker-prod-01

# Common issues:
# - Port already in use: lsof -i :50051
# - Volume permission: chmod 777 /data/worker
# - Out of memory: increase -m / memory limit
```

### Can't connect from Java backend

```bash
# Test from Java backend machine:
grpcurl -plaintext worker.example.com:50051 list

# If fails:
# 1. Check worker is running: docker ps
# 2. Check port: telnet worker.example.com 50051
# 3. Check logs: docker logs worker
# 4. Check firewall: ufw allow 50051/tcp
```

### Health check failing

```bash
curl -v http://localhost:8081/readyz

# If 503:
# - Worker still booting (wait 30s)
# - Storage misconfigured
# - Resource constraints
#
# Check logs:
docker logs -f worker | grep -i error
```

### High latency

```bash
# Check metrics
curl http://localhost:9100/metrics | grep worker_

# Look for:
# - worker_queue_length (high = overloaded)
# - worker_cpu_utilization_ratio (high = bottleneck)
# - worker_processing_time_ms (slow = needs optimization)
```

---

## Environment Variables Quick Reference

```bash
# REQUIRED (or use defaults)
WORKER_NODE_ID=worker-prod-01              # Unique per worker
WORKER_BIND_HOST=0.0.0.0                   # Listen on all interfaces
WORKER_BIND_PORT=50051                     # gRPC port

# OPTIONAL: Java Backend Integration
WORKER_COORDINATOR_TARGET=java-backend:50052    # Callback endpoint
WORKER_COORDINATOR_CA_FILE=/etc/worker/ca.crt   # TLS for Java backend

# OPTIONAL: Metrics & Health
WORKER_METRICS_PORT=9100                   # Set to 0 to disable
WORKER_HEALTH_PORT=8081                    # Set to 0 to disable

# OPTIONAL: Storage
WORKER_OUTPUT_DIR=/app/data/out            # Local disk
WORKER_STATE_DIR=/app/data/state           # Local disk
WORKER_REQUIRE_SHARED_STORAGE=false        # Use local, not S3

# OPTIONAL: TLS (recommended for production)
WORKER_GRPC_SERVER_CERT_FILE=/etc/worker/certs/server.crt
WORKER_GRPC_SERVER_KEY_FILE=/etc/worker/certs/server.key

# OPTIONAL: Resource Limits
WORKER_MAX_ACTIVE_TASKS=4
WORKER_CPU_TARGET=0.85
WORKER_MAX_QUEUE_SIZE=32

# OPTIONAL: Logging
WORKER_LOG_LEVEL=INFO                      # DEBUG, INFO, WARN, ERROR
```

---

## Expected Output

When you start a worker successfully, you should see:

```
2026-04-07T10:30:45Z [INFO] worker.server - Starting image worker...
2026-04-07T10:30:45Z [INFO] worker.core.node - Initializing WorkerNode
2026-04-07T10:30:45Z [INFO] worker.telemetry.metrics - Starting metrics server on 0.0.0.0:9100
2026-04-07T10:30:46Z [INFO] worker.telemetry.health - Health server started on 0.0.0.0:8081
2026-04-07T10:30:46Z [INFO] worker.core.node - Worker ready
2026-04-07T10:30:46Z [INFO] grpc - gRPC server listening on 0.0.0.0:50051
2026-04-07T10:30:46Z [INFO] worker.core.reporter - Connected to coordinator: java-backend:50052
```

Then access it:
```bash
$ curl http://localhost:8081/readyz
{"live":true,"ready":true,"node_id":"worker-prod-01","state":"READY"}

$ grpcurl -plaintext localhost:50051 list
imagenode.ImageNodeService
worker.WorkerControlService
```

---

## All Done! ✅

Your single worker is running and ready to:

1. **Accept gRPC calls** from Java backend on `:50051`
2. **Report health** via HTTP on `:8081` 
3. **Export metrics** in Prometheus format on `:9100` (optional)
4. **Store results** on local disk (or S3 if configured)
5. **Report back** to Java backend (if `WORKER_COORDINATOR_TARGET` set)

**Next**: Read [JAVA_INTEGRATION.md](JAVA_INTEGRATION.md) to integrate with your Java backend!
