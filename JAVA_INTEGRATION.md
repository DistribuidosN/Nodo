# Java Backend Integration Guide

How to integrate your Java backend server with the Python image processing workers.

## Overview

```
┌─────────────────┐
│ Java Backend    │
│ - Auth/DB       │
│ - Job Queue     │
│ - Metrics       │
└────────┬────────┘
         │ gRPC Calls (imagenode.proto)
         │ + Optional Callbacks (worker_node.proto)
         │
    ┌────┴────────────────┐
    │                     │
    v                     v
┌──────────────┐   ┌──────────────┐
│ Worker-1     │   │ Worker-2     │
│ Port 50051   │   │ Port 50051   │
│ (different   │   │ (different   │
│  host)       │   │  host)       │
└──────────────┘   └──────────────┘
```

## 1. Setup: Generate gRPC Stubs for Java

First, get the protobuf definitions from the workers repository:

```bash
# Copy proto files to your Java project
cp -r /path/to/workers/repo/proto your-java-project/src/main/proto/

# Or directly from the repo:
# - proto/imagenode.proto
# - proto/worker_node.proto
```

Generate Java stubs using Maven or Gradle:

**Maven (add to pom.xml):**
```xml
<dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-netty-shaded</artifactId>
    <version>1.71.0</version>
</dependency>
<dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-protobuf</artifactId>
    <version>1.71.0</version>
</dependency>
<dependency>
    <groupId>com.google.protobuf</groupId>
    <artifactId>protobuf-java</artifactId>
    <version>3.25.0</version>
</dependency>

<plugin>
    <groupId>org.xolstice.maven.plugins</groupId>
    <artifactId>protobuf-maven-plugin</artifactId>
    <version>0.6.1</version>
    <configuration>
        <protocArtifact>com.google.protobuf:protoc:3.25.0:exe:${os.detected.classifier}</protocArtifact>
        <pluginId>grpc-java</pluginId>
        <pluginArtifact>io.grpc:protoc-gen-grpc-java:1.71.0:exe:${os.detected.classifier}</pluginArtifact>
    </configuration>
    <executions>
        <execution>
            <goals>
                <goal>compile</goal>
                <goal>compile-custom</goal>
            </goals>
        </execution>
    </executions>
</plugin>
```

**Gradle (add to build.gradle):**
```gradle
plugins {
    id 'com.google.protobuf' version '0.9.4'
}

dependencies {
    implementation 'io.grpc:grpc-netty-shaded:1.71.0'
    implementation 'io.grpc:grpc-protobuf:1.71.0'
    implementation 'com.google.protobuf:protobuf-java:3.25.0'
}

protobuf {
    protoc {
        artifact = 'com.google.protobuf:protoc:3.25.0'
    }
    plugins {
        grpc {
            artifact = 'io.grpc:protoc-gen-grpc-java:1.71.0'
        }
    }
    generateProtoTasks {
        all()*.plugins {
            grpc {}
        }
    }
}
```

Run the build to generate stubs:
```bash
mvn clean compile  # Maven
# or
gradle build       # Gradle
```

## 2. Worker Discovery

Store worker endpoints in your database or configuration:

```java
public class WorkerRegistry {
    private List<WorkerEndpoint> workers = Arrays.asList(
        new WorkerEndpoint("worker-1", "10.0.1.100", 50051),
        new WorkerEndpoint("worker-2", "10.0.2.100", 50051),
        new WorkerEndpoint("worker-3", "10.0.3.100", 50051)
    );
    
    public List<WorkerEndpoint> getAllWorkers() {
        return workers;
    }
}

class WorkerEndpoint {
    String nodeId;
    String host;
    int port;
    
    // getters, setters, toString()
}
```

Or discover them dynamically (Kubernetes, Consul, etc.):

```java
// Kubernetes service discovery
List<WorkerEndpoint> workers = kubernetesClient.endpoints()
    .inNamespace("default")
    .list()
    .getItems()
    .stream()
    .filter(ep -> "image-workers".equals(ep.getMetadata().getName()))
    .flatMap(ep -> ep.getAddresses().stream())
    .map(addr -> new WorkerEndpoint(addr.getHostname(), addr.getIp(), 50051))
    .collect(Collectors.toList());
```

## 3. Create Worker Client

Build a client to connect to workers:

```java
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import proto.ImagenodeGrpc;
import proto.ImagenodeGrpc.ImagenodeServiceBlockingStub;

public class WorkerClient {
    private final String host;
    private final int port;
    private ManagedChannel channel;
    private ImagenodeServiceBlockingStub stub;
    
    public WorkerClient(String host, int port) {
        this.host = host;
        this.port = port;
        connect();
    }
    
    private void connect() {
        channel = ManagedChannelBuilder
            .forAddress(host, port)
            .usePlaintext()  // or .useTransportSecurity() for TLS
            .build();
        
        stub = ImagenodeServiceBlockingStub.newStub(channel);
    }
    
    public WorkerMetrics getMetrics() {
        try {
            GetMetricsRequest req = GetMetricsRequest.getDefaultInstance();
            return stub.getMetrics(req);
        } catch (Exception e) {
            // Handle error
            return null;
        }
    }
    
    public HealthCheckResponse getHealth() {
        try {
            HealthCheckRequest req = HealthCheckRequest.getDefaultInstance();
            return stub.healthCheck(req);
        } catch (Exception e) {
            return null;
        }
    }
    
    public void close() {
        channel.shutdown();
    }
}
```

## 4. Load-Aware Worker Selection

Choose the best worker based on current load:

```java
public class WorkerSelector {
    private final WorkerRegistry registry;
    private final Map<String, WorkerClient> clientCache = new ConcurrentHashMap<>();
    
    public WorkerSelector(WorkerRegistry registry) {
        this.registry = registry;
    }
    
    public WorkerEndpoint selectBestWorker() {
        List<WorkerEndpoint> healthy = new ArrayList<>();
        Map<String, Double> loadScores = new HashMap<>();
        
        // Check each worker's health and load
        for (WorkerEndpoint worker : registry.getAllWorkers()) {
            WorkerClient client = getOrCreateClient(worker);
            
            HealthCheckResponse health = client.getHealth();
            if (health == null || !health.getReady()) {
                continue;  // Skip unhealthy worker
            }
            
            WorkerMetrics metrics = client.getMetrics();
            if (metrics == null) {
                continue;
            }
            
            // Calculate load score (lower = better)
            // Factors: active tasks, queue length, CPU, memory
            double score = calculateLoadScore(metrics);
            loadScores.put(worker.getNodeId(), score);
            healthy.add(worker);
        }
        
        if (healthy.isEmpty()) {
            throw new IllegalStateException("No healthy workers available");
        }
        
        // Return worker with lowest score
        return healthy.stream()
            .min(Comparator.comparingDouble(w -> loadScores.get(w.getNodeId())))
            .orElseThrow();
    }
    
    private double calculateLoadScore(WorkerMetrics metrics) {
        // Normalize metrics to 0-1 range and weight them
        double activeTasks = Math.min(metrics.getActiveTasksCount() / 10.0, 1.0);
        double queueLength = Math.min(metrics.getQueueLength() / 50.0, 1.0);
        double cpuUsage = metrics.getCpuUtilizationRatio();
        double memUsage = metrics.getMemoryUtilizationRatio();
        
        // Weighted average (adjust weights for your use case)
        return (activeTasks * 0.4) + (queueLength * 0.3) + 
               (cpuUsage * 0.2) + (memUsage * 0.1);
    }
    
    private WorkerClient getOrCreateClient(WorkerEndpoint worker) {
        return clientCache.computeIfAbsent(worker.getNodeId(),
            key -> new WorkerClient(worker.getHost(), worker.getPort())
        );
    }
}
```

## 5. Submit Image Processing Task

Send a task to a worker:

```java
public class ImageProcessingService {
    private final WorkerSelector selector;
    private final Map<String, WorkerClient> clients;
    
    public String submitImageProcess(String imageId, byte[] imageData,
                                     List<Transform> transforms,
                                     int priorityLevel) throws Exception {
        
        // Select best worker
        WorkerEndpoint bestWorker = selector.selectBestWorker();
        WorkerClient client = clients.get(bestWorker.getNodeId());
        
        // Build the task
        InputImage.Builder imageBuilder = InputImage.newBuilder()
            .setImageId(imageId)
            .setContent(ByteString.copyFrom(imageData))
            .setFormat(ImageFormat.IMAGE_FORMAT_PNG)  // Adjust based on actual format
            .setWidth(1920)
            .setHeight(1080);
        
        Task.Builder taskBuilder = Task.newBuilder()
            .setTaskId(UUID.randomUUID().toString())
            .setIdempotencyKey("process-" + imageId + "-" + System.currentTimeMillis())
            .setPriority(priorityLevel)
            .setInput(imageBuilder.build())
            .setOutputFormat(ImageFormat.IMAGE_FORMAT_JPEG);
        
        // Add transformations
        for (Transform transform : transforms) {
            Transformation.Builder transBuilder = Transformation.newBuilder()
                .setType(transform.getType());
            
            for (Map.Entry<String, String> param : transform.getParams().entrySet()) {
                transBuilder.putParams(param.getKey(), param.getValue());
            }
            
            taskBuilder.addTransforms(transBuilder.build());
        }
        
        // Set deadline
        Timestamp deadline = Timestamp.newBuilder()
            .setSeconds(System.currentTimeUnit() / 1000 + 30)  // 30 seconds
            .build();
        taskBuilder.setDeadline(deadline);
        
        // Submit to worker
        SubmitTaskRequest request = SubmitTaskRequest.newBuilder()
            .setTask(taskBuilder.build())
            .build();
        
        SubmitTaskReply reply = client.submitTask(request);
        
        if (!reply.getAccepted()) {
            throw new RuntimeException("Worker rejected task: " + reply.getReason());
        }
        
        // Task accepted! Store task ID and worker ID in your database
        storeTaskMapping(reply.getTaskId(), bestWorker.getNodeId());
        
        return reply.getTaskId();
    }
    
    private void storeTaskMapping(String taskId, String workerId) {
        // Save to database for tracking
        // e.g., INSERT INTO image_tasks (task_id, worker_id, status) VALUES (?, ?, 'QUEUED')
    }
}
```

## 6. Optional: Implement Callback Receiver

If you want real-time updates, implement the `CoordinatorCallbackService`:

```java
import io.grpc.stub.StreamObserver;
import proto.WorkerNodeGrpc.CoordinatorCallbackServiceImplBase;
import proto.WorkerNode.*;

@GrpcService
public class WorkerCallbackService extends CoordinatorCallbackServiceImplBase {
    
    private final TaskStatusRepository taskRepository;
    
    @Override
    public void reportProgress(TaskProgress request, 
                               StreamObserver<com.google.protobuf.Empty> responseObserver) {
        try {
            String taskId = request.getTaskId();
            double progress = request.getProgress();
            
            // Update progress in database
            taskRepository.updateProgress(taskId, progress);
            
            // Notify UI via WebSocket, event bus, etc.
            notifyProgress(taskId, progress);
            
            responseObserver.onNext(com.google.protobuf.Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
    
    @Override
    public void reportResult(TaskResult request, 
                             StreamObserver<com.google.protobuf.Empty> responseObserver) {
        try {
            String taskId = request.getTaskId();
            String outputUri = request.getOutputUri();
            
            // Update task as complete
            taskRepository.markComplete(taskId, outputUri);
            
            // Download result from S3/local storage if needed
            String result = downloadResult(outputUri);
            
            // Save to database
            taskRepository.saveResult(taskId, result);
            
            responseObserver.onNext(com.google.protobuf.Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
    
    @Override
    public void heartbeat(WorkerHealth request, 
                         StreamObserver<com.google.protobuf.Empty> responseObserver) {
        try {
            String workerId = request.getNodeId();
            
            // Update worker status in database
            workerRepository.updateStatus(workerId, 
                request.getQueueLength(),
                request.getActiveTasks(),
                request.getCpuUtilizationRatio(),
                request.getMemoryUtilizationRatio()
            );
            
            responseObserver.onNext(com.google.protobuf.Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
```

Then start your gRPC server to listen on port 50052:

```java
@Configuration
public class GrpcServerConfig {
    
    @Bean
    public GrpcServerConfigurer grpcServerConfigurer() {
        return server -> server.maxInboundMessageSize(25 * 1024 * 1024);
    }
}
```

And configure workers to report to your server:

```bash
# In .env or Kubernetes ConfigMap:
WORKER_COORDINATOR_TARGET=java-backend.example.com:50052
```

## 7. Error Handling & Resilience

Implement circuit breaker for worker calls:

```java
public class ResilientWorkerClient {
    private final WorkerClient delegate;
    private final CircuitBreaker circuitBreaker;
    
    public ResilientWorkerClient(WorkerClient delegate) {
        this.delegate = delegate;
        
        this.circuitBreaker = CircuitBreaker.ofDefaults("worker-" + delegate.getHost());
        circuitBreaker.getEventPublisher()
            .onFailure(event -> logger.warn("Worker failed: " + event))
            .onStateTransition(event -> logger.info("Circuit state: " + event));
    }
    
    public SubmitTaskReply submitTask(SubmitTaskRequest request) {
        return circuitBreaker.executeSupplier(
            () -> delegate.submitTask(request)
        );
    }
    
    public WorkerMetrics getMetrics() {
        return circuitBreaker.executeSupplier(
            () -> delegate.getMetrics()
        );
    }
}
```

## 8. Testing

Test your integration locally:

```java
@SpringBootTest
@ActiveProfiles("test")
public class WorkerIntegrationTest {
    
    @Autowired
    private ImageProcessingService service;
    
    @Test
    public void testSubmitImage() throws Exception {
        byte[] imageData = /* load test image */;
        List<Transform> transforms = Arrays.asList(
            new Transform(OperationType.OPERATION_RESIZE, Map.of("width", "800"))
        );
        
        String taskId = service.submitImageProcess("test-img", imageData, transforms, 10);
        
        assertNotNull(taskId);
        assertTrue(taskId.length() > 0);
    }
    
    @Test
    public void testWorkerMetrics() {
        WorkerMetrics metrics = workerSelector.selectBestWorker().getMetrics();
        
        assertNotNull(metrics);
        assertTrue(metrics.getActiveTasks() >= 0);
    }
}
```

Or use docker-compose-dev.yml to run workers locally:

```bash
docker-compose -f docker-compose-dev.yml up

# In your test:
String workerHost = "localhost";
int workerPort = 50051;  // worker1
```

## 9. Production Checklist

- [ ] Updated worker deployment with correct `WORKER_COORDINATOR_TARGET`
- [ ] TLS certificates configured (mTLS if required)
- [ ] Worker registry configured (static or dynamic)
- [ ] Load balancing strategy implemented
- [ ] Callback service (optional) implemented
- [ ] Error handling & circuit breakers in place
- [ ] Metrics collection/monitoring set up
- [ ] Unit & integration tests passing
- [ ] Health checks working (`curl http://worker:8081/readyz`)
- [ ] Network connectivity tested (Java ↔ Workers)
- [ ] Graceful degradation if workers go down
- [ ] Logging/auditing for task tracking

---

## Quick Reference

| Task | Protocol | Port | Direction |
|------|----------|------|-----------|
| Submit image task | gRPC | 50051 | Java → Worker |
| Get metrics/health | HTTP | 9100/8081 | Java → Worker |
| Callback (optional) | gRPC | 50052 | Worker → Java |

For more details, see [README.md](README.md) and the proto definitions:
- [proto/imagenode.proto](proto/imagenode.proto)
- [proto/worker_node.proto](proto/worker_node.proto)
