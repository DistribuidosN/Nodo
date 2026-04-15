from __future__ import annotations

from collections import deque

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

from worker.models.types import ResourceSnapshot


class WorkerMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self._registry = registry or CollectorRegistry(auto_describe=True)
        self._recent_processing_ms: deque[float] = deque(maxlen=100)
        self.queue_length = Gauge("worker_queue_length", "Current queued tasks", registry=self._registry)
        self.report_queue_length = Gauge("worker_report_queue_length", "Pending coordinator events", registry=self._registry)
        self.queue_wait_ms = Histogram(
            "worker_queue_wait_ms",
            "Queue wait time in milliseconds",
            buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 15000),
            registry=self._registry,
        )
        self.processing_time_ms = Histogram(
            "worker_processing_time_ms",
            "Task processing time in milliseconds",
            buckets=(10, 25, 50, 100, 250, 500, 1000, 5000, 15000, 60000),
            registry=self._registry,
        )
        self.success_total = Counter("worker_success_total", "Completed tasks", registry=self._registry)
        self.failure_total = Counter("worker_failure_total", "Failed tasks", registry=self._registry)
        self.retry_total = Counter("worker_retry_total", "Retries scheduled", registry=self._registry)
        self.deadline_miss_total = Counter("worker_deadline_miss_total", "Missed task deadlines", registry=self._registry)
        self.tasks_rejected = Counter("worker_tasks_rejected_total", "Rejected tasks", registry=self._registry)
        self.tasks_cancelled = Counter("worker_tasks_cancelled_total", "Cancelled tasks", registry=self._registry)
        self.cpu_utilization = Gauge("worker_cpu_utilization_ratio", "CPU utilization ratio", registry=self._registry)
        self.memory_utilization = Gauge("worker_memory_utilization_ratio", "Memory utilization ratio", registry=self._registry)
        self.gpu_utilization = Gauge("worker_gpu_utilization_ratio", "GPU utilization ratio", registry=self._registry)
        self.io_utilization = Gauge("worker_io_utilization_ratio", "I/O utilization ratio", registry=self._registry)
        self.grpc_rtt_ms = Histogram(
            "worker_grpc_rtt_ms",
            "gRPC round trip time in milliseconds",
            buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500),
            registry=self._registry,
        )
        self.active_tasks = Gauge("worker_active_tasks", "Tasks currently running", registry=self._registry)
        self.capacity_effective = Gauge("worker_capacity_effective", "Effective worker capacity", registry=self._registry)
        self.coordinator_connected = Gauge("worker_coordinator_connected", "Coordinator channel state", registry=self._registry)
        self.readiness = Gauge("worker_readiness", "Worker readiness state", registry=self._registry)
        self.tasks_by_priority = Gauge("worker_tasks_by_priority", "Queued tasks by priority", ["priority"], registry=self._registry)

    def start_server(self, host: str, port: int) -> None:
        start_http_server(port=port, addr=host, registry=self._registry)

    def update_resources(self, snapshot: ResourceSnapshot) -> None:
        self.queue_length.set(snapshot.queue_length)
        self.active_tasks.set(snapshot.active_tasks)
        self.capacity_effective.set(snapshot.capacity_effective)
        self.cpu_utilization.set(snapshot.cpu_utilization)
        self.memory_utilization.set(snapshot.memory_utilization)
        self.gpu_utilization.set(snapshot.gpu_utilization)
        self.io_utilization.set(snapshot.io_utilization)

    def set_tasks_by_priority(self, counts: dict[int, int]) -> None:
        for priority, count in counts.items():
            self.tasks_by_priority.labels(priority=str(priority)).set(count)

    def set_report_queue_length(self, value: int) -> None:
        self.report_queue_length.set(value)

    def set_coordinator_connected(self, connected: bool) -> None:
        self.coordinator_connected.set(1 if connected else 0)

    def set_readiness(self, ready: bool) -> None:
        self.readiness.set(1 if ready else 0)

    def record_processing_time_ms(self, value: float) -> None:
        self.processing_time_ms.observe(value)
        self._recent_processing_ms.append(float(value))

    def recent_latency_summary(self) -> tuple[float, float]:
        if not self._recent_processing_ms:
            return 0.0, 0.0
        values = sorted(self._recent_processing_ms)
        avg = sum(values) / len(values)
        p95_index = max(0, min(len(values) - 1, round((len(values) - 1) * 0.95)))
        return avg, values[p95_index]

    def snapshot(self) -> dict[str, float]:
        avg_latency_ms, p95_latency_ms = self.recent_latency_summary()
        return {
            "queue_length": float(self.queue_length._value.get()),  # type: ignore[union-attr, attr-defined]
            "report_queue_length": float(self.report_queue_length._value.get()),  # type: ignore[union-attr, attr-defined]
            "success_total": float(self.success_total._value.get()),  # type: ignore[union-attr, attr-defined]
            "failure_total": float(self.failure_total._value.get()),  # type: ignore[union-attr, attr-defined]
            "retry_total": float(self.retry_total._value.get()),  # type: ignore[union-attr, attr-defined]
            "deadline_miss_total": float(self.deadline_miss_total._value.get()),  # type: ignore[union-attr, attr-defined]
            "tasks_rejected_total": float(self.tasks_rejected._value.get()),  # type: ignore[union-attr, attr-defined]
            "tasks_cancelled_total": float(self.tasks_cancelled._value.get()),  # type: ignore[union-attr, attr-defined]
            "cpu_utilization_ratio": float(self.cpu_utilization._value.get()),  # type: ignore[union-attr, attr-defined]
            "memory_utilization_ratio": float(self.memory_utilization._value.get()),  # type: ignore[union-attr, attr-defined]
            "gpu_utilization_ratio": float(self.gpu_utilization._value.get()),  # type: ignore[union-attr, attr-defined]
            "io_utilization_ratio": float(self.io_utilization._value.get()),  # type: ignore[union-attr, attr-defined]
            "active_tasks": float(self.active_tasks._value.get()),  # type: ignore[union-attr, attr-defined]
            "capacity_effective": float(self.capacity_effective._value.get()),  # type: ignore[union-attr, attr-defined]
            "coordinator_connected": float(self.coordinator_connected._value.get()),  # type: ignore[union-attr, attr-defined]
            "readiness": float(self.readiness._value.get()),  # type: ignore[union-attr, attr-defined]
            "avg_latency_ms": float(avg_latency_ms),
            "p95_latency_ms": float(p95_latency_ms),
        }
