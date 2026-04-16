from __future__ import annotations

from collections import deque

# Eliminamos la dependencia de prometheus_client para simplificar el worker
# y centrarnos solo en el flujo de orchestrator.proto.

class WorkerMetrics:
    """
    Gestiona las métricas internas del nodo.
    Esta clase ya no expone un servidor HTTP (Prometheus), sino que mantiene
    el estado en memoria para alimentar los mensajes gRPC de orchestrator.proto.
    """
    def __init__(self, registry: None = None) -> None:
        # Nota: el parámetro registry se mantiene por compatibilidad de firma, pero ya no se usa.
        
        # Historial para calcular latencias (Avg y P95) requeridas por NodeMetrics en el proto
        self._recent_processing_ms: deque[float] = deque(maxlen=100)
        
        # Estado de colas y tareas
        self.queue_length_val: float = 0.0
        self.report_queue_length_val: float = 0.0
        self.active_tasks_val: float = 0.0
        
        # Contadores de tareas (usados para estadísticas acumuladas)
        self.success_total_val: float = 0.0
        self.failure_total_val: float = 0.0
        self.retry_total_val: float = 0.0
        self.deadline_miss_total_val: float = 0.0
        self.tasks_rejected_val: float = 0.0
        self.tasks_cancelled_val: float = 0.0
        
        # Utilización de recursos (alimentan NodeMetrics)
        self.cpu_utilization_val: float = 0.0
        self.memory_utilization_val: float = 0.0
        self.gpu_utilization_val: float = 0.0
        self.io_utilization_val: float = 0.0
        
        # Estado de conectividad y capacidad
        self.capacity_effective_val: float = 0.0
        self.coordinator_connected_val: float = 0.0
        self.readiness_val: float = 0.0
        
        # Histograma simplificado para RTT de gRPC
        self.last_grpc_rtt_ms: float = 0.0

    def start_server(self, host: str, port: int) -> None:
        """Ya no se inicia un servidor HTTP de métricas."""
        pass

    def update_resources(self, snapshot: any) -> None:
        """Actualiza los valores de recursos basados en un snapshot del sistema."""
        self.queue_length_val = float(snapshot.queue_length)
        self.active_tasks_val = float(snapshot.active_tasks)
        self.capacity_effective_val = float(snapshot.capacity_effective)
        self.cpu_utilization_val = float(snapshot.cpu_utilization)
        self.memory_utilization_val = float(snapshot.memory_utilization)
        self.gpu_utilization_val = float(snapshot.gpu_utilization)
        self.io_utilization_val = float(snapshot.io_utilization)

    def set_tasks_by_priority(self, counts: dict[int, int]) -> None:
        """Mantiene compatibilidad de firma, aunque ya no exportamos por prioridad individualmente."""
        pass

    def set_report_queue_length(self, value: int) -> None:
        """Actualiza el tamaño de la cola de reportes pendientes al orquestador."""
        self.report_queue_length_val = float(value)

    def set_coordinator_connected(self, connected: bool) -> None:
        """Indica si estamos conectados al Orquestador Java."""
        self.coordinator_connected_val = 1.0 if connected else 0.0

    def set_readiness(self, ready: bool) -> None:
        """Indica si el nodo está listo para recibir más carga."""
        self.readiness_val = 1.0 if ready else 0.0

    def record_processing_time_ms(self, value: float) -> None:
        """Registra el tiempo que tomó procesar una imagen para cálculos de latencia."""
        self._recent_processing_ms.append(float(value))

    def recent_latency_summary(self) -> tuple[float, float]:
        """Calcula el promedio y el percentil 95 de las últimas 100 tareas."""
        if not self._recent_processing_ms:
            return 0.0, 0.0
        values = sorted(self._recent_processing_ms)
        avg = sum(values) / len(values)
        p95_index = max(0, min(len(values) - 1, round((len(values) - 1) * 0.95)))
        return avg, values[p95_index]

    def snapshot(self) -> dict[str, float]:
        """Genera un diccionario con el estado actual de todas las métricas."""
        avg_latency_ms, p95_latency_ms = self.recent_latency_summary()
        return {
            "queue_length": self.queue_length_val,
            "report_queue_length": self.report_queue_length_val,
            "success_total": self.success_total_val,
            "failure_total": self.failure_total_val,
            "retry_total": self.retry_total_val,
            "deadline_miss_total": self.deadline_miss_total_val,
            "tasks_rejected_total": self.tasks_rejected_val,
            "tasks_cancelled_total": self.tasks_cancelled_val,
            "cpu_utilization_ratio": self.cpu_utilization_val,
            "memory_utilization_ratio": self.memory_utilization_val,
            "gpu_utilization_ratio": self.gpu_utilization_val,
            "io_utilization_ratio": self.io_utilization_val,
            "active_tasks": self.active_tasks_val,
            "capacity_effective": self.capacity_effective_val,
            "coordinator_connected": self.coordinator_connected_val,
            "readiness": self.readiness_val,
            "avg_latency_ms": float(avg_latency_ms),
            "p95_latency_ms": float(p95_latency_ms),
        }

    # Propiedades para mantener compatibilidad con el código que usa .inc() o .observe()
    @property
    def tasks_rejected(self): return self._CounterProxy(self, "tasks_rejected_val")
    @property
    def tasks_cancelled(self): return self._CounterProxy(self, "tasks_cancelled_val")
    @property
    def success_total(self): return self._CounterProxy(self, "success_total_val")
    @property
    def failure_total(self): return self._CounterProxy(self, "failure_total_val")
    @property
    def retry_total(self): return self._CounterProxy(self, "retry_total_val")
    @property
    def deadline_miss_total(self): return self._CounterProxy(self, "deadline_miss_total_val")
    @property
    def grpc_rtt_ms(self): return self._HistogramProxy(self, "last_grpc_rtt_ms")
    @property
    def queue_wait_ms(self): return self._HistogramProxy(None, "") # Ignorado pero mantenido por compatibilidad
    @property
    def active_tasks(self): return self._GaugeProxy(self, "active_tasks_val")

    class _CounterProxy:
        def __init__(self, parent, attr): self.parent, self.attr = parent, attr
        def inc(self, amount=1): setattr(self.parent, self.attr, getattr(self.parent, self.attr) + amount)

    class _HistogramProxy:
        def __init__(self, parent, attr): self.parent, self.attr = parent, attr
        def observe(self, value): 
            if self.parent: setattr(self.parent, self.attr, value)

    class _GaugeProxy:
        def __init__(self, parent, attr): self.parent, self.attr = parent, attr
        def set(self, value): setattr(self.parent, self.attr, float(value))
