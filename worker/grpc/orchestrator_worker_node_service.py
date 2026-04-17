from __future__ import annotations

from typing import Any, cast

import grpc.aio
import psutil

from proto import orchestrator_pb2, orchestrator_pb2_grpc
from worker.core.node import WorkerNode
from worker.domain.models import Task, TransformationSpec
from worker.telemetry.metrics import WorkerMetrics

# Sincronización con orchestrator.proto
# Este servicer implementa los métodos que el Orquestador Java llama sobre el Worker.
# Se centra en:
# 1. GetMetrics: El orquestador pide estadísticas del nodo (CPU, RAM, Tareas hechas).
# 2. YieldTasks: Implementación de Work-Stealing donde este nodo "cede" tareas a otro.

PROTO = cast(Any, orchestrator_pb2)


class OrchestratorWorkerNodeServicer(orchestrator_pb2_grpc.WorkerNodeServicer):
    """
    Servicio gRPC que expone el nodo worker hacia el exterior (principalmente para el Orquestador).
    """
    def __init__(self, node: WorkerNode, metrics: WorkerMetrics) -> None:
        self._node = node
        self._metrics = metrics

    async def GetMetrics(self, request: orchestrator_pb2.QueueStatusRequest, context: grpc.aio.ServicerContext) -> orchestrator_pb2.NodeMetrics:
        """
        Retorna las métricas actuales del nodo al Orquestador.
        Utilizado para el balanceo de carga y monitoreo global en Java.
        """
        # Ya no usamos tracing para simplificar el flujo interno del worker.
        print(f"[DEBUG] >>> Orquestador solicitó MÉTRICAS (GetMetrics)")
        return await self._build_node_metrics()

    async def YieldTasks(self, request: orchestrator_pb2.StealRequest, context: grpc.aio.ServicerContext) -> orchestrator_pb2.StealResponse:
        """
        Método de Work-Stealing (Robo de tareas).
        Cuando el Orquestador detecta que otro nodo está libre, le pide a este nodo
        que 'ceda' algunas de sus tareas en cola.
        """
        print(f"[DEBUG] >>> Orquestador solicitó STEAL de {request.steal_count} tareas para {request.thief_node_id}")
        
        # Validación: asegurarnos de que la petición va dirigida a nosotros
        if request.victim_node_id and request.victim_node_id != self._node.current_health().node_id:
            print(f"[DEBUG] STEAL rechazado: victim_id no coincide ({request.victim_node_id} vs local)")
            return orchestrator_pb2.StealResponse(allowed=False, reason="victim node does not match this worker")
        
        # Intentamos obtener tareas de la cola local para entregarlas
        tasks = await self._node.yield_tasks(int(request.steal_count))
        if not tasks:
            print(f"[DEBUG] STEAL ignorado: no hay tareas en cola para ceder")
            return orchestrator_pb2.StealResponse(allowed=False, reason="no queued tasks available to yield")
        
        print(f"[DEBUG] STEAL exitoso: cediendo {len(tasks)} tareas")
        # Convertimos las tareas internas a formato Proto para el envío
        return orchestrator_pb2.StealResponse(
            stolen_tasks=[self._task_to_proto(item) for item in tasks],
            allowed=True,
            reason="ok",
        )

    async def _build_node_metrics(self) -> Any:
        """Construye el mensaje NodeMetrics reuniendo datos del sistema y del nodo."""
        state = await self._node.get_node_state()
        memory = psutil.virtual_memory()
        snapshot = self._metrics.snapshot()
        orchestrator_stats = self._node.orchestrator_stats()
        
        recent_steal_age = float(orchestrator_stats["recent_steal_age_seconds"])
        
        # Clasificación del estado para el Orquestador Java
        if state.mode.value != "active" or not state.accepting_tasks:
            status = "ERROR"
        elif recent_steal_age <= 5.0:
            status = "STEALING"
        elif state.active_tasks > 0 or state.queue_length > 0:
            status = "BUSY"
        else:
            status = "IDLE"
            
        return PROTO.NodeMetrics(
            node_id=state.node_id,
            ip_address=self._resolve_ip_address(),
            ram_used_mb=float((memory.total - memory.available) / (1024 * 1024)),
            ram_total_mb=float(memory.total / (1024 * 1024)),
            cpu_percent=float(state.cpu_utilization * 100.0),
            workers_busy=int(state.active_tasks),
            workers_total=int(self._node._config.max_active_tasks),
            queue_size=int(state.queue_length),
            queue_capacity=int(min(self._node._config.queue_high_watermark, self._node._config.max_queue_size)),
            tasks_done=int(orchestrator_stats["tasks_done"]),
            steals_performed=int(orchestrator_stats["steals_performed"]),
            avg_latency_ms=float(snapshot.get("avg_latency_ms", 0.0)),
            p95_latency_ms=float(snapshot.get("p95_latency_ms", 0.0)),
            uptime_seconds=int(orchestrator_stats["uptime_seconds"]),
            status=status,
        )

    def _task_to_proto(self, task: Task) -> Any:
        """Convierte un objeto Task interno en un mensaje ImageTask de gRPC."""
        filter_type, target_width, target_height = self._task_filter_projection(task.transforms)
        
        # Intentamos obtener el payload de la imagen (de memoria o disco)
        payload = task.input_image.payload or b""
        if not payload and task.input_image.input_path:
            payload = self._node._storage.read_bytes(task.input_image.input_path)
        elif not payload and task.input_image.input_uri:
            payload = self._node._storage.read_bytes(task.input_image.input_uri)
            
        enqueue_ts = int(task.created_at.timestamp() * 1000)
        
        return PROTO.ImageTask(
            task_id=task.task_id,
            image_data=payload,
            filename=task.metadata.get("source_file_name", f"{task.task_id}.{task.output_format or 'png'}"),
            filter_type=filter_type,
            target_width=target_width,
            target_height=target_height,
            enqueue_ts=enqueue_ts,
            priority=1 if task.priority >= 8 else 0,
        )

    def _task_filter_projection(self, transforms: list[TransformationSpec]) -> tuple[str, int, int]:
        """Proyecta las transformaciones internas al modelo simplificado del orquestador."""
        filter_type = "none"
        target_width = 0
        target_height = 0
        for transform in transforms:
            if transform.operation.value == "resize":
                target_width = int(transform.params.get("width", "0") or "0")
                target_height = int(transform.params.get("height", "0") or "0")
                continue
            if transform.operation.value in {"grayscale", "ocr", "inference"}:
                filter_type = transform.operation.value
        return filter_type, target_width, target_height

    def _resolve_ip_address(self) -> str:
        """Resuelve la IP con la que el nodo se identifica ante el orquestador."""
        host = self._node._config.bind_host.strip()
        if host and host not in {"0.0.0.0", "::", "127.0.0.1", "localhost"}:
            return host
        return "127.0.0.1"
