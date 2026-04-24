from __future__ import annotations
import asyncio
import logging
import time
from typing import Any, cast

import grpc
import psutil
from proto import orchestrator_pb2, orchestrator_pb2_grpc
from worker.application.ports.coordinator import TaskProviderPort, ResultReporterPort, HeartbeatPort
from worker.infrastructure.adapters.grpc import grpc_mappers

PROTO = cast(Any, orchestrator_pb2)
PROTO_GRPC = cast(Any, orchestrator_pb2_grpc)

class GrpcCoordinatorAdapter(TaskProviderPort, ResultReporterPort, HeartbeatPort):
    """
    Adaptador de infraestructura que implementa la comunicación con el orquestador vía gRPC.
    """
    def __init__(self, target: str, node_id: str, storage: Any) -> None:
        self._target = target
        self._node_id = node_id
        self._storage = storage
        self._logger = logging.getLogger("worker.grpc_adapter")
        self._channel: grpc.aio.Channel | None = None
        self._stub: Any | None = None
        self._connected = False

    async def _ensure_connection(self) -> Any:
        if self._channel is None:
            self._channel = grpc.aio.insecure_channel(self._target)
            self._stub = PROTO_GRPC.OrchestratorStub(self._channel)
        return self._stub

    async def pull_tasks(self, slots_free: int, state: Any) -> list[Any]:
        stub = await self._ensure_connection()
        metrics_proto = await self._build_metrics_proto(state)
        
        request = PROTO.PullRequest(
            node_id=self._node_id,
            slots_free=slots_free,
            metrics=metrics_proto
        )
        response = await stub.PullTasks(request, timeout=5.0)
        return [grpc_mappers.image_task_to_domain(t) for t in response.tasks]

    async def report_progress(self, event: Any) -> None:
        stub = await self._ensure_connection()
        worker_id = event.metadata.get("worker_id") or f"{event.node_id}-worker"
        request = PROTO.TaskProgress(
            task_id=event.task_id,
            worker_id=worker_id,
            progress_percentage=event.progress_pct,
            status_message=event.message,
        )
        await stub.UpdateTaskProgress(request, timeout=2.0)

    async def report_result(self, result: Any) -> None:
        stub = await self._ensure_connection()
        # Nota: El payload_bytes debe cargarse en el mapper o aquí
        payload = b""
        if result.state.value == "succeeded" and result.output_path:
            payload = self._storage.read_bytes(result.output_path)
            result.metadata["payload_bytes"] = payload

        metrics_proto = await self._build_metrics_proto() # Dummy o real
        proto_result = grpc_mappers.result_to_proto(result, self._node_id, metrics_proto)
        await stub.SubmitResult(proto_result, timeout=5.0)

    async def send_heartbeat(self, state: Any) -> None:
        stub = await self._ensure_connection()
        metrics_proto = await self._build_metrics_proto(state)
        request = PROTO.HeartbeatRequest(
            node_id=self._node_id,
            metrics=metrics_proto
        )
        await stub.SendHeartbeat(request, timeout=2.0)

    async def _build_metrics_proto(self, state: Any | None = None) -> Any:
        # Simplificación de la lógica de métricas (se puede mover a un mapper)
        memory = psutil.virtual_memory()
        return PROTO.NodeMetrics(
            node_id=self._node_id,
            ram_used_mb=float((memory.total - memory.available) / (1024 * 1024)),
            ram_total_mb=float(memory.total / (1024 * 1024)),
            # ... más campos según orchestrator.proto
        )

    async def close(self) -> None:
        if self._channel:
            await self._channel.close()
