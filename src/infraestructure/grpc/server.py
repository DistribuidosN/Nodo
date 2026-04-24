"""
infraestructure/grpc/server.py  —  WorkerNode gRPC server (async).

Usa grpc.aio para exponer GetMetrics y YieldTasks sin threads extras.
"""
from __future__ import annotations

import asyncio

import grpc
import grpc.aio

from infraestructure.proto import a2ws_pb2      as pb
from infraestructure.proto import a2ws_pb2_grpc as rpc

from config.settings import NODE_GRPC_PORT, QUEUE_CAPACITY, NODE_ID, get_logger
from domain.state import NodeState

log = get_logger(NODE_ID)


class WorkerNodeServicer(rpc.WorkerNodeServicer):
    """Servicer async del propio nodo."""

    def __init__(self, task_queue: asyncio.Queue, state: NodeState) -> None:
        self._q     = task_queue
        self._state = state

    async def GetMetrics(self, request, context) -> pb.NodeMetrics:
        d = self._state.metrics_dict(self._q.qsize())
        return pb.NodeMetrics(
            node_id          = d["node_id"],
            ip_address       = d["ip_address"],
            ram_used_mb      = d["ram_used_mb"],
            ram_total_mb     = d["ram_total_mb"],
            cpu_percent      = d["cpu_percent"],
            workers_busy     = d["workers_busy"],
            workers_total    = d["workers_total"],
            queue_size       = d["queue_size"],
            queue_capacity   = d["queue_capacity"],
            tasks_done       = d["tasks_done"],
            steals_performed = d["steals_performed"],
            avg_latency_ms   = d["avg_latency_ms"],
            p95_latency_ms   = d["p95_latency_ms"],
            uptime_seconds   = d["uptime_seconds"],
            status           = d["status"],
        )

    async def YieldTasks(self, request: pb.StealRequest, context) -> pb.StealResponse:
        half = QUEUE_CAPACITY // 2 or 1
        if self._q.qsize() <= half:
            return pb.StealResponse(
                allowed=False,
                reason=f"Cola ({self._q.qsize()}) ≤ mitad ({half}), no cedemos",
            )

        stolen: list[pb.ImageTask] = []
        for _ in range(request.steal_count):
            try:
                task = self._q.get_nowait()
                stolen.append(task)
                self._q.task_done()
            except asyncio.QueueEmpty:
                break

        log.info("[YIELD] cedí %d tarea(s) a %s", len(stolen), request.thief_node_id)
        return pb.StealResponse(
            allowed      = True,
            stolen_tasks = stolen,
            reason       = f"yielded {len(stolen)} tasks",
        )


async def serve_grpc_local(task_queue: asyncio.Queue,
                           state: NodeState) -> grpc.aio.Server:
    """Arranca el servidor gRPC async y lo devuelve para poder detenerlo."""
    server = grpc.aio.server(options=[
        ("grpc.max_receive_message_length", 50 * 1024 * 1024),
    ])
    rpc.add_WorkerNodeServicer_to_server(
        WorkerNodeServicer(task_queue, state), server
    )
    addr = f"[::]:{NODE_GRPC_PORT}"
    server.add_insecure_port(addr)
    await server.start()
    log.info("WorkerNode gRPC escuchando en %s", addr)
    return server
