"""
infraestructure/grpc/client.py  —  Cliente async gRPC hacia el Orquestador Java.

Usa grpc.aio (asyncio nativo) en lugar de el canal síncrono.
Todas las llamadas son `await`able.
"""
from __future__ import annotations

import asyncio

import grpc
import grpc.aio

from infraestructure.proto import a2ws_pb2      as pb
from infraestructure.proto import a2ws_pb2_grpc as rpc

from config.settings import NODE_ID, NODE_IP, ORCHESTRATOR_ADDR, get_logger
from domain.state import NodeState

log = get_logger(NODE_ID)

_CHANNEL_OPTIONS = [
    ("grpc.keepalive_time_ms",             10_000),
    ("grpc.keepalive_timeout_ms",           5_000),
    ("grpc.keepalive_permit_without_calls", True),
    ("grpc.max_receive_message_length",     50 * 1024 * 1024),
    ("grpc.max_send_message_length",        50 * 1024 * 1024),
]


def _make_metrics(state: NodeState, queue_size: int) -> pb.NodeMetrics:
    d = state.metrics_dict(queue_size)
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


class OrchestratorClient:
    """Canal grpc.aio + stub async hacia el Orquestador Java."""

    def __init__(self) -> None:
        self._channel = grpc.aio.insecure_channel(ORCHESTRATOR_ADDR, options=_CHANNEL_OPTIONS)
        self._stub    = rpc.OrchestratorStub(self._channel)
        log.info("Canal gRPC (async) abierto hacia %s", ORCHESTRATOR_ADDR)

    # ── Pull ──────────────────────────────────────────────────────────────────

    async def pull_tasks(self, slots_free: int,
                         state: NodeState, queue_size: int) -> pb.PullResponse:
        return await self._stub.PullTasks(
            pb.PullRequest(
                node_id    = NODE_ID,
                slots_free = slots_free,
                metrics    = _make_metrics(state, queue_size),
            ),
            timeout=5,
        )

    # ── Submit ────────────────────────────────────────────────────────────────

    async def submit_result(self, result: pb.TaskResult) -> pb.Ack:
        return await self._stub.SubmitResult(result, timeout=10)

    # ── Steal (autorización) ──────────────────────────────────────────────────

    async def steal_tasks(self, thief: str, victim: str, count: int) -> pb.StealResponse:
        return await self._stub.StealTasks(
            pb.StealRequest(
                thief_node_id  = thief,
                victim_node_id = victim,
                steal_count    = count,
            ),
            timeout=3,
        )

    # ── Progreso (best-effort) ────────────────────────────────────────────────

    async def update_progress(self, task_id: str, pct: int, msg: str) -> None:
        try:
            await self._stub.UpdateTaskProgress(
                pb.TaskProgress(
                    task_id             = task_id,
                    worker_id           = NODE_ID,
                    progress_percentage = pct,
                    status_message      = msg,
                ),
                timeout=2,
            )
        except Exception:
            pass

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    async def send_heartbeat(self, state: NodeState, queue_size: int) -> pb.Ack:
        return await self._stub.SendHeartbeat(
            pb.HeartbeatRequest(
                node_id    = NODE_ID,
                ip_address = NODE_IP,
                metrics    = _make_metrics(state, queue_size),
            ),
            timeout=3,
        )

    # ── Watch stream ──────────────────────────────────────────────────────────

    def watch_queue(self):
        """Devuelve un async-iterator de pb.QueueStatusResponse."""
        return self._stub.WatchQueue(pb.QueueStatusRequest(requester_id=NODE_ID))

    # ── Ciclo de vida ─────────────────────────────────────────────────────────

    async def close(self) -> None:
        await self._channel.close()
