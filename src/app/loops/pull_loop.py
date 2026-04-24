"""
app/loops/pull_loop.py  —  A2WS Pull Loop (async).

Pide trabajo al orquestador cuando la cola está libre.
Si la cola central está seca, delega en try_steal.
"""
from __future__ import annotations

import asyncio

import grpc

from config.settings import NODE_ID, QUEUE_CAPACITY, PULL_INTERVAL, get_logger
from domain.state import NodeState
from infraestructure.grpc.client import OrchestratorClient
from app.loops.steal_loop import try_steal

log = get_logger(NODE_ID)


async def pull_loop(client: OrchestratorClient,
                    task_queue: asyncio.Queue,
                    state: NodeState,
                    node_status: dict) -> None:
    """Corre indefinidamente como una corutina de asyncio."""
    log.info("[PULL] Pull loop activo (intervalo=%.1fs)", PULL_INTERVAL)

    while True:
        try:
            slots_free = QUEUE_CAPACITY - task_queue.qsize()

            if slots_free > 0:
                resp = await client.pull_tasks(slots_free, state, task_queue.qsize())

                if resp.tasks:
                    enqueued = 0
                    for task in resp.tasks:
                        try:
                            task_queue.put_nowait(task)
                            enqueued += 1
                        except asyncio.QueueFull:
                            log.warning("[PULL] cola llena, descartando task=%s", task.task_id)
                            break
                    if enqueued:
                        log.info("[PULL] recibida tarea → cola=%d/%d",
                                 task_queue.qsize(), QUEUE_CAPACITY)

                elif resp.queue_dry:
                    await try_steal(client, task_queue, state, node_status)

        except grpc.RpcError as e:
            log.warning("[PULL] error gRPC (%s): %s — reintentando...",
                        e.code().name, e.details())

        await asyncio.sleep(PULL_INTERVAL)
