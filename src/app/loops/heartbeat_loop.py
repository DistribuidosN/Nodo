"""
app/loops/heartbeat_loop.py  —  Ping periódico async al orquestador.
"""
from __future__ import annotations

import asyncio

import grpc

from config.settings import NODE_ID, HB_INTERVAL, get_logger
from domain.state import NodeState
from infraestructure.grpc.client import OrchestratorClient

log = get_logger(NODE_ID)


async def heartbeat_loop(client: OrchestratorClient,
                         task_queue: asyncio.Queue,
                         state: NodeState) -> None:
    while True:
        try:
            await client.send_heartbeat(state, task_queue.qsize())
            log.debug("[HB] ♥")
        except grpc.RpcError as e:
            log.warning("[HB] falló: %s", e.details())
        await asyncio.sleep(HB_INTERVAL)
