"""
app/loops/__init__.py  —  Lanza todas las corutinas concurrentes con asyncio.gather.

No hay threads. Todo corre en el mismo event loop:
  • worker_loop   — consume cola y procesa imágenes
  • pull_loop     — solicita trabajo al orquestador (A2WS)
  • watch_loop    — stream en tiempo real del estado global
  • heartbeat_loop — ping periódico
"""
from __future__ import annotations

import asyncio

from config.settings import NODE_ID, get_logger
from domain.state import NodeState
from infraestructure.grpc.client import OrchestratorClient
from app.worker import worker_loop
from app.loops.pull_loop import pull_loop
from app.loops.watch_loop import watch_loop
from app.loops.heartbeat_loop import heartbeat_loop

log = get_logger(NODE_ID)


async def start_all_loops(client: OrchestratorClient,
                          task_queue: asyncio.Queue,
                          state: NodeState) -> None:
    """
    Ejecuta todas las corutinas en paralelo con asyncio.gather.
    Devuelve cuando alguna lanza una excepción no capturada.
    """
    node_status: dict = {}   # compartido entre pull_loop y steal_loop

    log.info("Iniciando todas las corutinas (asyncio.gather)...")

    await asyncio.gather(
        worker_loop(client, task_queue, state),
        pull_loop(client, task_queue, state, node_status),
        watch_loop(client, node_status),
        heartbeat_loop(client, task_queue, state),
    )
