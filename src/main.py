"""
main.py  —  Punto de entrada. Un proceso = un worker.
Docker Compose escala a N réplicas para tener N workers.
"""
from __future__ import annotations

import asyncio

from config.settings import NODE_ID, NODE_IP, ORCHESTRATOR_ADDR, get_logger
from domain.state import NodeState
from infraestructure.grpc.client import OrchestratorClient
from infraestructure.grpc.server import serve_grpc_local
from app.loops import start_all_loops

log = get_logger(NODE_ID)


async def run() -> None:
    log.info("═" * 60)
    log.info("  A2WS Worker Node  ·  %s", NODE_ID)
    log.info("  IP          : %s", NODE_IP)
    log.info("  Orchestrator: %s", ORCHESTRATOR_ADDR)
    log.info("  Modelo      : 1 proceso · asyncio · ProcessPoolExecutor")
    log.info("═" * 60)

    task_queue = asyncio.Queue(maxsize=1)
    state      = NodeState()
    client     = OrchestratorClient()
    server     = await serve_grpc_local(task_queue, state)

    try:
        await start_all_loops(client, task_queue, state)
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("Shutdown señalado. Deteniendo %s...", NODE_ID)
    finally:
        await server.stop(grace=8)
        await client.close()
        log.info("%s detenido.", NODE_ID)


if __name__ == "__main__":
    asyncio.run(run())