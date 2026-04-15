"""
app/loops/watch_loop.py  —  Suscripción async al stream WatchQueue del orquestador.

Actualiza node_status (dict compartido) con el estado global de todos los nodos.
Lo usa el steal_loop para elegir víctima.
"""
from __future__ import annotations

import asyncio

import grpc

from config.settings import NODE_ID, get_logger
from infraestructure.grpc.client import OrchestratorClient

log = get_logger(NODE_ID)


async def watch_loop(client: OrchestratorClient, node_status: dict) -> None:
    while True:
        try:
            async for update in client.watch_queue():
                for n in update.nodes:
                    node_status[n.node_id] = {
                        "node_id":      n.node_id,
                        "ip_address":   getattr(n, "ip_address", n.node_id),
                        "queue_size":   n.queue_size,
                        "queue_cap":    n.queue_cap,
                        "workers_busy": n.workers_busy,
                        "available":    n.available,
                        "status":       n.status,
                    }
        except grpc.RpcError as e:
            log.warning("[WATCH] stream interrumpido (%s) — reconectando en 3s...", e.details())
            await asyncio.sleep(3)
