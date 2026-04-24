"""
app/loops/steal_loop.py  —  Work-stealing A2WS (async).

Política:
  1. Identifica el nodo más cargado (excluye el propio)
  2. Pide autorización al orquestador (StealTasks)
  3. Contacta al WorkerNode víctima via grpc.aio (YieldTasks)
  4. Encola las tareas robadas en la cola local async
"""
from __future__ import annotations

import asyncio

import grpc
import grpc.aio

from infraestructure.proto import a2ws_pb2      as pb
from infraestructure.proto import a2ws_pb2_grpc as rpc

from config.settings import NODE_ID, QUEUE_CAPACITY, get_logger
from domain.state import NodeState
from infraestructure.grpc.client import OrchestratorClient

log = get_logger(NODE_ID)

_VICTIM_OPTIONS = [
    ("grpc.max_receive_message_length", 50 * 1024 * 1024),
]


async def try_steal(client: OrchestratorClient,
                    task_queue: asyncio.Queue,
                    state: NodeState,
                    node_status: dict) -> None:
    """Intenta robar tareas del nodo más cargado. Silencioso si falla."""

    # 1. Elegir víctima (el nodo más cargado, excluyéndonos)
    candidates = [
        n for n in node_status.values()
        if n.get("node_id") != NODE_ID and n.get("queue_size", 0) > 1
    ]
    if not candidates:
        return

    victim      = max(candidates, key=lambda n: n["queue_size"])
    steal_count = max(1, QUEUE_CAPACITY - task_queue.qsize())

    state.set_status("STEALING")
    try:
        # 2. Autorización del orquestador
        auth = await client.steal_tasks(NODE_ID, victim["node_id"], steal_count)
        if not auth.allowed:
            log.debug("[STEAL] no autorizado: %s", auth.reason)
            return

        # 3. Contactar al WorkerNode víctima directamente (async)
        victim_addr    = victim.get("ip_address", victim["node_id"]) + ":50052"
        victim_channel = grpc.aio.insecure_channel(victim_addr, options=_VICTIM_OPTIONS)
        victim_stub    = rpc.WorkerNodeStub(victim_channel)

        yield_resp: pb.StealResponse = await victim_stub.YieldTasks(
            pb.StealRequest(
                thief_node_id  = NODE_ID,
                victim_node_id = victim["node_id"],
                steal_count    = steal_count,
            ),
            timeout=3,
        )
        await victim_channel.close()

        # 4. Encolar las tareas robadas
        if yield_resp.allowed and yield_resp.stolen_tasks:
            stolen = 0
            for task in yield_resp.stolen_tasks:
                try:
                    task_queue.put_nowait(task)
                    stolen += 1
                except asyncio.QueueFull:
                    break
            state.record_steal(stolen)
            log.info("[STEAL] ⬢ robé %d tarea(s) de %s", stolen, victim["node_id"])
        else:
            log.debug("[STEAL] víctima rechazó: %s", yield_resp.reason)

    except grpc.RpcError as e:
        log.debug("[STEAL] error gRPC: %s", e.details())
    finally:
        state.set_status("IDLE")
