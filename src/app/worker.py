"""
app/worker.py  —  Worker async: consume la cola, procesa imagen en subproceso.

El event loop no se bloquea: el CPU-bound process_image() corre en un
ProcessPoolExecutor de 1 worker (1 proceso hijo por contenedor).

Flujo:
  await task_queue.get()           ← espera sin bloquear
  loop.run_in_executor(pool, ...)  ← cede al OS el trabajo CPU
  await client.submit_result(...)  ← devuelve resultado async
"""
from __future__ import annotations

import asyncio
import functools
import time
from concurrent.futures import ProcessPoolExecutor

import grpc

from infraestructure.proto import a2ws_pb2 as pb

from config.settings import NODE_ID, get_logger
from domain.image_processor import process_image
from domain.state import NodeState
from infraestructure.grpc.client import OrchestratorClient

log = get_logger(NODE_ID)

# Un solo proceso hijo para CPU-bound (1 worker por contenedor)
_PROCESS_POOL = ProcessPoolExecutor(max_workers=1)


async def worker_loop(client: OrchestratorClient,
                      task_queue: asyncio.Queue,
                      state: NodeState) -> None:
    """Corutina principal del worker. Consume tareas y envía resultados."""
    log.info("Worker %s listo (asyncio + ProcessPoolExecutor)", NODE_ID)
    loop = asyncio.get_running_loop()

    while True:
        # Esperar la siguiente tarea sin bloquear
        task: pb.ImageTask = await task_queue.get()

        state.set_busy(True)
        t0 = time.monotonic()
        success, result_data, error_msg = True, b"", ""

        try:
            log.debug("iniciando task=%s filter=%s", task.task_id, task.filter_type)
            await client.update_progress(task.task_id, 10, "Iniciando procesamiento")

            # Ejecutar Pillow en proceso hijo → no bloquea el event loop
            result_data = await loop.run_in_executor(
                _PROCESS_POOL,
                functools.partial(
                    process_image,
                    task.image_data,
                    task.filter_type,
                    task.target_width,
                    task.target_height,
                ),
            )

            await client.update_progress(task.task_id, 90, "Enviando resultado")

        except Exception as exc:
            success   = False
            error_msg = str(exc)
            log.warning("ERROR task=%s: %s", task.task_id, exc)
            state.set_status("ERROR")

        finally:
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            state.record_done(elapsed_ms, error=not success)
            state.set_busy(False)
            task_queue.task_done()

        # Enviar resultado (con reintentos async)
        result = pb.TaskResult(
            task_id       = task.task_id,
            node_id       = NODE_ID,
            worker_id     = NODE_ID,
            success       = success,
            result_data   = result_data,
            error_msg     = error_msg,
            start_ts      = int(t0 * 1000),
            finish_ts     = int(time.time() * 1000),
            processing_ms = elapsed_ms,
        )

        for attempt in range(3):
            try:
                await client.submit_result(result)
                log.info("✔ task=%s en %dms", task.task_id, elapsed_ms)
                break
            except grpc.RpcError as e:
                log.error("submit_result falló (intento %d): %s", attempt + 1, e.details())
                await asyncio.sleep(0.5 * (attempt + 1))
