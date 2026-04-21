"""
worker_node.py  —  A2WS Image Processing Node
═══════════════════════════════════════════════════════════════════════════════
Topología fija:
  • 3 nodos Python, mismas capacidades de hardware
  • 6 workers por nodo  →  cola local de 6 slots (1 tarea por worker)
  • Puerto gRPC propio: 50052 (WorkerNode service)
  • Puerto orquestador: 50051 (Orchestrator service en Java)

Modelo A2WS:
  1. PullLoop  → pide trabajo al orquestador cuando hay slots libres
  2. Workers   → procesan de la cola local en paralelo
  3. StealLoop → si la cola central está seca, roba de otro nodo
  4. WatchLoop → stream en tiempo real del estado global

Variables de entorno:
  NODE_ID            node-1 / node-2 / node-3
  NODE_IP            IP local del nodo (para que el orquestador lo registre)
  ORCHESTRATOR_ADDR  host:puerto del orquestador Java  (default: localhost:50051)
  NODE_GRPC_PORT     puerto del WorkerNode service      (default: 50052)
  WORKER_COUNT       número de workers                  (default: 6)
  PULL_INTERVAL_S    segundos entre pull-loops           (default: 0.3)
  HB_INTERVAL_S      segundos entre heartbeats           (default: 4)
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import io
import os
import math
import queue
import socket
import statistics
import threading
import time
import uuid
import logging
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Optional

import grpc
import psutil

# Generados con:
#   python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. a2ws.proto
import a2ws_pb2       as pb
import a2ws_pb2_grpc  as rpc

# ─── Pillow opcional: si no está disponible, el procesamiento es simulado ─────
try:
    from PIL import Image, ImageFilter, ImageOps
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

# ═══════════════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN
# ═══════════════════════════════════════════════════════════════════════════

NODE_ID          = os.environ.get("NODE_ID",           "node-1")
NODE_IP          = os.environ.get("NODE_IP",           socket.gethostbyname(socket.gethostname()))
ORCHESTRATOR     = os.environ.get("ORCHESTRATOR_ADDR", "localhost:50051")
NODE_GRPC_PORT   = os.environ.get("NODE_GRPC_PORT",    "50052")
WORKER_COUNT     = int(os.environ.get("WORKER_COUNT",  "6"))
QUEUE_CAPACITY   = WORKER_COUNT          # 1 slot por worker: cola siempre = WORKER_COUNT
PULL_INTERVAL    = float(os.environ.get("PULL_INTERVAL_S", "0.3"))
HB_INTERVAL      = float(os.environ.get("HB_INTERVAL_S",  "4.0"))
LOG_LEVEL        = os.environ.get("LOG_LEVEL", "INFO")

logging.basicConfig(
    level    = getattr(logging, LOG_LEVEL, logging.INFO),
    format   = "%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
    datefmt  = "%H:%M:%S",
)
log = logging.getLogger(NODE_ID)


# ═══════════════════════════════════════════════════════════════════════════
#  ESTADO DEL NODO  (thread-safe)
# ═══════════════════════════════════════════════════════════════════════════

class NodeState:
    """
    Singleton compartido por todos los threads.
    Todos los accesos a contadores van bajo el mismo lock.
    """

    def __init__(self):
        self._lock         = threading.Lock()
        self.tasks_done    = 0
        self.tasks_error   = 0
        self.steals_done   = 0
        self.latencies_ms: list[float] = []   # últimas 200 latencias
        self.workers_busy  = 0
        self.start_time    = time.monotonic()
        self.current_status = "IDLE"          # IDLE | BUSY | STEALING | ERROR

    # ── Registro de tarea completada ─────────────────────────────────────────
    def record_done(self, latency_ms: float, error: bool = False):
        with self._lock:
            if error:
                self.tasks_error += 1
            else:
                self.tasks_done += 1
            self.latencies_ms.append(latency_ms)
            if len(self.latencies_ms) > 200:
                self.latencies_ms.pop(0)

    def record_steal(self, n: int = 1):
        with self._lock:
            self.steals_done += n

    def set_workers_busy(self, delta: int):
        with self._lock:
            self.workers_busy = max(0, self.workers_busy + delta)
            self.current_status = "BUSY" if self.workers_busy > 0 else "IDLE"

    def set_status(self, s: str):
        with self._lock:
            self.current_status = s

    # ── Snapshot de métricas para enviar al orquestador ──────────────────────
    def snapshot(self, local_q: queue.Queue) -> pb.NodeMetrics:
        mem     = psutil.virtual_memory()
        cpu_pct = psutil.cpu_percent(interval=None)
        with self._lock:
            lats = self.latencies_ms[:]
            avg  = statistics.mean(lats)        if lats else 0.0
            p95  = statistics.quantiles(lats, n=20)[18] if len(lats) >= 20 else (max(lats) if lats else 0.0)
            return pb.NodeMetrics(
                node_id          = NODE_ID,
                ip_address       = NODE_IP,
                ram_used_mb      = mem.used   / 1024**2,
                ram_total_mb     = mem.total  / 1024**2,
                cpu_percent      = cpu_pct,
                workers_busy     = self.workers_busy,
                workers_total    = WORKER_COUNT,
                queue_size       = local_q.qsize(),
                queue_capacity   = QUEUE_CAPACITY,
                tasks_done       = self.tasks_done,
                steals_performed = self.steals_done,
                avg_latency_ms   = avg,
                p95_latency_ms   = p95,
                uptime_seconds   = int(time.monotonic() - self.start_time),
                status           = self.current_status,
            )


# ═══════════════════════════════════════════════════════════════════════════
#  PROCESAMIENTO DE IMÁGENES
# ═══════════════════════════════════════════════════════════════════════════

def process_image(task: pb.ImageTask) -> bytes:
    """
    Procesa la imagen según filter_type.
    Soporta:  thumbnail · grayscale · sobel · blur · ocr (stub)

    Si PIL no está disponible o la imagen está vacía → simulación con sleep.
    """
    if not task.image_data or not PIL_AVAILABLE:
        # Modo simulación: simula tiempo proporcional a 'tamaño' enviado
        sim_ms = max(20, min(300, len(task.image_data) // 5_000))
        time.sleep(sim_ms / 1000)
        return b"simulated_result"

    img = Image.open(io.BytesIO(task.image_data)).convert("RGB")

    filter_type = task.filter_type.lower() if task.filter_type else "thumbnail"

    if filter_type == "thumbnail":
        w = task.target_width  or 256
        h = task.target_height or 256
        img.thumbnail((w, h), Image.LANCZOS)

    elif filter_type == "grayscale":
        img = ImageOps.grayscale(img)

    elif filter_type == "sobel":
        # Detección de bordes Sobel via PIL
        gray = ImageOps.grayscale(img)
        img  = gray.filter(ImageFilter.FIND_EDGES)

    elif filter_type == "blur":
        radius = 3
        img = img.filter(ImageFilter.GaussianBlur(radius=radius))

    elif filter_type == "ocr":
        # Stub OCR: devuelve imagen preprocesada lista para Tesseract
        img = ImageOps.grayscale(img)
        img = img.filter(ImageFilter.SHARPEN)

    else:
        log.warning("filter_type='%s' desconocido, aplicando thumbnail", filter_type)
        img.thumbnail((256, 256), Image.LANCZOS)

    buf = io.BytesIO()
    fmt = "JPEG" if img.mode == "RGB" else "PNG"
    img.save(buf, format=fmt, quality=85, optimize=True)
    return buf.getvalue()


def report_progress(stub: rpc.OrchestratorStub, task_id: str,
                    worker_id: str, pct: int, msg: str):
    """Envía progreso de una tarea (no bloqueante: falla silenciosa)."""
    try:
        stub.UpdateTaskProgress(
            pb.TaskProgress(
                task_id             = task_id,
                worker_id           = worker_id,
                progress_percentage = pct,
                status_message      = msg,
            ),
            timeout = 2,
        )
    except Exception:
        pass  # progreso es best-effort


# ═══════════════════════════════════════════════════════════════════════════
#  WORKER THREAD  —  consume de la cola local y envía resultados
# ═══════════════════════════════════════════════════════════════════════════

def worker_loop(worker_id: str, local_q: queue.Queue,
                state: NodeState, channel: grpc.Channel):
    stub = rpc.OrchestratorStub(channel)
    log.info("Worker %s listo", worker_id)

    while True:
        # Bloquear hasta que haya tarea disponible
        try:
            task: pb.ImageTask = local_q.get(timeout=1.0)
        except queue.Empty:
            continue

        state.set_workers_busy(+1)
        t0 = time.monotonic()
        success, result_data, error_msg = True, b"", ""

        try:
            log.debug("[%s] iniciando task=%s filter=%s",
                      worker_id, task.task_id, task.filter_type)

            report_progress(stub, task.task_id, worker_id, 10, "Iniciando procesamiento")

            result_data = process_image(task)

            report_progress(stub, task.task_id, worker_id, 90, "Guardando resultado")

        except Exception as exc:
            success   = False
            error_msg = str(exc)
            log.warning("[%s] ERROR task=%s: %s", worker_id, task.task_id, exc)
            state.set_status("ERROR")

        finally:
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            state.record_done(elapsed_ms, error=not success)
            state.set_workers_busy(-1)
            local_q.task_done()

        # Enviar resultado al orquestador
        result = pb.TaskResult(
            task_id       = task.task_id,
            node_id       = NODE_ID,
            worker_id     = worker_id,
            success       = success,
            result_data   = result_data,
            error_msg     = error_msg,
            start_ts      = int(t0 * 1000),
            finish_ts     = int(time.time() * 1000),
            processing_ms = elapsed_ms,
            metrics       = state.snapshot(local_q),
        )

        for attempt in range(3):
            try:
                stub.SubmitResult(result, timeout=10)
                log.info("[%s] ✔ task=%s en %dms (intento %d)",
                         worker_id, task.task_id, elapsed_ms, attempt + 1)
                break
            except grpc.RpcError as e:
                log.error("[%s] SubmitResult falló (intento %d): %s",
                          worker_id, attempt + 1, e.details())
                time.sleep(0.5 * (attempt + 1))


# ═══════════════════════════════════════════════════════════════════════════
#  PULL LOOP  —  solicita trabajo al orquestador (modelo A2WS)
# ═══════════════════════════════════════════════════════════════════════════

def pull_loop(stub: rpc.OrchestratorStub,
              local_q: queue.Queue,
              state: NodeState,
              node_status: dict):
    log.info("[PULL] Pull loop activo (intervalo=%.1fs)", PULL_INTERVAL)

    while True:
        try:
            slots_free = QUEUE_CAPACITY - local_q.qsize()

            if slots_free > 0:
                req  = pb.PullRequest(
                    node_id    = NODE_ID,
                    slots_free = slots_free,
                    metrics    = state.snapshot(local_q),
                )
                resp: pb.PullResponse = stub.PullTasks(req, timeout=5)

                if resp.tasks:
                    enqueued = 0
                    for task in resp.tasks:
                        try:
                            local_q.put_nowait(task)
                            enqueued += 1
                        except queue.Full:
                            log.warning("[PULL] cola local llena, descartando task=%s", task.task_id)
                            break
                    if enqueued:
                        log.info("[PULL] recibidas %d/%d tareas (cola=%d/%d)",
                                 enqueued, len(resp.tasks), local_q.qsize(), QUEUE_CAPACITY)

                elif resp.queue_dry:
                    # Cola central vacía → intentar work-stealing
                    _try_steal(stub, local_q, state, node_status)

        except grpc.RpcError as e:
            log.warning("[PULL] error gRPC (%s): %s — reintentando...",
                        e.code().name, e.details())

        time.sleep(PULL_INTERVAL)


# ═══════════════════════════════════════════════════════════════════════════
#  WORK-STEALING  —  roba tareas del nodo más cargado
# ═══════════════════════════════════════════════════════════════════════════

def _try_steal(stub: rpc.OrchestratorStub,
               local_q: queue.Queue,
               state: NodeState,
               node_status: dict):
    """
    Política A2WS:
     1. Identifica el nodo más cargado (excluye el propio)
     2. Solicita autorización al orquestador (StealTasks)
     3. Si autorizado, contacta al WorkerNode víctima (YieldTasks)
     4. Encola las tareas robadas localmente
    """
    candidates = [
        n for n in node_status.values()
        if n.get("node_id") != NODE_ID and n.get("queue_size", 0) > 1
    ]
    if not candidates:
        return

    victim = max(candidates, key=lambda n: n["queue_size"])
    steal_count = max(1, (QUEUE_CAPACITY - local_q.qsize()) // 2)

    state.set_status("STEALING")

    try:
        # Paso 1: pedir autorización al orquestador
        auth_resp: pb.StealResponse = stub.StealTasks(
            pb.StealRequest(
                thief_node_id  = NODE_ID,
                victim_node_id = victim["node_id"],
                steal_count    = steal_count,
            ),
            timeout = 3,
        )

        if not auth_resp.allowed:
            log.debug("[STEAL] no autorizado: %s", auth_resp.reason)
            return

        # Paso 2: contactar al WorkerNode de la víctima directamente
        victim_addr = victim.get("ip_address", victim["node_id"]) + ":50052"
        victim_channel = grpc.insecure_channel(victim_addr)
        victim_stub    = rpc.WorkerNodeStub(victim_channel)

        yield_resp: pb.StealResponse = victim_stub.YieldTasks(
            pb.StealRequest(
                thief_node_id  = NODE_ID,
                victim_node_id = victim["node_id"],
                steal_count    = steal_count,
            ),
            timeout = 3,
        )
        victim_channel.close()

        if yield_resp.allowed and yield_resp.stolen_tasks:
            stolen = 0
            for task in yield_resp.stolen_tasks:
                try:
                    local_q.put_nowait(task)
                    stolen += 1
                except queue.Full:
                    break
            state.record_steal(stolen)
            log.info("[STEAL] ⬢ robé %d tarea(s) de %s (cola=%d/%d)",
                     stolen, victim["node_id"], local_q.qsize(), QUEUE_CAPACITY)
        else:
            log.debug("[STEAL] víctima rechazó: %s", yield_resp.reason)

    except grpc.RpcError as e:
        log.debug("[STEAL] error gRPC: %s", e.details())
    finally:
        state.set_status("IDLE")


# ═══════════════════════════════════════════════════════════════════════════
#  WATCH LOOP  —  stream en tiempo real del estado global
# ═══════════════════════════════════════════════════════════════════════════

def watch_loop(stub: rpc.OrchestratorStub, node_status: dict):
    """Mantiene node_status actualizado con los datos de todos los nodos."""
    while True:
        try:
            req = pb.QueueStatusRequest(requester_id=NODE_ID)
            for update in stub.WatchQueue(req):
                for n in update.nodes:
                    node_status[n.node_id] = {
                        "node_id":      n.node_id,
                        "queue_size":   n.queue_size,
                        "queue_cap":    n.queue_cap,
                        "workers_busy": n.workers_busy,
                        "available":    n.available,
                        "status":       n.status,
                    }
        except grpc.RpcError as e:
            log.warning("[WATCH] stream interrumpido (%s) — reconectando en 3s...",
                        e.details())
            time.sleep(3)


# ═══════════════════════════════════════════════════════════════════════════
#  HEARTBEAT LOOP  —  ping periódico al orquestador
# ═══════════════════════════════════════════════════════════════════════════

def heartbeat_loop(stub: rpc.OrchestratorStub,
                   local_q: queue.Queue,
                   state: NodeState):
    while True:
        try:
            stub.SendHeartbeat(
                pb.HeartbeatRequest(
                    node_id    = NODE_ID,
                    ip_address = NODE_IP,
                    metrics    = state.snapshot(local_q),
                ),
                timeout = 3,
            )
            log.debug("[HB] ♥")
        except grpc.RpcError as e:
            log.warning("[HB] falló: %s", e.details())
        time.sleep(HB_INTERVAL)


# ═══════════════════════════════════════════════════════════════════════════
#  SERVIDOR gRPC LOCAL  —  WorkerNode service (para recibir YieldTasks)
# ═══════════════════════════════════════════════════════════════════════════

class WorkerNodeServicer(rpc.WorkerNodeServicer):

    def __init__(self, local_q: queue.Queue, state: NodeState):
        self.local_q = local_q
        self.state   = state

    def GetMetrics(self, request, context):
        """El orquestador (o dashboard) consulta métricas en tiempo real."""
        return self.state.snapshot(self.local_q)

    def YieldTasks(self, request: pb.StealRequest, context):
        """
        Un nodo ladrón nos pide que cedamos tareas.
        Cedemos hasta steal_count tareas de nuestra cola local.
        Solo cedemos si tenemos más de la mitad llena (política conservadora).
        """
        half = QUEUE_CAPACITY // 2
        if self.local_q.qsize() <= half:
            return pb.StealResponse(
                allowed = False,
                reason  = f"Cola local ({self.local_q.qsize()}) ≤ mitad ({half}), no cedemos"
            )

        stolen: list[pb.ImageTask] = []
        for _ in range(request.steal_count):
            try:
                task = self.local_q.get_nowait()
                stolen.append(task)
                self.local_q.task_done()
            except queue.Empty:
                break

        log.info("[YIELD] cedí %d tarea(s) a %s",
                 len(stolen), request.thief_node_id)

        return pb.StealResponse(
            allowed      = True,
            stolen_tasks = stolen,
            reason       = f"yielded {len(stolen)} tasks",
        )


def serve_grpc_local(local_q: queue.Queue, state: NodeState) -> grpc.Server:
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4),
        options=[
            ("grpc.max_receive_message_length", 50 * 1024 * 1024),
        ]
    )
    rpc.add_WorkerNodeServicer_to_server(
        WorkerNodeServicer(local_q, state), server
    )
    addr = f"[::]:{NODE_GRPC_PORT}"
    server.add_insecure_port(addr)
    server.start()
    log.info("WorkerNode gRPC escuchando en %s", addr)
    return server


# ═══════════════════════════════════════════════════════════════════════════
#  PUNTO DE ENTRADA
# ═══════════════════════════════════════════════════════════════════════════

def main():
    log.info("═" * 60)
    log.info("  A2WS Worker Node  ·  %s", NODE_ID)
    log.info("  IP          : %s", NODE_IP)
    log.info("  Orchestrator: %s", ORCHESTRATOR)
    log.info("  Workers     : %d  |  Queue: %d slots", WORKER_COUNT, QUEUE_CAPACITY)
    log.info("  PIL         : %s", "disponible" if PIL_AVAILABLE else "no disponible (modo simulación)")
    log.info("═" * 60)

    # Estado compartido y cola local
    local_q    = queue.Queue(maxsize=QUEUE_CAPACITY)
    state      = NodeState()
    node_status: dict = {}   # snapshot de otros nodos (actualizado por watch_loop)

    # Canal gRPC al orquestador (con keepalive y mensaje grande para imágenes)
    channel = grpc.insecure_channel(
        ORCHESTRATOR,
        options=[
            ("grpc.keepalive_time_ms",              10_000),
            ("grpc.keepalive_timeout_ms",            5_000),
            ("grpc.keepalive_permit_without_calls",  True),
            ("grpc.max_receive_message_length",      50 * 1024 * 1024),
            ("grpc.max_send_message_length",         50 * 1024 * 1024),
        ],
    )
    stub = rpc.OrchestratorStub(channel)

    # Servidor gRPC propio (WorkerNode service)
    node_server = serve_grpc_local(local_q, state)

    # ── Threads de workers (6 por nodo) ──────────────────────────────────────
    for i in range(WORKER_COUNT):
        threading.Thread(
            target  = worker_loop,
            args    = (f"{NODE_ID}-w{i+1}", local_q, state, channel),
            daemon  = True,
            name    = f"{NODE_ID}-worker-{i+1}",
        ).start()

    # ── Thread: WatchQueue stream ────────────────────────────────────────────
    threading.Thread(
        target = watch_loop,
        args   = (stub, node_status),
        daemon = True,
        name   = f"{NODE_ID}-watch",
    ).start()

    # ── Thread: Heartbeat periódico ──────────────────────────────────────────
    threading.Thread(
        target = heartbeat_loop,
        args   = (stub, local_q, state),
        daemon = True,
        name   = f"{NODE_ID}-heartbeat",
    ).start()

    log.info("Todos los threads activos. Iniciando pull loop...")

    # ── Loop principal: A2WS Pull (bloquea el main thread) ───────────────────
    try:
        pull_loop(stub, local_q, state, node_status)
    except KeyboardInterrupt:
        log.info("Shutdown señalado. Deteniendo %s...", NODE_ID)
    finally:
        node_server.stop(grace=8)
        channel.close()
        log.info("%s detenido.", NODE_ID)


if __name__ == "__main__":
    main()
