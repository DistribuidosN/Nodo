"""
config/settings.py  —  Variables de entorno, constantes y logger global.

Modelo de despliegue:
  • 1 contenedor = 1 worker (proceso único, sin threads internos)
  • Docker Compose levanta N réplicas (default: 6)
  • Todo I/O es async (grpc.aio + asyncio)
  • El procesamiento de imagen va a un ProcessPoolExecutor de 1 worker
    para no bloquear el event loop
"""
from __future__ import annotations

import logging
import os
import socket

# ── Identidad del nodo ────────────────────────────────────────────────────────
NODE_ID  = os.environ.get("NODE_ID",  "node-1")
NODE_IP  = os.environ.get("NODE_IP",  socket.gethostbyname(socket.gethostname()))

# ── Red ───────────────────────────────────────────────────────────────────────
ORCHESTRATOR_ADDR = os.environ.get("ORCHESTRATOR_ADDR", "localhost:50051")
NODE_GRPC_PORT    = os.environ.get("NODE_GRPC_PORT",    "50052")

# ── Capacidad: 1 proceso = 1 worker, cola de 1 slot ──────────────────────────
QUEUE_CAPACITY = 1

# ── Tiempos ───────────────────────────────────────────────────────────────────
PULL_INTERVAL = float(os.environ.get("PULL_INTERVAL_S", "0.3"))
HB_INTERVAL   = float(os.environ.get("HB_INTERVAL_S",  "4.0"))

# ── Logger ────────────────────────────────────────────────────────────────────
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

logging.basicConfig(
    level   = getattr(logging, LOG_LEVEL, logging.INFO),
    format  = "%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
    datefmt = "%H:%M:%S",
)

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
