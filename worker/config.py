from __future__ import annotations

import json
import os
import socket
import sys
from dataclasses import dataclass
from pathlib import Path

# Configuración centralizada del Worker.
# Este módulo se encarga de cargar variables de entorno y proveer una
# interfaz tipada para el resto de la aplicación.

def _load_dotenv_if_present() -> None:
    """Carga variables desde un archivo .env si existe."""
    env_path = Path(".env")
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        os.environ[key] = value


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value is not None else default


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value is not None else default


@dataclass(slots=True)
class WorkerConfig:
    """Configuración del Worker de Imagen."""
    node_id: str                      # Identificador único del nodo
    bind_host: str                    # Host para el servidor gRPC del worker
    bind_port: int                    # Puerto para el servidor gRPC del worker
    coordinator_target: str | None    # Dirección del Orquestador Java (ej: server:50051)
    
    # Directorios de trabajo
    input_dir: Path
    output_dir: Path
    state_dir: Path
    
    # Límites de procesamiento
    max_active_tasks: int             # Máximo de tareas paralelas
    process_pool_workers: int         # Tamaño del pool de procesos para filtros pesados
    
    # Configuración de cola local
    max_queue_size: int
    queue_high_watermark: int         # Punto donde dejamos de pedir/aceptar tareas
    queue_low_watermark: int          # Punto donde volvemos a pedir tareas
    
    # Parámetros de Heartbeat y Reportes
    heartbeat_interval_seconds: float
    report_queue_size: int            # Tamaño de la cola de resultados pendientes de envío
    cpu_target: float                 # Objetivo de uso de CPU (0.0 - 1.0)
    
    # Otros parámetros técnicos
    min_free_memory_bytes: int
    large_image_threshold_bytes: int
    retry_base_ms: int
    retry_max_ms: int
    graceful_shutdown_timeout_seconds: float
    log_level: str
    
    # Parámetros del Scheduler y Scoring
    score_cost_window_ms: float
    score_deadline_window_seconds: float
    score_aging_window_seconds: float
    scheduler_poll_seconds: float
    dedupe_ttl_seconds: float
    
    # Resiliencia de comunicación
    coordinator_failure_threshold: int
    coordinator_reconnect_base_seconds: float
    coordinator_reconnect_max_seconds: float
    
    # Comandos para backends de procesamiento (OCR, Inferencia)
    ocr_command: str | None = None
    inference_command: str | None = None

    @classmethod
    def from_env(cls) -> "WorkerConfig":
        """Instancia la configuración leyendo del entorno."""
        _load_dotenv_if_present()
        cpu_count = os.cpu_count() or 4
        max_active = _get_int("WORKER_MAX_ACTIVE_TASKS", max(2, cpu_count))
        high_watermark = _get_int("WORKER_QUEUE_HWM", max(8, int(max_active * 4)))
        
        return cls(
            node_id=os.getenv("WORKER_NODE_ID", socket.gethostname()),
            bind_host=os.getenv("WORKER_BIND_HOST", "127.0.0.1"),
            bind_port=_get_int("WORKER_BIND_PORT", 50051),
            coordinator_target=os.getenv("WORKER_COORDINATOR_TARGET") or os.getenv("APP_SERVER_GRPC_TARGET"),
            
            input_dir=Path(os.getenv("WORKER_INPUT_DIR", "data/input")),
            output_dir=Path(os.getenv("WORKER_OUTPUT_DIR", "data/output")),
            state_dir=Path(os.getenv("WORKER_STATE_DIR", "data/state")),
            
            max_active_tasks=max_active,
            process_pool_workers=_get_int("WORKER_PROCESS_POOL", max(1, cpu_count - 1)),
            
            max_queue_size=_get_int("WORKER_MAX_QUEUE_SIZE", max(high_watermark + 16, max_active * 6)),
            queue_high_watermark=high_watermark,
            queue_low_watermark=_get_int("WORKER_QUEUE_LWM", max(4, int(high_watermark * 0.66))),
            
            heartbeat_interval_seconds=_get_float("WORKER_HEARTBEAT_INTERVAL_SECONDS", 5.0),
            report_queue_size=_get_int("WORKER_REPORT_QUEUE_SIZE", 512),
            
            min_free_memory_bytes=_get_int("WORKER_MIN_FREE_MEMORY_BYTES", 256 * 1024 * 1024),
            large_image_threshold_bytes=_get_int("WORKER_LARGE_IMAGE_THRESHOLD_BYTES", 32 * 1024 * 1024),
            retry_base_ms=_get_int("WORKER_RETRY_BASE_MS", 500),
            retry_max_ms=_get_int("WORKER_RETRY_MAX_MS", 15_000),
            graceful_shutdown_timeout_seconds=_get_float("WORKER_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS", 10.0),
            log_level=os.getenv("WORKER_LOG_LEVEL", "INFO"),
            
            ocr_command=os.getenv("WORKER_OCR_COMMAND"),
            inference_command=os.getenv("WORKER_INFERENCE_COMMAND"),
            cpu_target=_get_float("WORKER_CPU_TARGET", 0.8),
            
            score_cost_window_ms=_get_float("WORKER_SCORE_COST_WINDOW_MS", 30000.0),
            score_deadline_window_seconds=_get_float("WORKER_SCORE_DEADLINE_WINDOW_SECONDS", 3600.0),
            score_aging_window_seconds=_get_float("WORKER_SCORE_AGING_WINDOW_SECONDS", 600.0),
            scheduler_poll_seconds=_get_float("WORKER_SCHEDULER_POLL_SECONDS", 0.1),
            dedupe_ttl_seconds=_get_float("WORKER_DEDUPE_TTL_SECONDS", 300.0),
            
            coordinator_failure_threshold=_get_int("WORKER_COORDINATOR_FAILURE_THRESHOLD", 3),
            coordinator_reconnect_base_seconds=_get_float("WORKER_COORDINATOR_RECONNECT_BASE_SECONDS", 1.0),
            coordinator_reconnect_max_seconds=_get_float("WORKER_COORDINATOR_RECONNECT_MAX_SECONDS", 15.0),
        )

    @property
    def bind_target(self) -> str:
        """Retorna el target gRPC para bind (host:puerto)."""
        return f"{self.bind_host}:{self.bind_port}"
