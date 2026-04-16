from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

# Configuración de logs para el Worker.
# Utiliza un formato JSON para que sea fácil de procesar por herramientas
# de agregación de logs (como ELK o Loki) en sistemas distribuidos.

class JsonFormatter(logging.Formatter):
    """
    Formatea los registros de log en formato JSON, incluyendo campos
    específicos del procesamiento de imágenes.
    """
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, str | int | None] = {
            "timestamp": datetime.now(tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Campos extra que solemos inyectar en los logs del worker
        for field_name in ("task_id", "image_id", "node_id", "attempt", "state", "latency_ms"):
            value = getattr(record, field_name, None)
            if value is not None:
                payload[field_name] = value
        
        # Manejo de excepciones
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
            
        # Inyección de datos extra pasados vía el argumento 'extra' de logging
        extra = getattr(record, "extra_data", None)
        if isinstance(extra, dict):
            payload.update(extra)  # type: ignore[arg-type]
            
        return json.dumps(payload, ensure_ascii=True)


def configure_logging(level: str = "INFO") -> None:
    """Configura el logger raíz para usar salida estándar y formato JSON."""
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
