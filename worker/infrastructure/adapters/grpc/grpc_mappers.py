from __future__ import annotations
from datetime import UTC, datetime
from typing import Any, cast

from proto import orchestrator_pb2
from worker.domain.models import (
    Task, InputImageRef, ExecutionResultRecord, ProgressEventRecord, 
    NodeState, ResourceSnapshot
)
from worker.core.filter_parser import parse_filters, infer_output_format

PROTO = cast(Any, orchestrator_pb2)

def image_task_to_domain(image_task: Any) -> Task:
    """Convierte un ImageTask de gRPC al modelo Task de dominio."""
    payload = bytes(image_task.image_data)
    
    # Extraer filtros y transformaciones
    raw_filters = _extract_filters(image_task)
    transforms, explicit_format = parse_filters(raw_filters)
    
    # Inferir formato de salida
    detected_format = image_task.image_format or "png"
    output_format = explicit_format or detected_format or infer_output_format(str(image_task.filename), "png")
    
    created_at = datetime.fromtimestamp(max(int(image_task.enqueue_ts), 0) / 1000, tz=UTC)
    priority = 9 if int(image_task.priority) > 0 else 5
    
    # Mezclar metadatos de gRPC con los generados localmente
    task_metadata = {
        "source_file_name": str(image_task.filename),
        "orchestrator_filter_type": str(image_task.filter_type).lower(),
        "orchestrator_enqueue_ts": str(int(image_task.enqueue_ts)),
    }
    # Añadir metadatos que vengan directamente del mensaje gRPC
    if hasattr(image_task, "metadata"):
        task_metadata.update(image_task.metadata)
        
    return Task(
        task_id=str(image_task.task_id),
        idempotency_key=f"orchestrator:{image_task.task_id}",
        priority=priority,
        created_at=created_at,
        deadline=None,
        max_retries=1,
        transforms=transforms,
        input_image=InputImageRef(
            image_id=f"image-{image_task.task_id}",
            payload=payload,
            image_format=detected_format,
            size_bytes=len(payload),
            width=image_task.target_width,
            height=image_task.target_height,
        ),
        output_format=output_format,
        metadata=task_metadata,
    )

def result_to_proto(result: ExecutionResultRecord, node_id: str, metrics_proto: Any) -> Any:
    """Convierte un ExecutionResultRecord de dominio al TaskResult de gRPC."""
    started_ms = int(result.started_at.timestamp() * 1000) if result.started_at else 0
    finished_ms = int(result.finished_at.timestamp() * 1000) if result.finished_at else 0
    
    # Determinar el worker_id (por ejemplo, basado en PID si existe)
    worker_id = result.metadata.get("worker_id") or f"{node_id}-worker"
    
    # Filtrar metadatos internos para no enviarlos por gRPC (ej: bits de imagen)
    # Solo enviamos strings seguros como resultado de IA (OCR, Inferencia)
    proto_metadata = {
        k: str(v) for k, v in result.metadata.items() 
        if k != "payload_bytes" and v is not None
    }
    
    return PROTO.TaskResult(
        task_id=result.task_id,
        node_id=node_id,
        worker_id=worker_id,
        success=result.state.value == "succeeded",
        result_data=result.metadata.get("payload_bytes", b""), # Se asume cargado previamente
        error_msg=result.error_message or "",
        start_ts=started_ms,
        finish_ts=finished_ms,
        processing_ms=int(result.metadata.get("processing_ms", "0")),
        metrics=metrics_proto,
        metadata=proto_metadata,
    )

def _extract_filters(image_task: Any) -> list[str]:
    """Lógica de mapeo de nombres de filtros del orquestador a DSL interno."""
    raw = str(image_task.filter_type or "").strip().lower()
    if not raw: return []
    
    # Mapa de compatibilidad
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    filters = []
    for p in parts:
        if p == "thumbnail":
            filters.append(f"resize:{image_task.target_width}x{image_task.target_height}")
        else:
            filters.append(p)
            
    # Resize explícito si no estaba en el string
    if not any(f.startswith("resize") for f in filters) and image_task.target_width > 0:
        filters.append(f"resize:{image_task.target_width}x{image_task.target_height}")
        
    return filters
