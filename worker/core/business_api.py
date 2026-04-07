from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from io import BytesIO
from typing import Any, Iterable
from uuid import uuid4

from PIL import Image, UnidentifiedImageError

from worker.core.filter_parser import infer_output_format, parse_filters
from worker.models.types import ExecutionResultRecord, InputImageRef, Task, TaskState


class BusinessRequestError(Exception):
    def __init__(self, message: str, *, code: str = "business_request_error") -> None:
        super().__init__(message)
        self.code = code


@dataclass(slots=True)
class BusinessRequest:
    image_data: bytes
    file_name: str
    filters: list[str]
    metadata: dict[str, str] = field(default_factory=lambda: {})


class ImageNodeBusinessService:
    def __init__(self, node: Any, metrics: Any) -> None:
        self._node = node
        self._metrics = metrics
        self._config = node._config

    async def process_to_result(self, request: BusinessRequest, timeout_seconds: float = 120.0) -> ExecutionResultRecord:
        task = self._build_task(request)
        reply = await self._node.submit_task(task)
        if not reply["accepted"]:
            raise BusinessRequestError(reply["reason"] or "worker rejected request")
        result = await self._node.wait_for_result(reply["task_id"], timeout_seconds=timeout_seconds)
        return result

    async def process_to_path(self, request: BusinessRequest) -> str:
        result = await self.process_to_result(request)
        if result.state is not TaskState.SUCCEEDED or not result.output_path:
            raise BusinessRequestError(result.error_message or "processing did not produce an output path")
        return result.output_path

    async def process_to_data(self, request: BusinessRequest) -> tuple[bytes, ExecutionResultRecord]:
        result = await self.process_to_result(request)
        if result.state is not TaskState.SUCCEEDED:
            raise BusinessRequestError(result.error_message or "processing failed")
        return self._node.read_output_bytes(result), result

    async def process_batch(self, requests: list[BusinessRequest]) -> list[tuple[bytes | None, ExecutionResultRecord | None, str | None]]:
        if len(requests) > self._config.max_batch_size:
            raise BusinessRequestError(
                f"batch too large: received {len(requests)} items, limit is {self._config.max_batch_size}"
            )
        tasks = [self._process_request_safe(item) for item in requests]
        return await asyncio.gather(*tasks)

    async def upload_large_image(self, chunks: Iterable[Any]) -> tuple[bytes, ExecutionResultRecord]:
        payload = bytearray()
        file_name = ""
        filters: list[str] = []
        for chunk in chunks:
            self.validate_payload_size(len(payload) + len(chunk.chunk_data))
            payload.extend(chunk.chunk_data)
            if chunk.file_name:
                file_name = chunk.file_name
            if chunk.filters:
                filters = list(chunk.filters)
        if not payload:
            raise BusinessRequestError("upload stream did not contain image bytes")
        request = BusinessRequest(image_data=bytes(payload), file_name=file_name or f"upload-{uuid4().hex}.png", filters=filters)
        return await self.process_to_data(request)

    def get_processed_file_paths(self) -> list[str]:
        return [
            item.output_path
            for item in self._node.list_completed_results()
            if item.state is TaskState.SUCCEEDED and item.output_path
        ]

    def find_path_by_name(self, file_name: str) -> str | None:
        result = self._node.find_result_by_source_name(file_name)
        if result is None or result.state is not TaskState.SUCCEEDED:
            return None
        return result.output_path

    def get_processed_images(self) -> list[bytes]:
        images: list[bytes] = []
        for result in self._node.list_completed_results():
            if result.state is not TaskState.SUCCEEDED:
                continue
            try:
                images.append(self._node.read_output_bytes(result))
            except FileNotFoundError:
                continue
        return images

    def find_image_by_name(self, file_name: str) -> bytes | None:
        result = self._node.find_result_by_source_name(file_name)
        if result is None or result.state is not TaskState.SUCCEEDED:
            return None
        return self._node.read_output_bytes(result)

    async def get_metrics(self) -> dict[str, float]:
        state = await self._node.get_node_state()
        snapshot = self._metrics.snapshot()
        snapshot.update(
            {
                "available_memory_bytes": float(state.available_memory_bytes),
                "reserved_memory_bytes": float(state.reserved_memory_bytes),
                "queue_length_live": float(state.queue_length),
                "active_tasks_live": float(state.active_tasks),
                "accepting_tasks": 1.0 if state.accepting_tasks else 0.0,
            }
        )
        return snapshot

    async def _process_request_safe(self, request: BusinessRequest) -> tuple[bytes | None, ExecutionResultRecord | None, str | None]:
        try:
            data, result = await self.process_to_data(request)
            return data, result, None
        except Exception as exc:
            return None, None, str(exc)

    def validate_payload_size(self, size_bytes: int) -> None:
        if size_bytes > self._config.max_request_bytes:
            raise BusinessRequestError(
                f"payload too large: received {size_bytes} bytes, limit is {self._config.max_request_bytes}"
            )

    def _build_task(self, request: BusinessRequest) -> Task:
        width, height, detected_format = self._validate_request(request)
        transforms, explicit_output_format = parse_filters(request.filters)
        inferred_format = detected_format or infer_output_format(request.file_name)
        output_format = explicit_output_format or inferred_format
        task_id = str(uuid4())
        image_id = f"image-{task_id}"
        coordinator_request_key = request.metadata.get("x-coordinator-request-key")
        idempotency_key = coordinator_request_key or _request_fingerprint(request)
        return Task(
            task_id=task_id,
            idempotency_key=f"imagenode:{idempotency_key}",
            priority=5,
            created_at=datetime.now(tz=UTC),
            deadline=None,
            max_retries=1,
            transforms=transforms,
            input_image=InputImageRef(
                image_id=image_id,
                payload=request.image_data,
                image_format=inferred_format,
                size_bytes=len(request.image_data),
                width=width,
                height=height,
            ),
            output_format=output_format,
            metadata={
                "source_file_name": request.file_name,
                "business_api": "imagenode",
                **request.metadata,
            },
        )

    def _validate_request(self, request: BusinessRequest) -> tuple[int, int, str]:
        if not request.image_data:
            raise BusinessRequestError("request does not contain image bytes")
        self.validate_payload_size(len(request.image_data))
        if len(request.filters) > self._config.max_filters_per_request:
            raise BusinessRequestError(
                f"too many filters: received {len(request.filters)}, limit is {self._config.max_filters_per_request}"
            )
        try:
            with Image.open(BytesIO(request.image_data)) as image:
                width, height = image.size
                detected_format = (image.format or "").lower()
        except Image.DecompressionBombError as exc:
            raise BusinessRequestError("image exceeds Pillow decompression safety limits") from exc
        except (UnidentifiedImageError, OSError, ValueError) as exc:
            raise BusinessRequestError("invalid or unsupported image payload") from exc

        if width <= 0 or height <= 0:
            raise BusinessRequestError("image dimensions must be positive")
        if width > self._config.max_image_width:
            raise BusinessRequestError(
                f"image width {width} exceeds limit {self._config.max_image_width}"
            )
        if height > self._config.max_image_height:
            raise BusinessRequestError(
                f"image height {height} exceeds limit {self._config.max_image_height}"
            )
        pixels = width * height
        if pixels > self._config.max_image_pixels:
            raise BusinessRequestError(
                f"image pixel count {pixels} exceeds limit {self._config.max_image_pixels}"
            )
        return width, height, detected_format


def _request_fingerprint(request: BusinessRequest) -> str:
    digest = hashlib.sha256()
    digest.update(request.file_name.encode("utf-8"))
    digest.update(b"\0")
    for item in request.filters:
        digest.update(item.encode("utf-8"))
        digest.update(b"\0")
    digest.update(hashlib.sha256(request.image_data).digest())
    return digest.hexdigest()
