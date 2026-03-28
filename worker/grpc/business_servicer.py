from __future__ import annotations

import asyncio
import contextlib
from uuid import uuid4

from proto import imagenode_pb2, imagenode_pb2_grpc
from worker.core.business_api import BusinessRequest, BusinessRequestError, ImageNodeBusinessService
from worker.telemetry.tracing import (
    copy_internal_trace_metadata_from_grpc,
    extract_context_from_grpc_metadata,
    inject_current_context,
    start_span,
)


_STREAM_STOP = object()


class ImageNodeBusinessServicer(imagenode_pb2_grpc.ImageNodeServiceServicer):
    def __init__(self, node, metrics, *, stream_concurrency: int = 4) -> None:
        self._node = node
        self._service = ImageNodeBusinessService(node=node, metrics=metrics)
        self._stream_concurrency = max(1, stream_concurrency)

    async def ProcessToPath(self, request, context):
        span_context = extract_context_from_grpc_metadata(context.invocation_metadata())
        with start_span(
            "worker.business.process_to_path",
            context=span_context,
            attributes={"worker.business.file_name": request.file_name or "<inline>"},
        ):
            business_request = self._build_request(request, context)
            try:
                result_path = await self._service.process_to_path(business_request)
                return imagenode_pb2.PathResponse(result_path=result_path, success=True, message="ok")
            except Exception as exc:
                return imagenode_pb2.PathResponse(success=False, message=str(exc))

    async def ProcessToData(self, request, context):
        span_context = extract_context_from_grpc_metadata(context.invocation_metadata())
        with start_span(
            "worker.business.process_to_data",
            context=span_context,
            attributes={"worker.business.file_name": request.file_name or "<inline>"},
        ):
            return await self._process_request_to_data_response(request, context)

    async def HealthCheck(self, request, context):
        span_context = extract_context_from_grpc_metadata(context.invocation_metadata())
        with start_span("worker.business.health_check", context=span_context):
            health = self._node.current_health()
            return imagenode_pb2.HealthCheckResponse(
                is_alive=health.live,
                is_ready=health.ready,
                node_id=health.node_id,
                message=health.message,
            )

    async def GetMetrics(self, request, context):
        span_context = extract_context_from_grpc_metadata(context.invocation_metadata())
        with start_span("worker.business.get_metrics", context=span_context):
            return imagenode_pb2.MetricsResponse(statistics=await self._service.get_metrics())

    async def UploadLargeImage(self, request_iterator, context):
        span_context = extract_context_from_grpc_metadata(context.invocation_metadata())
        with start_span("worker.business.upload_large_image", context=span_context):
            payload = bytearray()
            file_name = ""
            filters: list[str] = []
            async for chunk in request_iterator:
                try:
                    self._service.validate_payload_size(len(payload) + len(chunk.chunk_data))
                except BusinessRequestError as exc:
                    return imagenode_pb2.DataResponse(success=False, message=str(exc))
                payload.extend(chunk.chunk_data)
                if chunk.file_name:
                    file_name = chunk.file_name
                if chunk.filters:
                    filters = list(chunk.filters)
            business_request = BusinessRequest(
                image_data=bytes(payload),
                file_name=file_name or f"upload-{uuid4().hex}.png",
                filters=filters,
                metadata=self._metadata_from_context(context),
            )
            if not business_request.image_data:
                return imagenode_pb2.DataResponse(success=False, message="upload stream did not contain image bytes")
            try:
                image_data, result = await self._service.process_to_data(business_request)
                return imagenode_pb2.DataResponse(
                    image_data=image_data,
                    success=True,
                    file_name=business_request.file_name,
                    result_path=result.output_path or "",
                    message="ok",
                )
            except Exception as exc:
                return imagenode_pb2.DataResponse(success=False, file_name=business_request.file_name, message=str(exc))

    async def StreamBatchProcess(self, request_iterator, context):
        span_context = extract_context_from_grpc_metadata(context.invocation_metadata())
        with start_span("worker.business.stream_batch_process", context=span_context):
            queue: asyncio.Queue[object] = asyncio.Queue()
            pending: set[asyncio.Task] = set()
            semaphore = asyncio.Semaphore(self._stream_concurrency)
            base_metadata = self._metadata_from_context(context)

            async def run_one(request) -> None:
                try:
                    response = await self._process_request_to_data_response(request, context, base_metadata=base_metadata)
                except Exception as exc:
                    response = imagenode_pb2.DataResponse(
                        success=False,
                        file_name=request.file_name,
                        message=str(exc),
                    )
                finally:
                    semaphore.release()
                await queue.put(response)

            async def produce() -> None:
                try:
                    async for request in request_iterator:
                        await semaphore.acquire()
                        task = asyncio.create_task(run_one(request))
                        pending.add(task)
                        task.add_done_callback(pending.discard)
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)
                finally:
                    await queue.put(_STREAM_STOP)

            producer_task = asyncio.create_task(produce())
            try:
                while True:
                    item = await queue.get()
                    if item is _STREAM_STOP:
                        break
                    yield item
            finally:
                producer_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await producer_task
                for task in list(pending):
                    task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)

    async def GetProcessedFilePaths(self, request, context):
        span_context = extract_context_from_grpc_metadata(context.invocation_metadata())
        with start_span("worker.business.get_processed_file_paths", context=span_context):
            return imagenode_pb2.PathListResponse(paths=self._service.get_processed_file_paths())

    async def FindPathByName(self, request, context):
        span_context = extract_context_from_grpc_metadata(context.invocation_metadata())
        with start_span(
            "worker.business.find_path_by_name",
            context=span_context,
            attributes={"worker.business.file_name": request.file_name},
        ):
            result_path = self._service.find_path_by_name(request.file_name)
            if result_path is None:
                return imagenode_pb2.PathResponse(success=False, message="file not found")
            return imagenode_pb2.PathResponse(result_path=result_path, success=True, message="ok")

    async def GetProcessedImages(self, request, context):
        span_context = extract_context_from_grpc_metadata(context.invocation_metadata())
        with start_span("worker.business.get_processed_images", context=span_context):
            return imagenode_pb2.DataListResponse(images=self._service.get_processed_images())

    async def FindImageByName(self, request, context):
        span_context = extract_context_from_grpc_metadata(context.invocation_metadata())
        with start_span(
            "worker.business.find_image_by_name",
            context=span_context,
            attributes={"worker.business.file_name": request.file_name},
        ):
            image_data = self._service.find_image_by_name(request.file_name)
            if image_data is None:
                return imagenode_pb2.DataResponse(success=False, file_name=request.file_name, message="file not found")
            result = self._node.find_result_by_source_name(request.file_name)
            return imagenode_pb2.DataResponse(
                image_data=image_data,
                success=True,
                file_name=request.file_name,
                result_path=result.output_path if result and result.output_path else "",
                message="ok",
            )

    async def ProcessBatch(self, request, context):
        span_context = extract_context_from_grpc_metadata(context.invocation_metadata())
        with start_span(
            "worker.business.process_batch",
            context=span_context,
            attributes={"worker.business.batch_size": len(request.requests)},
        ):
            base_metadata = self._metadata_from_context(context)
            business_requests = [self._build_request(item, context, base_metadata=base_metadata) for item in request.requests]
            try:
                results = await self._service.process_batch(business_requests)
            except BusinessRequestError as exc:
                return imagenode_pb2.BatchDataResponse(
                    responses=[imagenode_pb2.DataResponse(success=False, message=str(exc))],
                    all_success=False,
                )
            responses: list[imagenode_pb2.DataResponse] = []
            all_success = True
            for payload, result, error in results:
                if payload is None or result is None:
                    all_success = False
                    responses.append(imagenode_pb2.DataResponse(success=False, message=error or "processing failed"))
                    continue
                responses.append(
                    imagenode_pb2.DataResponse(
                        image_data=payload,
                        success=True,
                        file_name=result.metadata.get("source_file_name", ""),
                        result_path=result.output_path or "",
                        message="ok",
                    )
                )
            return imagenode_pb2.BatchDataResponse(responses=responses, all_success=all_success)

    async def _process_request_to_data_response(self, request, context, *, base_metadata: dict[str, str] | None = None):
        business_request = self._build_request(request, context, base_metadata=base_metadata)
        try:
            image_data, result = await self._service.process_to_data(business_request)
            return imagenode_pb2.DataResponse(
                image_data=image_data,
                success=True,
                file_name=business_request.file_name,
                result_path=result.output_path or "",
                message="ok",
            )
        except BusinessRequestError as exc:
            return imagenode_pb2.DataResponse(success=False, file_name=business_request.file_name, message=str(exc))
        except Exception as exc:
            return imagenode_pb2.DataResponse(success=False, file_name=business_request.file_name, message=str(exc))

    def _build_request(self, request, context, *, base_metadata: dict[str, str] | None = None) -> BusinessRequest:
        metadata = dict(base_metadata or {})
        copy_internal_trace_metadata_from_grpc(metadata, context.invocation_metadata())
        inject_current_context(metadata)
        return BusinessRequest(
            image_data=request.image_data,
            file_name=request.file_name or f"request-{uuid4().hex}.png",
            filters=list(request.filters),
            metadata=metadata,
        )

    def _metadata_from_context(self, context) -> dict[str, str]:
        metadata: dict[str, str] = {}
        copy_internal_trace_metadata_from_grpc(metadata, context.invocation_metadata())
        for item in context.invocation_metadata():
            key = getattr(item, "key", "")
            value = getattr(item, "value", "")
            if key.startswith("x-coordinator-") and value:
                metadata[key] = value
        inject_current_context(metadata)
        return metadata
