from __future__ import annotations

import grpc.aio

from proto import worker_node_pb2, worker_node_pb2_grpc
from worker.core.node import WorkerNode
from worker.grpc.mappers import node_status_to_proto, task_from_proto
from worker.telemetry.tracing import (
    copy_internal_trace_metadata_from_grpc,
    extract_context_from_grpc_metadata,
    inject_current_context,
    start_span,
)


class WorkerControlServicer(worker_node_pb2_grpc.WorkerControlServiceServicer):
    def __init__(self, node: WorkerNode) -> None:
        self._node = node

    async def SubmitTask(self, request: worker_node_pb2.SubmitTaskRequest, context: grpc.aio.ServicerContext) -> worker_node_pb2.SubmitTaskReply:  # type: ignore[attr-defined, name-defined]
        span_context = extract_context_from_grpc_metadata(context.invocation_metadata())
        task = task_from_proto(request.task)  # type: ignore[assignment]
        with start_span(
            "worker.rpc.submit_task",
            context=span_context,
            attributes={"worker.task.id": task.task_id, "worker.image.id": task.input_image.image_id},
        ):
            copy_internal_trace_metadata_from_grpc(task.metadata, context.invocation_metadata())
            inject_current_context(task.metadata)
            reply = await self._node.submit_task(task)
            return worker_node_pb2.SubmitTaskReply(**reply)  # type: ignore[attr-defined, return-value]

    async def GetNodeStatus(self, request: worker_node_pb2.GetNodeStatusRequest, context: grpc.aio.ServicerContext) -> worker_node_pb2.NodeStatus:  # type: ignore[attr-defined, name-defined]
        span_context = extract_context_from_grpc_metadata(context.invocation_metadata())
        with start_span("worker.rpc.get_node_status", context=span_context):
            state = await self._node.get_node_state()
            return node_status_to_proto(state)

    async def CancelTask(self, request: worker_node_pb2.CancelTaskRequest, context: grpc.aio.ServicerContext) -> worker_node_pb2.CancelTaskReply:  # type: ignore[attr-defined, name-defined]
        span_context = extract_context_from_grpc_metadata(context.invocation_metadata())
        with start_span("worker.rpc.cancel_task", context=span_context, attributes={"worker.task.id": request.task_id}):  # type: ignore[attr-defined]
            response = await self._node.cancel_task(request.task_id, request.reason)  # type: ignore[attr-defined]
            return worker_node_pb2.CancelTaskReply(**response)  # type: ignore[attr-defined, return-value]

    async def DrainNode(self, request: worker_node_pb2.DrainNodeRequest, context: grpc.aio.ServicerContext) -> worker_node_pb2.DrainNodeReply:  # type: ignore[attr-defined, name-defined]
        span_context = extract_context_from_grpc_metadata(context.invocation_metadata())
        with start_span("worker.rpc.drain_node", context=span_context):
            response = self._node.drain(reject_new_tasks=request.reject_new_tasks)  # type: ignore[attr-defined]
            return worker_node_pb2.DrainNodeReply(**response)  # type: ignore[attr-defined, return-value]

    async def ShutdownNode(self, request: worker_node_pb2.ShutdownNodeRequest, context: grpc.aio.ServicerContext) -> worker_node_pb2.ShutdownNodeReply:  # type: ignore[attr-defined, name-defined]
        span_context = extract_context_from_grpc_metadata(context.invocation_metadata())
        with start_span("worker.rpc.shutdown_node", context=span_context):
            response = self._node.shutdown(request.grace_period_seconds)  # type: ignore[attr-defined]
            return worker_node_pb2.ShutdownNodeReply(**response)  # type: ignore[attr-defined, return-value]
