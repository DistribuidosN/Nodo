from __future__ import annotations

from proto import worker_node_pb2, worker_node_pb2_grpc
from worker.grpc.mappers import node_status_to_proto, task_from_proto


class WorkerControlServicer(worker_node_pb2_grpc.WorkerControlServiceServicer):
    def __init__(self, node) -> None:
        self._node = node

    async def SubmitTask(self, request, context):
        reply = await self._node.submit_task(task_from_proto(request.task))
        return worker_node_pb2.SubmitTaskReply(**reply)

    async def GetNodeStatus(self, request, context):
        state = await self._node.get_node_state()
        return node_status_to_proto(state)

    async def CancelTask(self, request, context):
        response = await self._node.cancel_task(request.task_id, request.reason)
        return worker_node_pb2.CancelTaskReply(**response)

    async def DrainNode(self, request, context):
        response = await self._node.drain(reject_new_tasks=request.reject_new_tasks)
        return worker_node_pb2.DrainNodeReply(**response)

    async def ShutdownNode(self, request, context):
        response = await self._node.shutdown(request.grace_period_seconds)
        return worker_node_pb2.ShutdownNodeReply(**response)
