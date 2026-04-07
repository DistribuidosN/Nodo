"""Canonical gRPC control service for worker management.

Use this module when exposing ``worker.WorkerControlService`` from the worker.
The older ``worker.grpc.servicer`` module remains as a compatibility import.
"""

from worker.grpc.servicer import WorkerControlServicer

__all__ = ["WorkerControlServicer"]
