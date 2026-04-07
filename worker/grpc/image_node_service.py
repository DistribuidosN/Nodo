"""Canonical gRPC business service for image processing requests.

Use this module when exposing ``imagenode.ImageNodeService`` from the worker.
The older ``worker.grpc.business_servicer`` module remains as a compatibility import.
"""

from worker.grpc.business_servicer import ImageNodeBusinessServicer

__all__ = ["ImageNodeBusinessServicer"]
