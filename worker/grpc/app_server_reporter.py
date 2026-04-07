"""Canonical outbound reporter for callbacks to the main application server.

Use this module when the worker needs to push progress, heartbeat or results
to the external Java application server. The older ``worker.grpc.reporter``
module remains as a compatibility import.
"""

from worker.grpc.reporter import CoordinatorReporter

__all__ = ["CoordinatorReporter"]
