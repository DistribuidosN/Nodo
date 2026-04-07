"""Canonical runtime module for the worker node.

Use this module when you want the main in-memory runtime of a worker.
The older ``worker.core.node`` module remains as a compatibility import.
"""

from worker.core.node import WorkerNode

__all__ = ["WorkerNode"]
