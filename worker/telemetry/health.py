from __future__ import annotations

import json
import threading
from collections.abc import Callable
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from worker.models.types import NodeHealth


HealthProvider = Callable[[], NodeHealth]


class HealthServer:
    def __init__(self, host: str, port: int, provider: HealthProvider) -> None:
        self._host = host
        self._port = port
        self._provider = provider
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def _make_handler(self):
        provider = self._provider

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                health = provider()
                if self.path == "/livez":
                    status = HTTPStatus.OK if health.live else HTTPStatus.SERVICE_UNAVAILABLE
                elif self.path == "/readyz":
                    status = HTTPStatus.OK if health.ready else HTTPStatus.SERVICE_UNAVAILABLE
                else:
                    status = HTTPStatus.NOT_FOUND

                body: dict[str, str | int | bool] = {
                    "node_id": health.node_id,
                    "state": health.state.value,
                    "live": health.live,
                    "ready": health.ready,
                    "coordinator_connected": health.coordinator_connected,
                    "queue_length": health.queue_length,
                    "active_tasks": health.active_tasks,
                    "message": health.message,
                    "timestamp": health.timestamp.isoformat(),
                }
                payload = json.dumps(body, ensure_ascii=True).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
                return

        return Handler

    def start(self) -> None:
        self._server = ThreadingHTTPServer((self._host, self._port), self._make_handler())
        self._thread = threading.Thread(target=self._server.serve_forever, name="worker-health-server", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
