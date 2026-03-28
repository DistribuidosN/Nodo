from __future__ import annotations

import json
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


@dataclass(slots=True)
class CoordinatorHealthSnapshot:
    node_id: str
    cluster_id: str
    leader: bool
    live: bool
    ready: bool
    ready_workers: int
    active_ownerships: int
    queue_length: int
    message: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(tz=UTC))


HealthProvider = Callable[[], CoordinatorHealthSnapshot]


class CoordinatorHealthServer:
    def __init__(self, host: str, port: int, provider: HealthProvider) -> None:
        self._host = host
        self._port = port
        self._provider = provider
        self._server = ThreadingHTTPServer((host, port), self._make_handler())
        self._thread: threading.Thread | None = None
        self._stopped = False

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

                body = {
                    "node_id": health.node_id,
                    "cluster_id": health.cluster_id,
                    "leader": health.leader,
                    "live": health.live,
                    "ready": health.ready,
                    "ready_workers": health.ready_workers,
                    "active_ownerships": health.active_ownerships,
                    "queue_length": health.queue_length,
                    "message": health.message,
                    "timestamp": health.timestamp.isoformat(),
                }
                payload = json.dumps(body, ensure_ascii=True).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format: str, *args) -> None:  # noqa: A003
                return

        return Handler

    def start(self) -> None:
        self._thread = threading.Thread(target=self._server.serve_forever, name="coordinator-health-server", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
