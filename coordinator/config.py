from __future__ import annotations

import os
import socket
from dataclasses import dataclass


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value is not None else default


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value is not None else default


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_workers(value: str) -> dict[str, str]:
    workers: dict[str, str] = {}
    for item in value.split(","):
        raw = item.strip()
        if not raw:
            continue
        node_id, _, target = raw.partition("=")
        if not node_id or not target:
            raise ValueError(f"invalid worker mapping '{raw}', expected node_id=host:port")
        workers[node_id.strip()] = target.strip()
    return workers


@dataclass(slots=True)
class CoordinatorConfig:
    node_id: str
    bind_host: str
    bind_port: int
    workers: dict[str, str]
    dispatch_concurrency: int
    dispatch_wait_seconds: float
    status_poll_seconds: float
    rpc_timeout_seconds: float
    log_level: str
    grpc_server_cert_file: str | None = None
    grpc_server_key_file: str | None = None
    grpc_server_client_ca_file: str | None = None
    grpc_server_require_client_auth: bool = False
    worker_ca_file: str | None = None
    worker_client_cert_file: str | None = None
    worker_client_key_file: str | None = None
    worker_server_name_override: str | None = None

    @classmethod
    def from_env(cls) -> "CoordinatorConfig":
        default_workers = "worker-1=127.0.0.1:50051,worker-2=127.0.0.1:50061,worker-3=127.0.0.1:50071"
        return cls(
            node_id=os.getenv("COORDINATOR_NODE_ID", socket.gethostname()),
            bind_host=os.getenv("COORDINATOR_BIND_HOST", "127.0.0.1"),
            bind_port=_get_int("COORDINATOR_BIND_PORT", 50052),
            workers=_parse_workers(os.getenv("COORDINATOR_WORKERS", default_workers)),
            dispatch_concurrency=_get_int("COORDINATOR_DISPATCH_CONCURRENCY", 4),
            dispatch_wait_seconds=_get_float("COORDINATOR_DISPATCH_WAIT_SECONDS", 0.2),
            status_poll_seconds=_get_float("COORDINATOR_STATUS_POLL_SECONDS", 1.0),
            rpc_timeout_seconds=_get_float("COORDINATOR_RPC_TIMEOUT_SECONDS", 120.0),
            log_level=os.getenv("COORDINATOR_LOG_LEVEL", "INFO"),
            grpc_server_cert_file=os.getenv("COORDINATOR_GRPC_SERVER_CERT_FILE"),
            grpc_server_key_file=os.getenv("COORDINATOR_GRPC_SERVER_KEY_FILE"),
            grpc_server_client_ca_file=os.getenv("COORDINATOR_GRPC_SERVER_CLIENT_CA_FILE"),
            grpc_server_require_client_auth=_get_bool("COORDINATOR_GRPC_SERVER_REQUIRE_CLIENT_AUTH", False),
            worker_ca_file=os.getenv("COORDINATOR_WORKER_CA_FILE"),
            worker_client_cert_file=os.getenv("COORDINATOR_WORKER_CLIENT_CERT_FILE"),
            worker_client_key_file=os.getenv("COORDINATOR_WORKER_CLIENT_KEY_FILE"),
            worker_server_name_override=os.getenv("COORDINATOR_WORKER_SERVER_NAME_OVERRIDE"),
        )

    @property
    def bind_target(self) -> str:
        return f"{self.bind_host}:{self.bind_port}"
