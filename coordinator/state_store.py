from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from proto import imagenode_pb2
from worker.core.storage import StorageClient


def _safe_name(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _dt_to_str(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat()


def _str_to_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value).astimezone(UTC)


def _bytes_to_str(value: bytes | None) -> str | None:
    if value is None:
        return None
    return base64.b64encode(value).decode("ascii")


def _str_to_bytes(value: str | None) -> bytes | None:
    if value is None:
        return None
    return base64.b64decode(value.encode("ascii"))


def _request_to_dict(request: imagenode_pb2.ProcessRequest) -> dict[str, Any]:
    return {
        "image_data": _bytes_to_str(bytes(request.image_data)),
        "file_name": request.file_name,
        "filters": list(request.filters),
    }


def _request_from_dict(payload: dict[str, Any]) -> imagenode_pb2.ProcessRequest:
    return imagenode_pb2.ProcessRequest(
        image_data=_str_to_bytes(payload.get("image_data")) or b"",
        file_name=payload.get("file_name", ""),
        filters=list(payload.get("filters", [])),
    )


@dataclass(slots=True)
class PersistedCoordinatorJob:
    request_key: str
    request: imagenode_pb2.ProcessRequest
    file_name: str
    preferred_kind: str
    owner_node_id: str | None = None
    lease_expires_at: datetime | None = None
    dispatch_attempts: int = 0
    queued: bool = True
    submitted_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    last_error: str = ""


@dataclass(slots=True)
class PersistedCoordinatorRecord:
    request_key: str
    file_name: str
    result_path: str = ""
    image_data: bytes | None = None
    node_id: str = ""
    message: str = ""
    updated_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))


class CoordinatorPendingStore:
    def __init__(self, storage: StorageClient | str, root_uri: str | None = None) -> None:
        if isinstance(storage, StorageClient):
            self._storage = storage
            self._pending_dir = self._storage.join(root_uri or "", "pending_requests")
        else:
            self._storage = StorageClient()
            self._pending_dir = self._storage.join(str(storage), "pending_requests")

    def ensure(self) -> None:
        self._storage.mkdir(self._pending_dir)

    def upsert(self, record: PersistedCoordinatorJob) -> None:
        self.ensure()
        uri = self._storage.join(self._pending_dir, f"{_safe_name(record.request_key)}.json")
        payload = {
            "request_key": record.request_key,
            "request": _request_to_dict(record.request),
            "file_name": record.file_name,
            "preferred_kind": record.preferred_kind,
            "owner_node_id": record.owner_node_id,
            "lease_expires_at": _dt_to_str(record.lease_expires_at),
            "dispatch_attempts": record.dispatch_attempts,
            "queued": record.queued,
            "submitted_at": _dt_to_str(record.submitted_at),
            "last_error": record.last_error,
        }
        self._storage.write_text(uri, json.dumps(payload, ensure_ascii=True))

    def remove(self, request_key: str) -> None:
        uri = self._storage.join(self._pending_dir, f"{_safe_name(request_key)}.json")
        if self._storage.exists(uri):
            self._storage.delete(uri)

    def load_all(self) -> list[PersistedCoordinatorJob]:
        self.ensure()
        entries: list[PersistedCoordinatorJob] = []
        for uri in self._storage.list_uris(self._pending_dir, suffix=".json"):
            payload = json.loads(self._storage.read_text(uri))
            entries.append(
                PersistedCoordinatorJob(
                    request_key=payload["request_key"],
                    request=_request_from_dict(payload["request"]),
                    file_name=payload.get("file_name", ""),
                    preferred_kind=payload.get("preferred_kind", "path"),
                    owner_node_id=payload.get("owner_node_id"),
                    lease_expires_at=_str_to_dt(payload.get("lease_expires_at")),
                    dispatch_attempts=int(payload.get("dispatch_attempts", 0)),
                    queued=bool(payload.get("queued", True)),
                    submitted_at=_str_to_dt(payload.get("submitted_at")) or datetime.now(tz=UTC),
                    last_error=payload.get("last_error", ""),
                )
            )
        return entries


class CoordinatorProcessedStore:
    def __init__(self, storage: StorageClient | str, root_uri: str | None = None) -> None:
        if isinstance(storage, StorageClient):
            self._storage = storage
            self._processed_dir = self._storage.join(root_uri or "", "processed_requests")
        else:
            self._storage = StorageClient()
            self._processed_dir = self._storage.join(str(storage), "processed_requests")

    def ensure(self) -> None:
        self._storage.mkdir(self._processed_dir)

    def upsert(self, record: PersistedCoordinatorRecord) -> None:
        self.ensure()
        uri = self._storage.join(self._processed_dir, f"{_safe_name(record.request_key)}.json")
        payload = {
            "request_key": record.request_key,
            "file_name": record.file_name,
            "result_path": record.result_path,
            "image_data": _bytes_to_str(record.image_data),
            "node_id": record.node_id,
            "message": record.message,
            "updated_at": _dt_to_str(record.updated_at),
        }
        self._storage.write_text(uri, json.dumps(payload, ensure_ascii=True))

    def load_all(self) -> list[PersistedCoordinatorRecord]:
        self.ensure()
        entries: list[PersistedCoordinatorRecord] = []
        for uri in self._storage.list_uris(self._processed_dir, suffix=".json"):
            payload = json.loads(self._storage.read_text(uri))
            entries.append(
                PersistedCoordinatorRecord(
                    request_key=payload["request_key"],
                    file_name=payload.get("file_name", ""),
                    result_path=payload.get("result_path", ""),
                    image_data=_str_to_bytes(payload.get("image_data")),
                    node_id=payload.get("node_id", ""),
                    message=payload.get("message", ""),
                    updated_at=_str_to_dt(payload.get("updated_at")) or datetime.now(tz=UTC),
                )
            )
        return entries
