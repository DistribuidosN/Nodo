from __future__ import annotations

import hashlib
import json
import time
from typing import Any
from uuid import uuid4

from worker.core.serde import (
    progress_from_dict,
    progress_to_dict,
    result_from_dict,
    result_to_dict,
    task_from_dict,
    task_to_dict,
)
from worker.core.storage import StorageClient
from worker.models.types import ExecutionResultRecord, PersistedTaskRecord, ProgressEventRecord, SpoolEventRecord, Task


def _safe_name(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class StateStore:
    def __init__(self, storage: StorageClient | str, root_uri: str | None = None) -> None:
        if isinstance(storage, StorageClient):
            self._storage = storage
            self._root_uri = root_uri or ""
        else:
            self._storage = StorageClient()
            self._root_uri = str(storage)
        self._completed_dir = self._storage.join(self._root_uri, "completed_tasks")
        self._legacy_state_file = self._storage.join(self._root_uri, "completed_tasks.jsonl")

    def ensure(self) -> None:
        self._storage.mkdir(self._completed_dir)

    def load_completed(self) -> dict[str, PersistedTaskRecord]:
        self.ensure()
        records: dict[str, PersistedTaskRecord] = {}
        for uri in self._storage.list_uris(self._completed_dir, suffix=".json"):
            payload = json.loads(self._storage.read_text(uri))
            result = result_from_dict(payload["result"])
            records[payload["idempotency_key"]] = PersistedTaskRecord(
                task_id=result.task_id,
                idempotency_key=payload["idempotency_key"],
                image_id=result.image_id,
                state=result.state,
                finished_at=result.finished_at or payload["result"]["finished_at"],
                output_path=result.output_path,
                output_format=result.output_format,
                error_code=result.error_code,
                error_message=result.error_message,
            )

        if self._storage.exists(self._legacy_state_file):
            legacy_payload = self._storage.read_text(self._legacy_state_file)
            for line in legacy_payload.splitlines():
                line = line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                result = result_from_dict(payload["result"])
                records[payload["idempotency_key"]] = PersistedTaskRecord(
                    task_id=result.task_id,
                    idempotency_key=payload["idempotency_key"],
                    image_id=result.image_id,
                    state=result.state,
                    finished_at=result.finished_at or payload["result"]["finished_at"],
                    output_path=result.output_path,
                    output_format=result.output_format,
                    error_code=result.error_code,
                    error_message=result.error_message,
                )
        return records

    def append_result(self, result: ExecutionResultRecord, idempotency_key: str) -> None:
        self.ensure()
        payload = {"idempotency_key": idempotency_key, "result": result_to_dict(result)}
        uri = self._storage.join(self._completed_dir, f"{_safe_name(idempotency_key)}.json")
        self._storage.write_text(uri, json.dumps(payload, ensure_ascii=True))


class PendingTaskStore:
    def __init__(self, storage: StorageClient | str, root_uri: str | None = None) -> None:
        if isinstance(storage, StorageClient):
            self._storage = storage
            self._pending_dir = self._storage.join(root_uri or "", "pending_tasks")
        else:
            self._storage = StorageClient()
            self._pending_dir = self._storage.join(str(storage), "pending_tasks")

    def ensure(self) -> None:
        self._storage.mkdir(self._pending_dir)

    def upsert(self, task: Task) -> None:
        self.ensure()
        uri = self._storage.join(self._pending_dir, f"{_safe_name(task.task_id)}.json")
        self._storage.write_text(uri, json.dumps(task_to_dict(task), ensure_ascii=True))

    def remove(self, task_id: str) -> None:
        uri = self._storage.join(self._pending_dir, f"{_safe_name(task_id)}.json")
        if self._storage.exists(uri):
            self._storage.delete(uri)

    def load_all(self) -> list[Task]:
        self.ensure()
        tasks: list[Task] = []
        for uri in self._storage.list_uris(self._pending_dir, suffix=".json"):
            payload = json.loads(self._storage.read_text(uri))
            tasks.append(task_from_dict(payload))
        return tasks


class EventSpoolStore:
    def __init__(self, storage: StorageClient | str, root_uri: str | None = None) -> None:
        if isinstance(storage, StorageClient):
            self._storage = storage
            self._spool_dir = self._storage.join(root_uri or "", "event_spool")
        else:
            self._storage = StorageClient()
            self._spool_dir = self._storage.join(str(storage), "event_spool")

    def ensure(self) -> None:
        self._storage.mkdir(self._spool_dir)

    def append(self, kind: str, payload: ProgressEventRecord | ExecutionResultRecord) -> str:
        self.ensure()
        event_id = f"{int(time.time() * 1000)}-{uuid4().hex}"
        event_payload: dict[str, Any]
        if kind == "progress":
            event_payload = progress_to_dict(payload)
        else:
            event_payload = result_to_dict(payload)
        uri = self._storage.join(self._spool_dir, f"{event_id}.json")
        self._storage.write_text(
            uri,
            json.dumps({"spool_id": event_id, "kind": kind, "payload": event_payload}, ensure_ascii=True),
        )
        return event_id

    def ack(self, spool_id: str) -> None:
        uri = self._storage.join(self._spool_dir, f"{spool_id}.json")
        if self._storage.exists(uri):
            self._storage.delete(uri)

    def load_pending(self) -> list[SpoolEventRecord]:
        self.ensure()
        entries: list[SpoolEventRecord] = []
        for uri in self._storage.list_uris(self._spool_dir, suffix=".json"):
            payload = json.loads(self._storage.read_text(uri))
            if payload["kind"] == "progress":
                parsed_payload = progress_from_dict(payload["payload"])
            else:
                parsed_payload = result_from_dict(payload["payload"])
            entries.append(SpoolEventRecord(spool_id=payload["spool_id"], kind=payload["kind"], payload=parsed_payload))
        return entries
