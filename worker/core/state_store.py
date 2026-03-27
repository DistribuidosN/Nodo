from __future__ import annotations

import json
import time
from pathlib import Path
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
from worker.models.types import ExecutionResultRecord, PersistedTaskRecord, ProgressEventRecord, SpoolEventRecord, Task, TaskState


def _atomic_write(path: Path, payload: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(path)


class StateStore:
    def __init__(self, state_dir: Path) -> None:
        self._state_dir = state_dir
        self._state_file = state_dir / "completed_tasks.jsonl"

    def ensure(self) -> None:
        self._state_dir.mkdir(parents=True, exist_ok=True)
        self._state_file.touch(exist_ok=True)

    def load_completed(self) -> dict[str, PersistedTaskRecord]:
        self.ensure()
        records: dict[str, PersistedTaskRecord] = {}
        with self._state_file.open("r", encoding="utf-8") as handle:
            for line in handle:
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
        with self._state_file.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


class PendingTaskStore:
    def __init__(self, state_dir: Path) -> None:
        self._pending_dir = state_dir / "pending_tasks"

    def ensure(self) -> None:
        self._pending_dir.mkdir(parents=True, exist_ok=True)

    def upsert(self, task: Task) -> None:
        self.ensure()
        path = self._pending_dir / f"{task.task_id}.json"
        _atomic_write(path, json.dumps(task_to_dict(task), ensure_ascii=True))

    def remove(self, task_id: str) -> None:
        path = self._pending_dir / f"{task_id}.json"
        if path.exists():
            path.unlink()

    def load_all(self) -> list[Task]:
        self.ensure()
        tasks: list[Task] = []
        for path in sorted(self._pending_dir.glob("*.json")):
            payload = json.loads(path.read_text(encoding="utf-8"))
            tasks.append(task_from_dict(payload))
        return tasks


class EventSpoolStore:
    def __init__(self, state_dir: Path) -> None:
        self._spool_dir = state_dir / "event_spool"

    def ensure(self) -> None:
        self._spool_dir.mkdir(parents=True, exist_ok=True)

    def append(self, kind: str, payload: ProgressEventRecord | ExecutionResultRecord) -> str:
        self.ensure()
        event_id = f"{int(time.time() * 1000)}-{uuid4().hex}"
        event_payload: dict[str, Any]
        if kind == "progress":
            event_payload = progress_to_dict(payload)
        else:
            event_payload = result_to_dict(payload)
        path = self._spool_dir / f"{event_id}.json"
        _atomic_write(path, json.dumps({"spool_id": event_id, "kind": kind, "payload": event_payload}, ensure_ascii=True))
        return event_id

    def ack(self, spool_id: str) -> None:
        path = self._spool_dir / f"{spool_id}.json"
        if path.exists():
            path.unlink()

    def load_pending(self) -> list[SpoolEventRecord]:
        self.ensure()
        entries: list[SpoolEventRecord] = []
        for path in sorted(self._spool_dir.glob("*.json")):
            payload = json.loads(path.read_text(encoding="utf-8"))
            if payload["kind"] == "progress":
                parsed_payload = progress_from_dict(payload["payload"])
            else:
                parsed_payload = result_from_dict(payload["payload"])
            entries.append(SpoolEventRecord(spool_id=payload["spool_id"], kind=payload["kind"], payload=parsed_payload))
        return entries
