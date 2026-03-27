from __future__ import annotations

from datetime import UTC, datetime

from worker.core.state_store import StateStore
from worker.models.types import ExecutionResultRecord, TaskState


def test_state_store_persists_completed_results(tmp_path):
    store = StateStore(tmp_path / "state")
    result = ExecutionResultRecord(
        task_id="task-1",
        image_id="image-1",
        node_id="node-1",
        state=TaskState.SUCCEEDED,
        attempt=1,
        output_path="/tmp/out.png",
        output_format="png",
        width=80,
        height=60,
        size_bytes=1024,
        finished_at=datetime.now(tz=UTC),
    )

    store.append_result(result, idempotency_key="idem-1")
    loaded = store.load_completed()

    assert "idem-1" in loaded
    assert loaded["idem-1"].task_id == "task-1"
    assert loaded["idem-1"].state == TaskState.SUCCEEDED
