from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

import pytest

from coordinator.config import CoordinatorConfig
from coordinator.runtime import CoordinatorRuntime, ProcessedRecord, RequestSlot
from proto import imagenode_pb2


def make_config(tmp_path) -> CoordinatorConfig:
    return CoordinatorConfig(
        node_id="coord-runtime-test",
        bind_host="127.0.0.1",
        bind_port=50052,
        state_dir=Path(tmp_path) / "coordinator-state",
        workers={},
        dispatch_concurrency=1,
        dispatch_wait_seconds=0.05,
        status_poll_seconds=0.1,
        rpc_timeout_seconds=1.0,
        ownership_lease_seconds=1.0,
        log_level="INFO",
    )


@pytest.mark.asyncio
async def test_coordinator_restores_processed_and_pending_state(tmp_path):
    config = make_config(tmp_path)
    runtime = CoordinatorRuntime(config)
    request = imagenode_pb2.ProcessRequest(
        image_data=b"payload",
        file_name="restored.png",
        filters=["grayscale"],
    )
    request_key = runtime._request_key(request)
    runtime._register_processed_record(
        ProcessedRecord(
            request_key=request_key,
            file_name="restored.png",
            result_path="file:///tmp/restored.png",
            node_id="worker-1",
            message="ok",
            updated_at=datetime.now(tz=UTC),
        ),
        persist=True,
    )

    pending_request = imagenode_pb2.ProcessRequest(
        image_data=b"payload-2",
        file_name="pending.png",
        filters=["resize:10x10"],
    )
    pending_key = runtime._request_key(pending_request)
    pending_slot = RequestSlot(
        request_key=pending_key,
        request=pending_request,
        file_name="pending.png",
        preferred_kind="path",
    )
    runtime._slots[pending_key] = pending_slot
    runtime._persist_slot(pending_slot)

    restored = CoordinatorRuntime(config)
    await restored.start()
    try:
        assert restored.get_record("restored.png") is not None
        await asyncio.sleep(0.1)
        assert pending_key in restored._slots
        assert restored._slots[pending_key].owner_node_id is None
        assert restored._metrics["restored_pending_total"] >= 1.0
    finally:
        await restored.close()
