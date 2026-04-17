from __future__ import annotations

from worker.config import WorkerConfig


def test_worker_config_returns_none_for_backend_commands_when_not_set(monkeypatch) -> None:
    monkeypatch.delenv("WORKER_OCR_COMMAND", raising=False)
    monkeypatch.delenv("WORKER_INFERENCE_COMMAND", raising=False)

    config = WorkerConfig.from_env()

    assert config.ocr_command is None
    assert config.inference_command is None
