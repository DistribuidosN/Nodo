from __future__ import annotations

from worker.config import WorkerConfig


def test_worker_config_defaults_backend_commands_when_not_explicitly_set(monkeypatch) -> None:
    monkeypatch.delenv("WORKER_OCR_COMMAND", raising=False)
    monkeypatch.delenv("WORKER_INFERENCE_COMMAND", raising=False)

    config = WorkerConfig.from_env()

    assert config.ocr_command is not None
    assert config.inference_command is not None
    assert "ocr_backend.py" in config.ocr_command
    assert "inference_backend.py" in config.inference_command
