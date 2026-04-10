from __future__ import annotations

import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import unquote, urlparse
from uuid import uuid4

if TYPE_CHECKING:
    from worker.config import WorkerConfig


def _is_windows_drive_path(value: str) -> bool:
    return len(value) >= 3 and value[1] == ":" and value[2] in ("\\", "/")


def _is_local_reference(value: str) -> bool:
    return value.startswith("file://") or _is_windows_drive_path(value) or "://" not in value


class StorageClient:
    def __init__(self) -> None:
        pass

    @classmethod
    def from_config(cls, _config: "WorkerConfig") -> "StorageClient":
        return cls()

    def join(self, base: str, *parts: str) -> str:
        clean_parts = [part.strip("/\\") for part in parts if part]
        if _is_local_reference(base):
            path = self._local_path(base)
            joined = path.joinpath(*clean_parts)
            return joined.resolve().as_uri() if base.startswith("file://") else str(joined)

        raise ValueError(f"unsupported storage URI scheme in {base}")

    def mkdir(self, uri: str) -> None:
        if _is_local_reference(uri):
            self._local_path(uri).mkdir(parents=True, exist_ok=True)

    def exists(self, uri: str) -> bool:
        if _is_local_reference(uri):
            return self._local_path(uri).exists()

        parsed = urlparse(uri)
        if parsed.scheme in {"http", "https"}:
            request = urllib.request.Request(uri, method="HEAD")
            try:
                with urllib.request.urlopen(request):
                    return True
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    return False
                raise
        raise ValueError(f"unsupported storage URI scheme in {uri}")

    def read_bytes(self, uri: str) -> bytes:
        if _is_local_reference(uri):
            return self._local_path(uri).read_bytes()

        parsed = urlparse(uri)
        if parsed.scheme in {"http", "https"}:
            with urllib.request.urlopen(uri) as response:
                return response.read()
        raise ValueError(f"unsupported storage URI scheme in {uri}")

    def read_text(self, uri: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(uri).decode(encoding)

    def write_bytes(self, uri: str, payload: bytes, content_type: str | None = None) -> None:
        if _is_local_reference(uri):
            path = self._local_path(uri)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = path.with_suffix(path.suffix + f".{uuid4().hex}.tmp")
            tmp_path.write_bytes(payload)
            tmp_path.replace(path)
            return

        raise ValueError(f"unsupported storage URI scheme in {uri}")

    def write_text(self, uri: str, payload: str, encoding: str = "utf-8") -> None:
        self.write_bytes(uri, payload.encode(encoding), content_type="application/json")

    def delete(self, uri: str) -> None:
        if _is_local_reference(uri):
            self._local_path(uri).unlink(missing_ok=True)
            return

        raise ValueError(f"unsupported delete target for {uri}")

    def list_uris(self, prefix_uri: str, *, suffix: str = "") -> list[str]:
        if _is_local_reference(prefix_uri):
            return self._list_local_uris(prefix_uri, suffix)

        raise ValueError(f"listing is not supported for {prefix_uri}")

    def _list_local_uris(self, prefix_uri: str, suffix: str) -> list[str]:
        root = self._local_path(prefix_uri)
        if not root.exists():
            return []

        local_entries: list[Path] = [item for item in root.iterdir() if item.is_file()]
        if suffix:
            local_entries = [item for item in local_entries if item.name.endswith(suffix)]
        return sorted(str(item) for item in local_entries)

    def _local_path(self, uri: str) -> Path:
        if uri.startswith("file://"):
            parsed = urlparse(uri)
            path = unquote(parsed.path)
            if os.name == "nt" and path.startswith("/") and len(path) > 2 and path[2] == ":":
                path = path.lstrip("/")
            return Path(path)
        return Path(uri)
