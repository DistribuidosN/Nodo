from __future__ import annotations
import os
from pathlib import Path
from uuid import uuid4
from worker.application.ports.storage import StoragePort

class LocalStorageAdapter(StoragePort):
    """
    Implementación concreta del almacenamiento en el sistema de archivos local.
    """

    def read_bytes(self, uri: str) -> bytes:
        path = self._to_path(uri)
        return path.read_bytes()

    def write_bytes(self, uri: str, payload: bytes) -> None:
        path = self._to_path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Escritura atómica usando un archivo temporal
        tmp_path = path.with_suffix(path.suffix + f".{uuid4().hex}.tmp")
        tmp_path.write_bytes(payload)
        tmp_path.replace(path)

    def read_text(self, uri: str) -> str:
        return self._to_path(uri).read_text(encoding="utf-8")

    def write_text(self, uri: str, content: str) -> None:
        path = self._to_path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def exists(self, uri: str) -> bool:
        return self._to_path(uri).exists()

    def delete(self, uri: str) -> None:
        self._to_path(uri).unlink(missing_ok=True)

    def mkdir(self, uri: str) -> None:
        self._to_path(uri).mkdir(parents=True, exist_ok=True)

    def list_uris(self, uri: str, suffix: str | None = None) -> list[str]:
        path = self._to_path(uri)
        if not path.is_dir():
            return []
        
        results = []
        pattern = f"*{suffix}" if suffix else "*"
        for item in path.glob(pattern):
            if item.is_file():
                results.append(str(item.absolute()))
        return results

    def join(self, *parts: str) -> str:
        # Si la primera parte es una URI, la manejamos con cuidado
        if parts and parts[0].startswith("file://"):
            import urllib.parse
            base = parts[0]
            # Unimos el resto como subrutas
            suffix = "/".join(p.strip("/") for p in parts[1:])
            return base.rstrip("/") + "/" + suffix
        
        return str(Path(*parts))

    def _to_path(self, uri: str) -> Path:
        """Lógica para normalizar URIs file:// o rutas locales."""
        if uri.startswith("file://"):
            import urllib.parse
            parsed = urllib.parse.urlparse(uri)
            path = urllib.parse.unquote(parsed.path)
            # En Windows, urlparse suele dejar un / delante de la letra de unidad (ex: /D:/...)
            if os.name == "nt" and len(path) > 2 and path[0] == "/" and path[2] == ":":
                path = path[1:]
            return Path(path)
        return Path(uri)
