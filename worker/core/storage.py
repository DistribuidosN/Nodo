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
    def __init__(
        self,
        *,
        endpoint_url: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        region: str | None = None,
        force_path_style: bool = False,
    ) -> None:
        self._endpoint_url = endpoint_url
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key
        self._region = region
        self._force_path_style = force_path_style
        self._s3_client = None

    @classmethod
    def from_config(cls, config: "WorkerConfig") -> "StorageClient":
        return cls(
            endpoint_url=config.storage_endpoint_url,
            access_key_id=config.storage_access_key_id,
            secret_access_key=config.storage_secret_access_key,
            region=config.storage_region,
            force_path_style=config.storage_force_path_style,
        )

    def join(self, base: str, *parts: str) -> str:
        clean_parts = [part.strip("/\\") for part in parts if part]
        if _is_local_reference(base):
            path = self._local_path(base)
            joined = path.joinpath(*clean_parts)
            return joined.resolve().as_uri() if base.startswith("file://") else str(joined)

        parsed = urlparse(base)
        if parsed.scheme in {"s3", "minio"}:
            bucket = parsed.netloc
            key_parts = [parsed.path.lstrip("/"), *clean_parts]
            key = "/".join(part for part in key_parts if part)
            return f"{parsed.scheme}://{bucket}/{key}"
        if parsed.scheme in {"http", "https"}:
            url = base.rstrip("/")
            suffix = "/".join(clean_parts)
            return f"{url}/{suffix}" if suffix else url
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
        if parsed.scheme in {"s3", "minio"}:
            client = self._get_s3_client()
            bucket, key = self._bucket_key(uri)
            try:
                client.head_object(Bucket=bucket, Key=key)
                return True
            except client.exceptions.ClientError as exc:  # pragma: no cover
                code = exc.response.get("Error", {}).get("Code")
                if code in {"404", "NoSuchKey", "NotFound"}:
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
        if parsed.scheme in {"s3", "minio"}:
            client = self._get_s3_client()
            bucket, key = self._bucket_key(uri)
            response = client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()
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

        parsed = urlparse(uri)
        if parsed.scheme in {"http", "https"}:
            request = urllib.request.Request(uri, data=payload, method="PUT")
            if content_type:
                request.add_header("Content-Type", content_type)
            with urllib.request.urlopen(request):
                return
        if parsed.scheme in {"s3", "minio"}:
            client = self._get_s3_client()
            bucket, key = self._bucket_key(uri)
            kwargs = {"Bucket": bucket, "Key": key, "Body": payload}
            if content_type:
                kwargs["ContentType"] = content_type
            client.put_object(**kwargs)
            return
        raise ValueError(f"unsupported storage URI scheme in {uri}")

    def write_text(self, uri: str, payload: str, encoding: str = "utf-8") -> None:
        self.write_bytes(uri, payload.encode(encoding), content_type="application/json")

    def delete(self, uri: str) -> None:
        if _is_local_reference(uri):
            self._local_path(uri).unlink(missing_ok=True)
            return

        parsed = urlparse(uri)
        if parsed.scheme in {"s3", "minio"}:
            client = self._get_s3_client()
            bucket, key = self._bucket_key(uri)
            client.delete_object(Bucket=bucket, Key=key)
            return
        raise ValueError(f"unsupported delete target for {uri}")

    def list_uris(self, prefix_uri: str, *, suffix: str = "") -> list[str]:
        if _is_local_reference(prefix_uri):
            root = self._local_path(prefix_uri)
            if not root.exists():
                return []
            entries = [item for item in root.iterdir() if item.is_file()]
            if suffix:
                entries = [item for item in entries if item.name.endswith(suffix)]
            return sorted(str(item) for item in entries)

        parsed = urlparse(prefix_uri)
        if parsed.scheme in {"s3", "minio"}:
            client = self._get_s3_client()
            bucket, prefix = self._bucket_key(prefix_uri)
            entries: list[str] = []
            continuation_token: str | None = None
            while True:
                kwargs = {"Bucket": bucket, "Prefix": prefix.rstrip("/") + "/"}
                if continuation_token:
                    kwargs["ContinuationToken"] = continuation_token
                response = client.list_objects_v2(**kwargs)
                for item in response.get("Contents", []):
                    key = item["Key"]
                    if key.endswith("/") or (suffix and not key.endswith(suffix)):
                        continue
                    entries.append(f"{parsed.scheme}://{bucket}/{key}")
                if not response.get("IsTruncated"):
                    break
                continuation_token = response.get("NextContinuationToken")
            return sorted(entries)
        raise ValueError(f"listing is not supported for {prefix_uri}")

    def _local_path(self, uri: str) -> Path:
        if uri.startswith("file://"):
            parsed = urlparse(uri)
            path = unquote(parsed.path)
            if os.name == "nt" and path.startswith("/") and len(path) > 2 and path[2] == ":":
                path = path.lstrip("/")
            return Path(path)
        return Path(uri)

    def _bucket_key(self, uri: str) -> tuple[str, str]:
        parsed = urlparse(uri)
        return parsed.netloc, parsed.path.lstrip("/")

    def _get_s3_client(self):
        if self._s3_client is not None:
            return self._s3_client
        try:
            import boto3
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("boto3 is required for s3:// and minio:// storage backends") from exc

        session = boto3.session.Session()
        config = None
        if self._force_path_style:
            from botocore.config import Config as BotocoreConfig

            config = BotocoreConfig(s3={"addressing_style": "path"})
        self._s3_client = session.client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret_access_key,
            region_name=self._region,
            config=config,
        )
        return self._s3_client
