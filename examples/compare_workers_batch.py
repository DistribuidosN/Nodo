from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Awaitable, Sequence
from io import BytesIO
from pathlib import Path
from typing import Any, Optional, Protocol, TypeVar, TypedDict, cast

import grpc
from PIL import Image

from proto import imagenode_pb2, imagenode_pb2_grpc


# The protobuf module is generated dynamically, and static analyzers like Pyright
# don't always see message attributes such as EmptyRequest or BatchRequest.
IMAGENODE_PB2: Any = imagenode_pb2


DEFAULT_TARGETS = [
    "127.0.0.1:50051",
    "127.0.0.1:50061",
    "127.0.0.1:50071",
]
DEFAULT_IMAGE_LIST = Path(__file__).resolve().parents[1] / "docs" / "demo" / "batch-image-list.txt"
DEFAULT_HEALTH_RETRIES = 15
DEFAULT_HEALTH_DELAY_SECONDS = 1.0


class HealthStatus(TypedDict):
    is_alive: bool
    is_ready: bool
    node_id: str
    message: str


class SavedFileSummary(TypedDict):
    source: str
    saved_to: str
    success: bool


class ResultSummary(TypedDict):
    source_file: str
    response_file: str
    success: bool
    result_path: str
    saved_to: str
    bytes: int
    width: Optional[int]
    height: Optional[int]
    format: str
    message: str


class WorkerReport(TypedDict):
    target: str
    health: HealthStatus
    filters: list[str]
    all_success: bool
    request_count: int
    response_count: int
    results: list[ResultSummary]
    saved_files: list[SavedFileSummary]


class FailureSummary(TypedDict):
    target: str
    error: str


class ComparisonReport(TypedDict):
    targets: list[str]
    filters: list[str]
    request_count: int
    source_images: list[str]
    reports: list[WorkerReport]
    failures: list[FailureSummary]


class HealthCheckResponseLike(Protocol):
    is_alive: bool
    is_ready: bool
    node_id: str
    message: str


class DataResponseLike(Protocol):
    image_data: bytes
    success: bool
    file_name: str
    result_path: str
    message: str


class BatchDataResponseLike(Protocol):
    responses: Sequence[DataResponseLike]
    all_success: bool


ResponseT = TypeVar("ResponseT", covariant=True)


class UnaryUnaryCallLike(Protocol[ResponseT]):
    def __call__(self, request: Any, *args: Any, **kwargs: Any) -> Awaitable[ResponseT]: ...


class ImageNodeServiceStubLike(Protocol):
    HealthCheck: UnaryUnaryCallLike[HealthCheckResponseLike]
    ProcessBatch: UnaryUnaryCallLike[BatchDataResponseLike]


class CliArgs(argparse.Namespace):
    targets: Optional[list[str]]
    files: Optional[list[str]]
    filters: list[str]
    output_root: str
    ca: Optional[str]
    cert: Optional[str]
    key: Optional[str]
    server_name: Optional[str]


def _build_channel(args: CliArgs, target: str) -> grpc.aio.Channel:
    if args.ca:
        credentials = grpc.ssl_channel_credentials(
            root_certificates=Path(args.ca).read_bytes(),
            private_key=Path(args.key).read_bytes() if args.key else None,
            certificate_chain=Path(args.cert).read_bytes() if args.cert else None,
        )
        options = [("grpc.ssl_target_name_override", args.server_name)] if args.server_name else None
        return grpc.aio.secure_channel(target, credentials, options=options)
    return grpc.aio.insecure_channel(target)


def _inspect_image(payload: bytes) -> tuple[Optional[int], Optional[int], str]:
    try:
        with Image.open(BytesIO(payload)) as image:
            return image.width, image.height, (image.format or "unknown").lower()
    except Exception:
        return None, None, "unknown"


def _result_extension(result_path: str, payload: bytes, source_suffix: str) -> str:
    suffix = Path(result_path).suffix
    if suffix:
        return suffix
    _, _, detected = _inspect_image(payload)
    mapping = {
        "jpeg": ".jpg",
        "jpg": ".jpg",
        "png": ".png",
        "tiff": ".tif",
        "tif": ".tif",
        "webp": ".webp",
        "bmp": ".bmp",
        "gif": ".gif",
    }
    return mapping.get(detected, source_suffix or ".bin")


def _load_default_images() -> list[Path]:
    image_paths: list[Path] = []
    for raw_line in DEFAULT_IMAGE_LIST.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        image_paths.append(Path(line).expanduser().resolve())
    return image_paths


async def _ensure_health(stub: ImageNodeServiceStubLike) -> HealthStatus:
    last_error: Optional[Exception] = None
    for attempt in range(DEFAULT_HEALTH_RETRIES):
        try:
            health_response = await stub.HealthCheck(IMAGENODE_PB2.EmptyRequest())
            status: HealthStatus = {
                "is_alive": health_response.is_alive,
                "is_ready": health_response.is_ready,
                "node_id": health_response.node_id,
                "message": health_response.message,
            }
            if status["is_alive"] and status["is_ready"]:
                return status
        except grpc.aio.AioRpcError as exc:
            last_error = exc
        if attempt < DEFAULT_HEALTH_RETRIES - 1:
            await asyncio.sleep(DEFAULT_HEALTH_DELAY_SECONDS)

    if last_error is not None:
        raise RuntimeError(f"healthcheck failed after retries: {last_error}") from last_error
    raise RuntimeError("healthcheck failed after retries")


async def _process_target(
    target: str,
    image_paths: list[Path],
    filters: list[str],
    output_root: Path,
    args: CliArgs,
) -> WorkerReport:
    channel = _build_channel(args, target)
    stub = cast(ImageNodeServiceStubLike, imagenode_pb2_grpc.ImageNodeServiceStub(channel))
    health = await _ensure_health(stub)

    requests: list[Any] = [
            IMAGENODE_PB2.ProcessRequest(
                image_data=image_path.read_bytes(),
                file_name=image_path.name,
                filters=filters,
            )
        for image_path in image_paths
    ]

    response = await stub.ProcessBatch(IMAGENODE_PB2.BatchRequest(requests=requests))
    response_items: list[DataResponseLike] = list(response.responses)

    worker_dir = output_root / health["node_id"]
    worker_dir.mkdir(parents=True, exist_ok=True)
    saved_files: list[SavedFileSummary] = []
    summary_rows: list[ResultSummary] = []
    for source_path, item in zip(image_paths, response_items):
        width, height, image_format = _inspect_image(bytes(item.image_data))
        extension = _result_extension(item.result_path, bytes(item.image_data), source_path.suffix)
        output_path = worker_dir / f"{source_path.stem}-processed{extension}"
        if item.success and item.image_data:
            output_path.write_bytes(item.image_data)
        saved_files.append(
            {
                "source": str(source_path),
                "saved_to": str(output_path),
                "success": item.success,
            }
        )
        summary_rows.append(
            {
                "source_file": source_path.name,
                "response_file": item.file_name,
                "success": item.success,
                "result_path": item.result_path,
                "saved_to": str(output_path),
                "bytes": len(item.image_data),
                "width": width,
                "height": height,
                "format": image_format,
                "message": item.message,
            }
        )

    report: WorkerReport = {
        "target": target,
        "health": health,
        "filters": filters,
        "all_success": response.all_success,
        "request_count": len(image_paths),
        "response_count": len(response.responses),
        "results": summary_rows,
        "saved_files": saved_files,
    }
    (worker_dir / "summary.json").write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    await channel.close()
    return report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compara el mismo lote de imagenes contra varios workers.")
    parser.add_argument("--target", action="append", dest="targets", help="Endpoint gRPC del worker. Repite la bandera para varios.")
    parser.add_argument("--file", action="append", dest="files", help="Ruta de imagen. Repite la bandera para armar el lote.")
    parser.add_argument("--filter", action="append", default=[], dest="filters", help="Filtro a aplicar al batch.")
    parser.add_argument("--output-root", default="results", help="Carpeta raiz donde guardar salidas y resumenes.")
    parser.add_argument("--ca")
    parser.add_argument("--cert")
    parser.add_argument("--key")
    parser.add_argument("--server-name")
    return parser


async def main() -> None:
    parser = _build_parser()
    args: CliArgs = parser.parse_args(namespace=CliArgs())

    image_paths = [Path(path).expanduser().resolve() for path in (args.files or _load_default_images())]
    missing = [str(path) for path in image_paths if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Imagenes no encontradas: {missing}")

    targets = args.targets or DEFAULT_TARGETS
    output_root = Path(args.output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    reports: list[WorkerReport] = []
    failures: list[FailureSummary] = []
    for target in targets:
        try:
            report = await _process_target(target, image_paths, list(args.filters), output_root, args)
            reports.append(report)
            print(
                f"target={target} node_id={report['health']['node_id']} "
                f"all_success={report['all_success']} responses={report['response_count']}"
            )
        except Exception as exc:
            failures.append({"target": target, "error": str(exc)})
            print(f"target={target} error={exc}")

    comparison: ComparisonReport = {
        "targets": targets,
        "filters": list(args.filters),
        "request_count": len(image_paths),
        "source_images": [str(path) for path in image_paths],
        "reports": reports,
        "failures": failures,
    }
    (output_root / "comparison-summary.json").write_text(
        json.dumps(comparison, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    if failures:
        raise SystemExit(1)

    if not all(report["all_success"] for report in reports):
        raise SystemExit(2)

    if not all(report["response_count"] == len(image_paths) for report in reports):
        raise SystemExit(3)


if __name__ == "__main__":
    asyncio.run(main())
