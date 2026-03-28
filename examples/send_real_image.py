from __future__ import annotations

import argparse
import asyncio
from io import BytesIO
from pathlib import Path

import grpc
from PIL import Image

from proto import imagenode_pb2, imagenode_pb2_grpc


def _build_channel(args: argparse.Namespace) -> grpc.aio.Channel:
    if args.ca:
        credentials = grpc.ssl_channel_credentials(
            root_certificates=Path(args.ca).read_bytes(),
            private_key=Path(args.key).read_bytes() if args.key else None,
            certificate_chain=Path(args.cert).read_bytes() if args.cert else None,
        )
        options = [("grpc.ssl_target_name_override", args.server_name)] if args.server_name else None
        return grpc.aio.secure_channel(args.target, credentials, options=options)
    return grpc.aio.insecure_channel(args.target)


def _inspect_image(payload: bytes) -> tuple[int | None, int | None, str]:
    try:
        with Image.open(BytesIO(payload)) as image:
            return image.width, image.height, (image.format or "unknown").lower()
    except Exception:
        return None, None, "unknown"


def _guess_extension(payload: bytes, fallback: str) -> str:
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
        "ico": ".ico",
    }
    return mapping.get(detected, fallback)


def _default_output_path(source_path: Path, output_bytes: bytes, result_path: str) -> Path:
    fallback_suffix = Path(result_path).suffix if result_path else source_path.suffix or ".bin"
    suffix = _guess_extension(output_bytes, fallback_suffix or ".bin")
    candidate = source_path.with_name(f"{source_path.stem}-processed{suffix}")
    index = 1
    while candidate.exists():
        candidate = source_path.with_name(f"{source_path.stem}-processed-{index}{suffix}")
        index += 1
    return candidate


async def main() -> None:
    parser = argparse.ArgumentParser(description="Envia una imagen real al contrato ImageNodeService.")
    parser.add_argument("--file", required=True, help="Ruta absoluta o relativa de la imagen a enviar.")
    parser.add_argument("--target", default="127.0.0.1:50051", help="Endpoint gRPC del worker.")
    parser.add_argument("--filter", action="append", default=[], dest="filters", help="Filtro a aplicar. Repite la bandera para varios filtros.")
    parser.add_argument("--mode", choices=("data", "path"), default="data", help="data devuelve bytes y los guarda localmente; path devuelve solo la ruta/URI remota.")
    parser.add_argument("--save-to", help="Ruta local donde guardar el resultado si mode=data.")
    parser.add_argument("--ca")
    parser.add_argument("--cert")
    parser.add_argument("--key")
    parser.add_argument("--server-name")
    args = parser.parse_args()

    source_path = Path(args.file).expanduser().resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"No existe la imagen: {source_path}")

    payload = source_path.read_bytes()
    original_width, original_height, original_format = _inspect_image(payload)
    channel = _build_channel(args)
    stub = imagenode_pb2_grpc.ImageNodeServiceStub(channel)
    request = imagenode_pb2.ProcessRequest(
        image_data=payload,
        file_name=source_path.name,
        filters=list(args.filters),
    )

    if args.mode == "path":
        response = await stub.ProcessToPath(request)
        print(
            f"target={args.target} success={response.success} "
            f"file_name={source_path.name} original_format={original_format} "
            f"original_size={original_width}x{original_height} "
            f"result_path={response.result_path} message={response.message}"
        )
        await channel.close()
        return

    response = await stub.ProcessToData(request)
    save_path: Path | None = None
    final_width: int | None = None
    final_height: int | None = None
    final_format = "unknown"
    if response.success and response.image_data:
        final_width, final_height, final_format = _inspect_image(response.image_data)
        save_path = Path(args.save_to).expanduser().resolve() if args.save_to else _default_output_path(source_path, response.image_data, response.result_path)
        save_path.write_bytes(response.image_data)

    print(
        f"target={args.target} success={response.success} file_name={response.file_name or source_path.name} "
        f"original_format={original_format} original_size={original_width}x{original_height} "
        f"final_format={final_format} final_size={final_width}x{final_height} "
        f"result_path={response.result_path} saved_to={save_path if save_path else ''} message={response.message}"
    )
    await channel.close()


if __name__ == "__main__":
    asyncio.run(main())
