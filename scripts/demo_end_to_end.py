from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path

import grpc
from PIL import Image, ImageDraw

from proto import imagenode_pb2, imagenode_pb2_grpc


DEMO_INPUT_FILE_NAME = "demo-input.png"


def _build_demo_image() -> Image.Image:
    image = Image.new("RGB", (320, 220), color=(22, 83, 143))
    draw = ImageDraw.Draw(image)
    for index in range(0, 320, 16):
        color = (22 + (index // 2) % 120, 83 + index % 90, 143 + index % 60)
        draw.rectangle((index, 0, min(index + 15, 319), 219), fill=color)
    draw.rounded_rectangle((24, 24, 296, 196), radius=18, outline="white", width=3)
    draw.text((42, 58), "Distributed", fill="white")
    draw.text((42, 88), "Image Worker", fill="white")
    draw.text((42, 118), "End-to-End Demo", fill="white")
    return image


def _encode_png(image: Image.Image) -> bytes:
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def _detect_extension(payload: bytes, fallback: str = ".png") -> str:
    with Image.open(BytesIO(payload)) as image:
        detected = (image.format or "").lower()
    return f".{detected}" if detected else fallback


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="127.0.0.1:50051")
    parser.add_argument("--output-dir", default="docs/demo")
    parser.add_argument("--ca")
    parser.add_argument("--cert")
    parser.add_argument("--key")
    parser.add_argument("--server-name")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    input_image = _build_demo_image()
    input_payload = _encode_png(input_image)
    input_path = output_dir / DEMO_INPUT_FILE_NAME
    input_path.write_bytes(input_payload)

    if args.ca:
        credentials = grpc.ssl_channel_credentials(
            root_certificates=Path(args.ca).read_bytes(),
            private_key=Path(args.key).read_bytes() if args.key else None,
            certificate_chain=Path(args.cert).read_bytes() if args.cert else None,
        )
        options = [("grpc.ssl_target_name_override", args.server_name)] if args.server_name else None
        channel = grpc.aio.secure_channel(args.target, credentials, options=options)
    else:
        channel = grpc.aio.insecure_channel(args.target)
    stub = imagenode_pb2_grpc.ImageNodeServiceStub(channel)

    health = await stub.HealthCheck(imagenode_pb2.EmptyRequest())
    metrics_before = await stub.GetMetrics(imagenode_pb2.EmptyRequest())
    response = await stub.ProcessToData(
        imagenode_pb2.ProcessRequest(
            image_data=input_payload,
            file_name=DEMO_INPUT_FILE_NAME,
            filters=[
                "grayscale",
                "resize:200x140",
                "watermark:UPB Demo|12|12|white",
            ],
        )
    )
    if not response.success:
        raise RuntimeError(f"demo request failed: {response.message}")

    output_path = output_dir / "demo-output.png"
    output_path.write_bytes(response.image_data)

    batch = await stub.ProcessBatch(
        imagenode_pb2.BatchRequest(
            requests=[
                imagenode_pb2.ProcessRequest(
                    image_data=input_payload,
                    file_name="demo-batch-1.png",
                    filters=["resize:160x110", "format:webp"],
                ),
                imagenode_pb2.ProcessRequest(
                    image_data=input_payload,
                    file_name="demo-batch-2.png",
                    filters=["rotate:12,true", "format:jpg"],
                ),
            ]
        )
    )
    metrics_after = await stub.GetMetrics(imagenode_pb2.EmptyRequest())

    batch_outputs: list[str] = []
    for index, item in enumerate(batch.responses, start=1):
        if not item.success:
            continue
        extension = _detect_extension(bytes(item.image_data))
        batch_path = output_dir / f"demo-batch-{index}{extension}"
        batch_path.write_bytes(item.image_data)
        batch_outputs.append(batch_path.name)

    summary = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "target": args.target,
        "health": {
            "is_alive": health.is_alive,
            "is_ready": health.is_ready,
            "node_id": health.node_id,
            "message": health.message,
        },
        "request": {
            "file_name": DEMO_INPUT_FILE_NAME,
            "filters": ["grayscale", "resize:200x140", "watermark:UPB Demo|12|12|white"],
        },
        "result": {
            "success": response.success,
            "file_name": response.file_name,
            "result_path": response.result_path,
            "bytes": len(response.image_data),
        },
        "batch": {
            "all_success": batch.all_success,
            "count": len(batch.responses),
            "saved_outputs": batch_outputs,
        },
        "metrics_before": dict(metrics_before.statistics),
        "metrics_after": dict(metrics_after.statistics),
    }
    (output_dir / "demo-summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    transcript = "\n".join(
        [
            f"target={args.target}",
            f"health_alive={health.is_alive} health_ready={health.is_ready} node_id={health.node_id}",
            f"single_success={response.success} result_path={response.result_path} bytes={len(response.image_data)}",
            f"batch_all_success={batch.all_success} batch_count={len(batch.responses)}",
            f"saved_input={input_path.name}",
            f"saved_output={output_path.name}",
            f"saved_batch_outputs={','.join(batch_outputs)}",
        ]
    )
    (output_dir / "demo-run.txt").write_text(transcript + "\n", encoding="utf-8")

    print(transcript)
    await channel.close()


if __name__ == "__main__":
    asyncio.run(main())
