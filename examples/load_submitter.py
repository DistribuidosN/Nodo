from __future__ import annotations

import argparse
import asyncio
from io import BytesIO
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import grpc
from PIL import Image
from google.protobuf.timestamp_pb2 import Timestamp

from proto import worker_node_pb2, worker_node_pb2_grpc


def build_payload(width: int, height: int) -> bytes:
    image = Image.new("RGB", (width, height), color=(40, 140, 220))
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


async def submit_one(stub, payload: bytes, width: int, height: int, priority: int) -> bool:
    created_at = Timestamp()
    created_at.GetCurrentTime()
    task_id = str(uuid4())
    request = worker_node_pb2.SubmitTaskRequest(
        task=worker_node_pb2.Task(
            task_id=task_id,
            idempotency_key=task_id,
            priority=priority,
            created_at=created_at,
            max_retries=1,
            output_format=worker_node_pb2.IMAGE_FORMAT_PNG,
            input=worker_node_pb2.InputImage(
                image_id=f"image-{task_id}",
                content=payload,
                format=worker_node_pb2.IMAGE_FORMAT_PNG,
                size_bytes=len(payload),
                width=width,
                height=height,
            ),
            transforms=[
                worker_node_pb2.Transformation(type=worker_node_pb2.OPERATION_GRAYSCALE),
                worker_node_pb2.Transformation(
                    type=worker_node_pb2.OPERATION_RESIZE,
                    params={"width": str(width // 2), "height": str(height // 2)},
                ),
            ],
        )
    )
    try:
        response = await stub.SubmitTask(request)
    except grpc.RpcError:
        return False
    return response.accepted


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="127.0.0.1:50051")
    parser.add_argument("--ca")
    parser.add_argument("--cert")
    parser.add_argument("--key")
    parser.add_argument("--server-name")
    parser.add_argument("--count", type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--width", type=int, default=640)
    parser.add_argument("--height", type=int, default=480)
    args = parser.parse_args()

    payload = build_payload(args.width, args.height)
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
    stub = worker_node_pb2_grpc.WorkerControlServiceStub(channel)
    semaphore = asyncio.Semaphore(args.concurrency)
    accepted = 0
    failed = 0

    async def runner(index: int) -> None:
        nonlocal accepted, failed
        async with semaphore:
            if await submit_one(stub, payload, args.width, args.height, priority=10 - (index % 10)):
                accepted += 1
            else:
                failed += 1

    started = perf_counter()
    await asyncio.gather(*(runner(index) for index in range(args.count)))
    elapsed = perf_counter() - started
    print(
        f"target={args.target} submitted={args.count} accepted={accepted} failed={failed} "
        f"elapsed_s={elapsed:.3f} rps={args.count / max(elapsed, 0.001):.2f}"
    )
    await channel.close()


if __name__ == "__main__":
    asyncio.run(main())
