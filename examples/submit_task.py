from __future__ import annotations

import asyncio
import argparse
from io import BytesIO
from pathlib import Path
from uuid import uuid4

import grpc
from PIL import Image
from google.protobuf.timestamp_pb2 import Timestamp

from proto import worker_node_pb2, worker_node_pb2_grpc


def build_sample_payload() -> bytes:
    image = Image.new("RGB", (240, 180), color=(25, 120, 200))
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="127.0.0.1:50051")
    parser.add_argument("--ca")
    parser.add_argument("--cert")
    parser.add_argument("--key")
    parser.add_argument("--server-name")
    args = parser.parse_args()

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

    created_at = Timestamp()
    created_at.GetCurrentTime()
    payload = build_sample_payload()
    task_id = str(uuid4())
    request = worker_node_pb2.SubmitTaskRequest(
        task=worker_node_pb2.Task(
            task_id=task_id,
            idempotency_key=task_id,
            priority=8,
            created_at=created_at,
            max_retries=2,
            output_format=worker_node_pb2.IMAGE_FORMAT_PNG,
            input=worker_node_pb2.InputImage(
                image_id="demo-image",
                content=payload,
                format=worker_node_pb2.IMAGE_FORMAT_PNG,
                size_bytes=len(payload),
                width=240,
                height=180,
            ),
            transforms=[
                worker_node_pb2.Transformation(type=worker_node_pb2.OPERATION_GRAYSCALE),
                worker_node_pb2.Transformation(
                    type=worker_node_pb2.OPERATION_RESIZE,
                    params={"width": "120", "height": "90"},
                ),
                worker_node_pb2.Transformation(
                    type=worker_node_pb2.OPERATION_ROTATE,
                    params={"angle": "15", "expand": "true"},
                ),
            ],
        )
    )
    response = await stub.SubmitTask(request)
    print(
        f"target={args.target} accepted={response.accepted} "
        f"task_id={response.task_id} queue_position={response.queue_position}"
    )
    await channel.close()


if __name__ == "__main__":
    asyncio.run(main())
