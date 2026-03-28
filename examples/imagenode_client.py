from __future__ import annotations

import argparse
import asyncio
from io import BytesIO
from pathlib import Path

import grpc
from PIL import Image

from proto import imagenode_pb2, imagenode_pb2_grpc


def build_payload() -> bytes:
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
    stub = imagenode_pb2_grpc.ImageNodeServiceStub(channel)

    response = await stub.ProcessToData(
        imagenode_pb2.ProcessRequest(
            image_data=build_payload(),
            file_name="business-demo.png",
            filters=["grayscale", "resize:120x90", "watermark:Demo|8|8|white"],
        )
    )
    print(
        f"target={args.target} success={response.success} "
        f"file_name={response.file_name} result_path={response.result_path}"
    )

    batch = await stub.ProcessBatch(
        imagenode_pb2.BatchRequest(
            requests=[
                imagenode_pb2.ProcessRequest(
                    image_data=build_payload(),
                    file_name="batch-1.png",
                    filters=["resize:80x60"],
                ),
                imagenode_pb2.ProcessRequest(
                    image_data=build_payload(),
                    file_name="batch-2.png",
                    filters=["rotate:15,true", "format:jpg"],
                ),
            ]
        )
    )
    print(f"batch_all_success={batch.all_success} batch_count={len(batch.responses)}")
    await channel.close()


if __name__ == "__main__":
    asyncio.run(main())
