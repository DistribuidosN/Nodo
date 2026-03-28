from __future__ import annotations

from pathlib import Path

import grpc


def _read_bytes(path: str | None) -> bytes | None:
    if not path:
        return None
    return Path(path).read_bytes()


def build_server_credentials(
    *,
    cert_chain_file: str | None,
    private_key_file: str | None,
    client_ca_file: str | None = None,
    require_client_auth: bool = False,
) -> grpc.ServerCredentials | None:
    cert_chain = _read_bytes(cert_chain_file)
    private_key = _read_bytes(private_key_file)
    if cert_chain is None or private_key is None:
        return None

    client_ca = _read_bytes(client_ca_file)
    return grpc.ssl_server_credentials(
        [(private_key, cert_chain)],
        root_certificates=client_ca,
        require_client_auth=require_client_auth,
    )


def build_channel_credentials(
    *,
    root_ca_file: str | None = None,
    cert_chain_file: str | None = None,
    private_key_file: str | None = None,
) -> grpc.ChannelCredentials | None:
    root_ca = _read_bytes(root_ca_file)
    cert_chain = _read_bytes(cert_chain_file)
    private_key = _read_bytes(private_key_file)
    if root_ca is None and cert_chain is None and private_key is None:
        return None
    return grpc.ssl_channel_credentials(
        root_certificates=root_ca,
        private_key=private_key,
        certificate_chain=cert_chain,
    )
