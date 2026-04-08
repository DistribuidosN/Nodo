from __future__ import annotations

import ipaddress
import secrets
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


ROOT = Path(__file__).resolve().parents[1]
SECRETS_DIR = ROOT / ".secrets"
PKI_DIR = SECRETS_DIR / "pki"


def _write_private_key(path: Path, key: rsa.RSAPrivateKey) -> None:
    path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )


def _write_cert(path: Path, cert: x509.Certificate) -> None:
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))


def _subject(common_name: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])


def _new_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _sign_ca(common_name: str) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key = _new_key()
    now = datetime.now(tz=UTC)
    subject = _subject(common_name)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return key, cert


def _sign_leaf(
    *,
    common_name: str,
    issuer_key: rsa.RSAPrivateKey,
    issuer_cert: x509.Certificate,
    dns_names: list[str] | None = None,
    ip_addresses: list[str] | None = None,
    client_auth: bool = False,
    server_auth: bool = False,
) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key = _new_key()
    now = datetime.now(tz=UTC)
    builder = (
        x509.CertificateBuilder()
        .subject_name(_subject(common_name))
        .issuer_name(issuer_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=825))
    )
    san_entries: list[x509.GeneralName] = []
    for item in dns_names or []:
        san_entries.append(x509.DNSName(item))
    for item in ip_addresses or []:
        san_entries.append(x509.IPAddress(ipaddress.ip_address(item)))
    if san_entries:
        builder = builder.add_extension(x509.SubjectAlternativeName(san_entries), critical=False)
    usages: list[x509.ObjectIdentifier] = []
    if server_auth:
        usages.append(ExtendedKeyUsageOID.SERVER_AUTH)
    if client_auth:
        usages.append(ExtendedKeyUsageOID.CLIENT_AUTH)
    if usages:
        builder = builder.add_extension(x509.ExtendedKeyUsage(usages), critical=False)
    cert = builder.sign(issuer_key, hashes.SHA256())
    return key, cert


def _write_secret_text(path: Path, value: str) -> None:
    if not path.exists():
        path.write_text(value, encoding="utf-8")


def main() -> None:
    PKI_DIR.mkdir(parents=True, exist_ok=True)

    ca_key_path = PKI_DIR / "root-ca.key"
    ca_cert_path = PKI_DIR / "root-ca.crt"
    if ca_key_path.exists() and ca_cert_path.exists():
        ca_key = serialization.load_pem_private_key(ca_key_path.read_bytes(), password=None)
        ca_cert = x509.load_pem_x509_certificate(ca_cert_path.read_bytes())
    else:
        ca_key, ca_cert = _sign_ca("distributed-image-worker-dev-ca")
        _write_private_key(ca_key_path, ca_key)
        _write_cert(ca_cert_path, ca_cert)

    profiles = [
        (
            "app-server-server",
            {
                "dns_names": ["app-server.service", "host.docker.internal", "localhost"],
                "ip_addresses": ["127.0.0.1"],
                "server_auth": True,
            },
        ),
        (
            "worker-server",
            {"dns_names": ["worker.service", "worker1", "worker2", "worker3", "localhost"], "ip_addresses": ["127.0.0.1"], "server_auth": True},
        ),
        ("app-server-client", {"client_auth": True}),
        ("worker-client", {"client_auth": True}),
        ("demo-client", {"client_auth": True}),
    ]

    for name, options in profiles:
        key_path = PKI_DIR / f"{name}.key"
        cert_path = PKI_DIR / f"{name}.crt"
        if key_path.exists() and cert_path.exists():
            continue
        key, cert = _sign_leaf(common_name=name, issuer_key=ca_key, issuer_cert=ca_cert, **options)
        _write_private_key(key_path, key)
        _write_cert(cert_path, cert)

    _write_secret_text(SECRETS_DIR / "minio_access_key.txt", f"minio-{secrets.token_hex(8)}")
    _write_secret_text(SECRETS_DIR / "minio_secret_key.txt", secrets.token_urlsafe(32))


if __name__ == "__main__":
    main()
