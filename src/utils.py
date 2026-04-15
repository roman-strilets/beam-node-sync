"""Utility helpers shared across the Beam sync package."""

from __future__ import annotations

from datetime import datetime, timezone

from .protocol import Address
from .protocol_models import EcPoint


def format_address(address: Address) -> str:
    """Format a ``(host, port)`` tuple as ``host:port``."""
    return f"{address[0]}:{address[1]}"


def parse_endpoint(value: str, default_port: int) -> Address:
    """Parse ``host`` or ``host:port`` into an address tuple."""
    host = value
    port = default_port
    if ":" in value:
        host, port_text = value.rsplit(":", 1)
        if not host:
            raise ValueError(f"invalid address: {value!r}")
        try:
            port = int(port_text)
        except ValueError as exc:
            raise ValueError(f"invalid port in address: {value!r}") from exc

    if not 0 < port < 65536:
        raise ValueError(f"port out of range in address: {value!r}")

    return host, port


def parse_fork_hashes(values: list[str]) -> list[bytes]:
    """Decode and validate a list of hex-encoded fork hashes."""
    fork_hashes: list[bytes] = []
    for value in values:
        try:
            raw = bytes.fromhex(value.strip())
        except ValueError as exc:
            raise ValueError(f"invalid fork hash hex: {value!r}") from exc
        if len(raw) != 32:
            raise ValueError(
                f"fork hash must be 32 bytes (64 hex chars), got {len(raw)}"
            )
        fork_hashes.append(raw)
    return fork_hashes


def extension_bits(version: int) -> int:
    """Compute the Beam extension capability bitmask."""
    if version < 4:
        return (1 << version) - 1
    return ((version - 4 + 1) << 4) - 1


def utc_now_iso() -> str:
    """Return the current UTC time in ISO-8601 form ending with ``Z``."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def format_commitment(point: EcPoint) -> str:
    """Format a commitment point as a stable string key."""
    return f"{point.x}:{1 if point.y else 0}"