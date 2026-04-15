"""Binary encoding and decoding helpers for the Beam wire protocol."""

from __future__ import annotations

import struct

from .protocol import MessageType, PROTO_MAGIC


def encode_uint(value: int) -> bytes:
    """Encode a non-negative integer using Beam's compact varuint format."""
    if value < 0:
        raise ValueError(f"value must be >= 0, got {value}")
    if value < 128:
        return bytes([value | 0x80])

    size = 1
    while value >= (1 << (size * 8)):
        size += 1
    return bytes([size]) + value.to_bytes(size, "little")


def decode_uint(buf: bytes | bytearray, offset: int = 0) -> tuple[int, int]:
    """Decode a Beam compact varuint from ``buf`` at ``offset``."""
    size = buf[offset]
    if (size >> 7) & 1:
        return size & 0x7F, 1

    count = size & 0x7F
    return int.from_bytes(buf[offset + 1 : offset + 1 + count], "little"), 1 + count


def encode_height_range(min_height: int, max_height: int) -> bytes:
    """Encode a Beam ``HeightRange`` payload."""
    if min_height < 0:
        raise ValueError(f"minimum height must be >= 0, got {min_height}")
    if max_height < min_height:
        raise ValueError(
            f"maximum height {max_height} must be >= minimum height {min_height}"
        )
    return encode_uint(min_height) + encode_uint(max_height - min_height)


def encode_system_state_id(height: int, block_hash: bytes) -> bytes:
    """Encode a Beam ``Block::SystemState::ID`` payload."""
    if height < 0:
        raise ValueError(f"block height must be >= 0, got {height}")
    if len(block_hash) != 32:
        raise ValueError(f"block hash must be 32 bytes, got {len(block_hash)}")
    return encode_uint(height) + block_hash


def encode_byte_buffer(value: bytes) -> bytes:
    """Encode a Beam ``ByteBuffer`` payload."""
    return encode_uint(len(value)) + value


def encode_get_body_pack_payload(
    *,
    top_height: int,
    top_hash: bytes,
    flag_perishable: int,
    flag_eternal: int,
    count_extra: int,
    block0: int,
    horizon_lo1: int,
    horizon_hi1: int,
) -> bytes:
    """Encode a Beam ``GetBodyPack`` request payload."""
    return b"".join(
        (
            encode_system_state_id(top_height, top_hash),
            bytes((flag_perishable, flag_eternal)),
            encode_uint(count_extra),
            encode_uint(block0),
            encode_uint(horizon_lo1),
            encode_uint(horizon_hi1),
        )
    )


def encode_body_payload(*, perishable: bytes, eternal: bytes) -> bytes:
    """Encode a Beam ``Body`` payload from raw body buffers."""
    return encode_byte_buffer(perishable) + encode_byte_buffer(eternal)


def make_header(message_type: MessageType, size: int) -> bytes:
    """Build an 8-byte Beam frame header."""
    header = bytearray(8)
    header[0:3] = PROTO_MAGIC
    header[3] = int(message_type)
    struct.pack_into("<I", header, 4, size)
    return bytes(header)


def parse_header(header: bytes) -> tuple[MessageType, int]:
    """Parse an 8-byte Beam frame header."""
    if header[0:3] != PROTO_MAGIC:
        raise ValueError(f"bad protocol magic: {header[0:3].hex()}")
    return MessageType(header[3]), struct.unpack_from("<I", header, 4)[0]