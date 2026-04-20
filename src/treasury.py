"""Helpers for Beam treasury payloads."""

from __future__ import annotations

import hashlib

from beam_p2p import MessageType, message_name
from beam_p2p.deserializers import (
    BufferReader,
    DeserializationError,
    deserialize_input,
    deserialize_output,
    split_body_pack_payload,
)
from beam_p2p.protocol_models import BlockOutput, TxOutput


KERNEL_SUBTYPE_STD = 1
TREASURY_AMOUNT_BYTES = 16
TREASURY_ASSET_ID_BYTES = 4


def treasury_payload_sha256(payload: bytes) -> str:
    """Return a stable digest for one raw treasury payload."""
    return hashlib.sha256(payload).hexdigest()


def extract_body_buffers(message_type: MessageType, payload: bytes) -> tuple[bytes, bytes]:
    """Return the ``(perishable, eternal)`` buffers from one body message."""
    if message_type == MessageType.BODY:
        return _extract_single_body_buffers(payload)
    if message_type == MessageType.BODY_PACK:
        bodies = split_body_pack_payload(payload)
        if len(bodies) != 1:
            raise DeserializationError(
                f"expected one body in {message_name(message_type)}, got {len(bodies)}"
            )
        perishable, eternal, _ = bodies[0]
        return perishable, eternal

    raise DeserializationError(
        f"unsupported treasury body message: {message_name(message_type)}"
    )


def deserialize_treasury_payload(payload: bytes) -> list[BlockOutput]:
    """Deserialize a Beam treasury blob into seed outputs."""
    reader = BufferReader(payload)
    reader.read_byte_buffer()

    group_count = reader.read_var_uint()
    outputs: list[BlockOutput] = []
    for _ in range(group_count):
        outputs.extend(_deserialize_treasury_group_outputs(reader))

    if reader.remaining != 0:
        raise DeserializationError(
            f"{reader.remaining} trailing byte(s) left after treasury parse"
        )

    return outputs


def _extract_single_body_buffers(payload: bytes) -> tuple[bytes, bytes]:
    reader = BufferReader(payload)
    perishable = reader.read_byte_buffer()
    eternal = reader.read_byte_buffer()

    if reader.remaining != 0:
        raise DeserializationError(
            f"{reader.remaining} trailing byte(s) left after Body parse"
        )

    return perishable, eternal


def _deserialize_treasury_group_outputs(reader: BufferReader) -> list[BlockOutput]:
    outputs = _deserialize_treasury_transaction_outputs(reader)
    _skip_treasury_group_value(reader)
    return outputs


def _deserialize_treasury_transaction_outputs(reader: BufferReader) -> list[BlockOutput]:
    input_count = reader.read_big_uint(4)
    for _ in range(input_count):
        deserialize_input(reader)

    output_count = reader.read_big_uint(4)
    outputs = [
        _block_output_from_tx_output(deserialize_output(reader))
        for _ in range(output_count)
    ]

    kernel_count_raw = reader.read_big_uint(4)
    kernels_mixed = bool(kernel_count_raw & (1 << 31))
    kernel_count = kernel_count_raw & 0x7FFFFFFF
    for _ in range(kernel_count):
        _skip_kernel(reader, assume_std=not kernels_mixed)

    reader.read_scalar()
    return outputs


def _skip_treasury_group_value(reader: BufferReader) -> None:
    value = reader.read_bytes(TREASURY_AMOUNT_BYTES)
    if value == (b"\xff" * TREASURY_AMOUNT_BYTES):
        reader.read_bytes(TREASURY_AMOUNT_BYTES)
        reader.read_bytes(TREASURY_ASSET_ID_BYTES)


def _skip_kernel(reader: BufferReader, *, assume_std: bool) -> None:
    subtype_id = KERNEL_SUBTYPE_STD if assume_std else reader.read_u8()
    if subtype_id != KERNEL_SUBTYPE_STD:
        raise DeserializationError(
            f"unsupported treasury kernel subtype: {subtype_id}"
        )
    _skip_std_kernel(reader)


def _skip_std_kernel(reader: BufferReader) -> None:
    flags = reader.read_u8()
    reader.read_fixed_hex(32)
    reader.read_fixed_hex(32)
    reader.read_scalar()

    if flags & 2:
        reader.read_var_uint()
    min_height = reader.read_var_uint() if flags & 4 else 0
    if flags & 8:
        min_height + reader.read_var_uint()

    if flags & 0x20:
        reader.read_hash32()

    if flags & 0x40:
        _skip_nested_kernels(reader)

    if flags & 0x80:
        flags2 = reader.read_u8()
        if flags2 & 2:
            reader.read_hash32()
            reader.read_var_uint()


def _skip_nested_kernels(reader: BufferReader) -> None:
    count = reader.read_var_uint()
    mixed = count == 0
    if mixed:
        count = reader.read_var_uint()

    for _ in range(count):
        _skip_kernel(reader, assume_std=not mixed)


def _block_output_from_tx_output(output: TxOutput) -> BlockOutput:
    return BlockOutput(
        commitment=output.commitment,
        coinbase=output.coinbase,
        recovery_only=False,
        confidential_proof=output.confidential_proof,
        public_proof=output.public_proof,
        incubation=output.incubation,
        asset_proof=output.asset_proof,
        extra_flags=output.extra_flags,
    )