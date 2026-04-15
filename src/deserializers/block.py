"""Beam block header and perishable-body deserializers."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Sequence

from ..protocol_models import BlockHeader, BlockOutput, DecodedBlock, TxCounts, TxOutput
from .core import BufferReader, DeserializationError
from .proofs import (
    deserialize_recovery_asset_proof,
    deserialize_recovery_confidential_range_proof,
    deserialize_recovery_public_range_proof,
)
from .tx import deserialize_input, deserialize_output


HEADER_POW_INDICES_SIZE = 104
DIFFICULTY_MANTISSA_BITS = 24
CHAINWORK_BITS = 256
DIFFICULTY_INF = (CHAINWORK_BITS - DIFFICULTY_MANTISSA_BITS) << DIFFICULTY_MANTISSA_BITS
MAINNET_FORK_HEIGHTS = (0, 321321, 777777, 1280000, 1820000, 1920000)
# Computed from Beam core Rules::UpdateChecksum() for mainnet.
MAINNET_FORK_HASHES = (
    bytes.fromhex("ed91a717313c6eb0e3f082411584d0da8f0c8af2a4ac01e5af1959e0ec4338bc"),
    bytes.fromhex("6d622e615cfd29d0f8cdd9bdd73ca0b769c8661b29d7ba9c45856c96bc2ec5bc"),
    bytes.fromhex("1ce8f721bf0c9fa7473795a97e365ad38bbc539aab821d6912d86f24e67720fc"),
    bytes.fromhex("3eaab6ab65b65f94d4f195aad47ec97e2182195b7b612e0eec0b981c653635d7"),
    bytes.fromhex("b5a8b6b3617812c0ea4efc2a37eec66cbf40bfb8a8eead0e07f9c26faf6fca5f"),
    bytes.fromhex("1a68bdc7d7756bb4640f232b6dee4988238996364655112d421503afe45b09f8"),
)
MAINNET_FORK3_HEIGHT = MAINNET_FORK_HEIGHTS[3]


def _update_hash_compact_uint(hasher: "hashlib._Hash", value: int) -> None:
    if value < 0:
        raise DeserializationError(f"hash integer must be unsigned, got {value}")
    while value >= 0x80:
        hasher.update(bytes(((value & 0x7F) | 0x80,)))
        value >>= 7
    hasher.update(bytes((value,)))


def _find_mainnet_fork_index(height: int) -> int:
    index = 0
    for candidate, fork_height in enumerate(MAINNET_FORK_HEIGHTS):
        if height >= fork_height:
            index = candidate
        else:
            break
    return index


def _get_rules_hash(height: int, peer_fork_hashes: list[bytes]) -> bytes | None:
    fork_index = _find_mainnet_fork_index(height)
    if fork_index < 2:
        return None
    if fork_index < len(MAINNET_FORK_HASHES):
        return MAINNET_FORK_HASHES[fork_index]
    if not peer_fork_hashes:
        raise DeserializationError(
            "peer Login payload did not include fork hashes needed to resolve "
            f"the rules hash for height {height}"
        )
    if len(peer_fork_hashes) > len(MAINNET_FORK_HEIGHTS):
        raise DeserializationError(
            "peer Login payload exposed more fork hashes than the supported table"
        )

    start_index = len(MAINNET_FORK_HEIGHTS) - len(peer_fork_hashes)
    peer_index = fork_index - start_index
    if 0 <= peer_index < len(peer_fork_hashes):
        return peer_fork_hashes[peer_index]

    raise DeserializationError(
        "peer Login payload only exposed fork hashes for mainnet fork indexes "
        f"{start_index}..{len(MAINNET_FORK_HEIGHTS) - 1}; cannot resolve the rules hash"
    )


def _difficulty_to_float(packed: int) -> float:
    order = packed >> 24
    mantissa = (1 << 24) | (packed & ((1 << 24) - 1))
    return math.ldexp(mantissa, order - 24)


def _unpack_difficulty_raw(packed_difficulty: int) -> int:
    if packed_difficulty >= DIFFICULTY_INF:
        return (1 << CHAINWORK_BITS) - 1

    order = packed_difficulty >> DIFFICULTY_MANTISSA_BITS
    mantissa = (1 << DIFFICULTY_MANTISSA_BITS) | (
        packed_difficulty & ((1 << DIFFICULTY_MANTISSA_BITS) - 1)
    )
    return mantissa << order


def _add_chainwork(chainwork: str, packed_difficulty: int) -> str:
    current = int.from_bytes(bytes.fromhex(chainwork), "big")
    updated = current + _unpack_difficulty_raw(packed_difficulty)
    if updated >= (1 << CHAINWORK_BITS):
        raise DeserializationError("HdrPack chainwork overflowed 256 bits")
    return updated.to_bytes(CHAINWORK_BITS // 8, "big").hex()


def _compute_block_hash(
    *,
    height: int,
    previous_hash: bytes,
    chainwork: bytes,
    kernels: bytes,
    definition: bytes,
    timestamp: int,
    packed_difficulty: int,
    rules_hash: bytes | None,
    pow_indices: bytes,
    pow_nonce: bytes,
) -> bytes:
    hasher = hashlib.sha256()
    _update_hash_compact_uint(hasher, height)
    hasher.update(previous_hash)
    hasher.update(chainwork)
    hasher.update(kernels)
    hasher.update(definition)
    _update_hash_compact_uint(hasher, timestamp)
    _update_hash_compact_uint(hasher, packed_difficulty)
    if rules_hash is not None and _find_mainnet_fork_index(height) >= 2:
        hasher.update(rules_hash)
    hasher.update(pow_indices)
    hasher.update(pow_nonce)
    return hasher.digest()


def _read_header_element(reader: BufferReader) -> tuple[str, str, int, int, bytes, bytes]:
    kernels = reader.read_hash32()
    definition = reader.read_hash32()
    timestamp = reader.read_var_uint()
    pow_indices = reader.read_bytes(HEADER_POW_INDICES_SIZE)
    packed_difficulty = reader.read_var_uint()
    pow_nonce = reader.read_bytes(8)
    return kernels, definition, timestamp, packed_difficulty, pow_indices, pow_nonce


def _build_block_header(
    *,
    height: int,
    previous_hash: str,
    chainwork: str,
    kernels: str,
    definition: str,
    timestamp: int,
    packed_difficulty: int,
    pow_indices: bytes,
    pow_nonce: bytes,
    peer_fork_hashes: list[bytes],
) -> BlockHeader:
    rules_hash = _get_rules_hash(height, peer_fork_hashes)
    block_hash = _compute_block_hash(
        height=height,
        previous_hash=bytes.fromhex(previous_hash),
        chainwork=bytes.fromhex(chainwork),
        kernels=bytes.fromhex(kernels),
        definition=bytes.fromhex(definition),
        timestamp=timestamp,
        packed_difficulty=packed_difficulty,
        rules_hash=rules_hash,
        pow_indices=pow_indices,
        pow_nonce=pow_nonce,
    )
    return BlockHeader(
        height=height,
        hash=block_hash.hex(),
        previous_hash=previous_hash,
        chainwork=chainwork,
        kernels=kernels,
        definition=definition,
        timestamp=timestamp,
        packed_difficulty=packed_difficulty,
        difficulty=_difficulty_to_float(packed_difficulty),
        rules_hash=rules_hash.hex() if rules_hash is not None else None,
        pow_indices_hex=pow_indices.hex(),
        pow_nonce_hex=pow_nonce.hex(),
    )


def deserialize_new_tip_payload(
    payload: bytes,
    peer_fork_hashes: list[bytes],
) -> BlockHeader:
    """Deserialize a ``NewTip`` payload into a block header."""
    reader = BufferReader(payload)
    height = reader.read_var_uint()
    previous_hash = reader.read_hash32()
    chainwork = reader.read_hash32()
    kernels, definition, timestamp, packed_difficulty, pow_indices, pow_nonce = (
        _read_header_element(reader)
    )

    if reader.remaining != 0:
        raise DeserializationError(
            f"{reader.remaining} trailing byte(s) left after NewTip header parse"
        )

    return _build_block_header(
        height=height,
        previous_hash=previous_hash,
        chainwork=chainwork,
        kernels=kernels,
        definition=definition,
        timestamp=timestamp,
        packed_difficulty=packed_difficulty,
        pow_indices=pow_indices,
        pow_nonce=pow_nonce,
        peer_fork_hashes=peer_fork_hashes,
    )


def deserialize_header_pack(
    payload: bytes,
    peer_fork_hashes: list[bytes],
) -> BlockHeader:
    """Deserialize an ``HdrPack`` payload containing exactly one header."""
    headers = deserialize_header_pack_payloads(payload, peer_fork_hashes)
    if len(headers) != 1:
        raise DeserializationError(f"expected exactly one header in HdrPack, got {len(headers)}")
    return headers[0]


def deserialize_header_pack_payloads(
    payload: bytes,
    peer_fork_hashes: list[bytes],
) -> list[BlockHeader]:
    """Deserialize an ``HdrPack`` payload into one or more headers."""
    reader = BufferReader(payload)
    height = reader.read_var_uint()
    previous_hash = reader.read_hash32()
    chainwork = reader.read_hash32()
    count = reader.read_var_uint()
    elements = [_read_header_element(reader) for _ in range(count)]

    if reader.remaining != 0:
        raise DeserializationError(
            f"{reader.remaining} trailing byte(s) left after HdrPack parse"
        )

    if not elements:
        return []

    headers: list[BlockHeader] = []
    current_height = height
    current_previous_hash = previous_hash
    current_chainwork = chainwork
    ascending_elements = list(reversed(elements))

    for index, (
        kernels,
        definition,
        timestamp,
        packed_difficulty,
        pow_indices,
        pow_nonce,
    ) in enumerate(ascending_elements):
        header = _build_block_header(
            height=current_height,
            previous_hash=current_previous_hash,
            chainwork=current_chainwork,
            kernels=kernels,
            definition=definition,
            timestamp=timestamp,
            packed_difficulty=packed_difficulty,
            pow_indices=pow_indices,
            pow_nonce=pow_nonce,
            peer_fork_hashes=peer_fork_hashes,
        )
        headers.append(header)

        if index + 1 == len(ascending_elements):
            break

        current_height += 1
        current_previous_hash = header.hash
        next_packed_difficulty = ascending_elements[index + 1][3]
        current_chainwork = _add_chainwork(current_chainwork, next_packed_difficulty)

    return headers


def deserialize_block_output(reader: BufferReader, height: int) -> BlockOutput:
    """Deserialize a Recovery1 block output."""
    flags = reader.read_u8()
    commitment = reader.read_point_x(bool(flags & 1))
    confidential_proof = (
        deserialize_recovery_confidential_range_proof(reader) if flags & 4 else None
    )
    public_proof = (
        deserialize_recovery_public_range_proof(reader) if flags & 8 else None
    )
    incubation = reader.read_var_uint() if flags & 0x10 else None

    asset_proof = None
    if flags & 0x20 and height >= MAINNET_FORK3_HEIGHT:
        asset_proof = deserialize_recovery_asset_proof(reader)

    return BlockOutput(
        commitment=commitment,
        coinbase=bool(flags & 2),
        recovery_only=True,
        confidential_proof=confidential_proof,
        public_proof=public_proof,
        incubation=incubation,
        asset_proof=asset_proof,
        extra_flags=reader.read_u8() if flags & 0x80 else None,
    )


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


def _read_body_buffers(reader: BufferReader) -> tuple[bytes, bytes]:
    return reader.read_byte_buffer(), reader.read_byte_buffer()


def _deserialize_full_perishable(
    perishable: bytes,
) -> tuple[list, list[BlockOutput], int, str | None]:
    reader = BufferReader(perishable)
    offset = reader.read_scalar()
    input_count = reader.read_big_uint(4)
    inputs = [deserialize_input(reader) for _ in range(input_count)]
    output_count = reader.read_big_uint(4)
    outputs = [
        _block_output_from_tx_output(deserialize_output(reader))
        for _ in range(output_count)
    ]
    if reader.remaining != 0:
        raise DeserializationError(
            f"{reader.remaining} trailing byte(s) left after full block perishable parse"
        )
    return inputs, outputs, output_count, offset


def _deserialize_recovery_perishable(
    perishable: bytes,
    header: BlockHeader,
) -> tuple[list, list[BlockOutput], int, str | None]:
    reader = BufferReader(perishable)
    input_count = reader.read_var_uint()
    inputs = []
    for _ in range(input_count):
        if not reader.read_bool():
            raise DeserializationError("Recovery1 input vector contained a null pointer")
        inputs.append(deserialize_input(reader))
    output_count = reader.read_var_uint()
    outputs = [deserialize_block_output(reader, header.height) for _ in range(output_count)]

    offset = None
    if reader.remaining:
        if reader.remaining < 32:
            raise DeserializationError(
                "recovery block payload ended before the block offset could be read"
            )
        offset = reader.read_scalar()
        if reader.remaining:
            full_input_count = reader.read_big_uint(4)
            full_inputs = [deserialize_input(reader) for _ in range(full_input_count)]
            full_output_count = reader.read_big_uint(4)
            full_outputs = [
                _block_output_from_tx_output(deserialize_output(reader))
                for _ in range(full_output_count)
            ]
            if reader.remaining != 0:
                raise DeserializationError(
                    f"{reader.remaining} trailing byte(s) left after Recovery1 block parse"
                )
            return full_inputs, full_outputs, full_output_count, offset

    return inputs, outputs, output_count, offset


def _deserialize_perishable(
    perishable: bytes,
    header: BlockHeader,
) -> tuple[list, list[BlockOutput], int, str | None]:
    try:
        return _deserialize_full_perishable(perishable)
    except DeserializationError as full_error:
        try:
            return _deserialize_recovery_perishable(perishable, header)
        except DeserializationError as recovery_error:
            raise DeserializationError(
                "failed to parse block perishable payload as either full or Recovery1 "
                f"format; full error: {full_error}; recovery error: {recovery_error}"
            ) from recovery_error


def _decode_kernel_count(eternal: bytes) -> tuple[int, bool]:
    if not eternal:
        return 0, False
    if len(eternal) < 4:
        raise DeserializationError("eternal block payload is too small to contain a kernel count")
    kernel_count_raw = int.from_bytes(eternal[:4], "big")
    return kernel_count_raw & 0x7FFFFFFF, bool(kernel_count_raw & (1 << 31))


def _deserialize_body_buffers(
    perishable: bytes,
    eternal: bytes,
    header: BlockHeader,
) -> DecodedBlock:
    inputs, outputs, output_count, offset = _deserialize_perishable(perishable, header)
    kernel_count, kernels_mixed = _decode_kernel_count(eternal)
    return DecodedBlock(
        header=header,
        inputs=inputs,
        outputs=outputs,
        counts=TxCounts(
            inputs=len(inputs),
            outputs=output_count,
            kernels=kernel_count,
            kernels_mixed=kernels_mixed,
        ),
        offset=offset,
    )


def deserialize_body_payload(payload: bytes, header: BlockHeader) -> DecodedBlock:
    """Deserialize a single-block ``Body`` payload."""
    reader = BufferReader(payload)
    perishable, eternal = _read_body_buffers(reader)
    if reader.remaining != 0:
        raise DeserializationError(
            f"{reader.remaining} trailing byte(s) left after Body parse"
        )
    return _deserialize_body_buffers(perishable, eternal, header)


def split_body_pack_payload(payload: bytes) -> list[tuple[bytes, bytes]]:
    """Split a ``BodyPack`` payload into raw perishable/eternal body buffers."""
    reader = BufferReader(payload)
    body_count = reader.read_var_uint()
    if body_count == 0:
        raise DeserializationError("BodyPack did not contain any block bodies")

    bodies = [_read_body_buffers(reader) for _ in range(body_count)]
    if reader.remaining != 0:
        raise DeserializationError(
            f"{reader.remaining} trailing byte(s) left after BodyPack parse"
        )
    return bodies


def deserialize_body_pack_payload(payload: bytes, header: BlockHeader) -> DecodedBlock:
    """Deserialize a ``BodyPack`` payload and return the first body."""
    perishable, eternal = split_body_pack_payload(payload)[0]
    return _deserialize_body_buffers(perishable, eternal, header)


def deserialize_body_pack_payloads(
    payload: bytes,
    headers: Sequence[BlockHeader],
) -> list[DecodedBlock]:
    """Deserialize a ``BodyPack`` payload into one decoded block per header."""
    bodies = split_body_pack_payload(payload)
    if len(bodies) > len(headers):
        raise DeserializationError(
            f"BodyPack returned {len(bodies)} bodies for only {len(headers)} header(s)"
        )

    return [
        _deserialize_body_buffers(perishable, eternal, headers[index])
        for index, (perishable, eternal) in enumerate(bodies)
    ]