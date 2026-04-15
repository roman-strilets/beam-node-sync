"""Transaction input and output deserializers."""

from __future__ import annotations

from ..protocol_models import TxInput, TxOutput
from .core import BufferReader
from .proofs import (
    deserialize_asset_proof,
    deserialize_confidential_range_proof,
    deserialize_public_range_proof,
)


def deserialize_input(reader: BufferReader) -> TxInput:
    """Deserialize one transaction input."""
    flags = reader.read_u8()
    return TxInput(commitment=reader.read_point_x(bool(flags & 1)))


def deserialize_output(reader: BufferReader) -> TxOutput:
    """Deserialize one transaction output."""
    flags = reader.read_u8()
    return TxOutput(
        commitment=reader.read_point_x(bool(flags & 1)),
        coinbase=bool(flags & 2),
        confidential_proof=deserialize_confidential_range_proof(reader) if flags & 4 else None,
        public_proof=deserialize_public_range_proof(reader) if flags & 8 else None,
        incubation=reader.read_var_uint() if flags & 0x10 else None,
        asset_proof=deserialize_asset_proof(reader) if flags & 0x20 else None,
        extra_flags=reader.read_u8() if flags & 0x80 else None,
    )