"""Application-level models for sync summaries and exported UTXOs."""

from __future__ import annotations

from dataclasses import dataclass

from .protocol import MessageType
from .protocol_models import BlockHeader


@dataclass(frozen=True)
class UtxoExportRecord:
    """One exported unspent output record."""

    commitment: str
    commitment_x: str
    commitment_y: bool
    create_height: int
    maturity_height: int
    mature: bool
    coinbase: bool
    recovery_only: bool
    incubation: int
    has_confidential_proof: bool
    has_public_proof: bool
    has_asset_proof: bool
    extra_flags: int | None

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation of the record."""
        return {
            "commitment": self.commitment,
            "commitment_x": self.commitment_x,
            "commitment_y": self.commitment_y,
            "create_height": self.create_height,
            "maturity_height": self.maturity_height,
            "mature": self.mature,
            "coinbase": self.coinbase,
            "recovery_only": self.recovery_only,
            "incubation": self.incubation,
            "has_confidential_proof": self.has_confidential_proof,
            "has_public_proof": self.has_public_proof,
            "has_asset_proof": self.has_asset_proof,
            "extra_flags": self.extra_flags,
        }


@dataclass(frozen=True)
class StagedBlockRecord:
    """One fetched block body persisted for later derivation."""

    header: BlockHeader
    body_message_type: MessageType
    body_payload: bytes
    source_node: str


@dataclass(frozen=True)
class SyncResult:
    """Summary returned after a sync-and-export run."""

    node: str
    target_height: int
    synced_height: int
    applied_blocks: int
    outputs_seen: int
    resolved_spends: int
    unresolved_spends: int
    exported_utxos: int
    duration_seconds: float


@dataclass(frozen=True)
class StageResult:
    """Summary returned after staging raw block payloads."""

    node: str
    target_height: int
    staged_height: int
    staged_blocks: int
    duration_seconds: float


@dataclass(frozen=True)
class DeriveResult:
    """Summary returned after deriving UTXOs from staged blocks."""

    target_height: int
    synced_height: int
    applied_blocks: int
    outputs_seen: int
    resolved_spends: int
    unresolved_spends: int
    exported_utxos: int
    duration_seconds: float