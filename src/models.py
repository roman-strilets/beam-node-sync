"""Application-level models for sync summaries and staged blocks."""

from __future__ import annotations

from dataclasses import dataclass

from .protocol import MessageType
from .protocol_models import BlockHeader


@dataclass(frozen=True)
class StagedBlockRecord:
    """One fetched block body persisted for later derivation."""

    header: BlockHeader
    body_message_type: MessageType
    body_payload: bytes
    source_node: str


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
    duration_seconds: float