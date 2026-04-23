"""Shared configuration and validation helpers for staged Beam sync."""

from __future__ import annotations

from dataclasses import dataclass, field

from beam_p2p import Address, DEFAULT_CONNECT_TIMEOUT, DEFAULT_REQUEST_TIMEOUT


@dataclass(frozen=True)
class SyncConfig:
    """Configuration for staging from a Beam node."""

    endpoint: Address
    state_db_path: str
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT
    fork_hashes: list[bytes] = field(default_factory=list)
    start_height: int | None = None
    stop_height: int | None = None
    progress_every: int = 1000
    fast_sync: bool = False
    verbose: bool = False


@dataclass(frozen=True)
class DeriveConfig:
    """Configuration for deriving UTXOs from staged block payloads."""

    state_db_path: str
    start_height: int | None = None
    stop_height: int | None = None
    progress_every: int = 1000


def requested_start_height(start_height: int | None) -> int:
    """Return the requested start height, defaulting to Beam genesis height 1."""
    return 1 if start_height is None else start_height


def raise_if_start_past_available_height(
    *,
    requested_start: int,
    target_height: int,
    completed_height: int,
    available_name: str,
) -> None:
    """Reject ranges that begin after the highest available height."""
    if requested_start > target_height and completed_height < requested_start:
        raise RuntimeError(
            f"requested start height {requested_start} is past available {available_name} {target_height}"
        )


def raise_if_non_contiguous_start(
    *,
    requested_start: int,
    next_height: int,
    mode_name: str,
    state_name: str,
) -> None:
    """Reject requests that would skip required contiguous state."""
    if requested_start > next_height:
        raise RuntimeError(
            f"{mode_name} requires contiguous {state_name}; state DB is through height {next_height - 1} "
            f"and can only continue from height {next_height}, not requested start height {requested_start}"
        )

