"""High-level Beam staged-sync pipeline orchestration."""

from __future__ import annotations

from .derive_runner import run_derive
from .stage_runner import run_stage
from .state_store import StateStore
from .sync_common import (
    DeriveConfig,
    SyncConfig,
    raise_if_non_contiguous_start,
    requested_start_height,
)


def run_staged(config: SyncConfig):
    """Run the staged Beam sync pipeline from a trusted node into local SQLite state."""
    requested_start = requested_start_height(config.start_height)
    store = StateStore(config.state_db_path)

    try:
        last_synced_height = store.last_synced_height()
    finally:
        store.close()

    if config.stop_height is not None and config.stop_height < last_synced_height:
        raise RuntimeError(
            f"state DB is already derived through {last_synced_height}, which is past requested stop height {config.stop_height}"
        )
    raise_if_non_contiguous_start(
        requested_start=requested_start,
        next_height=last_synced_height + 1,
        mode_name="staged mode",
        state_name="derived state",
    )

    stage_result = run_stage(config)
    derive_result = run_derive(
        DeriveConfig(
            state_db_path=config.state_db_path,
            start_height=config.start_height,
            stop_height=config.stop_height,
            progress_every=config.progress_every,
        )
    )
    return stage_result, derive_result