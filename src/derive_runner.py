"""Derivation runner for staged Beam block payloads."""

from __future__ import annotations

import sys
import time

from beam_p2p import MessageType, message_name
from beam_p2p.deserializers import deserialize_body_pack_payload, deserialize_body_payload

from .models import DeriveResult
from .state_store import StateStore
from .sync_common import (
    DeriveConfig,
    raise_if_non_contiguous_start,
    raise_if_start_past_available_height,
    requested_start_height,
)


def _decode_body_message(message_type: MessageType, payload: bytes, header):
    """Decode a staged or freshly-fetched body message against its header."""
    if message_type == MessageType.BODY:
        return deserialize_body_payload(payload, header)
    if message_type == MessageType.BODY_PACK:
        return deserialize_body_pack_payload(payload, header)
    raise RuntimeError(f"unsupported block body message: {message_name(message_type)}")


def run_derive(config: DeriveConfig) -> DeriveResult:
    """Derive UTXOs from previously staged headers and block payloads."""
    started = time.monotonic()
    store = StateStore(config.state_db_path)
    requested_start = requested_start_height(config.start_height)

    try:
        target_height = store.last_staged_height()
        if target_height <= 0:
            raise RuntimeError("state DB does not contain any staged blocks")
        if config.stop_height is not None:
            target_height = min(target_height, config.stop_height)

        last_synced_height = store.last_synced_height()
        if config.stop_height is not None and config.stop_height < last_synced_height:
            raise RuntimeError(
                f"state DB is already derived through {last_synced_height}, which is past requested stop height {config.stop_height}"
            )
        if last_synced_height > target_height:
            raise RuntimeError(
                f"derived state is already at height {last_synced_height}, which is past staged height {target_height}"
            )

        raise_if_start_past_available_height(
            requested_start=requested_start,
            target_height=target_height,
            completed_height=last_synced_height,
            available_name="staged height",
        )
        raise_if_non_contiguous_start(
            requested_start=requested_start,
            next_height=last_synced_height + 1,
            mode_name="derive mode",
            state_name="derived state",
        )
        start_height = last_synced_height + 1

        applied_blocks = 0
        outputs_seen = 0
        resolved_spends = 0
        unresolved_spends = 0

        if start_height <= target_height:
            expected_blocks = target_height - start_height + 1
            for staged_block in store.iter_staged_blocks(
                start_height=start_height,
                stop_height=target_height,
            ):
                block = _decode_body_message(
                    staged_block.body_message_type,
                    staged_block.body_payload,
                    staged_block.header,
                )
                stats = store.apply_block(staged_block.header, block)

                applied_blocks += 1
                outputs_seen += stats.inserted_outputs
                resolved_spends += stats.resolved_spends
                unresolved_spends += stats.unresolved_spends

                if (
                    applied_blocks == 1
                    or staged_block.header.height == target_height
                    or applied_blocks % config.progress_every == 0
                ):
                    print(
                        f"[*] derived height {staged_block.header.height}/{target_height}; "
                        f"outputs+={stats.inserted_outputs}, spends+={stats.resolved_spends}, "
                        f"missing+={stats.unresolved_spends}",
                        file=sys.stderr,
                    )

            if applied_blocks != expected_blocks:
                raise RuntimeError(
                    f"staged block set is incomplete between heights {start_height} and {target_height}"
                )
        synced_height = store.last_synced_height()
    finally:
        store.close()

    duration = time.monotonic() - started
    return DeriveResult(
        target_height=target_height,
        synced_height=synced_height,
        applied_blocks=applied_blocks,
        outputs_seen=outputs_seen,
        resolved_spends=resolved_spends,
        unresolved_spends=unresolved_spends,
        duration_seconds=duration,
    )