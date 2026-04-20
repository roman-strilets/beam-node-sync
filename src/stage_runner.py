"""Staging runner for Beam headers and raw block payloads."""

from __future__ import annotations

import sys
import time
from collections.abc import Iterator

from beam_p2p import BeamConnection, MessageType, encode_get_body_pack_payload, format_address, message_name
from beam_p2p.deserializers import deserialize_new_tip_payload

from .node_fetcher import BODY_FLAG_FULL, BODY_FLAG_NONE, BODY_FLAG_RECOVERY1, BodyFetchPlan, NodeBlockFetcher
from .models import StageResult
from .state_store import StateStore
from .sync_common import (
    SyncConfig,
    iter_contiguous_height_ranges,
    raise_if_start_past_available_height,
    requested_start_height,
)
from .treasury import extract_body_buffers, treasury_payload_sha256


HEADER_REQUEST_BATCH_SIZE = 2048  # kept for backward-compat; not used in staging logic
BODY_REQUEST_BATCH_SIZE = 256
BEAM_FAST_SYNC_HI = 1440
BEAM_FAST_SYNC_LO = BEAM_FAST_SYNC_HI * 3
BEAM_FAST_SYNC_TRIGGER_GAP = BEAM_FAST_SYNC_HI + (BEAM_FAST_SYNC_HI // 2)


class StageRunner:
    """Coordinate staged header and body downloads from a single Beam node."""

    def __init__(self, config: SyncConfig):
        self.config = config
        self.endpoint = format_address(config.endpoint)
        self.requested_start = requested_start_height(config.start_height)
        self.store = StateStore(config.state_db_path)
        self.connection = self._open_connection()
        self.fetcher = NodeBlockFetcher(
            self.connection,
            request_timeout=config.request_timeout,
            verbose=config.verbose,
        )
        self.started = 0.0
        self.target_height = 0
        self.synced_height = 0
        self.staged_blocks = 0
        self.expected_blocks = 0

    def run(self) -> StageResult:
        """Fetch headers and raw block payloads into SQLite without deriving UTXOs."""
        self.started = time.monotonic()

        try:
            self.target_height = self._resolve_target_height()
            self._store_treasury_payload_if_needed()
            self._validate_requested_start()
            self.synced_height = self.store.last_synced_height()
            self.expected_blocks = self._count_missing_blocks()
            self._stage_all()
            self._ensure_requested_range_is_fully_staged()
        finally:
            self.close()

        duration = time.monotonic() - self.started
        return StageResult(
            node=self.endpoint,
            target_height=self.target_height,
            staged_height=self.target_height,
            staged_blocks=self.staged_blocks,
            duration_seconds=duration,
        )

    def close(self) -> None:
        self.connection.close()
        self.store.close()

    def _log(self, message: str) -> None:
        if self.config.verbose:
            print(message, file=sys.stderr)

    def _open_connection(self) -> BeamConnection:
        """Create a Beam connection and establish it with the remote peer."""
        host, port = self.config.endpoint
        connection = BeamConnection(
            host=host,
            port=port,
            connect_timeout=self.config.connect_timeout,
            read_timeout=max(self.config.request_timeout, 1.0),
            verbose=self.config.verbose,
        )
        connection.connect()
        connection.handshake(0, self.config.fork_hashes)
        return connection

    def _resolve_target_height(self) -> int:
        tip_header = self._wait_for_tip_header()
        target_height = tip_header.height
        if self.config.stop_height is not None:
            target_height = min(target_height, self.config.stop_height)
        return target_height

    def _wait_for_tip_header(self):
        """Wait for the peer's current tip header."""
        message_type, payload = self.fetcher.recv_until(expected={MessageType.NEW_TIP})
        if message_type != MessageType.NEW_TIP:
            raise RuntimeError(f"expected NewTip, got {message_name(message_type)}")
        return deserialize_new_tip_payload(payload, self.connection.peer_fork_hashes)

    def _validate_requested_start(self) -> None:
        raise_if_start_past_available_height(
            requested_start=self.requested_start,
            target_height=self.target_height,
            completed_height=self.store.last_staged_height(),
            available_name="node tip",
        )

    def _request_treasury_payload(self) -> bytes | None:
        """Request the node treasury blob, if the network exposes one."""
        payload = encode_get_body_pack_payload(
            top_height=0,
            top_hash=bytes(32),
            flag_perishable=BODY_FLAG_NONE,
            flag_eternal=BODY_FLAG_FULL,
            count_extra=0,
            block0=0,
            horizon_lo1=0,
            horizon_hi1=0,
        )
        self.connection.send(MessageType.GET_BODY_PACK, payload)
        self._log(f"[*] {self.endpoint} requested treasury payload")

        try:
            message_type, response_payload = self.fetcher.recv_until(
                expected={MessageType.BODY, MessageType.BODY_PACK}
            )
        except RuntimeError as exc:
            if str(exc) == "node reported the requested data is missing":
                self._log(f"[*] {self.endpoint} did not provide a treasury payload")
                return None
            raise

        perishable, eternal = extract_body_buffers(message_type, response_payload)
        if perishable:
            raise RuntimeError("treasury response unexpectedly included a perishable payload")
        return eternal

    def _store_treasury_payload_if_needed(self) -> None:
        if self.store.treasury_payload_hash() is not None:
            return

        payload = self._request_treasury_payload()
        if payload is None:
            return

        self.store.store_treasury_payload(
            payload,
            payload_sha256=treasury_payload_sha256(payload),
            source_node=self.endpoint,
        )

    def _count_missing_blocks(self) -> int:
        return sum(
            1
            for _ in self.store.iter_heights_needing_block_staging(
                start_height=self.requested_start,
                stop_height=self.target_height,
            )
        )

    def _stage_all(self) -> None:
        """Fetch headers and bodies together in a single pass using fetch_blocks."""
        for phase_plan in self._iter_body_fetch_plans():
            for range_start, range_stop in iter_contiguous_height_ranges(
                self.store.iter_heights_needing_block_staging(
                    start_height=phase_plan.start_height,
                    stop_height=phase_plan.stop_height,
                ),
                BODY_REQUEST_BATCH_SIZE,
            ):
                batch_plan = BodyFetchPlan(
                    start_height=range_start,
                    stop_height=range_stop,
                    flag_perishable=phase_plan.flag_perishable,
                    flag_eternal=phase_plan.flag_eternal,
                    block0=phase_plan.block0,
                    horizon_lo1=phase_plan.horizon_lo1,
                    horizon_hi1=phase_plan.horizon_hi1,
                )
                blocks = self.fetcher.fetch_blocks(
                    start_height=range_start,
                    stop_height=range_stop,
                    plan=batch_plan,
                )
                for block in blocks:
                    assert block.raw_payload is not None
                    self.store.stage_header(block.header, source_node=self.endpoint)
                    self.store.stage_block_payload(
                        block.header,
                        message_type=MessageType.BODY,
                        payload=block.raw_payload,
                        source_node=self.endpoint,
                    )
                    self.staged_blocks += 1
                    self._report_staged_block(block.header.height)

    def _report_staged_block(self, height: int) -> None:
        if (
            self.staged_blocks == 1
            or height == self.target_height
            or self.staged_blocks % self.config.progress_every == 0
        ):
            print(
                f"[*] staged block {height}/{self.target_height} from {self.endpoint}; "
                f"completed={self.staged_blocks}/{self.expected_blocks}",
                file=sys.stderr,
            )

    def _should_use_sparse_fast_sync(self) -> bool:
        """Return ``True`` when Beam-style sparse fast sync is safe to use."""
        if not self.config.fast_sync:
            return False
        if self.requested_start != self.synced_height + 1:
            return False
        return (self.target_height - self.synced_height) > BEAM_FAST_SYNC_TRIGGER_GAP

    def _iter_body_fetch_plans(self) -> Iterator[BodyFetchPlan]:
        """Yield the staged body-fetch phases needed for the requested range."""
        requested_start = self.requested_start
        if requested_start > self.target_height:
            return

        if self._should_use_sparse_fast_sync():
            sparse_stop = self.target_height - BEAM_FAST_SYNC_HI
            sparse_txo_lo = max(0, self.target_height - BEAM_FAST_SYNC_LO)
            if requested_start <= sparse_stop:
                yield BodyFetchPlan(
                    start_height=requested_start,
                    stop_height=sparse_stop,
                    flag_perishable=BODY_FLAG_RECOVERY1,
                    flag_eternal=BODY_FLAG_FULL,
                    block0=self.synced_height,
                    horizon_lo1=sparse_txo_lo,
                    horizon_hi1=sparse_stop,
                )
                requested_start = sparse_stop + 1

        if requested_start <= self.target_height:
            yield BodyFetchPlan(
                start_height=requested_start,
                stop_height=self.target_height,
                flag_perishable=BODY_FLAG_FULL,
                flag_eternal=BODY_FLAG_FULL,
                block0=0,
                horizon_lo1=0,
                horizon_hi1=0,
            )

    def _ensure_requested_range_is_fully_staged(self) -> None:
        first_remaining = next(
            self.store.iter_missing_staged_headers(
                start_height=self.requested_start,
                stop_height=self.target_height,
            ),
            None,
        )
        if first_remaining is not None:
            raise RuntimeError(
                f"requested block range {self.requested_start}-{self.target_height} "
                f"is not fully staged; first missing body is height {first_remaining.height}"
            )


def run_stage(config: SyncConfig) -> StageResult:
    """Fetch headers and raw block payloads into SQLite without deriving UTXOs."""
    return StageRunner(config).run()