"""Staging runner for Beam headers and raw block payloads."""

from __future__ import annotations

import sys
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass

from .codec import encode_body_payload, encode_get_body_pack_payload, encode_height_range
from .connection import BeamConnection
from .deserializers import (
    deserialize_header_pack_payloads,
    deserialize_new_tip_payload,
    split_body_pack_payload,
)
from .models import StageResult
from .protocol import MessageType, message_name
from .state_store import StateStore
from .sync_common import (
    SyncConfig,
    batched,
    iter_contiguous_height_ranges,
    raise_if_start_past_available_height,
    requested_start_height,
)
from .treasury import extract_body_buffers, treasury_payload_sha256
from .utils import format_address


BODY_FLAG_FULL = 0
BODY_FLAG_NONE = 1
BODY_FLAG_RECOVERY1 = 2
HEADER_REQUEST_BATCH_SIZE = 2048
BODY_REQUEST_BATCH_SIZE = 256
BEAM_FAST_SYNC_HI = 1440
BEAM_FAST_SYNC_LO = BEAM_FAST_SYNC_HI * 3
BEAM_FAST_SYNC_TRIGGER_GAP = BEAM_FAST_SYNC_HI + (BEAM_FAST_SYNC_HI // 2)


@dataclass(frozen=True)
class BodyFetchPlan:
    """Describe one staged body-fetch phase."""

    start_height: int
    stop_height: int
    flag_perishable: int
    flag_eternal: int
    block0: int
    horizon_lo1: int
    horizon_hi1: int


class StageRunner:
    """Coordinate staged header and body downloads from a single Beam node."""

    def __init__(self, config: SyncConfig):
        self.config = config
        self.endpoint = format_address(config.endpoint)
        self.requested_start = requested_start_height(config.start_height)
        self.store = StateStore(config.state_db_path)
        self.connection = self._open_connection()
        self.started = 0.0
        self.target_height = 0
        self.synced_height = 0
        self.staged_headers = 0
        self.staged_blocks = 0
        self.expected_blocks = 0

    def run(self) -> StageResult:
        """Fetch headers and raw block payloads into SQLite without deriving UTXOs."""
        self.started = time.monotonic()

        try:
            self._connect()
            self.target_height = self._resolve_target_height()
            self._store_treasury_payload_if_needed()
            self._validate_requested_start()
            self.synced_height = self.store.last_synced_height()
            self._stage_headers()
            self.expected_blocks = self._count_missing_blocks()
            self._stage_blocks()
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
        """Create a Beam connection configured from the runner config."""
        host, port = self.config.endpoint
        return BeamConnection(
            host=host,
            port=port,
            connect_timeout=self.config.connect_timeout,
            read_timeout=max(self.config.request_timeout, 1.0),
            verbose=self.config.verbose,
        )

    def _connect(self) -> None:
        self.connection.connect()
        self.connection.handshake(0, self.config.fork_hashes)

    def _recv_until(
        self,
        *,
        expected: set[MessageType],
        timeout: float | None = None,
    ) -> tuple[MessageType, bytes]:
        """Receive messages until one of the expected message types is returned."""
        effective_timeout = self.config.request_timeout if timeout is None else timeout

        while True:
            message_type, payload = self.connection.recv_message(effective_timeout)

            if message_type in expected:
                return message_type, payload

            if message_type in {MessageType.PEER_INFO, MessageType.PEER_INFO_SELF}:
                self._log(
                    f"[*] {self.endpoint} ignored {message_name(message_type)} ({len(payload)}B)"
                )
                continue

            if message_type == MessageType.GET_TIME:
                self.connection.send_time()
                continue

            if message_type == MessageType.PING:
                self.connection.send(MessageType.PONG)
                continue

            if message_type == MessageType.BYE:
                raise RuntimeError("node sent Bye before sync completed")

            if message_type == MessageType.DATA_MISSING:
                raise RuntimeError("node reported the requested data is missing")

            if message_type in {
                MessageType.TIME,
                MessageType.AUTHENTICATION,
                MessageType.LOGIN,
                MessageType.NEW_TIP,
                MessageType.STATUS,
            }:
                self._log(
                    f"[*] {self.endpoint} <- {message_name(message_type)} ({len(payload)}B)"
                )
                continue

            self._log(
                f"[*] {self.endpoint} ignored {message_name(message_type)} ({len(payload)}B)"
            )

    def _resolve_target_height(self) -> int:
        tip_header = self._wait_for_tip_header()
        target_height = tip_header.height
        if self.config.stop_height is not None:
            target_height = min(target_height, self.config.stop_height)
        return target_height

    def _wait_for_tip_header(self):
        """Wait for the peer's current tip header."""
        message_type, payload = self._recv_until(expected={MessageType.NEW_TIP})
        if message_type != MessageType.NEW_TIP:
            raise RuntimeError(f"expected NewTip, got {message_name(message_type)}")
        return deserialize_new_tip_payload(payload, self.connection.peer_fork_hashes)

    def _validate_requested_start(self) -> None:
        raise_if_start_past_available_height(
            requested_start=self.requested_start,
            target_height=self.target_height,
            completed_height=self.store.last_staged_header_height(),
            available_name="node tip",
        )

    def _request_headers(self, *, start_height: int, stop_height: int) -> list:
        """Request a contiguous block-header range."""
        if start_height > stop_height:
            raise ValueError(
                f"start_height {start_height} must be <= stop_height {stop_height}"
            )

        self.connection.send(
            MessageType.ENUM_HDRS,
            encode_height_range(start_height, stop_height),
        )
        self._log(f"[*] {self.endpoint} requested headers {start_height}-{stop_height}")

        message_type, payload = self._recv_until(expected={MessageType.HDR_PACK})
        if message_type != MessageType.HDR_PACK:
            raise RuntimeError(f"expected HdrPack, got {message_name(message_type)}")

        headers = deserialize_header_pack_payloads(payload, self.connection.peer_fork_hashes)
        if not headers:
            raise RuntimeError(
                f"requested header range {start_height}-{stop_height}, node returned no headers"
            )
        if headers[0].height != start_height:
            raise RuntimeError(
                f"requested header range {start_height}-{stop_height}, got first header "
                f"for {headers[0].height}"
            )
        for previous_header, header in zip(headers, headers[1:]):
            if header.height != previous_header.height + 1:
                raise RuntimeError(
                    f"requested header range {start_height}-{stop_height}, got non-contiguous "
                    f"headers {previous_header.height} then {header.height}"
                )
        if headers[-1].height > stop_height:
            raise RuntimeError(
                f"requested header range {start_height}-{stop_height}, got header for "
                f"{headers[-1].height}"
            )
        return headers

    def _stage_headers(self) -> None:
        for range_start, range_stop in iter_contiguous_height_ranges(
            self.store.iter_missing_staged_header_heights(
                start_height=self.requested_start,
                stop_height=self.target_height,
            ),
            HEADER_REQUEST_BATCH_SIZE,
        ):
            next_height = range_start
            while next_height <= range_stop:
                headers = self._request_headers(
                    start_height=next_height,
                    stop_height=range_stop,
                )
                for header in headers:
                    self.store.stage_header(
                        header,
                        source_node=self.endpoint,
                    )
                    self.staged_headers += 1
                    self._report_staged_header(header.height)

                next_height = headers[-1].height + 1

    def _report_staged_header(self, height: int) -> None:
        if (
            self.staged_headers == 1
            or height == self.target_height
            or self.staged_headers % self.config.progress_every == 0
        ):
            print(
                f"[*] {self.endpoint} staged header {height}/{self.target_height}",
                file=sys.stderr,
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
            message_type, response_payload = self._recv_until(
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
            for _ in self.store.iter_missing_staged_headers(
                start_height=self.requested_start,
                stop_height=self.target_height,
            )
        )

    def _stage_blocks(self) -> None:
        for plan in self._iter_body_fetch_plans():
            missing_iter = self.store.iter_missing_staged_headers(
                start_height=plan.start_height,
                stop_height=plan.stop_height,
            )
            for batch_headers in batched(missing_iter, BODY_REQUEST_BATCH_SIZE):
                self._stage_body_batch(batch_headers, plan)

    def _stage_body_batch(self, batch_headers: list, plan: BodyFetchPlan) -> None:
        remaining_headers = batch_headers
        while remaining_headers:
            message_type, payload = self._request_body_range_payload(
                headers=remaining_headers,
                plan=plan,
            )
            single_body_payloads = self._body_payloads_from_message(message_type, payload)
            if not single_body_payloads:
                raise RuntimeError(
                    "node returned zero block bodies for a non-empty staged body request"
                )
            if len(single_body_payloads) > len(remaining_headers):
                raise RuntimeError(
                    "node returned more block bodies than requested in the current batch"
                )

            received_headers = remaining_headers[: len(single_body_payloads)]
            for header, single_payload in zip(received_headers, single_body_payloads):
                self.store.stage_block_payload(
                    header,
                    message_type=MessageType.BODY,
                    payload=single_payload,
                    source_node=self.endpoint,
                )
                self.staged_blocks += 1
                self._report_staged_block(header.height)

            remaining_headers = remaining_headers[len(single_body_payloads) :]

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

    def _request_body_range_payload(
        self,
        *,
        headers: Sequence,
        plan: BodyFetchPlan,
    ) -> tuple[MessageType, bytes]:
        """Request a contiguous block-body range and return the raw frame."""
        if not headers:
            raise ValueError("headers must not be empty")

        base_header = headers[0]
        top_header = headers[-1]
        payload = encode_get_body_pack_payload(
            top_height=top_header.height,
            top_hash=bytes.fromhex(top_header.hash),
            flag_perishable=plan.flag_perishable,
            flag_eternal=plan.flag_eternal,
            count_extra=top_header.height - base_header.height,
            block0=plan.block0,
            horizon_lo1=plan.horizon_lo1,
            horizon_hi1=plan.horizon_hi1,
        )
        self.connection.send(MessageType.GET_BODY_PACK, payload)
        self._log(
            "[*] "
            f"{self.endpoint} requested bodies {base_header.height}-{top_header.height} "
            f"(p={plan.flag_perishable}, e={plan.flag_eternal}, block0={plan.block0}, "
            f"lo={plan.horizon_lo1}, hi={plan.horizon_hi1})"
        )

        message_type, response_payload = self._recv_until(
            expected={MessageType.BODY, MessageType.BODY_PACK}
        )
        if message_type in {MessageType.BODY, MessageType.BODY_PACK}:
            return message_type, response_payload
        raise RuntimeError(
            f"unexpected message while waiting for block range: {message_name(message_type)}"
        )

    @staticmethod
    def _body_payloads_from_message(
        message_type: MessageType,
        payload: bytes,
    ) -> list[bytes]:
        """Split a raw body message into one single-body payload per block."""
        if message_type == MessageType.BODY:
            return [payload]
        if message_type == MessageType.BODY_PACK:
            return [
                encode_body_payload(perishable=perishable, eternal=eternal)
                for perishable, eternal in split_body_pack_payload(payload)
            ]
        raise RuntimeError(
            f"unsupported block body message: {message_name(message_type)}"
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