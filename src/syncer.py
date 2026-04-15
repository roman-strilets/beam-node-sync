"""Sequential Beam sync runner that reconstructs the current UTXO set."""

from __future__ import annotations

import sys
import time
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field

from .codec import encode_body_payload, encode_get_body_pack_payload, encode_height_range
from .connection import BeamConnection
from .deserializers import (
    deserialize_body_pack_payloads,
    deserialize_body_pack_payload,
    deserialize_body_payload,
    deserialize_header_pack_payloads,
    deserialize_new_tip_payload,
    split_body_pack_payload,
)
from .models import DeriveResult, StageResult, SyncResult
from .protocol import (
    Address,
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_REQUEST_TIMEOUT,
    MessageType,
    message_name,
)
from .state_store import StateStore
from .storage import JsonLineWriter
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
class SyncConfig:
    """Configuration for direct sync or staging from a Beam node."""

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


def _log(verbose: bool, message: str) -> None:
    if verbose:
        print(message, file=sys.stderr)


def _open_connection(endpoint: Address, config: SyncConfig) -> BeamConnection:
    """Create a Beam connection configured from ``config`` for one endpoint."""
    return BeamConnection(
        host=endpoint[0],
        port=endpoint[1],
        connect_timeout=config.connect_timeout,
        read_timeout=max(config.request_timeout, 1.0),
        verbose=config.verbose,
    )


def _requested_start_height(start_height: int | None) -> int:
    return 1 if start_height is None else start_height


def _raise_if_start_past_available_height(
    *,
    requested_start: int,
    target_height: int,
    completed_height: int,
    available_name: str,
) -> None:
    if requested_start > target_height and completed_height < requested_start:
        raise RuntimeError(
            f"requested start height {requested_start} is past available {available_name} {target_height}"
        )


def _raise_if_non_contiguous_start(
    *,
    requested_start: int,
    next_height: int,
    mode_name: str,
    state_name: str,
) -> None:
    if requested_start > next_height:
        raise RuntimeError(
            f"{mode_name} requires contiguous {state_name}; state DB is through height {next_height - 1} "
            f"and can only continue from height {next_height}, not requested start height {requested_start}"
        )


def _recv_until(
    connection: BeamConnection,
    *,
    endpoint: str,
    expected: set[MessageType],
    timeout: float,
) -> tuple[MessageType, bytes]:
    """Receive messages until one of the expected message types is returned."""
    while True:
        message_type, payload = connection.recv_message(timeout)

        if message_type in expected:
            return message_type, payload

        if message_type in {MessageType.PEER_INFO, MessageType.PEER_INFO_SELF}:
            _log(
                connection.verbose,
                f"[*] {endpoint} ignored {message_name(message_type)} ({len(payload)}B)",
            )
            continue

        if message_type == MessageType.GET_TIME:
            connection.send_time()
            continue

        if message_type == MessageType.PING:
            connection.send(MessageType.PONG)
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
            _log(
                connection.verbose,
                f"[*] {endpoint} <- {message_name(message_type)} ({len(payload)}B)",
            )
            continue

        _log(
            connection.verbose,
            f"[*] {endpoint} ignored {message_name(message_type)} ({len(payload)}B)",
        )


def _batched(items: Iterable, batch_size: int) -> Iterator[list]:
    """Yield lists of up to ``batch_size`` items from ``items``."""
    batch: list = []
    for item in items:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _iter_contiguous_height_ranges(
    heights: Iterable[int],
    batch_size: int,
) -> Iterator[tuple[int, int]]:
    """Yield contiguous height ranges capped at ``batch_size`` items each."""
    if batch_size <= 0:
        raise ValueError(f"batch_size must be > 0, got {batch_size}")

    start_height: int | None = None
    previous_height: int | None = None
    count = 0

    for height in heights:
        if start_height is None:
            start_height = height
            previous_height = height
            count = 1
            continue

        assert previous_height is not None
        if height != previous_height + 1 or count >= batch_size:
            yield start_height, previous_height
            start_height = height
            previous_height = height
            count = 1
            continue

        previous_height = height
        count += 1

    if start_height is not None:
        assert previous_height is not None
        yield start_height, previous_height


def _wait_for_tip_header(
    connection: BeamConnection,
    endpoint: str,
    timeout: float,
):
    """Wait for the peer's current tip header."""
    message_type, payload = _recv_until(
        connection,
        endpoint=endpoint,
        expected={MessageType.NEW_TIP},
        timeout=timeout,
    )
    if message_type != MessageType.NEW_TIP:
        raise RuntimeError(f"expected NewTip, got {message_name(message_type)}")
    return deserialize_new_tip_payload(payload, connection.peer_fork_hashes)


def _request_header(
    connection: BeamConnection,
    *,
    endpoint: str,
    height: int,
    timeout: float,
):
    """Request one block header by height."""
    headers = _request_headers(
        connection,
        endpoint=endpoint,
        start_height=height,
        stop_height=height,
        timeout=timeout,
    )
    if len(headers) != 1:
        raise RuntimeError(f"requested height {height}, got {len(headers)} headers")
    return headers[0]


def _request_headers(
    connection: BeamConnection,
    *,
    endpoint: str,
    start_height: int,
    stop_height: int,
    timeout: float,
) -> list:
    """Request a contiguous block-header range."""
    if start_height > stop_height:
        raise ValueError(
            f"start_height {start_height} must be <= stop_height {stop_height}"
        )

    connection.send(
        MessageType.ENUM_HDRS,
        encode_height_range(start_height, stop_height),
    )
    _log(
        connection.verbose,
        f"[*] {endpoint} requested headers {start_height}-{stop_height}",
    )

    message_type, payload = _recv_until(
        connection,
        endpoint=endpoint,
        expected={MessageType.HDR_PACK},
        timeout=timeout,
    )
    if message_type != MessageType.HDR_PACK:
        raise RuntimeError(f"expected HdrPack, got {message_name(message_type)}")
    headers = deserialize_header_pack_payloads(payload, connection.peer_fork_hashes)
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


def _request_body(
    connection: BeamConnection,
    *,
    endpoint: str,
    header,
    timeout: float,
):
    """Request one full block body by explicit header id."""
    message_type, payload = _request_body_payload(
        connection,
        endpoint=endpoint,
        header=header,
        timeout=timeout,
    )
    return _decode_body_message(message_type, payload, header)


def _request_body_payload(
    connection: BeamConnection,
    *,
    endpoint: str,
    header,
    timeout: float,
) -> tuple[MessageType, bytes]:
    """Request one full block body by explicit header id and return the raw frame."""
    payload = encode_get_body_pack_payload(
        top_height=header.height,
        top_hash=bytes.fromhex(header.hash),
        flag_perishable=BODY_FLAG_FULL,
        flag_eternal=BODY_FLAG_FULL,
        count_extra=0,
        block0=0,
        horizon_lo1=0,
        horizon_hi1=0,
    )
    connection.send(MessageType.GET_BODY_PACK, payload)
    _log(connection.verbose, f"[*] {endpoint} requested body for height {header.height}")

    message_type, payload = _recv_until(
        connection,
        endpoint=endpoint,
        expected={MessageType.BODY, MessageType.BODY_PACK},
        timeout=timeout,
    )
    if message_type == MessageType.BODY:
        return message_type, payload
    if message_type == MessageType.BODY_PACK:
        return message_type, payload
    raise RuntimeError(f"unexpected message while waiting for body: {message_name(message_type)}")


def _request_body_range_payload(
    connection: BeamConnection,
    *,
    endpoint: str,
    headers: Sequence,
    flag_perishable: int,
    flag_eternal: int,
    block0: int,
    horizon_lo1: int,
    horizon_hi1: int,
    timeout: float,
) -> tuple[MessageType, bytes]:
    """Request a contiguous block-body range and return the raw frame."""
    if not headers:
        raise ValueError("headers must not be empty")

    base_header = headers[0]
    top_header = headers[-1]
    payload = encode_get_body_pack_payload(
        top_height=top_header.height,
        top_hash=bytes.fromhex(top_header.hash),
        flag_perishable=flag_perishable,
        flag_eternal=flag_eternal,
        count_extra=top_header.height - base_header.height,
        block0=block0,
        horizon_lo1=horizon_lo1,
        horizon_hi1=horizon_hi1,
    )
    connection.send(MessageType.GET_BODY_PACK, payload)
    _log(
        connection.verbose,
        "[*] "
        f"{endpoint} requested bodies {base_header.height}-{top_header.height} "
        f"(p={flag_perishable}, e={flag_eternal}, block0={block0}, lo={horizon_lo1}, hi={horizon_hi1})",
    )

    message_type, payload = _recv_until(
        connection,
        endpoint=endpoint,
        expected={MessageType.BODY, MessageType.BODY_PACK},
        timeout=timeout,
    )
    if message_type == MessageType.BODY:
        return message_type, payload
    if message_type == MessageType.BODY_PACK:
        return message_type, payload
    raise RuntimeError(
        f"unexpected message while waiting for block range: {message_name(message_type)}"
    )


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
    raise RuntimeError(f"unsupported block body message: {message_name(message_type)}")


def _decode_body_messages(
    message_type: MessageType,
    payload: bytes,
    headers: Sequence,
) -> list:
    """Decode one or more blocks from a raw body message."""
    if message_type == MessageType.BODY:
        if len(headers) != 1:
            raise RuntimeError(
                f"expected one header for Body, got {len(headers)}"
            )
        return [deserialize_body_payload(payload, headers[0])]
    if message_type == MessageType.BODY_PACK:
        return deserialize_body_pack_payloads(payload, headers)
    raise RuntimeError(f"unsupported block body message: {message_name(message_type)}")


def _should_use_sparse_fast_sync(
    *,
    config: SyncConfig,
    target_height: int,
    synced_height: int,
    requested_start: int,
) -> bool:
    """Return ``True`` when Beam-style sparse fast sync is safe to use."""
    if not config.fast_sync:
        return False
    if requested_start != synced_height + 1:
        return False
    return (target_height - synced_height) > BEAM_FAST_SYNC_TRIGGER_GAP


def _iter_body_fetch_plans(
    *,
    config: SyncConfig,
    target_height: int,
    synced_height: int,
    requested_start: int,
) -> Iterator[tuple[int, int, int, int, int, int]]:
    """Yield staged body-fetch phases as ``(start, stop, flag_p, flag_e, block0, lo1, hi1)``."""
    if requested_start > target_height:
        return

    if _should_use_sparse_fast_sync(
        config=config,
        target_height=target_height,
        synced_height=synced_height,
        requested_start=requested_start,
    ):
        sparse_stop = target_height - BEAM_FAST_SYNC_HI
        sparse_txo_lo = max(0, target_height - BEAM_FAST_SYNC_LO)
        if requested_start <= sparse_stop:
            yield (
                requested_start,
                sparse_stop,
                BODY_FLAG_RECOVERY1,
                BODY_FLAG_FULL,
                synced_height,
                sparse_txo_lo,
                sparse_stop,
            )
            requested_start = sparse_stop + 1

    if requested_start <= target_height:
        yield (
            requested_start,
            target_height,
            BODY_FLAG_FULL,
            BODY_FLAG_FULL,
            0,
            0,
            0,
        )


def _decode_body_message(message_type: MessageType, payload: bytes, header):
    """Decode a staged or freshly-fetched body message against its header."""
    if message_type == MessageType.BODY:
        return deserialize_body_payload(payload, header)
    if message_type == MessageType.BODY_PACK:
        return deserialize_body_pack_payload(payload, header)
    raise RuntimeError(f"unsupported block body message: {message_name(message_type)}")


def run_stage(config: SyncConfig) -> StageResult:
    """Fetch headers and raw block payloads into SQLite without deriving UTXOs."""
    endpoint = format_address(config.endpoint)
    started = time.monotonic()
    store = StateStore(config.state_db_path)
    connection = _open_connection(config.endpoint, config)
    requested_start = _requested_start_height(config.start_height)

    try:
        connection.connect()
        connection.handshake(0, config.fork_hashes)

        tip_header = _wait_for_tip_header(connection, endpoint, config.request_timeout)
        target_height = tip_header.height
        if config.stop_height is not None:
            target_height = min(target_height, config.stop_height)

        _raise_if_start_past_available_height(
            requested_start=requested_start,
            target_height=target_height,
            completed_height=store.last_staged_header_height(),
            available_name="node tip",
        )

        synced_height = store.last_synced_height()

        staged_headers = 0
        for range_start, range_stop in _iter_contiguous_height_ranges(
            store.iter_missing_staged_header_heights(
                start_height=requested_start,
                stop_height=target_height,
            ),
            HEADER_REQUEST_BATCH_SIZE,
        ):
            next_height = range_start
            while next_height <= range_stop:
                headers = _request_headers(
                    connection,
                    endpoint=endpoint,
                    start_height=next_height,
                    stop_height=range_stop,
                    timeout=config.request_timeout,
                )
                for header in headers:
                    store.stage_header(
                        header,
                        source_node=endpoint,
                    )
                    staged_headers += 1

                    if (
                        staged_headers == 1
                        or header.height == target_height
                        or staged_headers % config.progress_every == 0
                    ):
                        print(
                            f"[*] {endpoint} staged header {header.height}/{target_height}",
                            file=sys.stderr,
                        )

                next_height = headers[-1].height + 1

        staged_blocks = 0
        expected_blocks = sum(
            1
            for _ in store.iter_missing_staged_headers(
                start_height=requested_start,
                stop_height=target_height,
            )
        )
        for (
            phase_start,
            phase_stop,
            flag_perishable,
            flag_eternal,
            block0,
            horizon_lo1,
            horizon_hi1,
        ) in _iter_body_fetch_plans(
            config=config,
            target_height=target_height,
            synced_height=synced_height,
            requested_start=requested_start,
        ):
            missing_iter = store.iter_missing_staged_headers(
                start_height=phase_start,
                stop_height=phase_stop,
            )
            for batch_headers in _batched(missing_iter, BODY_REQUEST_BATCH_SIZE):
                remaining_headers = batch_headers
                while remaining_headers:
                    message_type, payload = _request_body_range_payload(
                        connection,
                        endpoint=endpoint,
                        headers=remaining_headers,
                        flag_perishable=flag_perishable,
                        flag_eternal=flag_eternal,
                        block0=block0,
                        horizon_lo1=horizon_lo1,
                        horizon_hi1=horizon_hi1,
                        timeout=config.request_timeout,
                    )
                    single_body_payloads = _body_payloads_from_message(message_type, payload)
                    if len(single_body_payloads) > len(remaining_headers):
                        raise RuntimeError(
                            "node returned more block bodies than requested in the current batch"
                        )

                    received_headers = remaining_headers[: len(single_body_payloads)]
                    for header, single_payload in zip(received_headers, single_body_payloads):
                        store.stage_block_payload(
                            header,
                            message_type=MessageType.BODY,
                            payload=single_payload,
                            source_node=endpoint,
                        )
                        staged_blocks += 1

                        if (
                            staged_blocks == 1
                            or header.height == target_height
                            or staged_blocks % config.progress_every == 0
                        ):
                            print(
                                f"[*] staged block {header.height}/{target_height} from {endpoint}; "
                                f"completed={staged_blocks}/{expected_blocks}",
                                file=sys.stderr,
                            )

                    remaining_headers = remaining_headers[len(single_body_payloads) :]

        first_remaining = next(
            store.iter_missing_staged_headers(
                start_height=requested_start,
                stop_height=target_height,
            ),
            None,
        )
        if first_remaining is not None:
            raise RuntimeError(
                f"requested block range {requested_start}-{target_height} is not fully staged; "
                f"first missing body is height {first_remaining.height}"
            )
        staged_height = target_height
    finally:
        connection.close()
        store.close()

    duration = time.monotonic() - started
    return StageResult(
        node=endpoint,
        target_height=target_height,
        staged_height=staged_height,
        staged_blocks=staged_blocks,
        duration_seconds=duration,
    )


def run_derive(config: DeriveConfig, writer: JsonLineWriter) -> DeriveResult:
    """Derive UTXOs from previously staged headers and block payloads."""
    started = time.monotonic()
    store = StateStore(config.state_db_path)
    requested_start = _requested_start_height(config.start_height)

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

        _raise_if_start_past_available_height(
            requested_start=requested_start,
            target_height=target_height,
            completed_height=last_synced_height,
            available_name="staged height",
        )
        _raise_if_non_contiguous_start(
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

        exported_utxos = store.export_unspent(writer, target_height)
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
        exported_utxos=exported_utxos,
        duration_seconds=duration,
    )


def run_sync(config: SyncConfig, writer: JsonLineWriter) -> SyncResult:
    """Synchronize blocks from a trusted Beam node and export current UTXOs."""
    if config.fast_sync:
        stage_result = run_stage(config)
        derive_result = run_derive(
            DeriveConfig(
                state_db_path=config.state_db_path,
                start_height=config.start_height,
                stop_height=config.stop_height,
                progress_every=config.progress_every,
            ),
            writer,
        )
        return SyncResult(
            node=stage_result.node,
            target_height=derive_result.target_height,
            synced_height=derive_result.synced_height,
            applied_blocks=derive_result.applied_blocks,
            outputs_seen=derive_result.outputs_seen,
            resolved_spends=derive_result.resolved_spends,
            unresolved_spends=derive_result.unresolved_spends,
            exported_utxos=derive_result.exported_utxos,
            duration_seconds=stage_result.duration_seconds + derive_result.duration_seconds,
        )

    endpoint = format_address(config.endpoint)
    started = time.monotonic()
    store = StateStore(config.state_db_path)
    connection = _open_connection(config.endpoint, config)
    requested_start = _requested_start_height(config.start_height)

    try:
        connection.connect()
        connection.handshake(0, config.fork_hashes)

        tip_header = _wait_for_tip_header(connection, endpoint, config.request_timeout)
        target_height = tip_header.height
        if config.stop_height is not None:
            target_height = min(target_height, config.stop_height)

        last_synced_height = store.last_synced_height()
        if config.stop_height is not None and config.stop_height < last_synced_height:
            raise RuntimeError(
                f"state DB is already synced to {last_synced_height}, which is past requested stop height {config.stop_height}"
            )

        _raise_if_start_past_available_height(
            requested_start=requested_start,
            target_height=target_height,
            completed_height=last_synced_height,
            available_name="node tip",
        )
        _raise_if_non_contiguous_start(
            requested_start=requested_start,
            next_height=last_synced_height + 1,
            mode_name="direct mode",
            state_name="synced state",
        )
        start_height = last_synced_height + 1

        applied_blocks = 0
        outputs_seen = 0
        resolved_spends = 0
        unresolved_spends = 0

        if start_height <= target_height:
            next_header_height = start_height
            while next_header_height <= target_height:
                batch_stop = min(
                    target_height,
                    next_header_height + HEADER_REQUEST_BATCH_SIZE - 1,
                )
                headers = _request_headers(
                    connection,
                    endpoint=endpoint,
                    start_height=next_header_height,
                    stop_height=batch_stop,
                    timeout=config.request_timeout,
                )
                for header in headers:
                    block = _request_body(
                        connection,
                        endpoint=endpoint,
                        header=header,
                        timeout=config.request_timeout,
                    )
                    stats = store.apply_block(header, block)

                    applied_blocks += 1
                    outputs_seen += stats.inserted_outputs
                    resolved_spends += stats.resolved_spends
                    unresolved_spends += stats.unresolved_spends

                    if (
                        applied_blocks == 1
                        or header.height == target_height
                        or applied_blocks % config.progress_every == 0
                    ):
                        print(
                            f"[*] {endpoint} synced height {header.height}/{target_height}; "
                            f"outputs+={stats.inserted_outputs}, spends+={stats.resolved_spends}, "
                            f"missing+={stats.unresolved_spends}",
                            file=sys.stderr,
                        )

                next_header_height = headers[-1].height + 1

        exported_utxos = store.export_unspent(writer, target_height)
        synced_height = store.last_synced_height()
    finally:
        connection.close()
        store.close()

    duration = time.monotonic() - started
    return SyncResult(
        node=endpoint,
        target_height=target_height,
        synced_height=synced_height,
        applied_blocks=applied_blocks,
        outputs_seen=outputs_seen,
        resolved_spends=resolved_spends,
        unresolved_spends=unresolved_spends,
        exported_utxos=exported_utxos,
        duration_seconds=duration,
    )