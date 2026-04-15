from pathlib import Path

import pytest

from src.codec import encode_body_payload, encode_uint
from src.protocol import MessageType
from src.protocol_models import BlockHeader, DecodedBlock, EcPoint, TxCounts, TxInput
from src.state_store import StateStore
from src.storage import JsonLineWriter
from src.syncer import DeriveConfig, SyncConfig, run_derive, run_stage, run_sync


class _FakeConnection:
    verbose = False

    def connect(self) -> None:
        return None

    def handshake(self, *_args) -> None:
        return None

    def close(self) -> None:
        return None


@pytest.fixture(autouse=True)
def _disable_treasury_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.syncer._request_treasury_payload",
        lambda connection, *, endpoint, timeout: None,
    )


def _header(height: int, previous_hash: str) -> BlockHeader:
    return BlockHeader(
        height=height,
        hash=f"{height:064x}",
        previous_hash=previous_hash,
        chainwork="22" * 32,
        kernels="33" * 32,
        definition="44" * 32,
        timestamp=height,
        packed_difficulty=1,
        difficulty=1.0,
        rules_hash=None,
        pow_indices_hex="55" * 104,
        pow_nonce_hex="66" * 8,
    )


def _empty_block(header: BlockHeader) -> DecodedBlock:
    return DecodedBlock(
        header=header,
        inputs=[],
        outputs=[],
        counts=TxCounts(inputs=0, outputs=0, kernels=0, kernels_mixed=False),
        offset=None,
    )


def _empty_body_payload() -> bytes:
    perishable = (b"\x00" * 32) + (0).to_bytes(4, "big") + (0).to_bytes(4, "big")
    eternal = (0).to_bytes(4, "big")
    return encode_body_payload(perishable=perishable, eternal=eternal)


def _empty_body_pack_payload(count: int) -> bytes:
    body = _empty_body_payload()
    return encode_uint(count) + (body * count)


def _body_payload_with_input(commitment_x: str, *, y: bool = False) -> bytes:
    input_bytes = bytes((1 if y else 0,)) + bytes.fromhex(commitment_x)
    perishable = (b"\x00" * 32) + (1).to_bytes(4, "big") + input_bytes + (0).to_bytes(4, "big")
    eternal = (0).to_bytes(4, "big")
    return encode_body_payload(perishable=perishable, eternal=eternal)


def _treasury_payload(commitment_x: str, *, incubation: int = 0) -> bytes:
    output_flags = 0x10 if incubation else 0
    group = bytearray()
    group.extend((0).to_bytes(4, "big"))
    group.extend((1).to_bytes(4, "big"))
    group.append(output_flags)
    group.extend(bytes.fromhex(commitment_x))
    if incubation:
        group.extend(encode_uint(incubation))
    group.extend((1).to_bytes(4, "big"))
    group.append(0)
    group.extend(b"\x00" * 32)
    group.extend(b"\x00" * 32)
    group.extend(b"\x00" * 32)
    group.extend(b"\x00" * 32)
    group.extend((0).to_bytes(16, "big"))

    return encode_uint(0) + encode_uint(1) + bytes(group)


def _block_with_input(header: BlockHeader, commitment_x: str, *, y: bool = False) -> DecodedBlock:
    return DecodedBlock(
        header=header,
        inputs=[TxInput(commitment=EcPoint(x=commitment_x, y=y))],
        outputs=[],
        counts=TxCounts(inputs=1, outputs=0, kernels=0, kernels_mixed=False),
        offset=None,
    )


def _stage_headers_and_bodies(db_path: Path, heights: int) -> list[BlockHeader]:
    headers: list[BlockHeader] = []
    store = StateStore(str(db_path))
    try:
        previous_hash = "00" * 32
        for height in range(1, heights + 1):
            header = _header(height, previous_hash)
            headers.append(header)
            store.stage_header(header, source_node="node-a")
            store.stage_block_payload(
                header,
                message_type=MessageType.BODY,
                payload=_empty_body_payload(),
                source_node="node-a",
            )
            previous_hash = header.hash
    finally:
        store.close()
    return headers


def test_run_sync_rejects_start_height_past_resume_floor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "state.sqlite3"
    output_path = tmp_path / "utxos.jsonl"

    store = StateStore(str(db_path))
    try:
        header1 = _header(1, "00" * 32)
        store.apply_block(header1, _empty_block(header1))
    finally:
        store.close()

    header2 = _header(2, header1.hash)
    header3 = _header(3, header2.hash)

    monkeypatch.setattr("src.syncer._open_connection", lambda endpoint, config: _FakeConnection())
    monkeypatch.setattr("src.syncer._wait_for_tip_header", lambda connection, endpoint, timeout: header3)

    with JsonLineWriter(str(output_path)) as writer:
        with pytest.raises(RuntimeError, match="can only continue from height 2"):
            run_sync(
                SyncConfig(
                    endpoint=("node.example", 8100),
                    state_db_path=str(db_path),
                    start_height=3,
                ),
                writer,
            )


def test_run_stage_allows_fresh_sparse_range(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "state.sqlite3"
    headers = [
        _header(3, f"{2:064x}"),
        _header(4, f"{3:064x}"),
        _header(5, f"{4:064x}"),
    ]
    requested_header_ranges: list[tuple[int, int]] = []
    requested_body_ranges: list[tuple[int, int, int, int, int, int]] = []

    monkeypatch.setattr("src.syncer._open_connection", lambda endpoint, config: _FakeConnection())
    monkeypatch.setattr("src.syncer._wait_for_tip_header", lambda connection, endpoint, timeout: headers[-1])
    monkeypatch.setattr("src.syncer.HEADER_REQUEST_BATCH_SIZE", 8)
    monkeypatch.setattr("src.syncer.BODY_REQUEST_BATCH_SIZE", 8)

    header_by_height = {header.height: header for header in headers}

    def fake_request_headers(connection, *, endpoint, start_height, stop_height, timeout):
        requested_header_ranges.append((start_height, stop_height))
        return [header_by_height[height] for height in range(start_height, stop_height + 1)]

    def fake_request_body_range_payload(
        connection,
        *,
        endpoint,
        headers,
        flag_perishable,
        flag_eternal,
        block0,
        horizon_lo1,
        horizon_hi1,
        timeout,
    ):
        requested_body_ranges.append(
            (
                headers[0].height,
                headers[-1].height,
                flag_perishable,
                block0,
                horizon_lo1,
                horizon_hi1,
            )
        )
        return MessageType.BODY_PACK, _empty_body_pack_payload(len(headers))

    monkeypatch.setattr("src.syncer._request_headers", fake_request_headers)
    monkeypatch.setattr(
        "src.syncer._request_body_range_payload", fake_request_body_range_payload
    )

    result = run_stage(
        SyncConfig(
            endpoint=("node.example", 8100),
            state_db_path=str(db_path),
            start_height=3,
            stop_height=5,
            progress_every=10,
        )
    )

    assert requested_header_ranges == [(3, 5)]
    assert requested_body_ranges == [(3, 5, 0, 0, 0, 0)]
    assert result.target_height == 5
    assert result.staged_height == 5
    assert result.staged_blocks == 3

    store = StateStore(str(db_path))
    try:
        assert [header.height for header in store.iter_staged_headers(start_height=3, stop_height=5)] == [3, 4, 5]
        assert store.last_staged_height() == 0
    finally:
        store.close()


def test_run_stage_fetches_missing_bodies_in_requested_range(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "state.sqlite3"
    headers = []
    store = StateStore(str(db_path))
    try:
        header1 = _header(1, "00" * 32)
        header2 = _header(2, header1.hash)
        header3 = _header(3, header2.hash)
        headers = [header1, header2, header3]

        for header in headers:
            store.stage_header(header, source_node="node-a")

        store.stage_block_payload(
            header1,
            message_type=MessageType.BODY,
            payload=_empty_body_payload(),
            source_node="node-a",
        )
    finally:
        store.close()

    requested_ranges: list[tuple[int, int]] = []

    monkeypatch.setattr("src.syncer._open_connection", lambda endpoint, config: _FakeConnection())
    monkeypatch.setattr("src.syncer._wait_for_tip_header", lambda connection, endpoint, timeout: headers[-1])
    monkeypatch.setattr("src.syncer.BODY_REQUEST_BATCH_SIZE", 8)

    def fail_request_headers(*_args, **_kwargs):
        raise AssertionError("run_stage should not request headers when they are already staged")

    def fake_request_body_range_payload(
        connection,
        *,
        endpoint,
        headers,
        flag_perishable,
        flag_eternal,
        block0,
        horizon_lo1,
        horizon_hi1,
        timeout,
    ):
        requested_ranges.append((headers[0].height, headers[-1].height))
        return MessageType.BODY_PACK, _empty_body_pack_payload(len(headers))

    monkeypatch.setattr("src.syncer._request_headers", fail_request_headers)
    monkeypatch.setattr(
        "src.syncer._request_body_range_payload", fake_request_body_range_payload
    )

    result = run_stage(
        SyncConfig(
            endpoint=("node.example", 8100),
            state_db_path=str(db_path),
            start_height=2,
            stop_height=3,
            progress_every=10,
        )
    )

    assert requested_ranges == [(2, 3)]
    assert result.target_height == 3
    assert result.staged_height == 3
    assert result.staged_blocks == 2


def test_run_stage_fast_sync_uses_sparse_then_full_body_ranges(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "state.sqlite3"
    requested_header_ranges: list[tuple[int, int]] = []
    requested_ranges: list[tuple[int, int, int, int, int, int]] = []

    monkeypatch.setattr("src.syncer._open_connection", lambda endpoint, config: _FakeConnection())
    monkeypatch.setattr("src.syncer.HEADER_REQUEST_BATCH_SIZE", 8)
    monkeypatch.setattr("src.syncer.BODY_REQUEST_BATCH_SIZE", 3)
    monkeypatch.setattr("src.syncer.BEAM_FAST_SYNC_HI", 2)
    monkeypatch.setattr("src.syncer.BEAM_FAST_SYNC_LO", 4)
    monkeypatch.setattr("src.syncer.BEAM_FAST_SYNC_TRIGGER_GAP", 3)

    def fake_wait_for_tip_header(connection, endpoint, timeout):
        return _header(6, f"{5:064x}")

    def fake_request_headers(connection, *, endpoint, start_height, stop_height, timeout):
        requested_header_ranges.append((start_height, stop_height))
        return [_header(height, f"{height - 1:064x}") for height in range(start_height, stop_height + 1)]

    def fake_request_body_range_payload(
        connection,
        *,
        endpoint,
        headers,
        flag_perishable,
        flag_eternal,
        block0,
        horizon_lo1,
        horizon_hi1,
        timeout,
    ):
        requested_ranges.append(
            (
                headers[0].height,
                headers[-1].height,
                flag_perishable,
                block0,
                horizon_lo1,
                horizon_hi1,
            )
        )
        payload_type = MessageType.BODY if len(headers) == 1 else MessageType.BODY_PACK
        payload = (
            _empty_body_payload()
            if len(headers) == 1
            else _empty_body_pack_payload(len(headers))
        )
        return payload_type, payload

    monkeypatch.setattr("src.syncer._wait_for_tip_header", fake_wait_for_tip_header)
    monkeypatch.setattr("src.syncer._request_headers", fake_request_headers)
    monkeypatch.setattr(
        "src.syncer._request_body_range_payload", fake_request_body_range_payload
    )

    result = run_stage(
        SyncConfig(
            endpoint=("node.example", 8100),
            state_db_path=str(db_path),
            start_height=1,
            stop_height=6,
            fast_sync=True,
            progress_every=10,
        )
    )

    assert requested_header_ranges == [(1, 6)]
    assert requested_ranges == [
        (1, 3, 2, 0, 2, 4),
        (4, 4, 2, 0, 2, 4),
        (5, 6, 0, 0, 0, 0),
    ]
    assert result.staged_height == 6
    assert result.staged_blocks == 6


def test_run_sync_batches_header_requests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "state.sqlite3"
    output_path = tmp_path / "utxos.jsonl"
    requested_header_ranges: list[tuple[int, int]] = []

    tip_header = _header(3, f"{2:064x}")

    monkeypatch.setattr("src.syncer._open_connection", lambda endpoint, config: _FakeConnection())
    monkeypatch.setattr("src.syncer._wait_for_tip_header", lambda connection, endpoint, timeout: tip_header)
    monkeypatch.setattr("src.syncer.HEADER_REQUEST_BATCH_SIZE", 2)

    def fake_request_headers(connection, *, endpoint, start_height, stop_height, timeout):
        requested_header_ranges.append((start_height, stop_height))
        previous_hash = "00" * 32 if start_height == 1 else f"{start_height - 1:064x}"
        headers: list[BlockHeader] = []
        for height in range(start_height, stop_height + 1):
            header = _header(height, previous_hash)
            headers.append(header)
            previous_hash = header.hash
        return headers

    def fake_request_body(connection, *, endpoint, header, timeout):
        return _empty_block(header)

    monkeypatch.setattr("src.syncer._request_headers", fake_request_headers)
    monkeypatch.setattr("src.syncer._request_body", fake_request_body)

    with JsonLineWriter(str(output_path)) as writer:
        result = run_sync(
            SyncConfig(
                endpoint=("node.example", 8100),
                state_db_path=str(db_path),
                start_height=1,
                stop_height=3,
                progress_every=10,
            ),
            writer,
        )

    assert requested_header_ranges == [(1, 2), (3, 3)]
    assert result.target_height == 3
    assert result.synced_height == 3
    assert result.applied_blocks == 3


def test_run_derive_clamps_start_height_to_next_synced_height(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    output_path = tmp_path / "utxos.jsonl"
    headers = _stage_headers_and_bodies(db_path, heights=3)

    store = StateStore(str(db_path))
    try:
        store.apply_block(headers[0], _empty_block(headers[0]))
    finally:
        store.close()

    with JsonLineWriter(str(output_path)) as writer:
        result = run_derive(
            DeriveConfig(
                state_db_path=str(db_path),
                start_height=1,
                stop_height=3,
                progress_every=10,
            ),
            writer,
        )

    assert result.target_height == 3
    assert result.synced_height == 3
    assert result.applied_blocks == 2


def test_run_derive_rejects_start_height_past_resume_floor(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    output_path = tmp_path / "utxos.jsonl"
    headers = _stage_headers_and_bodies(db_path, heights=3)

    store = StateStore(str(db_path))
    try:
        store.apply_block(headers[0], _empty_block(headers[0]))
    finally:
        store.close()

    with JsonLineWriter(str(output_path)) as writer:
        with pytest.raises(RuntimeError, match="can only continue from height 2"):
            run_derive(
                DeriveConfig(
                    state_db_path=str(db_path),
                    start_height=3,
                    stop_height=3,
                    progress_every=10,
                ),
                writer,
            )


def test_run_sync_fast_sync_uses_stage_then_derive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage_calls: list[SyncConfig] = []

    def fake_run_stage(config: SyncConfig):
        stage_calls.append(config)
        return type("_StageResult", (), {
            "node": "node.example:8100",
            "target_height": 10,
            "staged_height": 10,
            "staged_blocks": 10,
            "duration_seconds": 1.5,
        })()

    def fake_run_derive(config: DeriveConfig, writer: JsonLineWriter):
        return type("_DeriveResult", (), {
            "target_height": 10,
            "synced_height": 10,
            "applied_blocks": 10,
            "outputs_seen": 7,
            "resolved_spends": 5,
            "unresolved_spends": 1,
            "exported_utxos": 3,
            "duration_seconds": 2.0,
        })()

    monkeypatch.setattr("src.syncer.run_stage", fake_run_stage)
    monkeypatch.setattr("src.syncer.run_derive", fake_run_derive)

    with JsonLineWriter(str(tmp_path / "utxos.jsonl")) as writer:
        result = run_sync(
            SyncConfig(
                endpoint=("node.example", 8100),
                state_db_path=str(tmp_path / "state.sqlite3"),
                fast_sync=True,
            ),
            writer,
        )

    assert len(stage_calls) == 1
    assert result.node == "node.example:8100"
    assert result.synced_height == 10
    assert result.outputs_seen == 7
    assert result.duration_seconds == 3.5


def test_run_stage_and_derive_use_stored_treasury_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "state.sqlite3"
    output_path = tmp_path / "utxos.jsonl"
    treasury_commitment = "aa".rjust(64, "0")
    header = _header(1, "00" * 32)

    monkeypatch.setattr("src.syncer._open_connection", lambda endpoint, config: _FakeConnection())
    monkeypatch.setattr("src.syncer._wait_for_tip_header", lambda connection, endpoint, timeout: header)
    monkeypatch.setattr(
        "src.syncer._request_treasury_payload",
        lambda connection, *, endpoint, timeout: _treasury_payload(treasury_commitment),
    )
    monkeypatch.setattr(
        "src.syncer._request_headers",
        lambda connection, *, endpoint, start_height, stop_height, timeout: [header],
    )
    monkeypatch.setattr(
        "src.syncer._request_body_range_payload",
        lambda connection, *, endpoint, headers, flag_perishable, flag_eternal, block0, horizon_lo1, horizon_hi1, timeout: (
            MessageType.BODY,
            _body_payload_with_input(treasury_commitment),
        ),
    )

    stage_result = run_stage(
        SyncConfig(
            endpoint=("node.example", 8100),
            state_db_path=str(db_path),
            start_height=1,
            stop_height=1,
            progress_every=10,
        )
    )

    assert stage_result.staged_blocks == 1

    store = StateStore(str(db_path))
    try:
        assert store.treasury_payload_hash() is not None
        assert store.treasury_imported_payload_hash() is None
    finally:
        store.close()

    with JsonLineWriter(str(output_path)) as writer:
        derive_result = run_derive(
            DeriveConfig(
                state_db_path=str(db_path),
                progress_every=10,
            ),
            writer,
        )

    assert derive_result.applied_blocks == 1
    assert derive_result.resolved_spends == 1
    assert derive_result.unresolved_spends == 0
    assert derive_result.exported_utxos == 0


def test_run_sync_imports_treasury_before_applying_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "state.sqlite3"
    output_path = tmp_path / "utxos.jsonl"
    treasury_commitment = "bb".rjust(64, "0")
    header = _header(1, "00" * 32)

    monkeypatch.setattr("src.syncer._open_connection", lambda endpoint, config: _FakeConnection())
    monkeypatch.setattr("src.syncer._wait_for_tip_header", lambda connection, endpoint, timeout: header)
    monkeypatch.setattr(
        "src.syncer._request_treasury_payload",
        lambda connection, *, endpoint, timeout: _treasury_payload(treasury_commitment),
    )
    monkeypatch.setattr(
        "src.syncer._request_headers",
        lambda connection, *, endpoint, start_height, stop_height, timeout: [header],
    )
    monkeypatch.setattr(
        "src.syncer._request_body",
        lambda connection, *, endpoint, header, timeout: _block_with_input(
            header,
            treasury_commitment,
        ),
    )

    with JsonLineWriter(str(output_path)) as writer:
        result = run_sync(
            SyncConfig(
                endpoint=("node.example", 8100),
                state_db_path=str(db_path),
                start_height=1,
                stop_height=1,
                progress_every=10,
            ),
            writer,
        )

    assert result.applied_blocks == 1
    assert result.resolved_spends == 1
    assert result.unresolved_spends == 0
    assert result.exported_utxos == 0