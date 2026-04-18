from pathlib import Path

import pytest

from src.codec import encode_body_payload, encode_uint
from src.derive_runner import run_derive
from src.node_fetcher import NodeBlockFetcher
from src.protocol import MessageType
from src.protocol_models import BlockHeader, DecodedBlock, EcPoint, TxCounts, TxInput
from src.stage_runner import StageRunner, run_stage
from src.state_store import StateStore
from src.sync_common import DeriveConfig, SyncConfig
from src.sync_pipeline import run_staged


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
    monkeypatch.setattr(StageRunner, "_request_treasury_payload", lambda self: None)


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


def test_run_staged_rejects_start_height_past_resume_floor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "state.sqlite3"

    store = StateStore(str(db_path))
    try:
        header1 = _header(1, "00" * 32)
        store.apply_block(header1, _empty_block(header1))
    finally:
        store.close()

    def fail_run_stage(_config: SyncConfig):
        raise AssertionError(
            "run_staged should reject an impossible derive resume before staging"
        )

    monkeypatch.setattr("src.sync_pipeline.run_stage", fail_run_stage)

    with pytest.raises(RuntimeError, match="can only continue from height 2"):
        run_staged(
            SyncConfig(
                endpoint=("node.example", 8100),
                state_db_path=str(db_path),
                start_height=3,
            )
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

    monkeypatch.setattr(StageRunner, "_open_connection", lambda self: _FakeConnection())
    monkeypatch.setattr(StageRunner, "_wait_for_tip_header", lambda self: headers[-1])
    monkeypatch.setattr("src.stage_runner.HEADER_REQUEST_BATCH_SIZE", 8)
    monkeypatch.setattr("src.stage_runner.BODY_REQUEST_BATCH_SIZE", 8)

    header_by_height = {header.height: header for header in headers}

    def fake_request_headers(self, *, start_height, stop_height):
        requested_header_ranges.append((start_height, stop_height))
        return [header_by_height[height] for height in range(start_height, stop_height + 1)]

    def fake_request_body_range_payload(self, *, headers, plan):
        requested_body_ranges.append(
            (
                headers[0].height,
                headers[-1].height,
                plan.flag_perishable,
                plan.block0,
                plan.horizon_lo1,
                plan.horizon_hi1,
            )
        )
        return MessageType.BODY_PACK, _empty_body_pack_payload(len(headers))

    monkeypatch.setattr(NodeBlockFetcher, "request_headers", fake_request_headers)
    monkeypatch.setattr(NodeBlockFetcher, "request_body_range_payload", fake_request_body_range_payload)

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

    requested_header_ranges: list[tuple[int, int]] = []
    requested_body_ranges: list[tuple[int, int]] = []

    monkeypatch.setattr(StageRunner, "_open_connection", lambda self: _FakeConnection())
    monkeypatch.setattr(StageRunner, "_wait_for_tip_header", lambda self: headers[-1])
    monkeypatch.setattr("src.stage_runner.BODY_REQUEST_BATCH_SIZE", 8)

    header_by_height = {header.height: header for header in headers}

    def fake_request_headers(self, *, start_height, stop_height):
        requested_header_ranges.append((start_height, stop_height))
        return [header_by_height[height] for height in range(start_height, stop_height + 1)]

    def fake_request_body_range_payload(self, *, headers, plan):
        requested_body_ranges.append((headers[0].height, headers[-1].height))
        return MessageType.BODY_PACK, _empty_body_pack_payload(len(headers))

    monkeypatch.setattr(NodeBlockFetcher, "request_headers", fake_request_headers)
    monkeypatch.setattr(NodeBlockFetcher, "request_body_range_payload", fake_request_body_range_payload)

    result = run_stage(
        SyncConfig(
            endpoint=("node.example", 8100),
            state_db_path=str(db_path),
            start_height=2,
            stop_height=3,
            progress_every=10,
        )
    )

    assert requested_header_ranges == [(2, 3)]
    assert requested_body_ranges == [(2, 3)]
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

    monkeypatch.setattr(StageRunner, "_open_connection", lambda self: _FakeConnection())
    monkeypatch.setattr("src.stage_runner.HEADER_REQUEST_BATCH_SIZE", 8)
    monkeypatch.setattr("src.stage_runner.BODY_REQUEST_BATCH_SIZE", 3)
    monkeypatch.setattr("src.stage_runner.BEAM_FAST_SYNC_HI", 2)
    monkeypatch.setattr("src.stage_runner.BEAM_FAST_SYNC_LO", 4)
    monkeypatch.setattr("src.stage_runner.BEAM_FAST_SYNC_TRIGGER_GAP", 3)

    def fake_request_headers(self, *, start_height, stop_height):
        requested_header_ranges.append((start_height, stop_height))
        return [_header(height, f"{height - 1:064x}") for height in range(start_height, stop_height + 1)]

    def fake_request_body_range_payload(self, *, headers, plan):
        requested_ranges.append(
            (
                headers[0].height,
                headers[-1].height,
                plan.flag_perishable,
                plan.block0,
                plan.horizon_lo1,
                plan.horizon_hi1,
            )
        )
        payload_type = MessageType.BODY if len(headers) == 1 else MessageType.BODY_PACK
        payload = (
            _empty_body_payload()
            if len(headers) == 1
            else _empty_body_pack_payload(len(headers))
        )
        return payload_type, payload

    monkeypatch.setattr(StageRunner, "_wait_for_tip_header", lambda self: _header(6, f"{5:064x}"))
    monkeypatch.setattr(NodeBlockFetcher, "request_headers", fake_request_headers)
    monkeypatch.setattr(NodeBlockFetcher, "request_body_range_payload", fake_request_body_range_payload)

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

    assert requested_header_ranges == [(1, 3), (4, 4), (5, 6)]
    assert requested_ranges == [
        (1, 3, 2, 0, 2, 4),
        (4, 4, 2, 0, 2, 4),
        (5, 6, 0, 0, 0, 0),
    ]
    assert result.staged_height == 6
    assert result.staged_blocks == 6


def test_run_derive_clamps_start_height_to_next_synced_height(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    headers = _stage_headers_and_bodies(db_path, heights=3)

    store = StateStore(str(db_path))
    try:
        store.apply_block(headers[0], _empty_block(headers[0]))
    finally:
        store.close()

    result = run_derive(
        DeriveConfig(
            state_db_path=str(db_path),
            start_height=1,
            stop_height=3,
            progress_every=10,
        )
    )

    assert result.target_height == 3
    assert result.synced_height == 3
    assert result.applied_blocks == 2


def test_run_derive_rejects_start_height_past_resume_floor(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    headers = _stage_headers_and_bodies(db_path, heights=3)

    store = StateStore(str(db_path))
    try:
        store.apply_block(headers[0], _empty_block(headers[0]))
    finally:
        store.close()

    with pytest.raises(RuntimeError, match="can only continue from height 2"):
        run_derive(
            DeriveConfig(
                state_db_path=str(db_path),
                start_height=3,
                stop_height=3,
                progress_every=10,
            )
        )


def test_run_staged_uses_stage_then_derive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage_calls: list[SyncConfig] = []
    derive_calls: list[DeriveConfig] = []

    def fake_run_stage(config: SyncConfig):
        stage_calls.append(config)
        return type("_StageResult", (), {
            "node": "node.example:8100",
            "target_height": 10,
            "staged_height": 10,
            "staged_blocks": 10,
            "duration_seconds": 1.5,
        })()

    def fake_run_derive(config: DeriveConfig):
        derive_calls.append(config)
        return type("_DeriveResult", (), {
            "target_height": 10,
            "synced_height": 10,
            "applied_blocks": 10,
            "outputs_seen": 7,
            "resolved_spends": 5,
            "unresolved_spends": 1,
            "duration_seconds": 2.0,
        })()

    monkeypatch.setattr("src.sync_pipeline.run_stage", fake_run_stage)
    monkeypatch.setattr("src.sync_pipeline.run_derive", fake_run_derive)

    stage_result, derive_result = run_staged(
        SyncConfig(
            endpoint=("node.example", 8100),
            state_db_path=str(tmp_path / "state.sqlite3"),
            start_height=1,
            stop_height=10,
            progress_every=25,
            fast_sync=True,
        )
    )

    assert len(stage_calls) == 1
    assert stage_calls[0].fast_sync is True
    assert len(derive_calls) == 1
    assert derive_calls[0].state_db_path == str(tmp_path / "state.sqlite3")
    assert derive_calls[0].start_height == 1
    assert derive_calls[0].stop_height == 10
    assert derive_calls[0].progress_every == 25
    assert stage_result.node == "node.example:8100"
    assert derive_result.synced_height == 10
    assert derive_result.outputs_seen == 7


def test_run_stage_and_derive_use_stored_treasury_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "state.sqlite3"
    treasury_commitment = "aa".rjust(64, "0")
    header = _header(1, "00" * 32)

    monkeypatch.setattr(StageRunner, "_open_connection", lambda self: _FakeConnection())
    monkeypatch.setattr(StageRunner, "_wait_for_tip_header", lambda self: header)
    monkeypatch.setattr(
        StageRunner,
        "_request_treasury_payload",
        lambda self: _treasury_payload(treasury_commitment),
    )
    monkeypatch.setattr(
        NodeBlockFetcher,
        "request_headers",
        lambda self, *, start_height, stop_height: [header],
    )
    monkeypatch.setattr(
        NodeBlockFetcher,
        "request_body_range_payload",
        lambda self, *, headers, plan: (MessageType.BODY, _body_payload_with_input(treasury_commitment)),
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

    derive_result = run_derive(
        DeriveConfig(
            state_db_path=str(db_path),
            progress_every=10,
        )
    )

    assert derive_result.applied_blocks == 1
    assert derive_result.resolved_spends == 1
    assert derive_result.unresolved_spends == 0