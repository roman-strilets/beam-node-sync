from pathlib import Path
import sqlite3

from beam_p2p import MessageType, encode_uint, format_commitment
from beam_p2p.protocol_models import BlockHeader, BlockOutput, DecodedBlock, EcPoint, TxCounts, TxInput

from src.derive_runner import run_derive
from src.state_store import COINBASE_MATURITY, StateStore
from src.sync_common import DeriveConfig


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


def _output(tag: str, *, coinbase: bool = False, incubation: int = 0) -> BlockOutput:
    return BlockOutput(
        commitment=EcPoint(x=tag.rjust(64, "0"), y=False),
        coinbase=coinbase,
        recovery_only=False,
        confidential_proof=None,
        public_proof=None,
        incubation=incubation,
        asset_proof=None,
        extra_flags=None,
    )


def _empty_body_payload() -> bytes:
    perishable = (b"\x00" * 32) + (0).to_bytes(4, "big") + (0).to_bytes(4, "big")
    eternal = (0).to_bytes(4, "big")
    return encode_uint(len(perishable)) + perishable + encode_uint(len(eternal)) + eternal


def _treasury_output(tag: str, *, incubation: int = 0) -> BlockOutput:
    return BlockOutput(
        commitment=EcPoint(x=tag.rjust(64, "0"), y=False),
        coinbase=False,
        recovery_only=False,
        confidential_proof=None,
        public_proof=None,
        incubation=incubation,
        asset_proof=None,
        extra_flags=None,
    )


def _inspect_rows(
    db_path: Path,
    query: str,
    params: tuple[object, ...] = (),
) -> list[sqlite3.Row]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(query, params).fetchall()
    finally:
        conn.close()


def _inspect_row(
    db_path: Path,
    query: str,
    params: tuple[object, ...] = (),
) -> sqlite3.Row | None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(query, params).fetchone()
    finally:
        conn.close()


def test_state_store_applies_spends_and_tracks_unspent_outputs(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"

    store = StateStore(str(db_path))
    try:
        header1 = _header(1, "00" * 32)
        first_output = _output("aa", coinbase=True, incubation=3)
        block1 = DecodedBlock(
            header=header1,
            inputs=[],
            outputs=[first_output],
            counts=TxCounts(inputs=0, outputs=1, kernels=0, kernels_mixed=False),
            offset=None,
        )
        stats1 = store.apply_block(header1, block1)
        assert stats1.inserted_outputs == 1
        assert stats1.resolved_spends == 0
        assert stats1.unresolved_spends == 0

        header2 = _header(2, header1.hash)
        second_output = _output("bb")
        block2 = DecodedBlock(
            header=header2,
            inputs=[TxInput(commitment=first_output.commitment)],
            outputs=[second_output],
            counts=TxCounts(inputs=1, outputs=1, kernels=0, kernels_mixed=False),
            offset=None,
        )
        stats2 = store.apply_block(header2, block2)
        assert stats2.inserted_outputs == 1
        assert stats2.resolved_spends == 1
        assert stats2.unresolved_spends == 0

        rows = _inspect_rows(
            db_path,
            """
            SELECT commitment, create_height
            FROM outputs
            WHERE spent_height IS NULL
            ORDER BY output_id ASC
            """
        )

        assert len(rows) == 1
        assert rows[0]["create_height"] == header2.height
        assert rows[0]["commitment"] == format_commitment(second_output.commitment)
    finally:
        store.close()


def test_state_store_computes_coinbase_maturity_height(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"

    store = StateStore(str(db_path))
    try:
        header = _header(1, "00" * 32)
        output = _output("cc", coinbase=True, incubation=2)
        block = DecodedBlock(
            header=header,
            inputs=[],
            outputs=[output],
            counts=TxCounts(inputs=0, outputs=1, kernels=0, kernels_mixed=False),
            offset=None,
        )
        store.apply_block(header, block)

        row = _inspect_row(db_path, "SELECT maturity_height, spent_height FROM outputs")
        assert row is not None
        assert row["maturity_height"] == header.height + COINBASE_MATURITY + 2
        assert row["spent_height"] is None
    finally:
        store.close()


def test_state_store_supports_duplicate_commitments_with_lifo_spends(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite3"

    store = StateStore(str(db_path))
    try:
        duplicate = EcPoint(x="dd".rjust(64, "0"), y=True)

        header1 = _header(1, "00" * 32)
        block1 = DecodedBlock(
            header=header1,
            inputs=[],
            outputs=[
                BlockOutput(
                    commitment=duplicate,
                    coinbase=False,
                    recovery_only=False,
                    confidential_proof=None,
                    public_proof=None,
                    incubation=0,
                    asset_proof=None,
                    extra_flags=None,
                )
            ],
            counts=TxCounts(inputs=0, outputs=1, kernels=0, kernels_mixed=False),
            offset=None,
        )
        store.apply_block(header1, block1)

        header2 = _header(2, header1.hash)
        block2 = DecodedBlock(
            header=header2,
            inputs=[],
            outputs=[
                BlockOutput(
                    commitment=duplicate,
                    coinbase=False,
                    recovery_only=False,
                    confidential_proof=None,
                    public_proof=None,
                    incubation=0,
                    asset_proof=None,
                    extra_flags=None,
                )
            ],
            counts=TxCounts(inputs=0, outputs=1, kernels=0, kernels_mixed=False),
            offset=None,
        )
        stats2 = store.apply_block(header2, block2)
        assert stats2.inserted_outputs == 1

        header3 = _header(3, header2.hash)
        block3 = DecodedBlock(
            header=header3,
            inputs=[TxInput(commitment=duplicate)],
            outputs=[],
            counts=TxCounts(inputs=1, outputs=0, kernels=0, kernels_mixed=False),
            offset=None,
        )
        stats3 = store.apply_block(header3, block3)
        assert stats3.resolved_spends == 1
        assert stats3.unresolved_spends == 0

        rows = _inspect_rows(
            db_path,
            """
            SELECT commitment, create_height, spent_height
            FROM outputs
            WHERE commitment = ?
            ORDER BY output_id ASC
            """,
            (format_commitment(duplicate),),
        )

        assert len(rows) == 2
        assert rows[0]["create_height"] == 1
        assert rows[0]["spent_height"] is None
        assert rows[1]["create_height"] == 2
        assert rows[1]["spent_height"] == 3
    finally:
        store.close()


def test_state_store_initializes_outputs_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"

    store = StateStore(str(db_path))
    try:
        columns = {row["name"]: row for row in _inspect_rows(db_path, "PRAGMA table_info(outputs)")}
        assert "output_id" in columns
        assert "commitment" in columns
        assert "spent_height" in columns

        indexes = {row["name"] for row in _inspect_rows(db_path, "PRAGMA index_list(outputs)")}
        assert "idx_outputs_unspent" in indexes
        assert "idx_outputs_commitment_unspent" in indexes
    finally:
        store.close()


def test_state_store_stages_block_payloads_in_order(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"

    store = StateStore(str(db_path))
    try:
        body_payload = _empty_body_payload()
        header1 = _header(1, "00" * 32)
        header2 = _header(2, header1.hash)

        store.stage_header(header1, source_node="node-a")
        store.stage_header(header2, source_node="node-a")
        store.stage_block_payload(
            header1,
            message_type=MessageType.BODY,
            payload=body_payload,
            source_node="node-a",
        )
        store.stage_block_payload(
            header2,
            message_type=MessageType.BODY,
            payload=body_payload,
            source_node="node-a",
        )

        assert store.last_staged_height() == 2
        staged = list(store.iter_staged_blocks(start_height=1, stop_height=2))
        assert [record.header.height for record in staged] == [1, 2]
        assert all(record.body_message_type == MessageType.BODY for record in staged)
        assert all(record.source_node == "node-a" for record in staged)
        assert all(record.body_payload == body_payload for record in staged)
    finally:
        store.close()


def test_run_derive_replays_staged_body_payloads(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    body_payload = _empty_body_payload()

    store = StateStore(str(db_path))
    try:
        header1 = _header(1, "00" * 32)
        header2 = _header(2, header1.hash)
        store.stage_header(header1, source_node="node-a")
        store.stage_header(header2, source_node="node-a")
        store.stage_block_payload(
            header1,
            message_type=MessageType.BODY,
            payload=body_payload,
            source_node="node-a",
        )
        store.stage_block_payload(
            header2,
            message_type=MessageType.BODY,
            payload=body_payload,
            source_node="node-a",
        )
    finally:
        store.close()

    result = run_derive(DeriveConfig(state_db_path=str(db_path), progress_every=10))

    assert result.target_height == 2
    assert result.synced_height == 2
    assert result.applied_blocks == 2
    assert result.outputs_seen == 0
    assert result.resolved_spends == 0
    assert result.unresolved_spends == 0

    store = StateStore(str(db_path))
    try:
        assert store.last_synced_height() == 2
        row = _inspect_row(db_path, "SELECT COUNT(*) AS n FROM outputs")
        assert row is not None
        assert row["n"] == 0
    finally:
        store.close()


def test_state_store_apply_block_marks_staged_header_as_applied(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"

    store = StateStore(str(db_path))
    try:
        header = _header(1, "00" * 32)
        block = DecodedBlock(
            header=header,
            inputs=[],
            outputs=[],
            counts=TxCounts(inputs=0, outputs=0, kernels=0, kernels_mixed=False),
            offset=None,
        )

        store.stage_header(header, source_node="node-a")
        store.apply_block(header, block)

        row = _inspect_row(db_path, "SELECT source_node, applied FROM headers WHERE height = 1")
        assert row is not None
        assert row["source_node"] == "node-a"
        assert row["applied"] == 1
    finally:
        store.close()


def test_state_store_imports_treasury_outputs_and_reconciles_missing_inputs(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.sqlite3"

    store = StateStore(str(db_path))
    try:
        treasury_output = _treasury_output("ee")
        header = _header(1, "00" * 32)
        block = DecodedBlock(
            header=header,
            inputs=[TxInput(commitment=treasury_output.commitment)],
            outputs=[],
            counts=TxCounts(inputs=1, outputs=0, kernels=0, kernels_mixed=False),
            offset=None,
        )

        stats = store.apply_block(header, block)
        assert stats.unresolved_spends == 1

        store.store_treasury_payload(
            b"treasury-payload",
            payload_sha256="payload-hash",
            source_node="node-a",
        )
        import_stats = store.import_treasury_outputs(
            [treasury_output],
            payload_sha256="payload-hash",
        )

        assert import_stats.inserted_outputs == 1
        assert import_stats.reconciled_spends == 1
        assert store.treasury_payload_hash() == "payload-hash"
        assert store.treasury_imported_payload_hash() == "payload-hash"

        row = _inspect_row(
            db_path,
            "SELECT create_height, spent_height, maturity_height FROM outputs",
        )
        assert row is not None
        assert row["create_height"] == 0
        assert row["spent_height"] == 1
        assert row["maturity_height"] == 0

        missing = _inspect_row(db_path, "SELECT COUNT(*) AS n FROM missing_inputs")
        assert missing is not None
        assert missing["n"] == 0

        repeated = store.import_treasury_outputs(
            [treasury_output],
            payload_sha256="payload-hash",
        )
        assert repeated.inserted_outputs == 0
        assert repeated.reconciled_spends == 0
    finally:
        store.close()


def test_state_store_tracks_contiguous_staged_height_with_gaps(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    body_payload = _empty_body_payload()

    store = StateStore(str(db_path))
    try:
        header1 = _header(1, "00" * 32)
        header2 = _header(2, header1.hash)
        header3 = _header(3, header2.hash)

        store.stage_header(header1, source_node="node-a")
        store.stage_header(header2, source_node="node-a")
        store.stage_header(header3, source_node="node-a")

        missing_before = list(store.iter_missing_staged_headers(start_height=1, stop_height=3))
        assert [header.height for header in missing_before] == [1, 2, 3]

        store.stage_block_payload(
            header2,
            message_type=MessageType.BODY,
            payload=body_payload,
            source_node="node-b",
        )
        assert store.last_staged_height() == 0

        missing_mid = list(store.iter_missing_staged_headers(start_height=1, stop_height=3))
        assert [header.height for header in missing_mid] == [1, 3]

        store.stage_block_payload(
            header1,
            message_type=MessageType.BODY,
            payload=body_payload,
            source_node="node-c",
        )
        assert store.last_staged_height() == 2

        store.stage_block_payload(
            header3,
            message_type=MessageType.BODY,
            payload=body_payload,
            source_node="node-b",
        )
        assert store.last_staged_height() == 3
    finally:
        store.close()


def test_state_store_supports_sparse_staged_header_ranges(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"
    body_payload = _empty_body_payload()

    store = StateStore(str(db_path))
    try:
        header3 = _header(3, f"{2:064x}")
        header4 = _header(4, header3.hash)
        header5 = _header(5, header4.hash)

        store.stage_header(header3, source_node="node-a")
        store.stage_header(header4, source_node="node-a")
        store.stage_header(header5, source_node="node-a")

        assert list(store.iter_missing_staged_header_heights(start_height=3, stop_height=5)) == []
        assert list(store.iter_missing_staged_header_heights(start_height=1, stop_height=5)) == [1, 2]

        store.stage_block_payload(
            header3,
            message_type=MessageType.BODY,
            payload=body_payload,
            source_node="node-a",
        )
        store.stage_block_payload(
            header4,
            message_type=MessageType.BODY,
            payload=body_payload,
            source_node="node-a",
        )
        store.stage_block_payload(
            header5,
            message_type=MessageType.BODY,
            payload=body_payload,
            source_node="node-a",
        )

        assert store.last_staged_height() == 0
        staged = list(store.iter_staged_blocks(start_height=3, stop_height=5))
        assert [record.header.height for record in staged] == [3, 4, 5]
    finally:
        store.close()


def test_state_store_bootstraps_headers_and_staged_blocks_schema(tmp_path: Path) -> None:
    db_path = tmp_path / "state.sqlite3"

    store = StateStore(str(db_path))
    try:
        tables = {row["name"] for row in _inspect_rows(db_path, "SELECT name FROM sqlite_master WHERE type = 'table'")}
        assert {"headers", "staged_blocks", "outputs", "missing_inputs", "state_metadata", "treasury_payload"} <= tables

        header_columns = {row["name"] for row in _inspect_rows(db_path, "PRAGMA table_info(headers)")}
        assert "source_node" in header_columns
        assert "applied" in header_columns

        foreign_keys = _inspect_rows(db_path, "PRAGMA foreign_key_list(staged_blocks)")
        assert len(foreign_keys) == 1
        assert foreign_keys[0]["table"] == "headers"

        staged_indexes = {row["name"] for row in _inspect_rows(db_path, "PRAGMA index_list(staged_blocks)")}
        assert "idx_staged_blocks_hash" in staged_indexes
    finally:
        store.close()