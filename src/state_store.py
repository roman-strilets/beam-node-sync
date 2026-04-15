"""SQLite-backed persistent state for the Beam sync prototype."""

from __future__ import annotations

from collections.abc import Iterator
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .models import StagedBlockRecord, UtxoExportRecord
from .protocol import MessageType
from .protocol_models import BlockHeader, DecodedBlock
from .storage import JsonLineWriter
from .utils import format_commitment


COINBASE_MATURITY = 240


@dataclass(frozen=True)
class ApplyStats:
    """Per-block state application statistics."""

    inserted_outputs: int
    resolved_spends: int
    unresolved_spends: int


class StateStore:
    """Persist synced headers and output state in SQLite."""

    def __init__(self, path: str):
        self.path = path
        db_path = Path(path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def close(self) -> None:
        self._conn.close()

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS headers (
                    height INTEGER PRIMARY KEY,
                    hash TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    chainwork TEXT NOT NULL,
                    kernels TEXT NOT NULL,
                    definition TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    packed_difficulty INTEGER NOT NULL,
                    difficulty REAL NOT NULL,
                    rules_hash TEXT,
                    pow_indices_hex TEXT NOT NULL,
                    pow_nonce_hex TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS missing_inputs (
                    block_height INTEGER NOT NULL,
                    commitment TEXT NOT NULL,
                    PRIMARY KEY(block_height, commitment)
                );
                """
            )

            self._init_outputs_schema()
            self._init_staged_schema()

    def _init_staged_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS staged_headers (
                height INTEGER PRIMARY KEY,
                hash TEXT NOT NULL,
                previous_hash TEXT NOT NULL,
                chainwork TEXT NOT NULL,
                kernels TEXT NOT NULL,
                definition TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                packed_difficulty INTEGER NOT NULL,
                difficulty REAL NOT NULL,
                rules_hash TEXT,
                pow_indices_hex TEXT NOT NULL,
                pow_nonce_hex TEXT NOT NULL,
                source_node TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS staged_blocks (
                height INTEGER PRIMARY KEY,
                block_hash TEXT NOT NULL,
                message_type INTEGER NOT NULL,
                payload BLOB NOT NULL,
                source_node TEXT NOT NULL,
                FOREIGN KEY(height) REFERENCES staged_headers(height)
            );

            CREATE INDEX IF NOT EXISTS idx_staged_blocks_hash
                ON staged_blocks(block_hash);
            """
        )

    def _init_outputs_schema(self) -> None:
        columns = {
            str(row["name"]): row for row in self._conn.execute("PRAGMA table_info(outputs)")
        }
        if not columns:
            self._create_outputs_table()
            self._create_outputs_indexes()
            return

        if "output_id" not in columns:
            self._migrate_legacy_outputs_table()

        self._create_outputs_indexes()

    def _create_outputs_table(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS outputs (
                output_id INTEGER PRIMARY KEY AUTOINCREMENT,
                commitment TEXT NOT NULL,
                commitment_x TEXT NOT NULL,
                commitment_y INTEGER NOT NULL,
                create_height INTEGER NOT NULL,
                create_block_hash TEXT NOT NULL,
                spent_height INTEGER,
                coinbase INTEGER NOT NULL,
                recovery_only INTEGER NOT NULL,
                incubation INTEGER NOT NULL,
                maturity_height INTEGER NOT NULL,
                has_confidential_proof INTEGER NOT NULL,
                has_public_proof INTEGER NOT NULL,
                has_asset_proof INTEGER NOT NULL,
                extra_flags INTEGER
            )
            """
        )

    def _create_outputs_indexes(self) -> None:
        self._conn.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_outputs_unspent
                ON outputs(spent_height, create_height, output_id);

            CREATE INDEX IF NOT EXISTS idx_outputs_commitment_unspent
                ON outputs(commitment, spent_height, output_id DESC);
            """
        )

    def _migrate_legacy_outputs_table(self) -> None:
        self._conn.execute("DROP TABLE IF EXISTS outputs_new")
        self._create_outputs_table_for_migration("outputs_new")
        self._conn.execute(
            """
            INSERT INTO outputs_new (
                commitment,
                commitment_x,
                commitment_y,
                create_height,
                create_block_hash,
                spent_height,
                coinbase,
                recovery_only,
                incubation,
                maturity_height,
                has_confidential_proof,
                has_public_proof,
                has_asset_proof,
                extra_flags
            )
            SELECT
                commitment,
                commitment_x,
                commitment_y,
                create_height,
                create_block_hash,
                spent_height,
                coinbase,
                recovery_only,
                incubation,
                maturity_height,
                has_confidential_proof,
                has_public_proof,
                has_asset_proof,
                extra_flags
            FROM outputs
            ORDER BY rowid ASC
            """
        )
        self._conn.execute("DROP TABLE outputs")
        self._conn.execute("ALTER TABLE outputs_new RENAME TO outputs")

    def _create_outputs_table_for_migration(self, table_name: str) -> None:
        self._conn.execute(
            f"""
            CREATE TABLE {table_name} (
                output_id INTEGER PRIMARY KEY AUTOINCREMENT,
                commitment TEXT NOT NULL,
                commitment_x TEXT NOT NULL,
                commitment_y INTEGER NOT NULL,
                create_height INTEGER NOT NULL,
                create_block_hash TEXT NOT NULL,
                spent_height INTEGER,
                coinbase INTEGER NOT NULL,
                recovery_only INTEGER NOT NULL,
                incubation INTEGER NOT NULL,
                maturity_height INTEGER NOT NULL,
                has_confidential_proof INTEGER NOT NULL,
                has_public_proof INTEGER NOT NULL,
                has_asset_proof INTEGER NOT NULL,
                extra_flags INTEGER
            )
            """
        )

    def last_synced_height(self) -> int:
        row = self._conn.execute(
            "SELECT COALESCE(MAX(height), 0) AS height FROM headers"
        ).fetchone()
        return int(row["height"])

    def last_header_hash(self) -> str | None:
        row = self._conn.execute(
            "SELECT hash FROM headers ORDER BY height DESC LIMIT 1"
        ).fetchone()
        return None if row is None else str(row["hash"])

    def last_staged_header_height(self) -> int:
        row = self._conn.execute(
            "SELECT COALESCE(MAX(height), 0) AS height FROM staged_headers"
        ).fetchone()
        return int(row["height"])

    def last_staged_header_hash(self) -> str | None:
        row = self._conn.execute(
            "SELECT hash FROM staged_headers ORDER BY height DESC LIMIT 1"
        ).fetchone()
        return None if row is None else str(row["hash"])

    def last_staged_height(self) -> int:
        height = 0
        expected = 1
        rows = self._conn.execute(
            """
            SELECT sh.height
            FROM staged_headers AS sh
            JOIN staged_blocks AS sb
                ON sb.height = sh.height AND sb.block_hash = sh.hash
            ORDER BY sh.height ASC
            """
        )
        for row in rows:
            current = int(row["height"])
            if current != expected:
                break
            height = current
            expected += 1
        return height

    def last_staged_hash(self) -> str | None:
        height = self.last_staged_height()
        if height <= 0:
            return None
        row = self._conn.execute(
            "SELECT hash FROM staged_headers WHERE height = ?",
            (height,),
        ).fetchone()
        return None if row is None else str(row["hash"])

    @staticmethod
    def _row_to_header(row: sqlite3.Row) -> BlockHeader:
        return BlockHeader(
            height=int(row["height"]),
            hash=str(row["hash"]),
            previous_hash=str(row["previous_hash"]),
            chainwork=str(row["chainwork"]),
            kernels=str(row["kernels"]),
            definition=str(row["definition"]),
            timestamp=int(row["timestamp"]),
            packed_difficulty=int(row["packed_difficulty"]),
            difficulty=float(row["difficulty"]),
            rules_hash=None if row["rules_hash"] is None else str(row["rules_hash"]),
            pow_indices_hex=str(row["pow_indices_hex"]),
            pow_nonce_hex=str(row["pow_nonce_hex"]),
        )

    def stage_header(
        self,
        header: BlockHeader,
        *,
        source_node: str,
    ) -> None:
        """Persist one canonical staged header.

        Staged headers may be inserted as sparse ranges for stage-only workflows.
        When adjacent headers already exist, their hashes must still line up.
        """
        existing = self._conn.execute(
            """
            SELECT hash, previous_hash
            FROM staged_headers
            WHERE height = ?
            """,
            (header.height,),
        ).fetchone()
        if existing is not None:
            existing_hash = str(existing["hash"])
            if existing_hash != header.hash:
                raise RuntimeError(
                    f"staged header hash mismatch at height {header.height}: expected {existing_hash}, got {header.hash}"
                )
            if str(existing["previous_hash"]) != header.previous_hash:
                raise RuntimeError(
                    f"staged header previous hash mismatch at height {header.height}: expected {existing['previous_hash']}, got {header.previous_hash}"
                )
            return

        previous_row = self._conn.execute(
            "SELECT hash FROM staged_headers WHERE height = ?",
            (header.height - 1,),
        ).fetchone()
        if previous_row is not None and header.previous_hash != str(previous_row["hash"]):
            raise RuntimeError(
                "staged header chain continuity check failed: previous hash does "
                f"not match staged header height {header.height - 1}"
            )

        next_row = self._conn.execute(
            "SELECT previous_hash FROM staged_headers WHERE height = ?",
            (header.height + 1,),
        ).fetchone()
        if next_row is not None and str(next_row["previous_hash"]) != header.hash:
            raise RuntimeError(
                "staged header chain continuity check failed: stored header at "
                f"height {header.height + 1} does not reference {header.hash}"
            )

        with self._conn:
            self._conn.execute(
                """
                INSERT INTO staged_headers (
                    height, hash, previous_hash, chainwork, kernels, definition,
                    timestamp, packed_difficulty, difficulty, rules_hash,
                    pow_indices_hex, pow_nonce_hex, source_node
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    header.height,
                    header.hash,
                    header.previous_hash,
                    header.chainwork,
                    header.kernels,
                    header.definition,
                    header.timestamp,
                    header.packed_difficulty,
                    header.difficulty,
                    header.rules_hash,
                    header.pow_indices_hex,
                    header.pow_nonce_hex,
                    source_node,
                ),
            )

    def iter_missing_staged_header_heights(
        self,
        *,
        start_height: int = 1,
        stop_height: int | None = None,
    ) -> Iterator[int]:
        """Yield heights that do not yet have a staged header."""
        if start_height <= 0:
            raise ValueError(f"start_height must be > 0, got {start_height}")
        if stop_height is not None and stop_height < start_height:
            return

        query = """
            SELECT height
            FROM staged_headers
            WHERE height >= ?
        """
        params: list[object] = [start_height]
        if stop_height is not None:
            query += " AND height <= ?"
            params.append(stop_height)
        query += " ORDER BY height ASC"

        expected = start_height
        for row in self._conn.execute(query, params):
            current = int(row["height"])
            while expected < current:
                yield expected
                expected += 1
            expected = current + 1

        if stop_height is not None:
            while expected <= stop_height:
                yield expected
                expected += 1

    def stage_block_payload(
        self,
        header: BlockHeader,
        *,
        message_type: MessageType,
        payload: bytes,
        source_node: str,
    ) -> None:
        """Persist one fetched raw block payload for a previously staged header."""
        if message_type not in {MessageType.BODY, MessageType.BODY_PACK}:
            raise ValueError(f"unsupported staged message type: {message_type}")

        row = self._conn.execute(
            "SELECT hash FROM staged_headers WHERE height = ?",
            (header.height,),
        ).fetchone()
        if row is None:
            raise RuntimeError(
                f"cannot stage block payload for height {header.height} before its header is staged"
            )
        expected_hash = str(row["hash"])
        if expected_hash != header.hash:
            raise RuntimeError(
                f"staged header hash mismatch at height {header.height}: expected {expected_hash}, got {header.hash}"
            )

        with self._conn:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO staged_blocks (
                    height, block_hash, message_type, payload, source_node
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    header.height,
                    header.hash,
                    int(message_type),
                    sqlite3.Binary(payload),
                    source_node,
                ),
            )

    def iter_staged_headers(
        self,
        *,
        start_height: int = 1,
        stop_height: int | None = None,
    ) -> Iterator[BlockHeader]:
        """Yield canonical staged headers in height order."""
        if start_height <= 0:
            raise ValueError(f"start_height must be > 0, got {start_height}")

        query = """
            SELECT
                height,
                hash,
                previous_hash,
                chainwork,
                kernels,
                definition,
                timestamp,
                packed_difficulty,
                difficulty,
                rules_hash,
                pow_indices_hex,
                pow_nonce_hex
            FROM staged_headers
            WHERE height >= ?
        """
        params: list[object] = [start_height]
        if stop_height is not None:
            query += " AND height <= ?"
            params.append(stop_height)
        query += " ORDER BY height ASC"

        for row in self._conn.execute(query, params):
            yield self._row_to_header(row)

    def iter_missing_staged_headers(
        self,
        *,
        start_height: int = 1,
        stop_height: int | None = None,
    ) -> Iterator[BlockHeader]:
        """Yield staged headers that do not yet have a corresponding body payload."""
        if start_height <= 0:
            raise ValueError(f"start_height must be > 0, got {start_height}")

        query = """
            SELECT
                sh.height,
                sh.hash,
                sh.previous_hash,
                sh.chainwork,
                sh.kernels,
                sh.definition,
                sh.timestamp,
                sh.packed_difficulty,
                sh.difficulty,
                sh.rules_hash,
                sh.pow_indices_hex,
                sh.pow_nonce_hex
            FROM staged_headers AS sh
            LEFT JOIN staged_blocks AS sb
                ON sb.height = sh.height AND sb.block_hash = sh.hash
            WHERE sh.height >= ? AND sb.height IS NULL
        """
        params: list[object] = [start_height]
        if stop_height is not None:
            query += " AND sh.height <= ?"
            params.append(stop_height)
        query += " ORDER BY sh.height ASC"

        for row in self._conn.execute(query, params):
            yield self._row_to_header(row)

    def iter_staged_blocks(
        self,
        *,
        start_height: int = 1,
        stop_height: int | None = None,
    ) -> Iterator[StagedBlockRecord]:
        """Yield staged block payloads in height order for replay."""
        if start_height <= 0:
            raise ValueError(f"start_height must be > 0, got {start_height}")

        query = """
            SELECT
                sh.height,
                sh.hash,
                sh.previous_hash,
                sh.chainwork,
                sh.kernels,
                sh.definition,
                sh.timestamp,
                sh.packed_difficulty,
                sh.difficulty,
                sh.rules_hash,
                sh.pow_indices_hex,
                sh.pow_nonce_hex,
                sb.message_type,
                sb.payload,
                sb.source_node AS block_source_node
            FROM staged_headers AS sh
            JOIN staged_blocks AS sb
                ON sb.height = sh.height AND sb.block_hash = sh.hash
            WHERE sh.height >= ?
        """
        params: list[object] = [start_height]
        if stop_height is not None:
            query += " AND sh.height <= ?"
            params.append(stop_height)
        query += " ORDER BY sh.height ASC"

        for row in self._conn.execute(query, params):
            yield StagedBlockRecord(
                header=self._row_to_header(row),
                body_message_type=MessageType(int(row["message_type"])),
                body_payload=bytes(row["payload"]),
                source_node=str(row["block_source_node"]),
            )

    def apply_block(self, header: BlockHeader, block: DecodedBlock) -> ApplyStats:
        """Apply one decoded block to the local SQLite state."""
        last_height = self.last_synced_height()
        if header.height != last_height + 1:
            raise RuntimeError(
                f"expected next block height {last_height + 1}, got {header.height}"
            )
        if last_height > 0:
            last_hash = self.last_header_hash()
            if last_hash is not None and header.previous_hash != last_hash:
                raise RuntimeError(
                    "header chain continuity check failed: previous hash does not "
                    f"match local height {last_height}"
                )

        inserted_outputs = 0
        resolved_spends = 0
        unresolved_spends = 0

        with self._conn:
            self._conn.execute(
                """
                INSERT INTO headers (
                    height, hash, previous_hash, chainwork, kernels, definition,
                    timestamp, packed_difficulty, difficulty, rules_hash,
                    pow_indices_hex, pow_nonce_hex
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    header.height,
                    header.hash,
                    header.previous_hash,
                    header.chainwork,
                    header.kernels,
                    header.definition,
                    header.timestamp,
                    header.packed_difficulty,
                    header.difficulty,
                    header.rules_hash,
                    header.pow_indices_hex,
                    header.pow_nonce_hex,
                ),
            )

            for tx_input in block.inputs:
                commitment = format_commitment(tx_input.commitment)
                row = self._conn.execute(
                    """
                    SELECT output_id
                    FROM outputs
                    WHERE commitment = ? AND spent_height IS NULL
                    ORDER BY output_id DESC
                    LIMIT 1
                    """,
                    (commitment,),
                ).fetchone()
                if row is not None:
                    self._conn.execute(
                        "UPDATE outputs SET spent_height = ? WHERE output_id = ?",
                        (header.height, int(row["output_id"])),
                    )
                    resolved_spends += 1
                else:
                    unresolved_spends += 1
                    self._conn.execute(
                        "INSERT OR IGNORE INTO missing_inputs(block_height, commitment) VALUES (?, ?)",
                        (header.height, commitment),
                    )

            for output in block.outputs:
                commitment = format_commitment(output.commitment)
                incubation = int(output.incubation or 0)
                maturity_height = header.height + incubation + (
                    COINBASE_MATURITY if output.coinbase else 0
                )
                self._conn.execute(
                    """
                    INSERT INTO outputs (
                        commitment, commitment_x, commitment_y, create_height,
                        create_block_hash, spent_height, coinbase, recovery_only,
                        incubation, maturity_height, has_confidential_proof,
                        has_public_proof, has_asset_proof, extra_flags
                    ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        commitment,
                        output.commitment.x,
                        1 if output.commitment.y else 0,
                        header.height,
                        header.hash,
                        1 if output.coinbase else 0,
                        1 if output.recovery_only else 0,
                        incubation,
                        maturity_height,
                        1 if output.confidential_proof is not None else 0,
                        1 if output.public_proof is not None else 0,
                        1 if output.asset_proof is not None else 0,
                        output.extra_flags,
                    ),
                )
                inserted_outputs += 1

        return ApplyStats(
            inserted_outputs=inserted_outputs,
            resolved_spends=resolved_spends,
            unresolved_spends=unresolved_spends,
        )

    def export_unspent(self, writer: JsonLineWriter, tip_height: int) -> int:
        """Write the current unspent output set to ``writer`` as JSON lines."""
        count = 0
        rows = self._conn.execute(
            """
            SELECT
                commitment,
                commitment_x,
                commitment_y,
                create_height,
                maturity_height,
                coinbase,
                recovery_only,
                incubation,
                has_confidential_proof,
                has_public_proof,
                has_asset_proof,
                extra_flags
            FROM outputs
            WHERE spent_height IS NULL
            ORDER BY create_height ASC, output_id ASC
            """
        )
        for row in rows:
            writer.write(
                UtxoExportRecord(
                    commitment=str(row["commitment"]),
                    commitment_x=str(row["commitment_x"]),
                    commitment_y=bool(row["commitment_y"]),
                    create_height=int(row["create_height"]),
                    maturity_height=int(row["maturity_height"]),
                    mature=tip_height >= int(row["maturity_height"]),
                    coinbase=bool(row["coinbase"]),
                    recovery_only=bool(row["recovery_only"]),
                    incubation=int(row["incubation"]),
                    has_confidential_proof=bool(row["has_confidential_proof"]),
                    has_public_proof=bool(row["has_public_proof"]),
                    has_asset_proof=bool(row["has_asset_proof"]),
                    extra_flags=row["extra_flags"],
                )
            )
            count += 1
        return count