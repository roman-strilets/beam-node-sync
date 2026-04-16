"""SQLAlchemy-backed persistent state for the Beam sync prototype."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import and_, create_engine, delete, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine, URL
from sqlalchemy.orm import Session, sessionmaker

from .db_models import (
    Base,
    HeaderEntity,
    MissingInputEntity,
    OutputEntity,
    StagedBlockEntity,
    StateMetadataEntity,
    TreasuryPayloadEntity,
)
from .models import StagedBlockRecord
from .protocol import MessageType
from .protocol_models import BlockHeader, BlockOutput, DecodedBlock
from .utils import format_commitment


COINBASE_MATURITY = 240
TREASURY_CREATE_BLOCK_HASH = "00" * 32
TREASURY_IMPORTED_PAYLOAD_SHA256_KEY = "treasury_imported_payload_sha256"


@dataclass(frozen=True)
class ApplyStats:
    """Per-block state application statistics."""

    inserted_outputs: int
    resolved_spends: int
    unresolved_spends: int


@dataclass(frozen=True)
class TreasuryImportStats:
    """Treasury bootstrap statistics."""

    inserted_outputs: int
    reconciled_spends: int


class StateStore:
    """Persist synced headers and output state in SQLite via SQLAlchemy ORM."""

    def __init__(self, path: str):
        self.path = path
        db_path = Path(path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._engine: Engine = self._create_engine(db_path)
        Base.metadata.create_all(self._engine)
        self._session_factory = sessionmaker(self._engine, expire_on_commit=False)

    def close(self) -> None:
        self._engine.dispose()

    @staticmethod
    def _create_engine(db_path: Path) -> Engine:
        return create_engine(URL.create("sqlite+pysqlite", database=str(db_path)))

    @contextmanager
    def _read_session(self) -> Iterator[Session]:
        session = self._session_factory()
        try:
            yield session
        finally:
            session.close()

    @contextmanager
    def _write_session(self) -> Iterator[Session]:
        session = self._session_factory()
        try:
            with session.begin():
                yield session
        finally:
            session.close()

    @staticmethod
    def _header_entity(session: Session, height: int) -> HeaderEntity | None:
        return session.get(HeaderEntity, height)

    def _assert_header_matches(
        self,
        entity: HeaderEntity,
        header: BlockHeader,
        *,
        context: str,
    ) -> None:
        if self._entity_to_header(entity) != header:
            raise RuntimeError(f"{context} mismatch at height {header.height}")

    @staticmethod
    def _entity_to_header(entity: HeaderEntity) -> BlockHeader:
        return BlockHeader(
            height=entity.height,
            hash=entity.hash,
            previous_hash=entity.previous_hash,
            chainwork=entity.chainwork,
            kernels=entity.kernels,
            definition=entity.definition,
            timestamp=entity.timestamp,
            packed_difficulty=entity.packed_difficulty,
            difficulty=entity.difficulty,
            rules_hash=entity.rules_hash,
            pow_indices_hex=entity.pow_indices_hex,
            pow_nonce_hex=entity.pow_nonce_hex,
        )

    @staticmethod
    def _last_synced_height_in_session(session: Session) -> int:
        value = session.scalar(
            select(func.coalesce(func.max(HeaderEntity.height), 0)).where(
                HeaderEntity.applied.is_(True)
            )
        )
        return int(value or 0)

    @staticmethod
    def _last_header_hash_in_session(session: Session) -> str | None:
        return session.scalar(
            select(HeaderEntity.hash)
            .where(HeaderEntity.applied.is_(True))
            .order_by(HeaderEntity.height.desc())
            .limit(1)
        )

    @staticmethod
    def _get_metadata_in_session(session: Session, key: str) -> str | None:
        entity = session.get(StateMetadataEntity, key)
        return None if entity is None else entity.value

    @staticmethod
    def _set_metadata_in_session(session: Session, key: str, value: str) -> None:
        session.execute(
            sqlite_insert(StateMetadataEntity)
            .values(key=key, value=value)
            .on_conflict_do_update(
                index_elements=[StateMetadataEntity.key],
                set_={"value": value},
            )
        )

    def last_synced_height(self) -> int:
        with self._read_session() as session:
            return self._last_synced_height_in_session(session)

    def last_header_hash(self) -> str | None:
        with self._read_session() as session:
            return self._last_header_hash_in_session(session)

    def last_staged_header_height(self) -> int:
        with self._read_session() as session:
            value = session.scalar(select(func.coalesce(func.max(HeaderEntity.height), 0)))
            return int(value or 0)

    def last_staged_header_hash(self) -> str | None:
        with self._read_session() as session:
            return session.scalar(
                select(HeaderEntity.hash)
                .order_by(HeaderEntity.height.desc())
                .limit(1)
            )

    def last_staged_height(self) -> int:
        height = 0
        expected = 1
        with self._read_session() as session:
            rows = session.execute(
                select(
                    HeaderEntity.height,
                    HeaderEntity.applied,
                    StagedBlockEntity.height,
                )
                .outerjoin(
                    StagedBlockEntity,
                    and_(
                        StagedBlockEntity.height == HeaderEntity.height,
                        StagedBlockEntity.block_hash == HeaderEntity.hash,
                    ),
                )
                .order_by(HeaderEntity.height.asc())
            )
            for current, applied, staged_height in rows:
                current_height = int(current)
                if current_height != expected:
                    break
                if not bool(applied) and staged_height is None:
                    break
                height = current_height
                expected += 1
        return height

    def last_staged_hash(self) -> str | None:
        height = self.last_staged_height()
        if height <= 0:
            return None
        with self._read_session() as session:
            entity = self._header_entity(session, height)
            return None if entity is None else entity.hash

    def _get_metadata(self, key: str) -> str | None:
        with self._read_session() as session:
            return self._get_metadata_in_session(session, key)

    def _set_metadata(self, key: str, value: str) -> None:
        with self._write_session() as session:
            self._set_metadata_in_session(session, key, value)

    def treasury_payload_hash(self) -> str | None:
        with self._read_session() as session:
            entity = session.get(TreasuryPayloadEntity, 1)
            return None if entity is None else entity.payload_sha256

    def treasury_payload(self) -> bytes | None:
        with self._read_session() as session:
            entity = session.get(TreasuryPayloadEntity, 1)
            return None if entity is None else bytes(entity.payload)

    def treasury_imported_payload_hash(self) -> str | None:
        return self._get_metadata(TREASURY_IMPORTED_PAYLOAD_SHA256_KEY)

    def store_treasury_payload(
        self,
        payload: bytes,
        *,
        payload_sha256: str,
        source_node: str,
    ) -> None:
        with self._write_session() as session:
            entity = session.get(TreasuryPayloadEntity, 1)
            if entity is not None:
                existing_hash = entity.payload_sha256
                if existing_hash != payload_sha256:
                    raise RuntimeError(
                        "stored treasury payload hash mismatch: expected "
                        f"{existing_hash}, got {payload_sha256}"
                    )
                return

            session.add(
                TreasuryPayloadEntity(
                    singleton=1,
                    payload_sha256=payload_sha256,
                    payload=payload,
                    source_node=source_node,
                )
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
        with self._write_session() as session:
            existing = self._header_entity(session, header.height)
            if existing is not None:
                self._assert_header_matches(existing, header, context="stored header")
                if existing.source_node is None:
                    existing.source_node = source_node
                return

            previous_entity = self._header_entity(session, header.height - 1)
            if previous_entity is not None and header.previous_hash != previous_entity.hash:
                raise RuntimeError(
                    "staged header chain continuity check failed: previous hash does "
                    f"not match staged header height {header.height - 1}"
                )

            next_entity = self._header_entity(session, header.height + 1)
            if next_entity is not None and next_entity.previous_hash != header.hash:
                raise RuntimeError(
                    "staged header chain continuity check failed: stored header at "
                    f"height {header.height + 1} does not reference {header.hash}"
                )

            session.add(
                HeaderEntity(
                    height=header.height,
                    hash=header.hash,
                    previous_hash=header.previous_hash,
                    chainwork=header.chainwork,
                    kernels=header.kernels,
                    definition=header.definition,
                    timestamp=header.timestamp,
                    packed_difficulty=header.packed_difficulty,
                    difficulty=header.difficulty,
                    rules_hash=header.rules_hash,
                    pow_indices_hex=header.pow_indices_hex,
                    pow_nonce_hex=header.pow_nonce_hex,
                    source_node=source_node,
                    applied=False,
                )
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

        expected = start_height
        with self._read_session() as session:
            stmt = select(HeaderEntity.height).where(HeaderEntity.height >= start_height)
            if stop_height is not None:
                stmt = stmt.where(HeaderEntity.height <= stop_height)
            stmt = stmt.order_by(HeaderEntity.height.asc())

            for current in session.scalars(stmt):
                current_height = int(current)
                while expected < current_height:
                    yield expected
                    expected += 1
                expected = current_height + 1

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

        with self._write_session() as session:
            header_entity = self._header_entity(session, header.height)
            if header_entity is None:
                raise RuntimeError(
                    f"cannot stage block payload for height {header.height} before its header is staged"
                )
            expected_hash = header_entity.hash
            if expected_hash != header.hash:
                raise RuntimeError(
                    f"staged header hash mismatch at height {header.height}: expected {expected_hash}, got {header.hash}"
                )

            session.execute(
                sqlite_insert(StagedBlockEntity)
                .values(
                    height=header.height,
                    block_hash=header.hash,
                    message_type=int(message_type),
                    payload=payload,
                    source_node=source_node,
                )
                .on_conflict_do_update(
                    index_elements=[StagedBlockEntity.height],
                    set_={
                        "block_hash": header.hash,
                        "message_type": int(message_type),
                        "payload": payload,
                        "source_node": source_node,
                    },
                )
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

        with self._read_session() as session:
            stmt = select(HeaderEntity).where(HeaderEntity.height >= start_height)
            if stop_height is not None:
                stmt = stmt.where(HeaderEntity.height <= stop_height)
            stmt = stmt.order_by(HeaderEntity.height.asc())

            for entity in session.scalars(stmt):
                yield self._entity_to_header(entity)

    def iter_missing_staged_headers(
        self,
        *,
        start_height: int = 1,
        stop_height: int | None = None,
    ) -> Iterator[BlockHeader]:
        """Yield staged headers that do not yet have a corresponding body payload."""
        if start_height <= 0:
            raise ValueError(f"start_height must be > 0, got {start_height}")

        with self._read_session() as session:
            stmt = (
                select(HeaderEntity)
                .outerjoin(
                    StagedBlockEntity,
                    and_(
                        StagedBlockEntity.height == HeaderEntity.height,
                        StagedBlockEntity.block_hash == HeaderEntity.hash,
                    ),
                )
                .where(
                    HeaderEntity.height >= start_height,
                    HeaderEntity.applied.is_(False),
                    StagedBlockEntity.height.is_(None),
                )
            )
            if stop_height is not None:
                stmt = stmt.where(HeaderEntity.height <= stop_height)
            stmt = stmt.order_by(HeaderEntity.height.asc())

            for entity in session.scalars(stmt):
                yield self._entity_to_header(entity)

    def iter_staged_blocks(
        self,
        *,
        start_height: int = 1,
        stop_height: int | None = None,
    ) -> Iterator[StagedBlockRecord]:
        """Yield staged block payloads in height order for replay."""
        if start_height <= 0:
            raise ValueError(f"start_height must be > 0, got {start_height}")

        with self._read_session() as session:
            stmt = (
                select(
                    HeaderEntity,
                    StagedBlockEntity.message_type,
                    StagedBlockEntity.payload,
                    StagedBlockEntity.source_node,
                )
                .join(
                    StagedBlockEntity,
                    and_(
                        StagedBlockEntity.height == HeaderEntity.height,
                        StagedBlockEntity.block_hash == HeaderEntity.hash,
                    ),
                )
                .where(
                    HeaderEntity.height >= start_height,
                    HeaderEntity.applied.is_(False),
                )
            )
            if stop_height is not None:
                stmt = stmt.where(HeaderEntity.height <= stop_height)
            stmt = stmt.order_by(HeaderEntity.height.asc())

            for header_entity, message_type, payload, source_node in session.execute(stmt):
                yield StagedBlockRecord(
                    header=self._entity_to_header(header_entity),
                    body_message_type=MessageType(int(message_type)),
                    body_payload=bytes(payload),
                    source_node=str(source_node),
                )

    def _insert_output_record(
        self,
        session: Session,
        *,
        commitment: str,
        commitment_x: str,
        commitment_y: bool,
        create_height: int,
        create_block_hash: str,
        spent_height: int | None,
        coinbase: bool,
        recovery_only: bool,
        incubation: int,
        maturity_height: int,
        has_confidential_proof: bool,
        has_public_proof: bool,
        has_asset_proof: bool,
        extra_flags: int | None,
    ) -> OutputEntity:
        entity = OutputEntity(
            commitment=commitment,
            commitment_x=commitment_x,
            commitment_y=commitment_y,
            create_height=create_height,
            create_block_hash=create_block_hash,
            spent_height=spent_height,
            coinbase=coinbase,
            recovery_only=recovery_only,
            incubation=incubation,
            maturity_height=maturity_height,
            has_confidential_proof=has_confidential_proof,
            has_public_proof=has_public_proof,
            has_asset_proof=has_asset_proof,
            extra_flags=extra_flags,
        )
        session.add(entity)
        session.flush()
        return entity

    def import_treasury_outputs(
        self,
        outputs: Sequence[BlockOutput],
        *,
        payload_sha256: str,
    ) -> TreasuryImportStats:
        inserted_outputs = 0
        reconciled_spends = 0

        with self._write_session() as session:
            imported_hash = self._get_metadata_in_session(
                session,
                TREASURY_IMPORTED_PAYLOAD_SHA256_KEY,
            )
            if imported_hash is not None:
                if imported_hash != payload_sha256:
                    raise RuntimeError(
                        "imported treasury payload hash mismatch: expected "
                        f"{imported_hash}, got {payload_sha256}"
                    )
                return TreasuryImportStats(inserted_outputs=0, reconciled_spends=0)

            stored_payload = session.get(TreasuryPayloadEntity, 1)
            if stored_payload is not None and stored_payload.payload_sha256 != payload_sha256:
                raise RuntimeError(
                    "stored treasury payload hash mismatch: expected "
                    f"{stored_payload.payload_sha256}, got {payload_sha256}"
                )

            for output in outputs:
                commitment = format_commitment(output.commitment)
                incubation = int(output.incubation or 0)
                inserted_output = self._insert_output_record(
                    session,
                    commitment=commitment,
                    commitment_x=output.commitment.x,
                    commitment_y=output.commitment.y,
                    create_height=0,
                    create_block_hash=TREASURY_CREATE_BLOCK_HASH,
                    spent_height=None,
                    coinbase=output.coinbase,
                    recovery_only=output.recovery_only,
                    incubation=incubation,
                    maturity_height=incubation,
                    has_confidential_proof=output.confidential_proof is not None,
                    has_public_proof=output.public_proof is not None,
                    has_asset_proof=output.asset_proof is not None,
                    extra_flags=output.extra_flags,
                )
                inserted_outputs += 1

                missing_height = session.scalar(
                    select(func.min(MissingInputEntity.block_height)).where(
                        MissingInputEntity.commitment == commitment
                    )
                )
                if missing_height is not None:
                    inserted_output.spent_height = int(missing_height)
                    session.execute(
                        delete(MissingInputEntity).where(
                            MissingInputEntity.commitment == commitment
                        )
                    )
                    reconciled_spends += 1

            self._set_metadata_in_session(
                session,
                TREASURY_IMPORTED_PAYLOAD_SHA256_KEY,
                payload_sha256,
            )

        return TreasuryImportStats(
            inserted_outputs=inserted_outputs,
            reconciled_spends=reconciled_spends,
        )

    def apply_block(self, header: BlockHeader, block: DecodedBlock) -> ApplyStats:
        """Apply one decoded block to the local SQLite state."""
        inserted_outputs = 0
        resolved_spends = 0
        unresolved_spends = 0

        with self._write_session() as session:
            last_height = self._last_synced_height_in_session(session)
            if header.height != last_height + 1:
                raise RuntimeError(
                    f"expected next block height {last_height + 1}, got {header.height}"
                )
            if last_height > 0:
                last_hash = self._last_header_hash_in_session(session)
                if last_hash is not None and header.previous_hash != last_hash:
                    raise RuntimeError(
                        "header chain continuity check failed: previous hash does not "
                        f"match local height {last_height}"
                    )

            existing = self._header_entity(session, header.height)
            if existing is None:
                session.add(
                    HeaderEntity(
                        height=header.height,
                        hash=header.hash,
                        previous_hash=header.previous_hash,
                        chainwork=header.chainwork,
                        kernels=header.kernels,
                        definition=header.definition,
                        timestamp=header.timestamp,
                        packed_difficulty=header.packed_difficulty,
                        difficulty=header.difficulty,
                        rules_hash=header.rules_hash,
                        pow_indices_hex=header.pow_indices_hex,
                        pow_nonce_hex=header.pow_nonce_hex,
                        source_node=None,
                        applied=True,
                    )
                )
            else:
                self._assert_header_matches(existing, header, context="stored header")
                existing.applied = True

            for tx_input in block.inputs:
                commitment = format_commitment(tx_input.commitment)
                output_entity = session.scalars(
                    select(OutputEntity)
                    .where(
                        OutputEntity.commitment == commitment,
                        OutputEntity.spent_height.is_(None),
                    )
                    .order_by(OutputEntity.output_id.desc())
                    .limit(1)
                ).first()
                if output_entity is not None:
                    output_entity.spent_height = header.height
                    resolved_spends += 1
                else:
                    unresolved_spends += 1
                    session.execute(
                        sqlite_insert(MissingInputEntity)
                        .values(block_height=header.height, commitment=commitment)
                        .on_conflict_do_nothing(
                            index_elements=[
                                MissingInputEntity.block_height,
                                MissingInputEntity.commitment,
                            ]
                        )
                    )

            for output in block.outputs:
                commitment = format_commitment(output.commitment)
                incubation = int(output.incubation or 0)
                maturity_height = header.height + incubation + (
                    COINBASE_MATURITY if output.coinbase else 0
                )
                self._insert_output_record(
                    session,
                    commitment=commitment,
                    commitment_x=output.commitment.x,
                    commitment_y=output.commitment.y,
                    create_height=header.height,
                    create_block_hash=header.hash,
                    spent_height=None,
                    coinbase=output.coinbase,
                    recovery_only=output.recovery_only,
                    incubation=incubation,
                    maturity_height=maturity_height,
                    has_confidential_proof=output.confidential_proof is not None,
                    has_public_proof=output.public_proof is not None,
                    has_asset_proof=output.asset_proof is not None,
                    extra_flags=output.extra_flags,
                )
                inserted_outputs += 1

        return ApplyStats(
            inserted_outputs=inserted_outputs,
            resolved_spends=resolved_spends,
            unresolved_spends=unresolved_spends,
        )
