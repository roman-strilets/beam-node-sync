"""SQLAlchemy ORM models for beam-node-sync persistent state."""

from __future__ import annotations

from sqlalchemy import (
    Boolean,
    Float,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base declarative class for SQLite-backed state."""


class HeaderEntity(Base):
    """Canonical header row, whether only staged or already applied."""

    __tablename__ = "headers"

    height: Mapped[int] = mapped_column(Integer, primary_key=True)
    hash: Mapped[str] = mapped_column(String, nullable=False)
    previous_hash: Mapped[str] = mapped_column(String, nullable=False)
    chainwork: Mapped[str] = mapped_column(Text, nullable=False)
    kernels: Mapped[str] = mapped_column(Text, nullable=False)
    definition: Mapped[str] = mapped_column(Text, nullable=False)
    timestamp: Mapped[int] = mapped_column(Integer, nullable=False)
    packed_difficulty: Mapped[int] = mapped_column(Integer, nullable=False)
    difficulty: Mapped[float] = mapped_column(Float, nullable=False)
    rules_hash: Mapped[str | None] = mapped_column(String, nullable=True)
    pow_indices_hex: Mapped[str] = mapped_column(Text, nullable=False)
    pow_nonce_hex: Mapped[str] = mapped_column(Text, nullable=False)
    source_node: Mapped[str | None] = mapped_column(Text, nullable=True)
    applied: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class StagedBlockEntity(Base):
    """Raw block payload captured for later replay."""

    __tablename__ = "staged_blocks"

    height: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("headers.height"),
        primary_key=True,
    )
    block_hash: Mapped[str] = mapped_column(String, nullable=False)
    message_type: Mapped[int] = mapped_column(Integer, nullable=False)
    payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    source_node: Mapped[str] = mapped_column(Text, nullable=False)


class OutputEntity(Base):
    """Derived output state row."""

    __tablename__ = "outputs"
    __table_args__ = ({"sqlite_autoincrement": True},)

    output_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    commitment: Mapped[str] = mapped_column(Text, nullable=False)
    commitment_x: Mapped[str] = mapped_column(Text, nullable=False)
    commitment_y: Mapped[bool] = mapped_column(Boolean, nullable=False)
    create_height: Mapped[int] = mapped_column(Integer, nullable=False)
    create_block_hash: Mapped[str] = mapped_column(Text, nullable=False)
    spent_height: Mapped[int | None] = mapped_column(Integer, nullable=True)
    coinbase: Mapped[bool] = mapped_column(Boolean, nullable=False)
    recovery_only: Mapped[bool] = mapped_column(Boolean, nullable=False)
    incubation: Mapped[int] = mapped_column(Integer, nullable=False)
    maturity_height: Mapped[int] = mapped_column(Integer, nullable=False)
    has_confidential_proof: Mapped[bool] = mapped_column(Boolean, nullable=False)
    has_public_proof: Mapped[bool] = mapped_column(Boolean, nullable=False)
    has_asset_proof: Mapped[bool] = mapped_column(Boolean, nullable=False)
    extra_flags: Mapped[int | None] = mapped_column(Integer, nullable=True)


class MissingInputEntity(Base):
    """Input commitments seen before their creating output is known locally."""

    __tablename__ = "missing_inputs"

    block_height: Mapped[int] = mapped_column(Integer, primary_key=True)
    commitment: Mapped[str] = mapped_column(Text, primary_key=True)


Index("idx_headers_applied_height", HeaderEntity.applied, HeaderEntity.height)
Index("idx_staged_blocks_hash", StagedBlockEntity.block_hash)
Index("idx_outputs_unspent", OutputEntity.spent_height, OutputEntity.create_height, OutputEntity.output_id)
Index(
    "idx_outputs_commitment_unspent",
    OutputEntity.commitment,
    OutputEntity.spent_height,
    OutputEntity.output_id.desc(),
)