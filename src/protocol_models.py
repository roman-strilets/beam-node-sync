"""Minimal protocol data models used by the Beam sync prototype."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EcPoint:
    """Compressed elliptic-curve point representation."""

    x: str
    y: bool


@dataclass(frozen=True)
class KernelSignature:
    """Signature used in public range proofs."""

    nonce_pub: EcPoint
    k: str


@dataclass(frozen=True)
class LrPair:
    """One inner-product round (L, R point pair)."""

    left: EcPoint
    right: EcPoint


@dataclass(frozen=True)
class InnerProduct:
    """Bulletproof inner-product argument."""

    rounds: list[LrPair]
    condensed: list[str]


@dataclass(frozen=True)
class ConfidentialRangeProof:
    """Confidential range proof."""

    kind: str
    a: EcPoint
    s: EcPoint
    t1: EcPoint
    t2: EcPoint
    tau_x: str
    mu: str
    t_dot: str
    inner_product: InnerProduct


@dataclass(frozen=True)
class Recovery:
    """Public range proof recovery metadata."""

    idx: int
    type: int
    sub_idx: int
    checksum: str


@dataclass(frozen=True)
class PublicRangeProof:
    """Public range proof."""

    kind: str
    value: int
    signature: KernelSignature
    recovery: Recovery


@dataclass(frozen=True)
class RecoveryConfidentialRangeProof:
    """Recovery-only confidential range proof."""

    kind: str
    a: EcPoint
    s: EcPoint
    t1: EcPoint
    t2: EcPoint
    mu: str


@dataclass(frozen=True)
class RecoveryPublicRangeProof:
    """Recovery-only public range proof."""

    kind: str
    value: int
    recovery: Recovery


@dataclass(frozen=True)
class SigmaConfigInfo:
    """Shape parameters for Sigma proofs."""

    n: int
    M: int
    N: int
    f_count: int


@dataclass(frozen=True)
class SigmaPart1:
    """Point commitments of a Sigma proof."""

    a: EcPoint
    b: EcPoint
    c: EcPoint
    d: EcPoint
    g_points: list[EcPoint]


@dataclass(frozen=True)
class SigmaPart2:
    """Scalar values of a Sigma proof."""

    z_a: str
    z_c: str
    z_r: str
    f_scalars: list[str]


@dataclass(frozen=True)
class SigmaProof:
    """Sigma proof payload."""

    cfg: SigmaConfigInfo
    part1: SigmaPart1
    part2: SigmaPart2


@dataclass(frozen=True)
class AssetProof:
    """Asset ownership proof."""

    begin: int
    generator: EcPoint
    sigma: SigmaProof


@dataclass(frozen=True)
class RecoveryAssetProof:
    """Recovery-only asset proof."""

    generator: EcPoint


@dataclass(frozen=True)
class TxInput:
    """One transaction input."""

    commitment: EcPoint


@dataclass(frozen=True)
class TxOutput:
    """One transaction output."""

    commitment: EcPoint
    coinbase: bool
    confidential_proof: ConfidentialRangeProof | None = None
    public_proof: PublicRangeProof | None = None
    incubation: int | None = None
    asset_proof: AssetProof | None = None
    extra_flags: int | None = None


@dataclass(frozen=True)
class BlockOutput:
    """One decoded block output."""

    commitment: EcPoint
    coinbase: bool
    recovery_only: bool
    confidential_proof: ConfidentialRangeProof | RecoveryConfidentialRangeProof | None = None
    public_proof: PublicRangeProof | RecoveryPublicRangeProof | None = None
    incubation: int | None = None
    asset_proof: AssetProof | RecoveryAssetProof | None = None
    extra_flags: int | None = None


@dataclass(frozen=True)
class TxCounts:
    """Block component counts."""

    inputs: int
    outputs: int
    kernels: int
    kernels_mixed: bool


@dataclass(frozen=True)
class BlockHeader:
    """Decoded Beam block header metadata."""

    height: int
    hash: str
    previous_hash: str
    chainwork: str
    kernels: str
    definition: str
    timestamp: int
    packed_difficulty: int
    difficulty: float
    rules_hash: str | None
    pow_indices_hex: str
    pow_nonce_hex: str


@dataclass(frozen=True)
class DecodedBlock:
    """Parsed Beam block body with just the fields needed for sync."""

    header: BlockHeader
    inputs: list[TxInput]
    outputs: list[BlockOutput]
    counts: TxCounts
    offset: str | None = None
    raw_payload: bytes | None = None