"""Range-proof deserializers needed for Beam output parsing."""

from __future__ import annotations

from dataclasses import dataclass

from ..protocol_models import (
    AssetProof,
    ConfidentialRangeProof,
    InnerProduct,
    KernelSignature,
    LrPair,
    PublicRangeProof,
    Recovery,
    RecoveryAssetProof,
    RecoveryConfidentialRangeProof,
    RecoveryPublicRangeProof,
    SigmaConfigInfo,
    SigmaPart1,
    SigmaPart2,
    SigmaProof,
    EcPoint,
)
from .core import BufferReader, decode_lsb_bits, decode_msb_bits


ASSET_PROOF_N = 4
ASSET_PROOF_M = 3
INNER_PRODUCT_CYCLES = 6


@dataclass(frozen=True)
class SigmaConfig:
    """Configuration container for Sigma proofs."""

    n: int
    M: int

    @property
    def f_count(self) -> int:
        return self.M * (self.n - 1)


def deserialize_confidential_range_proof(reader: BufferReader) -> ConfidentialRangeProof:
    """Deserialize a confidential range proof."""
    a_x = reader.read_fixed_hex(32)
    s_x = reader.read_fixed_hex(32)
    t1_x = reader.read_fixed_hex(32)
    t2_x = reader.read_fixed_hex(32)
    tau_x = reader.read_scalar()
    mu = reader.read_scalar()
    t_dot = reader.read_scalar()

    lr_l_xs: list[str] = []
    lr_r_xs: list[str] = []
    for _ in range(INNER_PRODUCT_CYCLES):
        lr_l_xs.append(reader.read_fixed_hex(32))
        lr_r_xs.append(reader.read_fixed_hex(32))
    condensed = [reader.read_scalar(), reader.read_scalar()]

    bits = decode_lsb_bits(
        reader.read_bytes(((INNER_PRODUCT_CYCLES * 2 + 7) & ~7) // 8),
        INNER_PRODUCT_CYCLES * 2 + 4,
    )

    rounds = [
        LrPair(
            left=EcPoint(lr_l_xs[i], bits[i * 2]),
            right=EcPoint(lr_r_xs[i], bits[i * 2 + 1]),
        )
        for i in range(INNER_PRODUCT_CYCLES)
    ]

    return ConfidentialRangeProof(
        kind="confidential",
        a=EcPoint(a_x, bits[12]),
        s=EcPoint(s_x, bits[13]),
        t1=EcPoint(t1_x, bits[14]),
        t2=EcPoint(t2_x, bits[15]),
        tau_x=tau_x,
        mu=mu,
        t_dot=t_dot,
        inner_product=InnerProduct(rounds=rounds, condensed=condensed),
    )


def deserialize_public_range_proof(reader: BufferReader) -> PublicRangeProof:
    """Deserialize a public range proof."""
    return PublicRangeProof(
        kind="public",
        value=reader.read_var_uint(),
        signature=KernelSignature(
            nonce_pub=reader.read_point(),
            k=reader.read_scalar(),
        ),
        recovery=Recovery(
            idx=reader.read_big_uint(8),
            type=reader.read_big_uint(4),
            sub_idx=reader.read_big_uint(4),
            checksum=reader.read_hash32(),
        ),
    )


def deserialize_recovery_confidential_range_proof(
    reader: BufferReader,
) -> RecoveryConfidentialRangeProof:
    """Deserialize a Recovery1 confidential range proof."""
    a_x = reader.read_fixed_hex(32)
    s_x = reader.read_fixed_hex(32)
    t1_x = reader.read_fixed_hex(32)
    t2_x = reader.read_fixed_hex(32)
    mu = reader.read_scalar()
    flags = reader.read_u8()
    return RecoveryConfidentialRangeProof(
        kind="confidential_recovery",
        a=EcPoint(a_x, bool(flags & 1)),
        s=EcPoint(s_x, bool(flags & 2)),
        t1=EcPoint(t1_x, bool(flags & 4)),
        t2=EcPoint(t2_x, bool(flags & 8)),
        mu=mu,
    )


def deserialize_recovery_public_range_proof(
    reader: BufferReader,
) -> RecoveryPublicRangeProof:
    """Deserialize a Recovery1 public range proof."""
    return RecoveryPublicRangeProof(
        kind="public_recovery",
        value=reader.read_var_uint(),
        recovery=Recovery(
            idx=reader.read_big_uint(8),
            type=reader.read_big_uint(4),
            sub_idx=reader.read_big_uint(4),
            checksum=reader.read_hash32(),
        ),
    )


def deserialize_recovery_asset_proof(reader: BufferReader) -> RecoveryAssetProof:
    """Deserialize a Recovery1 asset proof."""
    return RecoveryAssetProof(generator=reader.read_point())


def deserialize_asset_proof(reader: BufferReader) -> AssetProof:
    """Deserialize an asset proof used by regular outputs."""
    cfg = SigmaConfig(n=ASSET_PROOF_N, M=ASSET_PROOF_M)
    begin = reader.read_var_uint()
    generator_x = reader.read_fixed_hex(32)
    sigma, extra_bits = deserialize_sigma_proof(reader, cfg, extra_bits=1)
    return AssetProof(
        begin=begin,
        generator=EcPoint(generator_x, extra_bits[0]),
        sigma=sigma,
    )


def deserialize_sigma_proof(
    reader: BufferReader,
    cfg: SigmaConfig,
    extra_bits: int,
) -> tuple[SigmaProof, list[bool]]:
    """Deserialize a Sigma proof and return it plus extra trailing bits."""
    a_x = reader.read_fixed_hex(32)
    b_x = reader.read_fixed_hex(32)
    c_x = reader.read_fixed_hex(32)
    d_x = reader.read_fixed_hex(32)
    z_a = reader.read_scalar()
    z_c = reader.read_scalar()
    z_r = reader.read_scalar()
    g_xs = [reader.read_fixed_hex(32) for _ in range(cfg.M)]
    f_scalars = [reader.read_scalar() for _ in range(cfg.f_count)]

    bit_count = 4 + cfg.M + extra_bits
    bits = decode_msb_bits(reader.read_bytes((bit_count + 7) // 8), bit_count)

    g_points = [EcPoint(g_xs[index], bits[4 + index]) for index in range(cfg.M)]
    extra = bits[4 + cfg.M :]

    return (
        SigmaProof(
            cfg=SigmaConfigInfo(n=cfg.n, M=cfg.M, N=pow(cfg.n, cfg.M), f_count=cfg.f_count),
            part1=SigmaPart1(
                a=EcPoint(a_x, bits[0]),
                b=EcPoint(b_x, bits[1]),
                c=EcPoint(c_x, bits[2]),
                d=EcPoint(d_x, bits[3]),
                g_points=g_points,
            ),
            part2=SigmaPart2(
                z_a=z_a,
                z_c=z_c,
                z_r=z_r,
                f_scalars=f_scalars,
            ),
        ),
        extra,
    )