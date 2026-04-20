from beam_p2p import encode_uint
from beam_p2p.deserializers import (
    deserialize_body_pack_payloads,
    deserialize_body_payload,
    deserialize_header_pack,
    deserialize_header_pack_payloads,
    split_body_pack_payload,
)
from beam_p2p.deserializers.block import _get_rules_hash
from beam_p2p.protocol_models import BlockHeader


def _encode_header_element(
    *,
    kernels_byte: int,
    definition_byte: int,
    timestamp: int,
    packed_difficulty: int,
    pow_indices_byte: int,
    pow_nonce_byte: int,
) -> bytes:
    return b"".join(
        (
            bytes((kernels_byte,)) * 32,
            bytes((definition_byte,)) * 32,
            encode_uint(timestamp),
            bytes((pow_indices_byte,)) * 104,
            encode_uint(packed_difficulty),
            bytes((pow_nonce_byte,)) * 8,
        )
    )


def _unpack_difficulty_raw(packed_difficulty: int) -> int:
    return (
        (1 << 24) | (packed_difficulty & ((1 << 24) - 1))
    ) << (packed_difficulty >> 24)


def test_deserialize_body_payload_with_empty_full_block() -> None:
    perishable = (b"\x00" * 32) + (0).to_bytes(4, "big") + (0).to_bytes(4, "big")
    eternal = (0).to_bytes(4, "big")
    payload = encode_uint(len(perishable)) + perishable + encode_uint(len(eternal)) + eternal

    header = BlockHeader(
        height=1,
        hash="00" * 32,
        previous_hash="11" * 32,
        chainwork="22" * 32,
        kernels="33" * 32,
        definition="44" * 32,
        timestamp=123,
        packed_difficulty=1,
        difficulty=1.0,
        rules_hash=None,
        pow_indices_hex="55" * 104,
        pow_nonce_hex="66" * 8,
    )

    block = deserialize_body_payload(payload, header)

    assert block.header.height == 1
    assert block.counts.inputs == 0
    assert block.counts.outputs == 0
    assert block.counts.kernels == 0
    assert block.offset == "00" * 32


def test_deserialize_body_payload_with_recovery1_block() -> None:
    perishable = (
        encode_uint(0)
        + encode_uint(1)
        + bytes((0x02,))
        + (b"\x11" * 32)
    )
    eternal = (0).to_bytes(4, "big")
    payload = encode_uint(len(perishable)) + perishable + encode_uint(len(eternal)) + eternal

    header = BlockHeader(
        height=1,
        hash="00" * 32,
        previous_hash="11" * 32,
        chainwork="22" * 32,
        kernels="33" * 32,
        definition="44" * 32,
        timestamp=123,
        packed_difficulty=1,
        difficulty=1.0,
        rules_hash=None,
        pow_indices_hex="55" * 104,
        pow_nonce_hex="66" * 8,
    )

    block = deserialize_body_payload(payload, header)

    assert block.counts.inputs == 0
    assert block.counts.outputs == 1
    assert block.outputs[0].recovery_only is True
    assert block.offset is None


def test_deserialize_body_payload_with_recovery1_input_vector() -> None:
    perishable = (
        encode_uint(1)
        + b"\x01"
        + bytes((0x00,))
        + (b"\x22" * 32)
        + encode_uint(0)
    )
    eternal = (0).to_bytes(4, "big")
    payload = encode_uint(len(perishable)) + perishable + encode_uint(len(eternal)) + eternal

    header = BlockHeader(
        height=1,
        hash="00" * 32,
        previous_hash="11" * 32,
        chainwork="22" * 32,
        kernels="33" * 32,
        definition="44" * 32,
        timestamp=123,
        packed_difficulty=1,
        difficulty=1.0,
        rules_hash=None,
        pow_indices_hex="55" * 104,
        pow_nonce_hex="66" * 8,
    )

    block = deserialize_body_payload(payload, header)

    assert block.counts.inputs == 1
    assert block.counts.outputs == 0


def test_get_rules_hash_uses_local_mainnet_table_for_historical_forks() -> None:
    current_only_peer_hashes = [
        bytes.fromhex("1a68bdc7d7756bb4640f232b6dee4988238996364655112d421503afe45b09f8")
    ]

    assert _get_rules_hash(321321, current_only_peer_hashes) is None
    assert _get_rules_hash(777777, []).hex() == (
        "1ce8f721bf0c9fa7473795a97e365ad38bbc539aab821d6912d86f24e67720fc"
    )
    assert _get_rules_hash(1280000, current_only_peer_hashes).hex() == (
        "3eaab6ab65b65f94d4f195aad47ec97e2182195b7b612e0eec0b981c653635d7"
    )
    assert _get_rules_hash(1820000, current_only_peer_hashes).hex() == (
        "b5a8b6b3617812c0ea4efc2a37eec66cbf40bfb8a8eead0e07f9c26faf6fca5f"
    )
    assert _get_rules_hash(1920000, current_only_peer_hashes).hex() == (
        "1a68bdc7d7756bb4640f232b6dee4988238996364655112d421503afe45b09f8"
    )


def test_deserialize_header_pack_parses_single_header() -> None:
    payload = b"".join(
        (
            encode_uint(10),
            b"\x01" * 32,
            b"\x02" * 32,
            encode_uint(1),
            _encode_header_element(
                kernels_byte=0x03,
                definition_byte=0x04,
                timestamp=123456,
                packed_difficulty=0x123456,
                pow_indices_byte=0x05,
                pow_nonce_byte=0x06,
            ),
        )
    )

    header = deserialize_header_pack(payload, [bytes([index]) * 32 for index in range(6)])

    assert header.height == 10
    assert header.previous_hash == ("01" * 32)
    assert header.chainwork == ("02" * 32)
    assert header.kernels == ("03" * 32)
    assert header.definition == ("04" * 32)
    assert header.timestamp == 123456
    assert header.pow_indices_hex == ("05" * 104)
    assert header.pow_nonce_hex == ("06" * 8)
    assert len(header.hash) == 64


def test_deserialize_header_pack_payloads_parses_multiple_headers() -> None:
    base_chainwork = 100
    lower_packed_difficulty = 1
    upper_packed_difficulty = 3
    payload = b"".join(
        (
            encode_uint(10),
            b"\x01" * 32,
            base_chainwork.to_bytes(32, "big"),
            encode_uint(2),
            _encode_header_element(
                kernels_byte=0x11,
                definition_byte=0x21,
                timestamp=1001,
                packed_difficulty=upper_packed_difficulty,
                pow_indices_byte=0x31,
                pow_nonce_byte=0x41,
            ),
            _encode_header_element(
                kernels_byte=0x10,
                definition_byte=0x20,
                timestamp=1000,
                packed_difficulty=lower_packed_difficulty,
                pow_indices_byte=0x30,
                pow_nonce_byte=0x40,
            ),
        )
    )

    headers = deserialize_header_pack_payloads(
        payload,
        [bytes([index]) * 32 for index in range(6)],
    )

    assert [header.height for header in headers] == [10, 11]
    assert headers[0].previous_hash == ("01" * 32)
    assert headers[0].chainwork == base_chainwork.to_bytes(32, "big").hex()
    assert headers[1].previous_hash == headers[0].hash
    assert headers[1].chainwork == (
        base_chainwork + _unpack_difficulty_raw(upper_packed_difficulty)
    ).to_bytes(32, "big").hex()
    assert headers[0].kernels == ("10" * 32)
    assert headers[1].kernels == ("11" * 32)
    assert headers[0].timestamp == 1000
    assert headers[1].timestamp == 1001


def test_split_body_pack_payload_returns_all_bodies() -> None:
    perishable = (b"\x00" * 32) + (0).to_bytes(4, "big") + (0).to_bytes(4, "big")
    eternal = (0).to_bytes(4, "big")
    body = encode_uint(len(perishable)) + perishable + encode_uint(len(eternal)) + eternal
    payload = encode_uint(2) + body + body

    bodies = split_body_pack_payload(payload)

    assert len(bodies) == 2
    assert bodies[0][:2] == (perishable, eternal)
    assert bodies[1][:2] == (perishable, eternal)


def test_deserialize_body_pack_payloads_decodes_each_body() -> None:
    perishable = (b"\x00" * 32) + (0).to_bytes(4, "big") + (0).to_bytes(4, "big")
    eternal = (0).to_bytes(4, "big")
    body = encode_uint(len(perishable)) + perishable + encode_uint(len(eternal)) + eternal
    payload = encode_uint(2) + body + body

    header1 = BlockHeader(
        height=1,
        hash="01" * 32,
        previous_hash="11" * 32,
        chainwork="22" * 32,
        kernels="33" * 32,
        definition="44" * 32,
        timestamp=123,
        packed_difficulty=1,
        difficulty=1.0,
        rules_hash=None,
        pow_indices_hex="55" * 104,
        pow_nonce_hex="66" * 8,
    )
    header2 = BlockHeader(
        height=2,
        hash="02" * 32,
        previous_hash=header1.hash,
        chainwork="22" * 32,
        kernels="33" * 32,
        definition="44" * 32,
        timestamp=124,
        packed_difficulty=1,
        difficulty=1.0,
        rules_hash=None,
        pow_indices_hex="55" * 104,
        pow_nonce_hex="66" * 8,
    )

    blocks = deserialize_body_pack_payloads(payload, [header1, header2])

    assert [block.header.height for block in blocks] == [1, 2]
    assert all(block.counts.inputs == 0 for block in blocks)