from src.codec import decode_uint, encode_body_payload, encode_get_body_pack_payload, encode_uint


def test_compact_uint_round_trip() -> None:
    values = [0, 1, 63, 127, 128, 255, 256, 65535, 1_000_000]
    for value in values:
        encoded = encode_uint(value)
        decoded, size = decode_uint(encoded)
        assert decoded == value
        assert size == len(encoded)


def test_encode_get_body_pack_payload_contains_all_fields() -> None:
    payload = encode_get_body_pack_payload(
        top_height=7,
        top_hash=b"\x11" * 32,
        flag_perishable=0,
        flag_eternal=0,
        count_extra=0,
        block0=0,
        horizon_lo1=0,
        horizon_hi1=0,
    )
    assert payload.startswith(bytes((0x87,)) + (b"\x11" * 32))
    assert payload.endswith(bytes((0x80, 0x80, 0x80, 0x80)))


def test_encode_body_payload_wraps_both_byte_buffers() -> None:
    payload = encode_body_payload(perishable=b"abc", eternal=b"xy")
    assert payload == bytes((0x83,)) + b"abc" + bytes((0x82,)) + b"xy"