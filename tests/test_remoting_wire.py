import struct
from dataclasses import replace
from random import Random
from uuid import UUID

import pytest

from movie.remoting import (
    BOOTSTRAP_MAX_FRAME_BYTES,
    COMMON_HEADER_SIZE,
    CONTROL_LANE_ID,
    AssociationRole,
    DeserializationRejected,
    FrameCodec,
    FrameTooLargeError,
    FrameType,
    GoAway,
    Hello,
    HelloAccept,
    HelloReject,
    InvalidPreambleError,
    MalformedFrameError,
    ProtocolValidationError,
    ReasonCode,
    RecipientUnavailable,
    ResolveRejected,
    ResolveRequest,
    ResolveResponse,
    SerializerDescriptor,
    SerializerRoute,
    StreamKind,
    StreamPreamble,
    UnsupportedFeatureError,
    UnsupportedFrameError,
    UserMessage,
    WireCodecError,
    WrongStreamError,
    decode_common_header,
    decode_frame,
    decode_preamble,
    encode_common_header,
    encode_frame,
    encode_preamble,
    negotiate_capabilities,
)

ASSOCIATION_UID = UUID("00112233-4455-6677-8899-aabbccddeeff")
SYSTEM_UID = UUID("10213243-5465-7687-98a9-bacbdcedfe0f")
PEER_UID = UUID("20314253-6475-8697-a8b9-cadbecfd0e1f")
ACTOR_UID = UUID("30415263-7485-96a7-b8c9-daebfc0d1e2f")

DESCRIPTOR = SerializerDescriptor(
    7,
    "json-contracts",
    1,
    2,
    frozenset({"chat/v1", "chat/v2"}),
    frozenset({"chat/v2"}),
)
ROUTE = SerializerRoute(SYSTEM_UID, 7, ("chat/v2",))


def all_frames():
    limits = {
        "maximum_frame_bytes": 65_536,
        "lane_count": 4,
        "outbound_message_limit": 100,
        "outbound_byte_limit": 1_000_000,
        "inbound_message_limit": 200,
        "inbound_byte_limit": 2_000_000,
    }
    return [
        Hello(
            1,
            0,
            AssociationRole.INITIATOR,
            "movie-system",
            SYSTEM_UID,
            ASSOCIATION_UID,
            "localhost",
            8023,
            **limits,
            serializers=(DESCRIPTOR,),
            capabilities=("trace/v1",),
        ),
        HelloAccept(
            ASSOCIATION_UID,
            0,
            **limits,
            serializer_routes=(ROUTE,),
            capabilities=("trace/v1",),
        ),
        GoAway(ReasonCode.PROTOCOL_VIOLATION, "closing"),
        HelloReject(ReasonCode.INCOMPATIBLE_VERSION, "major"),
        ResolveRequest(1, "movie-system", "/user/target"),
        ResolveResponse(2, SYSTEM_UID, ACTOR_UID, "/user/target"),
        ResolveRejected(3, ReasonCode.ACTOR_NOT_FOUND, "missing"),
        UserMessage(
            ASSOCIATION_UID,
            2,
            42,
            SYSTEM_UID,
            PEER_UID,
            ACTOR_UID,
            7,
            "chat/v2",
            b"{\"text\":\"hello\"}",
        ),
        RecipientUnavailable(
            ASSOCIATION_UID,
            2,
            42,
            ACTOR_UID,
            ReasonCode.MAILBOX_FULL,
            "full",
        ),
        DeserializationRejected(
            ASSOCIATION_UID,
            2,
            42,
            ACTOR_UID,
            ReasonCode.MALFORMED_PAYLOAD,
            "bad json",
        ),
    ]


@pytest.mark.parametrize("seed", [0x4D4F5631, 0xA55A5AA5])
def test_bounded_random_wire_inputs_have_typed_failures_and_canonical_successes(
    seed,
) -> None:
    random = Random(seed)
    frames = all_frames()
    preambles = (
        StreamPreamble(StreamKind.CONTROL, ASSOCIATION_UID, CONTROL_LANE_ID),
        StreamPreamble(StreamKind.MULTIPLEXED, ASSOCIATION_UID, CONTROL_LANE_ID),
        StreamPreamble(StreamKind.DELIVERY_LANE, ASSOCIATION_UID, 2),
    )
    decoded_frames = 0
    rejected_frames = 0
    decoded_preambles = 0
    rejected_preambles = 0
    for iteration in range(1_000):
        if iteration % 2:
            payload = bytearray(
                encode_frame(
                    random.choice(frames),
                    stream_kind=StreamKind.MULTIPLEXED,
                )
            )
            for _ in range(random.randrange(4)):
                index = random.randrange(len(payload))
                payload[index] ^= random.randrange(1, 256)
            payload = bytes(payload)
        else:
            payload = random.randbytes(random.randrange(0, 513))
        try:
            frame = decode_frame(payload, stream_kind=StreamKind.MULTIPLEXED)
        except WireCodecError:
            rejected_frames += 1
        else:
            decoded_frames += 1
            canonical = encode_frame(frame, stream_kind=StreamKind.MULTIPLEXED)
            assert (
                decode_frame(canonical, stream_kind=StreamKind.MULTIPLEXED)
                == frame
            )

        if iteration % 2:
            preamble_payload = bytearray(encode_preamble(random.choice(preambles)))
            for _ in range(random.randrange(3)):
                index = random.randrange(len(preamble_payload))
                preamble_payload[index] ^= random.randrange(1, 256)
            preamble_payload = bytes(preamble_payload)
        else:
            preamble_payload = random.randbytes(random.randrange(0, 48))
        try:
            preamble = decode_preamble(preamble_payload)
        except WireCodecError:
            rejected_preambles += 1
        else:
            decoded_preambles += 1
            assert decode_preamble(encode_preamble(preamble)) == preamble
    assert decoded_frames > 0
    assert rejected_frames > 0
    assert decoded_preambles > 0
    assert rejected_preambles > 0


def stream_for(frame):
    return StreamKind.DELIVERY_LANE if isinstance(frame, UserMessage) else StreamKind.CONTROL


@pytest.mark.parametrize("frame", all_frames(), ids=lambda frame: type(frame).__name__)
def test_every_documented_frame_round_trips(frame):
    encoded = encode_frame(frame, stream_kind=stream_for(frame))

    assert decode_frame(encoded, stream_kind=stream_for(frame)) == frame
    assert len(encoded) == struct.unpack(">I", encoded[:4])[0] + 4


def test_frame_codec_binds_the_negotiated_limit():
    frame = GoAway(ReasonCode.PROTOCOL_VIOLATION, "x")
    exact_size = len(encode_frame(frame))
    codec = FrameCodec(exact_size)

    assert codec.decode(codec.encode(frame)) == frame
    with pytest.raises(FrameTooLargeError):
        FrameCodec(exact_size - 1).encode(frame)


def test_control_preamble_has_golden_big_endian_bytes():
    preamble = StreamPreamble(StreamKind.CONTROL, ASSOCIATION_UID, CONTROL_LANE_ID)

    encoded = encode_preamble(preamble)

    assert encoded == bytes.fromhex(
        "4d4f5631 00 00112233445566778899aabbccddeeff ffff"
    )
    assert decode_preamble(encoded) == preamble


def test_delivery_preamble_has_golden_big_endian_bytes():
    preamble = StreamPreamble(StreamKind.DELIVERY_LANE, ASSOCIATION_UID, 0x0203)

    encoded = encode_preamble(preamble, lane_count=0x0204)

    assert encoded == bytes.fromhex(
        "4d4f5631 01 00112233445566778899aabbccddeeff 0203"
    )
    assert decode_preamble(encoded, lane_count=0x0204) == preamble


def test_common_header_has_golden_big_endian_bytes():
    encoded = encode_common_header(
        FrameType.RESOLVE_REQUEST,
        body_length=5,
        correlation_id=0x0102030405060708,
    )

    assert encoded == bytes.fromhex(
        "00000011 10 00 0001 0102030405060708"
    )
    header = decode_common_header(encoded)
    assert header.frame_length == 17
    assert header.body_length == 5
    assert header.total_length == 21
    assert header.frame_type is FrameType.RESOLVE_REQUEST
    assert header.correlation_id == 0x0102030405060708


def test_user_message_has_golden_big_endian_bytes():
    message = UserMessage(
        ASSOCIATION_UID,
        0x0203,
        0x0102030405060708,
        SYSTEM_UID,
        PEER_UID,
        ACTOR_UID,
        7,
        "chat/v2",
        b"{}",
    )

    encoded = encode_frame(message, stream_kind=StreamKind.DELIVERY_LANE)

    assert encoded == bytes.fromhex(
        "00000069 20 00 0001 0000000000000000 "
        "00112233445566778899aabbccddeeff 0203 0102030405060708 "
        "102132435465768798a9bacbdcedfe0f "
        "2031425364758697a8b9cadbecfd0e1f "
        "30415263748596a7b8c9daebfc0d1e2f "
        "00000007 0007 00000002 636861742f7632 7b7d"
    )
    assert decode_frame(encoded, stream_kind=StreamKind.DELIVERY_LANE) == message


@pytest.mark.parametrize(
    ("frame", "fixture"),
    [
        (
            all_frames()[0],
            "000000af0100000100000000000000000001000000000c6d6f7669652d73797374656d"
            "102132435465768798a9bacbdcedfe0f00112233445566778899aabbccddeeff00096c6f"
            "63616c686f73741f570001000000040000006400000000000f4240000000c800000000"
            "001e8480000100000007000e6a736f6e2d636f6e747261637473000100020002000763"
            "6861742f76310007636861742f763200010007636861742f7632000100087472616365"
            "2f7631",
        ),
        (
            all_frames()[1],
            "0000006902000001000000000000000000112233445566778899aabbccddeeff00000001"
            "000000040000006400000000000f4240000000c800000000001e848000011021324354"
            "65768798a9bacbdcedfe0f0000000700010007636861742f76320001000874726163652f"
            "7631",
        ),
    ],
    ids=("hello-v1", "hello-accept-v1"),
)
def test_handshake_frames_match_v1_compatibility_fixtures(frame, fixture):
    payload = bytes.fromhex(fixture)

    assert encode_frame(frame, stream_kind=StreamKind.CONTROL) == payload
    assert decode_frame(payload, stream_kind=StreamKind.CONTROL) == frame


@pytest.mark.parametrize("length", [0, 22, 24])
def test_preamble_requires_its_exact_length(length):
    with pytest.raises(InvalidPreambleError, match="exactly"):
        decode_preamble(b"\0" * length)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda data: data.__setitem__(slice(0, 4), b"NOPE"),
        lambda data: data.__setitem__(4, 3),
        lambda data: data.__setitem__(slice(5, 21), b"\0" * 16),
        lambda data: data.__setitem__(slice(21, 23), b"\0\0"),
    ],
)
def test_control_preamble_rejects_malformed_fields(mutate):
    data = bytearray(encode_preamble(StreamPreamble(StreamKind.CONTROL, ASSOCIATION_UID, 0xFFFF)))
    mutate(data)

    with pytest.raises(InvalidPreambleError):
        decode_preamble(bytes(data))


def test_preamble_validates_lane_range_association_and_expected_kind():
    lane = encode_preamble(StreamPreamble(StreamKind.DELIVERY_LANE, ASSOCIATION_UID, 2))

    with pytest.raises(InvalidPreambleError, match="lane count"):
        decode_preamble(lane, lane_count=2)
    with pytest.raises(InvalidPreambleError, match="association"):
        decode_preamble(lane, expected_association_uid=PEER_UID)
    with pytest.raises(InvalidPreambleError, match="expected CONTROL"):
        decode_preamble(lane, expected_kind=StreamKind.CONTROL)
    with pytest.raises(ProtocolValidationError):
        encode_preamble(StreamPreamble(StreamKind.DELIVERY_LANE, ASSOCIATION_UID, 0xFFFF))


def test_multiplexed_preamble_accepts_control_and_delivery_frames():
    preamble = StreamPreamble(StreamKind.MULTIPLEXED, ASSOCIATION_UID, 0xFFFF)
    control = all_frames()[2]
    delivery = all_frames()[7]

    assert decode_preamble(encode_preamble(preamble)) == preamble
    assert decode_frame(
        encode_frame(control, stream_kind=StreamKind.MULTIPLEXED),
        stream_kind=StreamKind.MULTIPLEXED,
    ) == control
    assert decode_frame(
        encode_frame(delivery, stream_kind=StreamKind.MULTIPLEXED),
        stream_kind=StreamKind.MULTIPLEXED,
    ) == delivery


def test_header_rejects_short_input_and_impossible_declared_length():
    with pytest.raises(MalformedFrameError, match="requires"):
        decode_common_header(b"\0" * (COMMON_HEADER_SIZE - 1))

    data = struct.pack(">IBBHQ", 11, FrameType.GOAWAY, 0, 1, 0)
    with pytest.raises(MalformedFrameError, match="smaller"):
        decode_common_header(data)


def test_header_rejects_unknown_type_flags_version_and_correlations():
    valid = bytearray(encode_common_header(FrameType.GOAWAY, 0, 0))

    unknown_type = valid.copy()
    unknown_type[4] = 0xFE
    with pytest.raises(UnsupportedFrameError):
        decode_common_header(bytes(unknown_type))

    flags = valid.copy()
    flags[5] = 1
    with pytest.raises(UnsupportedFeatureError, match="flags"):
        decode_common_header(bytes(flags))

    version = valid.copy()
    version[6:8] = b"\0\2"
    with pytest.raises(UnsupportedFeatureError, match="version"):
        decode_common_header(bytes(version))

    nonzero = valid.copy()
    nonzero[8:16] = (1).to_bytes(8, "big")
    with pytest.raises(MalformedFrameError, match="zero"):
        decode_common_header(bytes(nonzero))

    resolve = struct.pack(">IBBHQ", 12, FrameType.RESOLVE_RESPONSE, 0, 1, 0)
    with pytest.raises(MalformedFrameError, match="nonzero"):
        decode_common_header(resolve)


def test_bootstrap_limit_is_enforced_from_header_before_a_body_is_present():
    oversized = struct.pack(
        ">IBBHQ",
        BOOTSTRAP_MAX_FRAME_BYTES - 3,
        FrameType.HELLO,
        0,
        1,
        0,
    )

    with pytest.raises(FrameTooLargeError, match=str(BOOTSTRAP_MAX_FRAME_BYTES)):
        decode_common_header(oversized, maximum_frame_bytes=2 * BOOTSTRAP_MAX_FRAME_BYTES)


@pytest.mark.parametrize("frame", all_frames(), ids=lambda frame: type(frame).__name__)
def test_every_frame_rejects_truncation_and_bytes_after_its_boundary(frame):
    encoded = encode_frame(frame)

    with pytest.raises(MalformedFrameError, match="truncated"):
        decode_frame(encoded[:-1])
    with pytest.raises(MalformedFrameError, match="after its declared boundary"):
        decode_frame(encoded + b"\0")


def test_frames_are_restricted_to_their_documented_stream_kind():
    control = GoAway(ReasonCode.PROTOCOL_VIOLATION)
    delivery = UserMessage(
        ASSOCIATION_UID,
        0,
        0,
        SYSTEM_UID,
        PEER_UID,
        ACTOR_UID,
        7,
        "chat/v2",
        b"",
    )

    with pytest.raises(WrongStreamError):
        encode_frame(control, stream_kind=StreamKind.DELIVERY_LANE)
    with pytest.raises(WrongStreamError):
        decode_frame(encode_frame(delivery), stream_kind=StreamKind.CONTROL)
    with pytest.raises(WrongStreamError):
        decode_frame(encode_frame(control), stream_kind=0)  # type: ignore[arg-type]


def raw_frame(frame_type: FrameType, body: bytes, correlation_id: int = 0) -> bytes:
    return encode_common_header(frame_type, len(body), correlation_id) + body


@pytest.mark.parametrize(
    "body",
    [
        struct.pack(">HH", ReasonCode.ACTOR_NOT_FOUND, 1) + b"\xff",
        struct.pack(">HH", ReasonCode.ACTOR_NOT_FOUND, 3) + b"e\xcc\x81",
        struct.pack(">HH", ReasonCode.ACTOR_NOT_FOUND, 2) + b"x",
    ],
)
def test_wire_strings_reject_invalid_utf8_non_nfc_and_bad_lengths(body):
    with pytest.raises(MalformedFrameError):
        decode_frame(raw_frame(FrameType.RESOLVE_REJECTED, body, correlation_id=1))


def test_wire_rejects_unknown_role_and_reason_enums():
    hello = bytearray(encode_frame(all_frames()[0]))
    hello[COMMON_HEADER_SIZE + 4] = 2
    with pytest.raises(MalformedFrameError, match="role"):
        decode_frame(bytes(hello))

    unknown_reason = raw_frame(FrameType.GOAWAY, b"\xff\xff\0\0")
    with pytest.raises(MalformedFrameError, match="reason"):
        decode_frame(unknown_reason)


@pytest.mark.parametrize(
    "frame",
    [
        ResolveRejected(1, ReasonCode.MAILBOX_FULL),
        RecipientUnavailable(
            ASSOCIATION_UID,
            0,
            0,
            ACTOR_UID,
            ReasonCode.MALFORMED_PAYLOAD,
        ),
        DeserializationRejected(
            ASSOCIATION_UID,
            0,
            0,
            ACTOR_UID,
            ReasonCode.ACTOR_NOT_FOUND,
        ),
    ],
)
def test_reason_codes_are_validated_for_resolution_and_advisory_frames(frame):
    with pytest.raises(ProtocolValidationError, match="not valid"):
        encode_frame(frame)

    frame_type = (
        FrameType.RESOLVE_REJECTED
        if isinstance(frame, ResolveRejected)
        else (
            FrameType.RECIPIENT_UNAVAILABLE
            if isinstance(frame, RecipientUnavailable)
            else FrameType.DESERIALIZATION_REJECTED
        )
    )
    valid = all_frames()[
        {
            FrameType.RESOLVE_REJECTED: 6,
            FrameType.RECIPIENT_UNAVAILABLE: 8,
            FrameType.DESERIALIZATION_REJECTED: 9,
        }[frame_type]
    ]
    encoded = bytearray(encode_frame(valid))
    reason_offset = 16 if frame_type is FrameType.RESOLVE_REJECTED else 58
    encoded[reason_offset : reason_offset + 2] = int(frame.reason).to_bytes(2, "big")
    with pytest.raises(MalformedFrameError, match="not valid"):
        decode_frame(bytes(encoded))


@pytest.mark.parametrize(
    "path",
    [
        "",
        "relative",
        "//double",
        "/empty//segment",
        "/bad%20segment",
        "/caf\N{LATIN SMALL LETTER E WITH ACUTE}",
    ],
)
def test_actor_paths_must_use_canonical_remote_resolvable_grammar(path):
    with pytest.raises(ProtocolValidationError, match="actor path"):
        encode_frame(ResolveRequest(1, "movie-system", path))


@pytest.mark.parametrize(
    "name",
    ["", "bad/name", "caf\N{LATIN SMALL LETTER E WITH ACUTE}", "x" * 256],
)
def test_system_names_must_use_canonical_remote_resolvable_grammar(name):
    with pytest.raises(ProtocolValidationError, match="actor-system name"):
        encode_frame(ResolveRequest(1, name, "/target"))


def base_user_message(*, manifest: str = "xxx", payload: bytes = b"") -> UserMessage:
    return UserMessage(
        ASSOCIATION_UID,
        0,
        0,
        SYSTEM_UID,
        PEER_UID,
        ACTOR_UID,
        7,
        manifest,
        payload,
    )


@pytest.mark.parametrize(
    ("offset", "replacement", "message"),
    [
        (16, b"\0" * 16, "association UID"),
        (32, b"\xff\xff", "lane ID"),
        (90, b"\0\0\0\0", "serializer ID"),
        (94, (1_025).to_bytes(2, "big"), "manifest"),
        (96, (1).to_bytes(4, "big"), "lengths"),
    ],
)
def test_user_message_rejects_identity_lane_serializer_and_length_boundaries(
    offset, replacement, message
):
    encoded = bytearray(encode_frame(base_user_message()))
    encoded[offset : offset + len(replacement)] = replacement

    with pytest.raises(MalformedFrameError, match=message):
        decode_frame(bytes(encoded))


def test_user_message_manifest_requires_valid_utf8_nfc_and_nonempty_text():
    invalid_utf8 = bytearray(encode_frame(base_user_message()))
    invalid_utf8[100] = 0xFF
    with pytest.raises(MalformedFrameError, match="UTF-8"):
        decode_frame(bytes(invalid_utf8))

    non_nfc = bytearray(encode_frame(base_user_message()))
    non_nfc[100:103] = b"e\xcc\x81"
    with pytest.raises(MalformedFrameError, match="NFC"):
        decode_frame(bytes(non_nfc))

    empty = bytearray(encode_frame(base_user_message()))
    empty[94:96] = b"\0\0"
    empty[96:100] = (3).to_bytes(4, "big")
    with pytest.raises(MalformedFrameError, match="empty"):
        decode_frame(bytes(empty))


def test_user_message_manifest_accepts_exact_limit_and_rejects_one_more():
    exact = base_user_message(manifest="x" * 1_024)

    assert decode_frame(encode_frame(exact)) == exact
    with pytest.raises(ProtocolValidationError, match="1024"):
        encode_frame(base_user_message(manifest="x" * 1_025))


def test_unsigned_wire_fields_accept_maxima_and_reject_overflow():
    message = replace(
        base_user_message(),
        lane_id=0xFFFE,
        lane_sequence=(1 << 64) - 1,
        serializer_id=(1 << 32) - 1,
    )
    request = ResolveRequest((1 << 64) - 1, "movie-system", "/target")

    assert decode_frame(encode_frame(message)) == message
    assert decode_frame(encode_frame(request)) == request
    with pytest.raises(ProtocolValidationError, match="lane sequence"):
        encode_frame(replace(message, lane_sequence=1 << 64))
    with pytest.raises(ProtocolValidationError, match="serializer ID"):
        encode_frame(replace(message, serializer_id=1 << 32))
    with pytest.raises(ProtocolValidationError, match="correlation ID"):
        encode_frame(replace(request, correlation_id=1 << 64))


def test_string16_accepts_65535_utf8_bytes_and_rejects_65536():
    exact = GoAway(ReasonCode.PROTOCOL_VIOLATION, "x" * 65_535)

    assert decode_frame(encode_frame(exact)) == exact
    with pytest.raises(ProtocolValidationError, match="65535"):
        encode_frame(replace(exact, detail="x" * 65_536))


def test_actor_path_segment_accepts_255_bytes_and_rejects_256():
    exact = ResolveRequest(1, "movie-system", "/" + "x" * 255)

    assert decode_frame(encode_frame(exact)) == exact
    with pytest.raises(ProtocolValidationError, match="1-255"):
        encode_frame(replace(exact, actor_path="/" + "x" * 256))


def test_local_integer_uuid_enum_and_payload_validation_is_strict():
    with pytest.raises(ProtocolValidationError, match="lane ID"):
        encode_frame(replace(base_user_message(), lane_id=-1))
    with pytest.raises(ProtocolValidationError, match="UUID"):
        encode_frame(replace(base_user_message(), recipient_actor_uid=UUID(int=0)))
    with pytest.raises(ProtocolValidationError, match="bytes"):
        encode_frame(replace(base_user_message(), payload=bytearray()))  # type: ignore[arg-type]
    with pytest.raises(ProtocolValidationError, match="ReasonCode"):
        encode_frame(GoAway(1))  # type: ignore[arg-type]
    with pytest.raises(ProtocolValidationError, match="correlation"):
        encode_frame(replace(base_user_message(), correlation_id=1))


def test_hello_accept_rejects_noncanonical_route_order_on_encode_and_decode():
    limits = {
        "maximum_frame_bytes": 65_536,
        "lane_count": 1,
        "outbound_message_limit": 1,
        "outbound_byte_limit": 1,
        "inbound_message_limit": 1,
        "inbound_byte_limit": 1,
    }
    first = SerializerRoute(UUID(int=1), 7, ("a",))
    second = SerializerRoute(UUID(int=2), 7, ("a",))
    canonical = HelloAccept(
        ASSOCIATION_UID,
        0,
        **limits,
        serializer_routes=(first, second),
    )

    with pytest.raises(ProtocolValidationError, match="canonical"):
        encode_frame(replace(canonical, serializer_routes=(second, first)))

    encoded = bytearray(encode_frame(canonical))
    first_route = bytes(encoded[66:91])
    second_route = bytes(encoded[91:116])
    encoded[66:91] = second_route
    encoded[91:116] = first_route
    with pytest.raises(MalformedFrameError, match="canonical"):
        decode_frame(bytes(encoded))


def test_hello_accept_rejects_noncanonical_manifest_order_from_wire():
    frame = replace(
        all_frames()[1],
        serializer_routes=(SerializerRoute(SYSTEM_UID, 7, ("a", "b")),),
        capabilities=(),
    )
    encoded = bytearray(encode_frame(frame))
    encoded[90], encoded[93] = encoded[93], encoded[90]

    with pytest.raises(MalformedFrameError, match="manifests"):
        decode_frame(bytes(encoded))


def test_capability_negotiation_is_a_utf8_sorted_intersection():
    assert negotiate_capabilities(("z", "a", "unused"), ("a", "z")) == ("a", "z")
    with pytest.raises(ProtocolValidationError, match="NFC"):
        negotiate_capabilities(("e\N{COMBINING ACUTE ACCENT}",), ())
