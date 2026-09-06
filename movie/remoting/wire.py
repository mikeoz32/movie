"""Bounded big-endian codec for the Movie remoting v1 wire contract."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import TypeAlias
from unicodedata import is_normalized
from uuid import UUID

from movie.remoting.errors import (
    FrameTooLargeError,
    InvalidPreambleError,
    MalformedFrameError,
    ProtocolValidationError,
    SerializerRegistryError,
    UnsupportedFeatureError,
    UnsupportedFrameError,
    UnsupportedHeaderVersionError,
    WrongStreamError,
)
from movie.remoting.serialization import (
    MAX_MANIFEST_BYTES,
    SerializerDescriptor,
    SerializerRoute,
)

MAGIC = b"MOV1"
ALPN = "movie-remoting/1"
PROTOCOL_MAJOR = 1
PROTOCOL_MINOR = 0
HEADER_VERSION = 1
BOOTSTRAP_MAX_FRAME_BYTES = 1 << 20
PREAMBLE_SIZE = 23
COMMON_HEADER_SIZE = 16
MINIMUM_GOAWAY_FRAME_BYTES = COMMON_HEADER_SIZE + 4
CONTROL_LANE_ID = 0xFFFF
MAX_U8 = (1 << 8) - 1
MAX_U16 = (1 << 16) - 1
MAX_U32 = (1 << 32) - 1
MAX_U64 = (1 << 64) - 1
_HEADER_AFTER_LENGTH_SIZE = COMMON_HEADER_SIZE - 4
_USER_MESSAGE_FIXED_BODY_SIZE = 84
_USER_MESSAGE_PREFIX = struct.Struct(">IBBHQ16sHQ16s16s16sIHI")
_USER_MESSAGE_BODY = struct.Struct(">16sHQ16s16s16sIHI")


class StreamKind(IntEnum):
    CONTROL = 0
    DELIVERY_LANE = 1
    MULTIPLEXED = 2


class AssociationRole(IntEnum):
    INITIATOR = 0
    RESPONDER = 1


class FrameType(IntEnum):
    HELLO = 0x01
    HELLO_ACCEPT = 0x02
    GOAWAY = 0x03
    HELLO_REJECT = 0x04
    RESOLVE_REQUEST = 0x10
    RESOLVE_RESPONSE = 0x11
    RESOLVE_REJECTED = 0x12
    USER_MESSAGE = 0x20
    RECIPIENT_UNAVAILABLE = 0x30
    DESERIALIZATION_REJECTED = 0x31


class ReasonCode(IntEnum):
    NORMAL_SHUTDOWN = 0x0000
    INCOMPATIBLE_VERSION = 0x0001
    SYSTEM_NAME_MISMATCH = 0x0002
    DUPLICATE_ASSOCIATION = 0x0003
    UNSUPPORTED_FRAME = 0x0004
    PROTOCOL_VIOLATION = 0x0005
    FLOW_CONTROL_VIOLATION = 0x0006
    ACTOR_NOT_FOUND = 0x0010
    ACTOR_STOPPING = 0x0011
    MAILBOX_FULL = 0x0012
    INVALID_PATH = 0x0013
    UNKNOWN_SERIALIZER = 0x0020
    UNSUPPORTED_MANIFEST = 0x0021
    MALFORMED_PAYLOAD = 0x0022


@dataclass(frozen=True, slots=True)
class StreamPreamble:
    stream_kind: StreamKind
    association_uid: UUID
    lane_id: int


@dataclass(frozen=True, slots=True)
class FrameHeader:
    frame_length: int
    frame_type: FrameType
    flags: int
    header_version: int
    correlation_id: int

    @property
    def total_length(self) -> int:
        return self.frame_length + 4

    @property
    def body_length(self) -> int:
        return self.frame_length - _HEADER_AFTER_LENGTH_SIZE


@dataclass(frozen=True, slots=True)
class Hello:
    protocol_major: int
    protocol_minor: int
    role: AssociationRole
    system_name: str
    system_incarnation_uid: UUID
    association_uid: UUID
    endpoint_host: str
    endpoint_port: int
    maximum_frame_bytes: int
    lane_count: int
    outbound_message_limit: int
    outbound_byte_limit: int
    inbound_message_limit: int
    inbound_byte_limit: int
    serializers: tuple[SerializerDescriptor, ...] = ()
    capabilities: tuple[str, ...] = ()
    correlation_id: int = 0


@dataclass(frozen=True, slots=True)
class HelloAccept:
    association_uid: UUID
    protocol_minor: int
    maximum_frame_bytes: int
    lane_count: int
    outbound_message_limit: int
    outbound_byte_limit: int
    inbound_message_limit: int
    inbound_byte_limit: int
    serializer_routes: tuple[SerializerRoute, ...] = ()
    capabilities: tuple[str, ...] = ()
    correlation_id: int = 0


@dataclass(frozen=True, slots=True)
class GoAway:
    reason: ReasonCode
    detail: str = ""
    correlation_id: int = 0


@dataclass(frozen=True, slots=True)
class HelloReject:
    reason: ReasonCode
    detail: str = ""
    correlation_id: int = 0


@dataclass(frozen=True, slots=True)
class ResolveRequest:
    correlation_id: int
    system_name: str
    actor_path: str


@dataclass(frozen=True, slots=True)
class ResolveResponse:
    correlation_id: int
    system_incarnation_uid: UUID
    actor_uid: UUID
    actor_path: str


@dataclass(frozen=True, slots=True)
class ResolveRejected:
    correlation_id: int
    reason: ReasonCode
    detail: str = ""


@dataclass(frozen=True, slots=True)
class UserMessage:
    association_uid: UUID
    lane_id: int
    lane_sequence: int
    sender_incarnation_uid: UUID
    recipient_incarnation_uid: UUID
    recipient_actor_uid: UUID
    serializer_id: int
    manifest: str
    payload: bytes
    correlation_id: int = 0


@dataclass(frozen=True, slots=True)
class RecipientUnavailable:
    association_uid: UUID
    lane_id: int
    lane_sequence: int
    recipient_actor_uid: UUID
    reason: ReasonCode
    detail: str = ""
    correlation_id: int = 0


@dataclass(frozen=True, slots=True)
class DeserializationRejected:
    association_uid: UUID
    lane_id: int
    lane_sequence: int
    recipient_actor_uid: UUID
    reason: ReasonCode
    detail: str = ""
    correlation_id: int = 0


Frame: TypeAlias = (
    Hello
    | HelloAccept
    | GoAway
    | HelloReject
    | ResolveRequest
    | ResolveResponse
    | ResolveRejected
    | UserMessage
    | RecipientUnavailable
    | DeserializationRejected
)

_FRAME_TYPES: dict[type[object], FrameType] = {
    Hello: FrameType.HELLO,
    HelloAccept: FrameType.HELLO_ACCEPT,
    GoAway: FrameType.GOAWAY,
    HelloReject: FrameType.HELLO_REJECT,
    ResolveRequest: FrameType.RESOLVE_REQUEST,
    ResolveResponse: FrameType.RESOLVE_RESPONSE,
    ResolveRejected: FrameType.RESOLVE_REJECTED,
    UserMessage: FrameType.USER_MESSAGE,
    RecipientUnavailable: FrameType.RECIPIENT_UNAVAILABLE,
    DeserializationRejected: FrameType.DESERIALIZATION_REJECTED,
}
_CONTROL_FRAME_TYPES = frozenset(FrameType) - {FrameType.USER_MESSAGE}
_ZERO_CORRELATION_TYPES = {
    FrameType.HELLO,
    FrameType.HELLO_ACCEPT,
    FrameType.GOAWAY,
    FrameType.HELLO_REJECT,
    FrameType.USER_MESSAGE,
    FrameType.RECIPIENT_UNAVAILABLE,
    FrameType.DESERIALIZATION_REJECTED,
}
_BOOTSTRAP_FRAME_TYPES = {
    FrameType.HELLO,
    FrameType.HELLO_ACCEPT,
    FrameType.HELLO_REJECT,
}
_FRAME_REASON_CODES = {
    FrameType.RESOLVE_REJECTED: {
        ReasonCode.SYSTEM_NAME_MISMATCH,
        ReasonCode.ACTOR_NOT_FOUND,
        ReasonCode.ACTOR_STOPPING,
        ReasonCode.INVALID_PATH,
    },
    FrameType.RECIPIENT_UNAVAILABLE: {
        ReasonCode.ACTOR_NOT_FOUND,
        ReasonCode.ACTOR_STOPPING,
        ReasonCode.MAILBOX_FULL,
    },
    FrameType.DESERIALIZATION_REJECTED: {
        ReasonCode.UNKNOWN_SERIALIZER,
        ReasonCode.UNSUPPORTED_MANIFEST,
        ReasonCode.MALFORMED_PAYLOAD,
    },
}
_UNRESERVED = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._~-"
)


def _uint(
    value: int,
    maximum: int,
    field: str,
    error_type: type[Exception],
    *,
    minimum: int = 0,
) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not minimum <= value <= maximum:
        raise error_type(f"{field} must be between {minimum} and {maximum}")
    return value


def _uuid(value: UUID, field: str, error_type: type[Exception]) -> UUID:
    if not isinstance(value, UUID) or not value.int:
        raise error_type(f"{field} must be a nonzero UUID")
    return value


def _text_bytes(
    value: str,
    field: str,
    maximum: int,
    error_type: type[Exception],
    *,
    nonempty: bool = False,
) -> bytes:
    if not isinstance(value, str):
        raise error_type(f"{field} must be a string")
    if not is_normalized("NFC", value):
        raise error_type(f"{field} must be NFC-normalized")
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise error_type(f"{field} must be valid UTF-8") from error
    if nonempty and not encoded:
        raise error_type(f"{field} must not be empty")
    if len(encoded) > maximum:
        raise error_type(f"{field} exceeds {maximum} UTF-8 bytes")
    return encoded


def _system_name(value: str, error_type: type[Exception]) -> bytes:
    encoded = _text_bytes(value, "actor-system name", 255, error_type, nonempty=True)
    if any(character not in _UNRESERVED for character in value):
        raise error_type("actor-system name contains a non-URI-unreserved character")
    return encoded


def _actor_path(value: str, error_type: type[Exception]) -> bytes:
    encoded = _text_bytes(value, "actor path", MAX_U16, error_type, nonempty=True)
    if not value.startswith("/") or value.startswith("//"):
        raise error_type("actor path must start with exactly one slash")
    segments = value[1:].split("/")
    if not segments or any(not segment for segment in segments):
        raise error_type("actor path must not contain empty segments")
    for segment in segments:
        segment_bytes = segment.encode("ascii", errors="strict") if segment.isascii() else b""
        if not 1 <= len(segment_bytes) <= 255 or any(
            character not in _UNRESERVED for character in segment
        ):
            raise error_type("actor path segment is not 1-255 ASCII URI-unreserved bytes")
    return encoded


class _Writer:
    def __init__(self) -> None:
        self.data = bytearray()

    def u8(self, value: int, field: str) -> None:
        self.data.extend(struct.pack(">B", _uint(value, MAX_U8, field, ProtocolValidationError)))

    def u16(self, value: int, field: str) -> None:
        self.data.extend(struct.pack(">H", _uint(value, MAX_U16, field, ProtocolValidationError)))

    def u32(self, value: int, field: str) -> None:
        self.data.extend(struct.pack(">I", _uint(value, MAX_U32, field, ProtocolValidationError)))

    def u64(self, value: int, field: str) -> None:
        self.data.extend(struct.pack(">Q", _uint(value, MAX_U64, field, ProtocolValidationError)))

    def uuid(self, value: UUID, field: str) -> None:
        self.data.extend(_uuid(value, field, ProtocolValidationError).bytes)

    def raw(self, value: bytes) -> None:
        self.data.extend(value)

    def string16(
        self, value: str, field: str, maximum: int = MAX_U16, *, nonempty: bool = False
    ) -> None:
        encoded = _text_bytes(
            value, field, min(maximum, MAX_U16), ProtocolValidationError, nonempty=nonempty
        )
        self.u16(len(encoded), f"{field} length")
        self.raw(encoded)


class _Reader:
    def __init__(self, data: bytes | memoryview) -> None:
        self._data = memoryview(data)
        self._position = 0

    @property
    def remaining(self) -> int:
        return len(self._data) - self._position

    def _take(self, length: int, field: str) -> memoryview:
        if length > self.remaining:
            raise MalformedFrameError(f"truncated {field}")
        start = self._position
        self._position += length
        return self._data[start : start + length]

    def u8(self, field: str) -> int:
        return struct.unpack(">B", self._take(1, field))[0]

    def u16(self, field: str) -> int:
        return struct.unpack(">H", self._take(2, field))[0]

    def u32(self, field: str) -> int:
        return struct.unpack(">I", self._take(4, field))[0]

    def u64(self, field: str) -> int:
        return struct.unpack(">Q", self._take(8, field))[0]

    def uuid(self, field: str) -> UUID:
        value = UUID(bytes=bytes(self._take(16, field)))
        return _uuid(value, field, MalformedFrameError)

    def raw(self, length: int, field: str) -> bytes:
        return bytes(self._take(length, field))

    def string16(
        self, field: str, maximum: int = MAX_U16, *, nonempty: bool = False
    ) -> str:
        length = self.u16(f"{field} length")
        if length > maximum:
            raise MalformedFrameError(f"{field} exceeds {maximum} UTF-8 bytes")
        encoded = self.raw(length, field)
        try:
            value = encoded.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise MalformedFrameError(f"{field} is not valid UTF-8") from error
        _text_bytes(value, field, maximum, MalformedFrameError, nonempty=nonempty)
        return value

    def finish(self) -> None:
        if self.remaining:
            raise MalformedFrameError(f"frame body has {self.remaining} trailing bytes")


def _stream_kind(value: StreamKind, error_type: type[Exception]) -> StreamKind:
    if not isinstance(value, StreamKind):
        raise error_type("stream kind must be a StreamKind value")
    return value


def _validate_preamble(
    preamble: StreamPreamble,
    error_type: type[Exception],
    *,
    lane_count: int | None = None,
    expected_association_uid: UUID | None = None,
    expected_kind: StreamKind | None = None,
) -> None:
    kind = _stream_kind(preamble.stream_kind, error_type)
    association_uid = _uuid(preamble.association_uid, "association UID", error_type)
    lane_id = _uint(preamble.lane_id, MAX_U16, "lane ID", error_type)
    if kind is StreamKind.CONTROL and lane_id != CONTROL_LANE_ID:
        raise error_type("control stream lane ID must be 0xffff")
    if kind is StreamKind.MULTIPLEXED and lane_id != CONTROL_LANE_ID:
        raise error_type("multiplexed connection lane ID must be 0xffff")
    if kind is StreamKind.DELIVERY_LANE and lane_id == CONTROL_LANE_ID:
        raise error_type("delivery-lane stream cannot use lane ID 0xffff")
    if lane_count is not None:
        _uint(lane_count, MAX_U16, "lane count", error_type)
        if kind is StreamKind.DELIVERY_LANE and lane_id >= lane_count:
            raise error_type(f"lane ID {lane_id} is outside negotiated lane count {lane_count}")
    if expected_association_uid is not None and association_uid != expected_association_uid:
        raise error_type("preamble association UID does not match the active association")
    if expected_kind is not None:
        _stream_kind(expected_kind, error_type)
        if kind is not expected_kind:
            raise error_type(f"expected {expected_kind.name} stream, got {kind.name}")


def encode_preamble(preamble: StreamPreamble, *, lane_count: int | None = None) -> bytes:
    _validate_preamble(preamble, ProtocolValidationError, lane_count=lane_count)
    return b"".join(
        (
            MAGIC,
            bytes((preamble.stream_kind,)),
            preamble.association_uid.bytes,
            struct.pack(">H", preamble.lane_id),
        )
    )


def decode_preamble(
    data: bytes,
    *,
    lane_count: int | None = None,
    expected_association_uid: UUID | None = None,
    expected_kind: StreamKind | None = None,
) -> StreamPreamble:
    if not isinstance(data, bytes) or len(data) != PREAMBLE_SIZE:
        raise InvalidPreambleError(f"stream preamble must be exactly {PREAMBLE_SIZE} bytes")
    if data[:4] != MAGIC:
        raise InvalidPreambleError("invalid stream preamble magic")
    try:
        kind = StreamKind(data[4])
    except ValueError as error:
        raise InvalidPreambleError(f"unknown stream kind {data[4]}") from error
    preamble = StreamPreamble(kind, UUID(bytes=data[5:21]), struct.unpack(">H", data[21:23])[0])
    _validate_preamble(
        preamble,
        InvalidPreambleError,
        lane_count=lane_count,
        expected_association_uid=expected_association_uid,
        expected_kind=expected_kind,
    )
    return preamble


def _effective_limit(frame_type: FrameType, maximum_frame_bytes: int) -> int:
    _uint(
        maximum_frame_bytes,
        MAX_U32,
        "maximum frame bytes",
        ProtocolValidationError,
        minimum=COMMON_HEADER_SIZE,
    )
    if frame_type in _BOOTSTRAP_FRAME_TYPES:
        return min(maximum_frame_bytes, BOOTSTRAP_MAX_FRAME_BYTES)
    return maximum_frame_bytes


def _validate_correlation(
    frame_type: FrameType, correlation_id: int, error_type: type[Exception]
) -> None:
    _uint(correlation_id, MAX_U64, "correlation ID", error_type)
    if frame_type in _ZERO_CORRELATION_TYPES and correlation_id:
        raise error_type(f"{frame_type.name} requires correlation ID zero")
    if frame_type not in _ZERO_CORRELATION_TYPES and not correlation_id:
        raise error_type(f"{frame_type.name} requires a nonzero correlation ID")


def encode_common_header(
    frame_type: FrameType,
    body_length: int,
    correlation_id: int,
    *,
    maximum_frame_bytes: int = BOOTSTRAP_MAX_FRAME_BYTES,
) -> bytes:
    if not isinstance(frame_type, FrameType):
        raise ProtocolValidationError("frame type must be a FrameType value")
    _uint(
        body_length,
        MAX_U32 - _HEADER_AFTER_LENGTH_SIZE,
        "frame body length",
        ProtocolValidationError,
    )
    _validate_correlation(frame_type, correlation_id, ProtocolValidationError)
    frame_length = _HEADER_AFTER_LENGTH_SIZE + body_length
    total_length = frame_length + 4
    limit = _effective_limit(frame_type, maximum_frame_bytes)
    if total_length > limit:
        raise FrameTooLargeError(f"frame size {total_length} exceeds limit {limit}")
    return struct.pack(">IBBHQ", frame_length, frame_type, 0, HEADER_VERSION, correlation_id)


def decode_common_header(
    data: bytes,
    *,
    maximum_frame_bytes: int = BOOTSTRAP_MAX_FRAME_BYTES,
) -> FrameHeader:
    if not isinstance(data, bytes) or len(data) < COMMON_HEADER_SIZE:
        raise MalformedFrameError(f"common frame header requires {COMMON_HEADER_SIZE} bytes")
    frame_length, frame_type_value, flags, version, correlation_id = struct.unpack(
        ">IBBHQ", data[:COMMON_HEADER_SIZE]
    )
    if frame_length < _HEADER_AFTER_LENGTH_SIZE:
        raise MalformedFrameError("frame length is smaller than the common header")
    total_length = frame_length + 4
    _uint(
        maximum_frame_bytes,
        MAX_U32,
        "maximum frame bytes",
        ProtocolValidationError,
        minimum=COMMON_HEADER_SIZE,
    )
    if total_length > maximum_frame_bytes:
        raise FrameTooLargeError(
            f"declared frame size {total_length} exceeds limit {maximum_frame_bytes}"
        )
    try:
        frame_type = FrameType(frame_type_value)
    except ValueError as error:
        raise UnsupportedFrameError(f"unknown frame type 0x{frame_type_value:02x}") from error
    limit = _effective_limit(frame_type, maximum_frame_bytes)
    if total_length > limit:
        raise FrameTooLargeError(f"declared frame size {total_length} exceeds limit {limit}")
    if flags:
        raise UnsupportedFeatureError(f"unsupported mandatory frame flags 0x{flags:02x}")
    if version != HEADER_VERSION:
        raise UnsupportedHeaderVersionError(
            f"unsupported frame header version {version}"
        )
    _validate_correlation(frame_type, correlation_id, MalformedFrameError)
    return FrameHeader(frame_length, frame_type, flags, version, correlation_id)


def _validate_stream(frame_type: FrameType, stream_kind: StreamKind | None) -> None:
    if stream_kind is None:
        return
    _stream_kind(stream_kind, WrongStreamError)
    if stream_kind is StreamKind.MULTIPLEXED:
        return
    expected = (
        StreamKind.CONTROL if frame_type in _CONTROL_FRAME_TYPES else StreamKind.DELIVERY_LANE
    )
    if stream_kind is not expected:
        raise WrongStreamError(f"{frame_type.name} is valid only on a {expected.name} stream")


def _write_descriptor(writer: _Writer, descriptor: SerializerDescriptor) -> None:
    if not isinstance(descriptor, SerializerDescriptor):
        raise ProtocolValidationError("serializer entry must be a SerializerDescriptor")
    writer.u32(descriptor.serializer_id, "serializer ID")
    writer.string16(descriptor.name, "serializer name", nonempty=True)
    writer.u16(descriptor.protocol_major, "serializer protocol major")
    writer.u16(descriptor.protocol_minor, "serializer protocol minor")
    readable = sorted(descriptor.readable_manifests, key=lambda item: item.encode("utf-8"))
    writable = sorted(descriptor.writable_manifests, key=lambda item: item.encode("utf-8"))
    writer.u16(len(readable), "readable manifest count")
    for manifest in readable:
        writer.string16(manifest, "readable manifest", MAX_MANIFEST_BYTES, nonempty=True)
    writer.u16(len(writable), "writable manifest count")
    for manifest in writable:
        writer.string16(manifest, "writable manifest", MAX_MANIFEST_BYTES, nonempty=True)


def _read_descriptor(reader: _Reader) -> SerializerDescriptor:
    serializer_id = reader.u32("serializer ID")
    name = reader.string16("serializer name", nonempty=True)
    protocol_major = reader.u16("serializer protocol major")
    protocol_minor = reader.u16("serializer protocol minor")
    readable = tuple(
        reader.string16("readable manifest", MAX_MANIFEST_BYTES, nonempty=True)
        for _ in range(reader.u16("readable manifest count"))
    )
    writable = tuple(
        reader.string16("writable manifest", MAX_MANIFEST_BYTES, nonempty=True)
        for _ in range(reader.u16("writable manifest count"))
    )
    if len(set(readable)) != len(readable) or len(set(writable)) != len(writable):
        raise MalformedFrameError("serializer descriptor contains duplicate manifests")
    try:
        return SerializerDescriptor(
            serializer_id,
            name,
            protocol_major,
            protocol_minor,
            frozenset(readable),
            frozenset(writable),
        )
    except SerializerRegistryError as error:
        raise MalformedFrameError(str(error)) from error


def _write_limits(
    writer: _Writer,
    maximum_frame_bytes: int,
    lane_count: int,
    outbound_message_limit: int,
    outbound_byte_limit: int,
    inbound_message_limit: int,
    inbound_byte_limit: int,
) -> None:
    writer.u32(maximum_frame_bytes, "maximum frame bytes")
    if maximum_frame_bytes < COMMON_HEADER_SIZE:
        raise ProtocolValidationError(
            f"maximum frame bytes must be at least {COMMON_HEADER_SIZE}"
        )
    writer.u16(lane_count, "lane count")
    writer.u32(outbound_message_limit, "outbound message limit")
    writer.u64(outbound_byte_limit, "outbound byte limit")
    writer.u32(inbound_message_limit, "inbound message limit")
    writer.u64(inbound_byte_limit, "inbound byte limit")


def _read_limits(reader: _Reader) -> tuple[int, int, int, int, int, int]:
    maximum_frame_bytes = reader.u32("maximum frame bytes")
    if maximum_frame_bytes < COMMON_HEADER_SIZE:
        raise MalformedFrameError(
            f"maximum frame bytes must be at least {COMMON_HEADER_SIZE}"
        )
    return (
        maximum_frame_bytes,
        reader.u16("lane count"),
        reader.u32("outbound message limit"),
        reader.u64("outbound byte limit"),
        reader.u32("inbound message limit"),
        reader.u64("inbound byte limit"),
    )


def _write_capabilities(writer: _Writer, capabilities: tuple[str, ...]) -> None:
    if len(capabilities) > MAX_U16:
        raise ProtocolValidationError("capability count exceeds 65535")
    for capability in capabilities:
        _text_bytes(
            capability,
            "capability name",
            MAX_U16,
            ProtocolValidationError,
            nonempty=True,
        )
    if len(set(capabilities)) != len(capabilities):
        raise ProtocolValidationError("capability names must be unique")
    writer.u16(len(capabilities), "capability count")
    for capability in capabilities:
        writer.string16(capability, "capability name", nonempty=True)


def _read_capabilities(reader: _Reader) -> tuple[str, ...]:
    capabilities = tuple(
        reader.string16("capability name", nonempty=True)
        for _ in range(reader.u16("capability count"))
    )
    if len(set(capabilities)) != len(capabilities):
        raise MalformedFrameError("capability names must be unique")
    return capabilities


def _route_key(route: SerializerRoute) -> tuple[bytes, int, tuple[bytes, ...]]:
    return (
        route.origin_incarnation_uid.bytes,
        route.serializer_id,
        tuple(manifest.encode("utf-8") for manifest in route.manifests),
    )


def _write_routes(writer: _Writer, routes: tuple[SerializerRoute, ...]) -> None:
    if len(routes) > MAX_U16:
        raise ProtocolValidationError("serializer route count exceeds 65535")
    if any(not isinstance(route, SerializerRoute) for route in routes):
        raise ProtocolValidationError("serializer route entry has the wrong type")
    if tuple(sorted(routes, key=_route_key)) != routes:
        raise ProtocolValidationError("serializer routes are not in canonical order")
    route_ids = [(route.origin_incarnation_uid, route.serializer_id) for route in routes]
    if len(set(route_ids)) != len(route_ids):
        raise ProtocolValidationError("serializer routes contain a duplicate origin and ID")
    writer.u16(len(routes), "serializer route count")
    for route in routes:
        writer.uuid(route.origin_incarnation_uid, "route origin incarnation UID")
        writer.u32(route.serializer_id, "route serializer ID")
        writer.u16(len(route.manifests), "route manifest count")
        for manifest in route.manifests:
            writer.string16(manifest, "route manifest", MAX_MANIFEST_BYTES, nonempty=True)


def _read_routes(reader: _Reader) -> tuple[SerializerRoute, ...]:
    routes: list[SerializerRoute] = []
    for _ in range(reader.u16("serializer route count")):
        origin_uid = reader.uuid("route origin incarnation UID")
        serializer_id = reader.u32("route serializer ID")
        manifests = tuple(
            reader.string16("route manifest", MAX_MANIFEST_BYTES, nonempty=True)
            for _ in range(reader.u16("route manifest count"))
        )
        canonical_manifests = tuple(sorted(set(manifests), key=lambda item: item.encode("utf-8")))
        if not manifests or manifests != canonical_manifests:
            raise MalformedFrameError("route manifests are empty, duplicated, or noncanonical")
        try:
            routes.append(SerializerRoute(origin_uid, serializer_id, manifests))
        except SerializerRegistryError as error:
            raise MalformedFrameError(str(error)) from error
    result = tuple(routes)
    if result != tuple(sorted(result, key=_route_key)):
        raise MalformedFrameError("serializer routes are not in canonical order")
    route_ids = [(route.origin_incarnation_uid, route.serializer_id) for route in result]
    if len(set(route_ids)) != len(route_ids):
        raise MalformedFrameError("serializer routes contain a duplicate origin and ID")
    return result


def _write_reason(writer: _Writer, reason: ReasonCode, detail: str) -> None:
    if not isinstance(reason, ReasonCode):
        raise ProtocolValidationError("reason must be a ReasonCode value")
    writer.u16(reason, "reason code")
    writer.string16(detail, "reason detail")


def _read_reason(reader: _Reader) -> tuple[ReasonCode, str]:
    value = reader.u16("reason code")
    try:
        reason = ReasonCode(value)
    except ValueError as error:
        raise MalformedFrameError(f"unknown reason code 0x{value:04x}") from error
    return reason, reader.string16("reason detail")


def _validate_frame_reason(
    frame_type: FrameType,
    reason: ReasonCode,
    error_type: type[Exception],
) -> None:
    allowed = _FRAME_REASON_CODES.get(frame_type)
    if allowed is not None and reason not in allowed:
        raise error_type(f"reason {reason.name} is not valid for {frame_type.name}")


def _write_advisory(
    writer: _Writer,
    association_uid: UUID,
    lane_id: int,
    lane_sequence: int,
    recipient_actor_uid: UUID,
    reason: ReasonCode,
    detail: str,
) -> None:
    writer.uuid(association_uid, "association UID")
    writer.u16(lane_id, "lane ID")
    if lane_id == CONTROL_LANE_ID:
        raise ProtocolValidationError("advisory lane ID cannot be 0xffff")
    writer.u64(lane_sequence, "lane sequence")
    writer.uuid(recipient_actor_uid, "recipient actor UID")
    _write_reason(writer, reason, detail)


def _read_advisory(reader: _Reader) -> tuple[UUID, int, int, UUID, ReasonCode, str]:
    association_uid = reader.uuid("association UID")
    lane_id = reader.u16("lane ID")
    if lane_id == CONTROL_LANE_ID:
        raise MalformedFrameError("advisory lane ID cannot be 0xffff")
    lane_sequence = reader.u64("lane sequence")
    recipient_actor_uid = reader.uuid("recipient actor UID")
    reason, detail = _read_reason(reader)
    return association_uid, lane_id, lane_sequence, recipient_actor_uid, reason, detail


def _encode_body(frame: Frame) -> bytes:
    writer = _Writer()
    if isinstance(frame, Hello):
        writer.u16(frame.protocol_major, "protocol major")
        writer.u16(frame.protocol_minor, "protocol minor")
        if not isinstance(frame.role, AssociationRole):
            raise ProtocolValidationError("HELLO role must be an AssociationRole value")
        writer.u8(frame.role, "role")
        system_name = _system_name(frame.system_name, ProtocolValidationError)
        writer.u16(len(system_name), "actor-system name length")
        writer.raw(system_name)
        writer.uuid(frame.system_incarnation_uid, "system incarnation UID")
        writer.uuid(frame.association_uid, "association UID")
        writer.string16(frame.endpoint_host, "endpoint host", nonempty=True)
        writer.u16(frame.endpoint_port, "endpoint port")
        _write_limits(
            writer,
            frame.maximum_frame_bytes,
            frame.lane_count,
            frame.outbound_message_limit,
            frame.outbound_byte_limit,
            frame.inbound_message_limit,
            frame.inbound_byte_limit,
        )
        if len(frame.serializers) > MAX_U16:
            raise ProtocolValidationError("serializer descriptor count exceeds 65535")
        if any(
            not isinstance(descriptor, SerializerDescriptor) for descriptor in frame.serializers
        ):
            raise ProtocolValidationError(
                "HELLO serializer entry must be a SerializerDescriptor"
            )
        ids = [descriptor.serializer_id for descriptor in frame.serializers]
        names = [descriptor.name for descriptor in frame.serializers]
        if len(set(ids)) != len(ids) or len(set(names)) != len(names):
            raise ProtocolValidationError("HELLO serializer IDs and names must be unique")
        writer.u16(len(frame.serializers), "serializer descriptor count")
        for descriptor in frame.serializers:
            _write_descriptor(writer, descriptor)
        _write_capabilities(writer, frame.capabilities)
    elif isinstance(frame, HelloAccept):
        writer.uuid(frame.association_uid, "association UID")
        writer.u16(frame.protocol_minor, "protocol minor")
        _write_limits(
            writer,
            frame.maximum_frame_bytes,
            frame.lane_count,
            frame.outbound_message_limit,
            frame.outbound_byte_limit,
            frame.inbound_message_limit,
            frame.inbound_byte_limit,
        )
        _write_routes(writer, frame.serializer_routes)
        _write_capabilities(writer, frame.capabilities)
    elif isinstance(frame, (GoAway, HelloReject)):
        _write_reason(writer, frame.reason, frame.detail)
    elif isinstance(frame, ResolveRequest):
        system_name = _system_name(frame.system_name, ProtocolValidationError)
        writer.u16(len(system_name), "actor-system name length")
        writer.raw(system_name)
        actor_path = _actor_path(frame.actor_path, ProtocolValidationError)
        writer.u16(len(actor_path), "actor path length")
        writer.raw(actor_path)
    elif isinstance(frame, ResolveResponse):
        writer.uuid(frame.system_incarnation_uid, "system incarnation UID")
        writer.uuid(frame.actor_uid, "actor UID")
        actor_path = _actor_path(frame.actor_path, ProtocolValidationError)
        writer.u16(len(actor_path), "actor path length")
        writer.raw(actor_path)
    elif isinstance(frame, ResolveRejected):
        _validate_frame_reason(
            FrameType.RESOLVE_REJECTED,
            frame.reason,
            ProtocolValidationError,
        )
        _write_reason(writer, frame.reason, frame.detail)
    elif isinstance(frame, UserMessage):
        writer.uuid(frame.association_uid, "association UID")
        writer.u16(frame.lane_id, "lane ID")
        if frame.lane_id == CONTROL_LANE_ID:
            raise ProtocolValidationError("user-message lane ID cannot be 0xffff")
        writer.u64(frame.lane_sequence, "lane sequence")
        writer.uuid(frame.sender_incarnation_uid, "sender incarnation UID")
        writer.uuid(frame.recipient_incarnation_uid, "recipient incarnation UID")
        writer.uuid(frame.recipient_actor_uid, "recipient actor UID")
        writer.u32(frame.serializer_id, "serializer ID")
        if frame.serializer_id == 0:
            raise ProtocolValidationError("user-message serializer ID must be nonzero")
        manifest = _text_bytes(
            frame.manifest,
            "serializer manifest",
            MAX_MANIFEST_BYTES,
            ProtocolValidationError,
            nonempty=True,
        )
        if not isinstance(frame.payload, bytes):
            raise ProtocolValidationError("user-message payload must be bytes")
        writer.u16(len(manifest), "manifest length")
        writer.u32(len(frame.payload), "payload length")
        writer.raw(manifest)
        writer.raw(frame.payload)
    elif isinstance(frame, (RecipientUnavailable, DeserializationRejected)):
        frame_type = (
            FrameType.RECIPIENT_UNAVAILABLE
            if isinstance(frame, RecipientUnavailable)
            else FrameType.DESERIALIZATION_REJECTED
        )
        _validate_frame_reason(frame_type, frame.reason, ProtocolValidationError)
        _write_advisory(
            writer,
            frame.association_uid,
            frame.lane_id,
            frame.lane_sequence,
            frame.recipient_actor_uid,
            frame.reason,
            frame.detail,
        )
    else:
        raise ProtocolValidationError(f"unsupported frame dataclass {type(frame).__name__}")
    return bytes(writer.data)


def _encode_user_message_frame(
    frame: UserMessage,
    maximum_frame_bytes: int,
) -> bytes:
    _validate_correlation(
        FrameType.USER_MESSAGE,
        frame.correlation_id,
        ProtocolValidationError,
    )
    association_uid = _uuid(
        frame.association_uid,
        "association UID",
        ProtocolValidationError,
    )
    lane_id = _uint(frame.lane_id, MAX_U16, "lane ID", ProtocolValidationError)
    if lane_id == CONTROL_LANE_ID:
        raise ProtocolValidationError("user-message lane ID cannot be 0xffff")
    lane_sequence = _uint(
        frame.lane_sequence,
        MAX_U64,
        "lane sequence",
        ProtocolValidationError,
    )
    sender_uid = _uuid(
        frame.sender_incarnation_uid,
        "sender incarnation UID",
        ProtocolValidationError,
    )
    recipient_uid = _uuid(
        frame.recipient_incarnation_uid,
        "recipient incarnation UID",
        ProtocolValidationError,
    )
    actor_uid = _uuid(
        frame.recipient_actor_uid,
        "recipient actor UID",
        ProtocolValidationError,
    )
    serializer_id = _uint(
        frame.serializer_id,
        MAX_U32,
        "serializer ID",
        ProtocolValidationError,
    )
    if serializer_id == 0:
        raise ProtocolValidationError("user-message serializer ID must be nonzero")
    manifest = _text_bytes(
        frame.manifest,
        "serializer manifest",
        MAX_MANIFEST_BYTES,
        ProtocolValidationError,
        nonempty=True,
    )
    if not isinstance(frame.payload, bytes):
        raise ProtocolValidationError("user-message payload must be bytes")
    payload_length = _uint(
        len(frame.payload),
        MAX_U32,
        "payload length",
        ProtocolValidationError,
    )
    total_length = _USER_MESSAGE_PREFIX.size + len(manifest) + payload_length
    limit = _effective_limit(FrameType.USER_MESSAGE, maximum_frame_bytes)
    if total_length > limit:
        raise FrameTooLargeError(f"frame size {total_length} exceeds limit {limit}")
    return b"".join(
        (
            _USER_MESSAGE_PREFIX.pack(
                total_length - 4,
                FrameType.USER_MESSAGE,
                0,
                HEADER_VERSION,
                0,
                association_uid.bytes,
                lane_id,
                lane_sequence,
                sender_uid.bytes,
                recipient_uid.bytes,
                actor_uid.bytes,
                serializer_id,
                len(manifest),
                payload_length,
            ),
            manifest,
            frame.payload,
        )
    )


def _read_system_name(reader: _Reader) -> str:
    value = reader.string16("actor-system name", 255, nonempty=True)
    _system_name(value, MalformedFrameError)
    return value


def _read_actor_path(reader: _Reader) -> str:
    value = reader.string16("actor path", nonempty=True)
    _actor_path(value, MalformedFrameError)
    return value


def _decode_body(header: FrameHeader, body: memoryview) -> Frame:
    reader = _Reader(body)
    frame: Frame
    if header.frame_type is FrameType.HELLO:
        protocol_major = reader.u16("protocol major")
        protocol_minor = reader.u16("protocol minor")
        role_value = reader.u8("role")
        try:
            role = AssociationRole(role_value)
        except ValueError as error:
            raise MalformedFrameError(f"unknown association role {role_value}") from error
        system_name = _read_system_name(reader)
        system_uid = reader.uuid("system incarnation UID")
        association_uid = reader.uuid("association UID")
        endpoint_host = reader.string16("endpoint host", nonempty=True)
        endpoint_port = reader.u16("endpoint port")
        limits = _read_limits(reader)
        serializers = tuple(
            _read_descriptor(reader) for _ in range(reader.u16("serializer descriptor count"))
        )
        ids = [descriptor.serializer_id for descriptor in serializers]
        names = [descriptor.name for descriptor in serializers]
        if len(set(ids)) != len(ids) or len(set(names)) != len(names):
            raise MalformedFrameError("HELLO serializer IDs and names must be unique")
        capabilities = _read_capabilities(reader)
        frame = Hello(
            protocol_major,
            protocol_minor,
            role,
            system_name,
            system_uid,
            association_uid,
            endpoint_host,
            endpoint_port,
            *limits,
            serializers,
            capabilities,
            header.correlation_id,
        )
    elif header.frame_type is FrameType.HELLO_ACCEPT:
        association_uid = reader.uuid("association UID")
        protocol_minor = reader.u16("protocol minor")
        limits = _read_limits(reader)
        routes = _read_routes(reader)
        capabilities = _read_capabilities(reader)
        frame = HelloAccept(
            association_uid,
            protocol_minor,
            *limits,
            routes,
            capabilities,
            header.correlation_id,
        )
    elif header.frame_type in (FrameType.GOAWAY, FrameType.HELLO_REJECT):
        reason, detail = _read_reason(reader)
        frame = (
            GoAway(reason, detail, header.correlation_id)
            if header.frame_type is FrameType.GOAWAY
            else HelloReject(reason, detail, header.correlation_id)
        )
    elif header.frame_type is FrameType.RESOLVE_REQUEST:
        frame = ResolveRequest(
            header.correlation_id,
            _read_system_name(reader),
            _read_actor_path(reader),
        )
    elif header.frame_type is FrameType.RESOLVE_RESPONSE:
        frame = ResolveResponse(
            header.correlation_id,
            reader.uuid("system incarnation UID"),
            reader.uuid("actor UID"),
            _read_actor_path(reader),
        )
    elif header.frame_type is FrameType.RESOLVE_REJECTED:
        reason, detail = _read_reason(reader)
        _validate_frame_reason(header.frame_type, reason, MalformedFrameError)
        frame = ResolveRejected(header.correlation_id, reason, detail)
    elif header.frame_type is FrameType.USER_MESSAGE:
        association_uid = reader.uuid("association UID")
        lane_id = reader.u16("lane ID")
        if lane_id == CONTROL_LANE_ID:
            raise MalformedFrameError("user-message lane ID cannot be 0xffff")
        lane_sequence = reader.u64("lane sequence")
        sender_uid = reader.uuid("sender incarnation UID")
        recipient_uid = reader.uuid("recipient incarnation UID")
        actor_uid = reader.uuid("recipient actor UID")
        serializer_id = reader.u32("serializer ID")
        if serializer_id == 0:
            raise MalformedFrameError("user-message serializer ID must be nonzero")
        manifest_length = reader.u16("manifest length")
        if manifest_length > MAX_MANIFEST_BYTES:
            raise MalformedFrameError(
                f"serializer manifest exceeds {MAX_MANIFEST_BYTES} UTF-8 bytes"
            )
        payload_length = reader.u32("payload length")
        if manifest_length + payload_length != reader.remaining:
            raise MalformedFrameError("manifest and payload lengths do not exactly fit frame body")
        manifest_bytes = reader.raw(manifest_length, "serializer manifest")
        try:
            manifest = manifest_bytes.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise MalformedFrameError("serializer manifest is not valid UTF-8") from error
        _text_bytes(
            manifest,
            "serializer manifest",
            MAX_MANIFEST_BYTES,
            MalformedFrameError,
            nonempty=True,
        )
        payload = reader.raw(payload_length, "payload")
        frame = UserMessage(
            association_uid,
            lane_id,
            lane_sequence,
            sender_uid,
            recipient_uid,
            actor_uid,
            serializer_id,
            manifest,
            payload,
            header.correlation_id,
        )
    elif header.frame_type in (
        FrameType.RECIPIENT_UNAVAILABLE,
        FrameType.DESERIALIZATION_REJECTED,
    ):
        advisory = _read_advisory(reader)
        _validate_frame_reason(header.frame_type, advisory[4], MalformedFrameError)
        frame = (
            RecipientUnavailable(*advisory, header.correlation_id)
            if header.frame_type is FrameType.RECIPIENT_UNAVAILABLE
            else DeserializationRejected(*advisory, header.correlation_id)
        )
    else:
        raise UnsupportedFrameError(f"unsupported frame type {header.frame_type.name}")
    reader.finish()
    return frame


def _decode_user_message_frame(data: bytes, header: FrameHeader) -> UserMessage:
    body = memoryview(data)[COMMON_HEADER_SIZE:]
    if len(body) < _USER_MESSAGE_FIXED_BODY_SIZE:
        raise MalformedFrameError("truncated user-message fixed body")
    (
        association_bytes,
        lane_id,
        lane_sequence,
        sender_bytes,
        recipient_bytes,
        actor_bytes,
        serializer_id,
        manifest_length,
        payload_length,
    ) = _USER_MESSAGE_BODY.unpack_from(body)
    association_uid = _uuid(
        UUID(bytes=association_bytes),
        "association UID",
        MalformedFrameError,
    )
    if lane_id == CONTROL_LANE_ID:
        raise MalformedFrameError("user-message lane ID cannot be 0xffff")
    sender_uid = _uuid(
        UUID(bytes=sender_bytes),
        "sender incarnation UID",
        MalformedFrameError,
    )
    recipient_uid = _uuid(
        UUID(bytes=recipient_bytes),
        "recipient incarnation UID",
        MalformedFrameError,
    )
    actor_uid = _uuid(
        UUID(bytes=actor_bytes),
        "recipient actor UID",
        MalformedFrameError,
    )
    if serializer_id == 0:
        raise MalformedFrameError("user-message serializer ID must be nonzero")
    if manifest_length > MAX_MANIFEST_BYTES:
        raise MalformedFrameError(
            f"serializer manifest exceeds {MAX_MANIFEST_BYTES} UTF-8 bytes"
        )
    if (
        _USER_MESSAGE_FIXED_BODY_SIZE + manifest_length + payload_length
        != header.body_length
    ):
        raise MalformedFrameError("manifest and payload lengths do not exactly fit frame body")
    manifest_start = COMMON_HEADER_SIZE + _USER_MESSAGE_FIXED_BODY_SIZE
    manifest_end = manifest_start + manifest_length
    manifest_bytes = data[manifest_start:manifest_end]
    try:
        manifest = manifest_bytes.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise MalformedFrameError("serializer manifest is not valid UTF-8") from error
    _text_bytes(
        manifest,
        "serializer manifest",
        MAX_MANIFEST_BYTES,
        MalformedFrameError,
        nonempty=True,
    )
    return UserMessage(
        association_uid,
        lane_id,
        lane_sequence,
        sender_uid,
        recipient_uid,
        actor_uid,
        serializer_id,
        manifest,
        data[manifest_end:],
        header.correlation_id,
    )


def encode_frame(
    frame: Frame,
    *,
    maximum_frame_bytes: int = BOOTSTRAP_MAX_FRAME_BYTES,
    stream_kind: StreamKind | None = None,
) -> bytes:
    try:
        frame_type = _FRAME_TYPES[type(frame)]
    except KeyError as error:
        raise ProtocolValidationError(
            f"unsupported frame dataclass {type(frame).__name__}"
        ) from error
    _validate_stream(frame_type, stream_kind)
    if isinstance(frame, UserMessage):
        return _encode_user_message_frame(frame, maximum_frame_bytes)
    correlation_id = frame.correlation_id
    body = _encode_body(frame)
    header = encode_common_header(
        frame_type,
        len(body),
        correlation_id,
        maximum_frame_bytes=maximum_frame_bytes,
    )
    return header + body


def decode_frame(
    data: bytes,
    *,
    maximum_frame_bytes: int = BOOTSTRAP_MAX_FRAME_BYTES,
    stream_kind: StreamKind | None = None,
) -> Frame:
    header = decode_common_header(data, maximum_frame_bytes=maximum_frame_bytes)
    _validate_stream(header.frame_type, stream_kind)
    if len(data) < header.total_length:
        raise MalformedFrameError(
            f"truncated frame: declared {header.total_length} bytes, received {len(data)}"
        )
    if len(data) > header.total_length:
        raise MalformedFrameError(
            f"frame has {len(data) - header.total_length} bytes after its declared boundary"
        )
    if header.frame_type is FrameType.USER_MESSAGE:
        return _decode_user_message_frame(data, header)
    return _decode_body(header, memoryview(data)[COMMON_HEADER_SIZE:])


class FrameCodec:
    """Codec bound to one bootstrap or negotiated complete-frame limit."""

    def __init__(self, maximum_frame_bytes: int = BOOTSTRAP_MAX_FRAME_BYTES) -> None:
        _uint(
            maximum_frame_bytes,
            MAX_U32,
            "maximum frame bytes",
            ProtocolValidationError,
            minimum=COMMON_HEADER_SIZE,
        )
        self.maximum_frame_bytes = maximum_frame_bytes

    def encode(self, frame: Frame, *, stream_kind: StreamKind | None = None) -> bytes:
        return encode_frame(
            frame,
            maximum_frame_bytes=self.maximum_frame_bytes,
            stream_kind=stream_kind,
        )

    def decode(self, data: bytes, *, stream_kind: StreamKind | None = None) -> Frame:
        return decode_frame(
            data,
            maximum_frame_bytes=self.maximum_frame_bytes,
            stream_kind=stream_kind,
        )

    def decode_header(self, data: bytes) -> FrameHeader:
        return decode_common_header(data, maximum_frame_bytes=self.maximum_frame_bytes)


def negotiate_capabilities(
    first: tuple[str, ...], second: tuple[str, ...]
) -> tuple[str, ...]:
    """Return a deterministic intersection for an identical HELLO_ACCEPT body."""
    for capability in first + second:
        _text_bytes(
            capability,
            "capability name",
            MAX_U16,
            ProtocolValidationError,
            nonempty=True,
        )
    return tuple(sorted(set(first) & set(second), key=lambda item: item.encode("utf-8")))
