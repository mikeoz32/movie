from dataclasses import dataclass
from uuid import UUID

import pytest

from movie.remoting import (
    DeserializationError,
    SerializationError,
    SerializerDescriptor,
    SerializerNegotiationError,
    SerializerRegistryBuilder,
    SerializerRegistryError,
    SerializerRoute,
    UnknownSerializerError,
    UnsupportedManifestError,
    negotiate_serializers,
)

FIRST_UID = UUID(int=1)
SECOND_UID = UUID(int=2)


@dataclass
class Message:
    text: str


class ChildMessage(Message):
    pass


class TextSerializer:
    def __init__(self) -> None:
        self.serialize_calls = 0

    def serialize(self, value: object, manifest: str, protocol_minor: int) -> bytes:
        self.serialize_calls += 1
        if not isinstance(value, Message) or manifest != "message/v1":
            raise ValueError("unsupported value")
        return value.text.encode()

    def deserialize(self, payload: bytes, manifest: str, protocol_minor: int) -> object:
        if manifest != "message/v1":
            raise ValueError("unsupported manifest")
        return Message(payload.decode())


def descriptor(
    *,
    serializer_id: int = 7,
    name: str = "text-contract",
    major: int = 1,
    minor: int = 0,
    readable: frozenset[str] = frozenset({"message/v1"}),
    writable: frozenset[str] = frozenset({"message/v1"}),
) -> SerializerDescriptor:
    return SerializerDescriptor(serializer_id, name, major, minor, readable, writable)


def test_registry_uses_only_explicit_exact_type_bindings():
    serializer = TextSerializer()
    registry = (
        SerializerRegistryBuilder()
        .register(descriptor(), serializer)
        .bind(Message, 7, "message/v1")
        .build()
    )

    encoded = registry.serialize(Message("hello"))

    assert encoded.serializer_id == 7
    assert encoded.manifest == "message/v1"
    assert encoded.payload == b"hello"
    assert registry.deserialize(7, "message/v1", encoded.payload) == Message("hello")
    with pytest.raises(UnknownSerializerError, match="exact-type"):
        registry.serialize(ChildMessage("not inherited"))
    with pytest.raises(UnknownSerializerError):
        registry.serialize("no default serializer")


def test_registry_is_an_immutable_snapshot_of_builder_state():
    builder = SerializerRegistryBuilder().register(descriptor(), TextSerializer())
    registry = builder.bind(Message, 7, "message/v1").build()

    builder.register(descriptor(serializer_id=8, name="other"), TextSerializer())

    assert [item.serializer_id for item in registry.descriptors] == [7]
    with pytest.raises(TypeError):
        registry.bindings[Message] = registry.bindings[Message]  # type: ignore[index]


def test_route_is_checked_before_serializer_is_called():
    serializer = TextSerializer()
    registry = (
        SerializerRegistryBuilder()
        .register(descriptor(), serializer)
        .bind(Message, 7, "message/v1")
        .build()
    )
    wrong_route = SerializerRoute(FIRST_UID, 8, ("message/v1",))

    with pytest.raises(UnsupportedManifestError):
        registry.serialize_for_route(Message("never encoded"), wrong_route, 0)

    assert serializer.serialize_calls == 0


def test_registry_rejects_unknown_or_directionally_invalid_decode():
    registry = SerializerRegistryBuilder().register(descriptor(), TextSerializer()).build()

    with pytest.raises(UnknownSerializerError):
        registry.deserialize(99, "message/v1", b"value")
    with pytest.raises(UnsupportedManifestError):
        registry.deserialize(7, "message/v2", b"value")
    with pytest.raises(DeserializationError, match="must be bytes"):
        registry.deserialize(7, "message/v1", bytearray(b"value"))  # type: ignore[arg-type]


class BrokenSerializer:
    def serialize(self, value: object, manifest: str, protocol_minor: int) -> bytes:
        raise RuntimeError("encode failed")

    def deserialize(self, payload: bytes, manifest: str, protocol_minor: int) -> object:
        raise RuntimeError("decode failed")


class NonBytesSerializer(BrokenSerializer):
    def serialize(self, value: object, manifest: str, protocol_minor: int) -> bytes:
        return bytearray(b"wrong")  # type: ignore[return-value]


@pytest.mark.parametrize(
    ("serializer", "operation", "error_type"),
    [
        (BrokenSerializer(), "serialize", SerializationError),
        (BrokenSerializer(), "deserialize", DeserializationError),
        (NonBytesSerializer(), "serialize", SerializationError),
    ],
)
def test_registry_wraps_serializer_failures(serializer, operation, error_type):
    registry = (
        SerializerRegistryBuilder()
        .register(descriptor(), serializer)
        .bind(Message, 7, "message/v1")
        .build()
    )

    with pytest.raises(error_type):
        if operation == "serialize":
            registry.serialize(Message("value"))
        else:
            registry.deserialize(7, "message/v1", b"value")


@pytest.mark.parametrize(
    "bad_descriptor",
    [
        lambda: descriptor(serializer_id=0),
        lambda: descriptor(serializer_id=1 << 32),
        lambda: descriptor(name=""),
        lambda: descriptor(name="e\N{COMBINING ACUTE ACCENT}"),
        lambda: descriptor(major=-1),
        lambda: descriptor(minor=1 << 16),
        lambda: descriptor(readable=frozenset({""})),
        lambda: descriptor(writable=frozenset({"x" * 1_025})),
    ],
)
def test_descriptor_rejects_invalid_protocol_values(bad_descriptor):
    with pytest.raises(SerializerRegistryError):
        bad_descriptor()


def test_builder_rejects_duplicates_unregistered_bindings_and_unwritable_manifests():
    builder = SerializerRegistryBuilder().register(descriptor(), TextSerializer())

    with pytest.raises(SerializerRegistryError, match="already registered"):
        builder.register(descriptor(), TextSerializer())
    with pytest.raises(SerializerRegistryError, match="already registered"):
        builder.register(descriptor(serializer_id=8), TextSerializer())
    with pytest.raises(SerializerRegistryError, match="must be registered"):
        builder.bind(str, 99, "message/v1")
    with pytest.raises(SerializerRegistryError, match="cannot write"):
        builder.bind(str, 7, "message/v2")
    with pytest.raises(SerializerRegistryError, match="already bound"):
        builder.bind(Message, 7, "message/v1").bind(Message, 7, "message/v1")


def test_negotiation_is_directional_canonical_and_selects_lower_minor():
    first = descriptor(
        minor=4,
        readable=frozenset({"common/v1", "from-second/v1"}),
        writable=frozenset({"common/v1", "from-first/v1"}),
    )
    second = descriptor(
        minor=2,
        readable=frozenset({"common/v1", "from-first/v1"}),
        writable=frozenset({"common/v1", "from-second/v1"}),
    )

    result = negotiate_serializers(SECOND_UID, [first], FIRST_UID, [second])

    assert result.protocol_minor_for(7) == 2
    assert result.routes == (
        SerializerRoute(FIRST_UID, 7, ("common/v1", "from-second/v1")),
        SerializerRoute(SECOND_UID, 7, ("common/v1", "from-first/v1")),
    )
    assert result == negotiate_serializers(FIRST_UID, [second], SECOND_UID, [first])


def test_registry_passes_the_negotiated_minor_to_the_serializer() -> None:
    observed = []

    class VersionedSerializer:
        def serialize(self, value, manifest, protocol_minor):
            observed.append(("write", protocol_minor))
            return value.text.encode()

        def deserialize(self, payload, manifest, protocol_minor):
            observed.append(("read", protocol_minor))
            return Message(payload.decode())

    registry = (
        SerializerRegistryBuilder()
        .register(descriptor(minor=4), VersionedSerializer())
        .bind(Message, 7, "message/v1")
        .build()
    )
    route = SerializerRoute(FIRST_UID, 7, ("message/v1",))

    encoded = registry.serialize_for_route(Message("value"), route, 2)
    assert registry.deserialize(7, "message/v1", encoded.payload, 2) == Message("value")
    assert observed == [("write", 2), ("read", 2)]


def test_serializer_lock_allows_same_registration_reentry() -> None:
    holder = {}

    class ReentrantSerializer:
        def serialize(self, value, manifest, protocol_minor):
            if value.text == "outer":
                return holder["registry"].serialize(Message("inner")).payload
            return value.text.encode()

        def deserialize(self, payload, manifest, protocol_minor):
            return Message(payload.decode())

    registry = (
        SerializerRegistryBuilder()
        .register(descriptor(), ReentrantSerializer())
        .bind(Message, 7, "message/v1")
        .build()
    )
    holder["registry"] = registry

    assert registry.serialize(Message("outer")).payload == b"inner"


def test_negotiation_omits_only_an_empty_direction_and_unshared_ids():
    first = descriptor(
        readable=frozenset(),
        writable=frozenset({"first/v1"}),
    )
    second = descriptor(
        readable=frozenset({"first/v1"}),
        writable=frozenset({"second/v1"}),
    )

    result = negotiate_serializers(
        FIRST_UID,
        [first, descriptor(serializer_id=8, name="first-only")],
        SECOND_UID,
        [second],
    )

    assert result.routes == (SerializerRoute(FIRST_UID, 7, ("first/v1",)),)
    assert [item.serializer_id for item in result.serializers] == [7]


@pytest.mark.parametrize(
    ("first", "second"),
    [
        (descriptor(), descriptor(name="different")),
        (descriptor(major=1), descriptor(major=2)),
        (descriptor(), descriptor(serializer_id=8)),
    ],
)
def test_negotiation_rejects_id_name_and_major_conflicts(first, second):
    with pytest.raises(SerializerNegotiationError):
        negotiate_serializers(FIRST_UID, [first], SECOND_UID, [second])


def test_negotiation_rejects_duplicate_descriptors_and_incarnations():
    item = descriptor()

    with pytest.raises(SerializerNegotiationError, match="repeats"):
        negotiate_serializers(FIRST_UID, [item, item], SECOND_UID, [])
    with pytest.raises(SerializerNegotiationError, match="must differ"):
        negotiate_serializers(FIRST_UID, [], FIRST_UID, [])
