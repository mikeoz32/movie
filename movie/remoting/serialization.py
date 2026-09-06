"""Explicit payload serializer registration and directional negotiation."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from threading import RLock
from types import MappingProxyType
from typing import Protocol, runtime_checkable
from unicodedata import is_normalized
from uuid import UUID

from movie.remoting.errors import (
    DeserializationError,
    SerializationError,
    SerializerNegotiationError,
    SerializerRegistryError,
    UnknownSerializerError,
    UnsupportedManifestError,
)

MAX_U16 = (1 << 16) - 1
MAX_U32 = (1 << 32) - 1
MAX_MANIFEST_BYTES = 1_024


@runtime_checkable
class Serializer(Protocol):
    """A serializer for explicitly bound message contracts."""

    def serialize(self, value: object, manifest: str, protocol_minor: int) -> bytes:
        """Encode a value according to a stable manifest."""

    def deserialize(self, payload: bytes, manifest: str, protocol_minor: int) -> object:
        """Decode bytes according to a stable manifest."""


def _encoded_text(value: str, field: str, maximum: int, *, nonempty: bool = True) -> bytes:
    if not isinstance(value, str):
        raise SerializerRegistryError(f"{field} must be a string")
    if not is_normalized("NFC", value):
        raise SerializerRegistryError(f"{field} must be NFC-normalized")
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise SerializerRegistryError(f"{field} must be valid UTF-8") from error
    if nonempty and not encoded:
        raise SerializerRegistryError(f"{field} must not be empty")
    if len(encoded) > maximum:
        raise SerializerRegistryError(f"{field} exceeds {maximum} UTF-8 bytes")
    return encoded


def _uint(value: int, maximum: int, field: str, *, nonzero: bool = False) -> None:
    if not isinstance(value, int) or isinstance(value, bool):
        raise SerializerRegistryError(f"{field} must be an integer")
    minimum = 1 if nonzero else 0
    if not minimum <= value <= maximum:
        raise SerializerRegistryError(f"{field} must be between {minimum} and {maximum}")


@dataclass(frozen=True, slots=True)
class SerializerDescriptor:
    """Stable, immutable wire description of one payload serializer."""

    serializer_id: int
    name: str
    protocol_major: int
    protocol_minor: int
    readable_manifests: frozenset[str]
    writable_manifests: frozenset[str]

    def __post_init__(self) -> None:
        _uint(self.serializer_id, MAX_U32, "serializer ID", nonzero=True)
        _encoded_text(self.name, "serializer name", MAX_U16)
        _uint(self.protocol_major, MAX_U16, "serializer protocol major")
        _uint(self.protocol_minor, MAX_U16, "serializer protocol minor")

        try:
            readable = frozenset(self.readable_manifests)
            writable = frozenset(self.writable_manifests)
        except TypeError as error:
            raise SerializerRegistryError(
                "serializer manifests must be an iterable of strings"
            ) from error
        if len(readable) > MAX_U16 or len(writable) > MAX_U16:
            raise SerializerRegistryError("manifest count exceeds 65535")
        for manifest in readable | writable:
            _encoded_text(manifest, "serializer manifest", MAX_MANIFEST_BYTES)
        object.__setattr__(self, "readable_manifests", readable)
        object.__setattr__(self, "writable_manifests", writable)


@dataclass(frozen=True, slots=True)
class SerializerRoute:
    """Manifests one actor-system incarnation may send with a serializer."""

    origin_incarnation_uid: UUID
    serializer_id: int
    manifests: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.origin_incarnation_uid, UUID) or not self.origin_incarnation_uid.int:
            raise SerializerRegistryError("route origin incarnation UID must be a nonzero UUID")
        _uint(self.serializer_id, MAX_U32, "route serializer ID", nonzero=True)
        if isinstance(self.manifests, str):
            raise SerializerRegistryError("route manifests must be an iterable of strings")
        manifests = tuple(self.manifests)
        if len(manifests) > MAX_U16:
            raise SerializerRegistryError("route manifest count exceeds 65535")
        for manifest in manifests:
            _encoded_text(manifest, "route manifest", MAX_MANIFEST_BYTES)
        if len(set(manifests)) != len(manifests):
            raise SerializerRegistryError("route manifests must be unique")
        canonical = tuple(sorted(manifests, key=lambda item: item.encode("utf-8")))
        if not canonical:
            raise SerializerRegistryError("a serializer route must contain a manifest")
        object.__setattr__(self, "manifests", canonical)


@dataclass(frozen=True, slots=True)
class NegotiatedSerializer:
    """The protocol minor selected for one compatible serializer ID."""

    serializer_id: int
    protocol_minor: int


@dataclass(frozen=True, slots=True)
class SerializerNegotiation:
    """Canonical directional routes and negotiated serializer versions."""

    routes: tuple[SerializerRoute, ...]
    serializers: tuple[NegotiatedSerializer, ...]

    def protocol_minor_for(self, serializer_id: int) -> int:
        for serializer in self.serializers:
            if serializer.serializer_id == serializer_id:
                return serializer.protocol_minor
        raise UnknownSerializerError(f"serializer ID {serializer_id} was not negotiated")


@dataclass(frozen=True, slots=True)
class SerializerBinding:
    """An exact Python type's explicit serializer and manifest selection."""

    message_type: type[object]
    serializer_id: int
    manifest: str


@dataclass(frozen=True, slots=True)
class SerializedPayload:
    """The wire-relevant result of successful payload serialization."""

    serializer_id: int
    manifest: str
    payload: bytes


@dataclass(frozen=True, slots=True)
class _Registration:
    descriptor: SerializerDescriptor
    serializer: Serializer
    lock: RLock


class SerializerRegistry:
    """An immutable collection of serializers and exact-type bindings."""

    __slots__ = ("_bindings", "_descriptors", "_registrations")

    def __init__(
        self,
        registrations: Mapping[int, _Registration],
        bindings: Mapping[type[object], SerializerBinding],
    ) -> None:
        copied_registrations = dict(registrations)
        self._registrations = MappingProxyType(copied_registrations)
        self._bindings = MappingProxyType(dict(bindings))
        self._descriptors = tuple(
            registration.descriptor
            for _, registration in sorted(copied_registrations.items())
        )

    @property
    def descriptors(self) -> tuple[SerializerDescriptor, ...]:
        return self._descriptors

    @property
    def bindings(self) -> Mapping[type[object], SerializerBinding]:
        return self._bindings

    def descriptor(self, serializer_id: int) -> SerializerDescriptor:
        try:
            return self._registrations[serializer_id].descriptor
        except KeyError as error:
            raise UnknownSerializerError(
                f"serializer ID {serializer_id} is not registered"
            ) from error

    def binding_for(self, message_type: type[object]) -> SerializerBinding:
        try:
            return self._bindings[message_type]
        except KeyError as error:
            raise UnknownSerializerError(
                f"message type {message_type.__qualname__} has no exact-type binding"
            ) from error

    def serialize(self, value: object) -> SerializedPayload:
        binding = self.binding_for(type(value))
        descriptor = self._registrations[binding.serializer_id].descriptor
        return self._serialize(binding, value, descriptor.protocol_minor)

    def serialize_for_route(
        self,
        value: object,
        route: SerializerRoute,
        protocol_minor: int,
    ) -> SerializedPayload:
        """Check directional negotiation before invoking application serialization."""
        binding = self.binding_for(type(value))
        if binding.serializer_id != route.serializer_id or binding.manifest not in route.manifests:
            raise UnsupportedManifestError(
                f"manifest {binding.manifest!r} is not sendable on serializer route "
                f"{route.serializer_id}"
            )
        return self._serialize(binding, value, protocol_minor)

    def _serialize(
        self,
        binding: SerializerBinding,
        value: object,
        protocol_minor: int,
    ) -> SerializedPayload:
        registration = self._registrations[binding.serializer_id]
        _uint(protocol_minor, registration.descriptor.protocol_minor, "serializer protocol minor")
        try:
            with registration.lock:
                payload = registration.serializer.serialize(
                    value,
                    binding.manifest,
                    protocol_minor,
                )
        except Exception as error:
            raise SerializationError(
                f"serializer ID {binding.serializer_id} failed for manifest {binding.manifest!r}"
            ) from error
        if not isinstance(payload, bytes):
            raise SerializationError(
                f"serializer ID {binding.serializer_id} returned "
                f"{type(payload).__name__}, not bytes"
            )
        return SerializedPayload(binding.serializer_id, binding.manifest, payload)

    def deserialize(
        self,
        serializer_id: int,
        manifest: str,
        payload: bytes,
        protocol_minor: int | None = None,
    ) -> object:
        try:
            registration = self._registrations[serializer_id]
        except KeyError as error:
            raise UnknownSerializerError(
                f"serializer ID {serializer_id} is not registered"
            ) from error
        if manifest not in registration.descriptor.readable_manifests:
            raise UnsupportedManifestError(
                f"serializer ID {serializer_id} cannot read manifest {manifest!r}"
            )
        if not isinstance(payload, bytes):
            raise DeserializationError("serialized payload must be bytes")
        selected_minor = (
            registration.descriptor.protocol_minor
            if protocol_minor is None
            else protocol_minor
        )
        _uint(
            selected_minor,
            registration.descriptor.protocol_minor,
            "serializer protocol minor",
        )
        try:
            with registration.lock:
                return registration.serializer.deserialize(
                    payload,
                    manifest,
                    selected_minor,
                )
        except Exception as error:
            raise DeserializationError(
                f"serializer ID {serializer_id} failed for manifest {manifest!r}"
            ) from error


class SerializerRegistryBuilder:
    """Mutable construction boundary for an immutable serializer registry."""

    def __init__(self) -> None:
        self._registrations: dict[int, _Registration] = {}
        self._names: dict[str, int] = {}
        self._bindings: dict[type[object], SerializerBinding] = {}

    def include(self, registry: SerializerRegistry) -> SerializerRegistryBuilder:
        """Copy an immutable registry into this builder before adding bindings."""
        if not isinstance(registry, SerializerRegistry):
            raise SerializerRegistryError("included serializers must be a SerializerRegistry")
        for serializer_id, registration in registry._registrations.items():
            descriptor = registration.descriptor
            if serializer_id in self._registrations:
                raise SerializerRegistryError(
                    f"serializer ID {serializer_id} is already registered"
                )
            if descriptor.name in self._names:
                raise SerializerRegistryError(
                    f"serializer name {descriptor.name!r} is already registered"
                )
            self._registrations[serializer_id] = registration
            self._names[descriptor.name] = serializer_id
        for binding in registry.bindings.values():
            self.bind(
                binding.message_type,
                binding.serializer_id,
                binding.manifest,
            )
        return self

    def register(
        self, descriptor: SerializerDescriptor, serializer: Serializer
    ) -> SerializerRegistryBuilder:
        if not isinstance(descriptor, SerializerDescriptor):
            raise SerializerRegistryError("descriptor must be a SerializerDescriptor")
        if descriptor.serializer_id in self._registrations:
            raise SerializerRegistryError(
                f"serializer ID {descriptor.serializer_id} is already registered"
            )
        if descriptor.name in self._names:
            raise SerializerRegistryError(
                f"serializer name {descriptor.name!r} is already registered"
            )
        if not isinstance(serializer, Serializer):
            raise SerializerRegistryError("serializer must implement serialize and deserialize")
        self._registrations[descriptor.serializer_id] = _Registration(
            descriptor,
            serializer,
            RLock(),
        )
        self._names[descriptor.name] = descriptor.serializer_id
        return self

    def bind(
        self, message_type: type[object], serializer_id: int, manifest: str
    ) -> SerializerRegistryBuilder:
        if not isinstance(message_type, type):
            raise SerializerRegistryError("message binding key must be a type")
        if message_type in self._bindings:
            raise SerializerRegistryError(
                f"message type {message_type.__qualname__} is already bound"
            )
        try:
            descriptor = self._registrations[serializer_id].descriptor
        except KeyError as error:
            raise SerializerRegistryError(
                f"serializer ID {serializer_id} must be registered before binding"
            ) from error
        if manifest not in descriptor.writable_manifests:
            raise SerializerRegistryError(
                f"serializer ID {serializer_id} cannot write manifest {manifest!r}"
            )
        self._bindings[message_type] = SerializerBinding(message_type, serializer_id, manifest)
        return self

    def build(self) -> SerializerRegistry:
        return SerializerRegistry(self._registrations, self._bindings)


def _descriptor_maps(
    descriptors: Iterable[SerializerDescriptor], peer: str
) -> tuple[dict[int, SerializerDescriptor], dict[str, int]]:
    by_id: dict[int, SerializerDescriptor] = {}
    by_name: dict[str, int] = {}
    for descriptor in descriptors:
        if not isinstance(descriptor, SerializerDescriptor):
            raise SerializerNegotiationError(
                f"{peer} serializer entry must be a SerializerDescriptor"
            )
        if descriptor.serializer_id in by_id:
            raise SerializerNegotiationError(
                f"{peer} repeats serializer ID {descriptor.serializer_id}"
            )
        if descriptor.name in by_name:
            raise SerializerNegotiationError(
                f"{peer} repeats serializer name {descriptor.name!r}"
            )
        by_id[descriptor.serializer_id] = descriptor
        by_name[descriptor.name] = descriptor.serializer_id
    return by_id, by_name


def negotiate_serializers(
    first_origin_uid: UUID,
    first_descriptors: Iterable[SerializerDescriptor],
    second_origin_uid: UUID,
    second_descriptors: Iterable[SerializerDescriptor],
) -> SerializerNegotiation:
    """Derive both directional routes independent of local peer perspective."""
    if not isinstance(first_origin_uid, UUID) or not first_origin_uid.int:
        raise SerializerNegotiationError("first origin incarnation UID must be a nonzero UUID")
    if not isinstance(second_origin_uid, UUID) or not second_origin_uid.int:
        raise SerializerNegotiationError("second origin incarnation UID must be a nonzero UUID")
    if first_origin_uid == second_origin_uid:
        raise SerializerNegotiationError("peer incarnation UIDs must differ")

    first_by_id, first_by_name = _descriptor_maps(first_descriptors, "first peer")
    second_by_id, second_by_name = _descriptor_maps(second_descriptors, "second peer")
    for name in first_by_name.keys() & second_by_name.keys():
        if first_by_name[name] != second_by_name[name]:
            raise SerializerNegotiationError(
                f"serializer name {name!r} has conflicting IDs "
                f"{first_by_name[name]} and {second_by_name[name]}"
            )

    routes: list[SerializerRoute] = []
    negotiated: list[NegotiatedSerializer] = []
    for serializer_id in sorted(first_by_id.keys() & second_by_id.keys()):
        first = first_by_id[serializer_id]
        second = second_by_id[serializer_id]
        if first.name != second.name:
            raise SerializerNegotiationError(
                f"serializer ID {serializer_id} has conflicting names "
                f"{first.name!r} and {second.name!r}"
            )
        if first.protocol_major != second.protocol_major:
            raise SerializerNegotiationError(
                f"serializer ID {serializer_id} has conflicting protocol majors "
                f"{first.protocol_major} and {second.protocol_major}"
            )

        negotiated.append(
            NegotiatedSerializer(serializer_id, min(first.protocol_minor, second.protocol_minor))
        )
        first_sendable = first.writable_manifests & second.readable_manifests
        second_sendable = second.writable_manifests & first.readable_manifests
        if first_sendable:
            routes.append(SerializerRoute(first_origin_uid, serializer_id, tuple(first_sendable)))
        if second_sendable:
            routes.append(SerializerRoute(second_origin_uid, serializer_id, tuple(second_sendable)))

    routes.sort(
        key=lambda route: (
            route.origin_incarnation_uid.bytes,
            route.serializer_id,
            tuple(manifest.encode("utf-8") for manifest in route.manifests),
        )
    )
    return SerializerNegotiation(tuple(routes), tuple(negotiated))


def negotiate_serializer_routes(
    first_origin_uid: UUID,
    first_descriptors: Iterable[SerializerDescriptor],
    second_origin_uid: UUID,
    second_descriptors: Iterable[SerializerDescriptor],
) -> tuple[SerializerRoute, ...]:
    return negotiate_serializers(
        first_origin_uid,
        first_descriptors,
        second_origin_uid,
        second_descriptors,
    ).routes
