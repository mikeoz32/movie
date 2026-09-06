"""Internal v1 cluster control payload contracts."""

from __future__ import annotations

import ipaddress
import json
import re
from dataclasses import dataclass
from types import MappingProxyType
from uuid import UUID

from movie.remoting.errors import ProtocolValidationError
from movie.remoting.serialization import (
    SerializerDescriptor,
    SerializerRegistry,
    SerializerRegistryBuilder,
)

__all__: list[str] = []

_MAX_PAYLOAD_BYTES = 1 << 20
_MAX_MEMBERS = 256
_NAME = re.compile(r"[A-Za-z0-9._~-]{1,255}\Z")
_HOST = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?\Z")
_STATUSES = frozenset({"joining", "up", "leaving", "left"})
_REACHABILITIES = frozenset({"reachable", "unreachable"})
_FINGERPRINT = re.compile(r"[0-9a-f]{64}\Z")


def _validate_name(value: str, field: str) -> None:
    if not isinstance(value, str) or not value.isascii() or _NAME.fullmatch(value) is None:
        raise ProtocolValidationError(
            f"{field} must be 1-255 ASCII URI-unreserved characters"
        )


def _validate_uuid(value: UUID, field: str) -> None:
    if not isinstance(value, UUID) or not value.int:
        raise ProtocolValidationError(f"{field} must be a nonzero UUID")


def _validate_host(value: str) -> None:
    if not isinstance(value, str) or not value or len(value) > 255 or not value.isascii():
        raise ProtocolValidationError("host must be 1-255 canonical ASCII characters")
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        address = None
    if address is not None and address.is_unspecified:
        raise ProtocolValidationError("host must not be an unspecified address")
    if ":" in value:
        try:
            canonical = str(ipaddress.IPv6Address(value))
        except ValueError as error:
            raise ProtocolValidationError("host must be canonical IPv6 or a DNS name") from error
        if canonical != value:
            raise ProtocolValidationError("host must be canonical IPv6 or a DNS name")
    elif _HOST.fullmatch(value) is None:
        raise ProtocolValidationError("host must be canonical IPv4 or a DNS name")


def _validate_port(value: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= 65_535:
        raise ProtocolValidationError("port must be an integer between 1 and 65535")


def _validate_nonnegative(value: int, field: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ProtocolValidationError(f"{field} must be a nonnegative integer")


def _validate_choice(value: str, choices: frozenset[str], field: str) -> None:
    if not isinstance(value, str) or value not in choices:
        expected = ", ".join(sorted(choices))
        raise ProtocolValidationError(f"{field} must be one of: {expected}")


@dataclass(frozen=True, slots=True)
class _WireMember:
    system_name: str
    incarnation_uid: UUID
    host: str
    port: int
    status: str
    reachability: str

    def __post_init__(self) -> None:
        _validate_name(self.system_name, "member system name")
        _validate_uuid(self.incarnation_uid, "member incarnation UID")
        _validate_host(self.host)
        _validate_port(self.port)
        _validate_choice(self.status, _STATUSES, "member status")
        _validate_choice(self.reachability, _REACHABILITIES, "member reachability")


def _validate_members(value: tuple[_WireMember, ...]) -> None:
    if not isinstance(value, tuple):
        raise ProtocolValidationError("members must be a tuple")
    if len(value) > _MAX_MEMBERS:
        raise ProtocolValidationError("members must contain at most 256 entries")
    if any(type(member) is not _WireMember for member in value):
        raise ProtocolValidationError("members must contain only _WireMember values")
    identities = {(member.system_name, member.incarnation_uid) for member in value}
    if len(identities) != len(value):
        raise ProtocolValidationError("members must have unique system-name and incarnation UIDs")
    active_names = [member.system_name for member in value if member.status != "left"]
    if len(set(active_names)) != len(active_names):
        raise ProtocolValidationError(
            "members cannot contain multiple active incarnations with one system name"
        )


@dataclass(frozen=True, slots=True)
class _JoinRequest:
    cluster_name: str
    config_fingerprint: str
    source_system_name: str
    source_incarnation_uid: UUID
    source_host: str
    source_port: int
    source_control_actor_uid: UUID
    target_incarnation_uid: UUID
    request_id: UUID

    def __post_init__(self) -> None:
        _validate_name(self.cluster_name, "cluster name")
        if (
            not isinstance(self.config_fingerprint, str)
            or _FINGERPRINT.fullmatch(self.config_fingerprint) is None
        ):
            raise ProtocolValidationError(
                "cluster configuration fingerprint must be 64 lowercase hexadecimal characters"
            )
        _validate_name(self.source_system_name, "source system name")
        _validate_uuid(self.source_incarnation_uid, "source incarnation UID")
        _validate_host(self.source_host)
        _validate_port(self.source_port)
        _validate_uuid(self.source_control_actor_uid, "source control actor UID")
        _validate_uuid(self.target_incarnation_uid, "target incarnation UID")
        _validate_uuid(self.request_id, "request ID")


@dataclass(frozen=True, slots=True)
class _JoinAccepted:
    cluster_name: str
    source_system_name: str
    source_incarnation_uid: UUID
    target_incarnation_uid: UUID
    request_id: UUID
    membership_token: UUID
    revision: int
    members: tuple[_WireMember, ...]

    def __post_init__(self) -> None:
        _validate_name(self.cluster_name, "cluster name")
        _validate_name(self.source_system_name, "source system name")
        _validate_uuid(self.source_incarnation_uid, "source incarnation UID")
        _validate_uuid(self.target_incarnation_uid, "target incarnation UID")
        _validate_uuid(self.request_id, "request ID")
        _validate_uuid(self.membership_token, "membership token")
        _validate_nonnegative(self.revision, "revision")
        _validate_members(self.members)


@dataclass(frozen=True, slots=True)
class _JoinConfirm:
    cluster_name: str
    source_system_name: str
    source_incarnation_uid: UUID
    target_incarnation_uid: UUID
    request_id: UUID
    membership_token: UUID

    def __post_init__(self) -> None:
        _validate_name(self.cluster_name, "cluster name")
        _validate_name(self.source_system_name, "source system name")
        _validate_uuid(self.source_incarnation_uid, "source incarnation UID")
        _validate_uuid(self.target_incarnation_uid, "target incarnation UID")
        _validate_uuid(self.request_id, "request ID")
        _validate_uuid(self.membership_token, "membership token")


@dataclass(frozen=True, slots=True)
class _Heartbeat:
    cluster_name: str
    source_system_name: str
    source_incarnation_uid: UUID
    target_incarnation_uid: UUID
    membership_token: UUID
    sequence: int

    def __post_init__(self) -> None:
        _validate_name(self.cluster_name, "cluster name")
        _validate_name(self.source_system_name, "source system name")
        _validate_uuid(self.source_incarnation_uid, "source incarnation UID")
        _validate_uuid(self.target_incarnation_uid, "target incarnation UID")
        _validate_uuid(self.membership_token, "membership token")
        _validate_nonnegative(self.sequence, "sequence")


@dataclass(frozen=True, slots=True)
class _HeartbeatAck:
    cluster_name: str
    source_system_name: str
    source_incarnation_uid: UUID
    target_incarnation_uid: UUID
    membership_token: UUID
    sequence: int
    revision: int
    members: tuple[_WireMember, ...]

    def __post_init__(self) -> None:
        _validate_name(self.cluster_name, "cluster name")
        _validate_name(self.source_system_name, "source system name")
        _validate_uuid(self.source_incarnation_uid, "source incarnation UID")
        _validate_uuid(self.target_incarnation_uid, "target incarnation UID")
        _validate_uuid(self.membership_token, "membership token")
        _validate_nonnegative(self.sequence, "sequence")
        _validate_nonnegative(self.revision, "revision")
        _validate_members(self.members)


@dataclass(frozen=True, slots=True)
class _MembershipUpdate:
    cluster_name: str
    source_system_name: str
    source_incarnation_uid: UUID
    target_incarnation_uid: UUID
    membership_token: UUID
    revision: int
    members: tuple[_WireMember, ...]

    def __post_init__(self) -> None:
        _validate_name(self.cluster_name, "cluster name")
        _validate_name(self.source_system_name, "source system name")
        _validate_uuid(self.source_incarnation_uid, "source incarnation UID")
        _validate_uuid(self.target_incarnation_uid, "target incarnation UID")
        _validate_uuid(self.membership_token, "membership token")
        _validate_nonnegative(self.revision, "revision")
        _validate_members(self.members)


@dataclass(frozen=True, slots=True)
class _Leave:
    cluster_name: str
    source_system_name: str
    source_incarnation_uid: UUID
    target_incarnation_uid: UUID
    membership_token: UUID
    request_id: UUID

    def __post_init__(self) -> None:
        _validate_name(self.cluster_name, "cluster name")
        _validate_name(self.source_system_name, "source system name")
        _validate_uuid(self.source_incarnation_uid, "source incarnation UID")
        _validate_uuid(self.target_incarnation_uid, "target incarnation UID")
        _validate_uuid(self.membership_token, "membership token")
        _validate_uuid(self.request_id, "request ID")


@dataclass(frozen=True, slots=True)
class _LeaveAck:
    cluster_name: str
    source_system_name: str
    source_incarnation_uid: UUID
    target_incarnation_uid: UUID
    request_id: UUID
    revision: int
    members: tuple[_WireMember, ...]

    def __post_init__(self) -> None:
        _validate_name(self.cluster_name, "cluster name")
        _validate_name(self.source_system_name, "source system name")
        _validate_uuid(self.source_incarnation_uid, "source incarnation UID")
        _validate_uuid(self.target_incarnation_uid, "target incarnation UID")
        _validate_uuid(self.request_id, "request ID")
        _validate_nonnegative(self.revision, "revision")
        _validate_members(self.members)


_MANIFESTS = MappingProxyType(
    {
        _JoinRequest: "movie-cluster/join-request/v1",
        _JoinAccepted: "movie-cluster/join-accepted/v1",
        _JoinConfirm: "movie-cluster/join-confirm/v1",
        _Heartbeat: "movie-cluster/heartbeat/v1",
        _HeartbeatAck: "movie-cluster/heartbeat-ack/v1",
        _MembershipUpdate: "movie-cluster/membership-update/v1",
        _Leave: "movie-cluster/leave/v1",
        _LeaveAck: "movie-cluster/leave-ack/v1",
    }
)
_TYPES_BY_MANIFEST = MappingProxyType(
    {manifest: message_type for message_type, manifest in _MANIFESTS.items()}
)

_MEMBER_KEYS = frozenset(
    {"system_name", "incarnation_uid", "host", "port", "status", "reachability"}
)
_MESSAGE_KEYS = {
    _JoinRequest: frozenset(
        {
            "cluster_name",
            "config_fingerprint",
            "source_system_name",
            "source_incarnation_uid",
            "source_host",
            "source_port",
            "source_control_actor_uid",
            "target_incarnation_uid",
            "request_id",
        }
    ),
    _JoinAccepted: frozenset(
        {
            "cluster_name",
            "source_system_name",
            "source_incarnation_uid",
            "target_incarnation_uid",
            "request_id",
            "membership_token",
            "revision",
            "members",
        }
    ),
    _JoinConfirm: frozenset(
        {
            "cluster_name",
            "source_system_name",
            "source_incarnation_uid",
            "target_incarnation_uid",
            "request_id",
            "membership_token",
        }
    ),
    _Heartbeat: frozenset(
        {
            "cluster_name",
            "source_system_name",
            "source_incarnation_uid",
            "target_incarnation_uid",
            "membership_token",
            "sequence",
        }
    ),
    _HeartbeatAck: frozenset(
        {
            "cluster_name",
            "source_system_name",
            "source_incarnation_uid",
            "target_incarnation_uid",
            "membership_token",
            "sequence",
            "revision",
            "members",
        }
    ),
    _MembershipUpdate: frozenset(
        {
            "cluster_name",
            "source_system_name",
            "source_incarnation_uid",
            "target_incarnation_uid",
            "membership_token",
            "revision",
            "members",
        }
    ),
    _Leave: frozenset(
        {
            "cluster_name",
            "source_system_name",
            "source_incarnation_uid",
            "target_incarnation_uid",
            "membership_token",
            "request_id",
        }
    ),
    _LeaveAck: frozenset(
        {
            "cluster_name",
            "source_system_name",
            "source_incarnation_uid",
            "target_incarnation_uid",
            "request_id",
            "revision",
            "members",
        }
    ),
}


def _member_to_json(member: _WireMember) -> dict[str, object]:
    return {
        "system_name": member.system_name,
        "incarnation_uid": str(member.incarnation_uid),
        "host": member.host,
        "port": member.port,
        "status": member.status,
        "reachability": member.reachability,
    }


def _message_to_json(value: object) -> dict[str, object]:
    if type(value) is _JoinRequest:
        message = value
        return {
            "cluster_name": message.cluster_name,
            "config_fingerprint": message.config_fingerprint,
            "source_system_name": message.source_system_name,
            "source_incarnation_uid": str(message.source_incarnation_uid),
            "source_host": message.source_host,
            "source_port": message.source_port,
            "source_control_actor_uid": str(message.source_control_actor_uid),
            "target_incarnation_uid": str(message.target_incarnation_uid),
            "request_id": str(message.request_id),
        }
    if type(value) is _JoinAccepted:
        message = value
        return {
            "cluster_name": message.cluster_name,
            "source_system_name": message.source_system_name,
            "source_incarnation_uid": str(message.source_incarnation_uid),
            "target_incarnation_uid": str(message.target_incarnation_uid),
            "request_id": str(message.request_id),
            "membership_token": str(message.membership_token),
            "revision": message.revision,
            "members": [_member_to_json(member) for member in message.members],
        }
    if type(value) is _JoinConfirm:
        message = value
        return {
            "cluster_name": message.cluster_name,
            "source_system_name": message.source_system_name,
            "source_incarnation_uid": str(message.source_incarnation_uid),
            "target_incarnation_uid": str(message.target_incarnation_uid),
            "request_id": str(message.request_id),
            "membership_token": str(message.membership_token),
        }
    if type(value) is _Heartbeat:
        message = value
        return {
            "cluster_name": message.cluster_name,
            "source_system_name": message.source_system_name,
            "source_incarnation_uid": str(message.source_incarnation_uid),
            "target_incarnation_uid": str(message.target_incarnation_uid),
            "membership_token": str(message.membership_token),
            "sequence": message.sequence,
        }
    if type(value) is _HeartbeatAck:
        message = value
        return {
            "cluster_name": message.cluster_name,
            "source_system_name": message.source_system_name,
            "source_incarnation_uid": str(message.source_incarnation_uid),
            "target_incarnation_uid": str(message.target_incarnation_uid),
            "membership_token": str(message.membership_token),
            "sequence": message.sequence,
            "revision": message.revision,
            "members": [_member_to_json(member) for member in message.members],
        }
    if type(value) is _MembershipUpdate:
        message = value
        return {
            "cluster_name": message.cluster_name,
            "source_system_name": message.source_system_name,
            "source_incarnation_uid": str(message.source_incarnation_uid),
            "target_incarnation_uid": str(message.target_incarnation_uid),
            "membership_token": str(message.membership_token),
            "revision": message.revision,
            "members": [_member_to_json(member) for member in message.members],
        }
    if type(value) is _Leave:
        message = value
        return {
            "cluster_name": message.cluster_name,
            "source_system_name": message.source_system_name,
            "source_incarnation_uid": str(message.source_incarnation_uid),
            "target_incarnation_uid": str(message.target_incarnation_uid),
            "membership_token": str(message.membership_token),
            "request_id": str(message.request_id),
        }
    if type(value) is _LeaveAck:
        message = value
        return {
            "cluster_name": message.cluster_name,
            "source_system_name": message.source_system_name,
            "source_incarnation_uid": str(message.source_incarnation_uid),
            "target_incarnation_uid": str(message.target_incarnation_uid),
            "request_id": str(message.request_id),
            "revision": message.revision,
            "members": [_member_to_json(member) for member in message.members],
        }
    raise ProtocolValidationError(f"unsupported cluster message type {type(value).__name__}")


def _object_without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ProtocolValidationError(f"JSON object repeats key {key!r}")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ProtocolValidationError(f"invalid JSON constant {value!r}")


def _require_object(
    value: object, expected_keys: frozenset[str], field: str
) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ProtocolValidationError(f"{field} must be a JSON object")
    if set(value) != expected_keys:
        raise ProtocolValidationError(f"{field} has missing or extra fields")
    return value


def _uuid_from_json(value: object, field: str) -> UUID:
    if not isinstance(value, str):
        raise ProtocolValidationError(f"{field} must be a canonical UUID string")
    try:
        parsed = UUID(value)
    except ValueError as error:
        raise ProtocolValidationError(f"{field} must be a canonical UUID string") from error
    if str(parsed) != value:
        raise ProtocolValidationError(f"{field} must be a canonical UUID string")
    _validate_uuid(parsed, field)
    return parsed


def _member_from_json(value: object) -> _WireMember:
    item = _require_object(value, _MEMBER_KEYS, "member")
    return _WireMember(
        system_name=item["system_name"],
        incarnation_uid=_uuid_from_json(item["incarnation_uid"], "member incarnation UID"),
        host=item["host"],
        port=item["port"],
        status=item["status"],
        reachability=item["reachability"],
    )


def _members_from_json(value: object) -> tuple[_WireMember, ...]:
    if not isinstance(value, list):
        raise ProtocolValidationError("members must be a JSON array")
    if len(value) > _MAX_MEMBERS:
        raise ProtocolValidationError("members must contain at most 256 entries")
    return tuple(_member_from_json(member) for member in value)


def _message_from_json(value: object, message_type: type[object]) -> object:
    item = _require_object(value, _MESSAGE_KEYS[message_type], "cluster message")
    if message_type is _JoinRequest:
        return _JoinRequest(
            cluster_name=item["cluster_name"],
            config_fingerprint=item["config_fingerprint"],
            source_system_name=item["source_system_name"],
            source_incarnation_uid=_uuid_from_json(
                item["source_incarnation_uid"], "source incarnation UID"
            ),
            source_host=item["source_host"],
            source_port=item["source_port"],
            source_control_actor_uid=_uuid_from_json(
                item["source_control_actor_uid"], "source control actor UID"
            ),
            target_incarnation_uid=_uuid_from_json(
                item["target_incarnation_uid"], "target incarnation UID"
            ),
            request_id=_uuid_from_json(item["request_id"], "request ID"),
        )
    if message_type is _JoinAccepted:
        return _JoinAccepted(
            cluster_name=item["cluster_name"],
            source_system_name=item["source_system_name"],
            source_incarnation_uid=_uuid_from_json(
                item["source_incarnation_uid"], "source incarnation UID"
            ),
            target_incarnation_uid=_uuid_from_json(
                item["target_incarnation_uid"], "target incarnation UID"
            ),
            request_id=_uuid_from_json(item["request_id"], "request ID"),
            membership_token=_uuid_from_json(item["membership_token"], "membership token"),
            revision=item["revision"],
            members=_members_from_json(item["members"]),
        )
    if message_type is _JoinConfirm:
        return _JoinConfirm(
            cluster_name=item["cluster_name"],
            source_system_name=item["source_system_name"],
            source_incarnation_uid=_uuid_from_json(
                item["source_incarnation_uid"], "source incarnation UID"
            ),
            target_incarnation_uid=_uuid_from_json(
                item["target_incarnation_uid"], "target incarnation UID"
            ),
            request_id=_uuid_from_json(item["request_id"], "request ID"),
            membership_token=_uuid_from_json(item["membership_token"], "membership token"),
        )
    if message_type is _Heartbeat:
        return _Heartbeat(
            cluster_name=item["cluster_name"],
            source_system_name=item["source_system_name"],
            source_incarnation_uid=_uuid_from_json(
                item["source_incarnation_uid"], "source incarnation UID"
            ),
            target_incarnation_uid=_uuid_from_json(
                item["target_incarnation_uid"], "target incarnation UID"
            ),
            membership_token=_uuid_from_json(item["membership_token"], "membership token"),
            sequence=item["sequence"],
        )
    if message_type is _HeartbeatAck:
        return _HeartbeatAck(
            cluster_name=item["cluster_name"],
            source_system_name=item["source_system_name"],
            source_incarnation_uid=_uuid_from_json(
                item["source_incarnation_uid"], "source incarnation UID"
            ),
            target_incarnation_uid=_uuid_from_json(
                item["target_incarnation_uid"], "target incarnation UID"
            ),
            membership_token=_uuid_from_json(item["membership_token"], "membership token"),
            sequence=item["sequence"],
            revision=item["revision"],
            members=_members_from_json(item["members"]),
        )
    if message_type is _MembershipUpdate:
        return _MembershipUpdate(
            cluster_name=item["cluster_name"],
            source_system_name=item["source_system_name"],
            source_incarnation_uid=_uuid_from_json(
                item["source_incarnation_uid"], "source incarnation UID"
            ),
            target_incarnation_uid=_uuid_from_json(
                item["target_incarnation_uid"], "target incarnation UID"
            ),
            membership_token=_uuid_from_json(item["membership_token"], "membership token"),
            revision=item["revision"],
            members=_members_from_json(item["members"]),
        )
    if message_type is _Leave:
        return _Leave(
            cluster_name=item["cluster_name"],
            source_system_name=item["source_system_name"],
            source_incarnation_uid=_uuid_from_json(
                item["source_incarnation_uid"], "source incarnation UID"
            ),
            target_incarnation_uid=_uuid_from_json(
                item["target_incarnation_uid"], "target incarnation UID"
            ),
            membership_token=_uuid_from_json(item["membership_token"], "membership token"),
            request_id=_uuid_from_json(item["request_id"], "request ID"),
        )
    if message_type is _LeaveAck:
        return _LeaveAck(
            cluster_name=item["cluster_name"],
            source_system_name=item["source_system_name"],
            source_incarnation_uid=_uuid_from_json(
                item["source_incarnation_uid"], "source incarnation UID"
            ),
            target_incarnation_uid=_uuid_from_json(
                item["target_incarnation_uid"], "target incarnation UID"
            ),
            request_id=_uuid_from_json(item["request_id"], "request ID"),
            revision=item["revision"],
            members=_members_from_json(item["members"]),
        )
    raise ProtocolValidationError(f"unsupported cluster message type {message_type.__name__}")


def _validate_protocol_minor(protocol_minor: int) -> None:
    if (
        not isinstance(protocol_minor, int)
        or isinstance(protocol_minor, bool)
        or protocol_minor != 0
    ):
        raise ProtocolValidationError("cluster serializer protocol minor must be 0")


class _ClusterSerializer:
    def serialize(self, value: object, manifest: str, protocol_minor: int) -> bytes:
        _validate_protocol_minor(protocol_minor)
        if not isinstance(manifest, str) or manifest not in _TYPES_BY_MANIFEST:
            raise ProtocolValidationError(f"unknown cluster manifest {manifest!r}")
        expected_type = _TYPES_BY_MANIFEST[manifest]
        if type(value) is not expected_type:
            raise ProtocolValidationError(
                f"manifest {manifest!r} requires exact type {expected_type.__name__}"
            )
        return json.dumps(
            _message_to_json(value),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    def deserialize(self, payload: bytes, manifest: str, protocol_minor: int) -> object:
        _validate_protocol_minor(protocol_minor)
        if not isinstance(manifest, str) or manifest not in _TYPES_BY_MANIFEST:
            raise ProtocolValidationError(f"unknown cluster manifest {manifest!r}")
        if not isinstance(payload, bytes):
            raise ProtocolValidationError("cluster payload must be bytes")
        if len(payload) > _MAX_PAYLOAD_BYTES:
            raise ProtocolValidationError("cluster payload exceeds 1 MiB")
        try:
            value = json.loads(
                payload.decode("utf-8", errors="strict"),
                object_pairs_hook=_object_without_duplicates,
                parse_constant=_reject_json_constant,
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ProtocolValidationError("cluster payload must be valid UTF-8 JSON") from error
        return _message_from_json(value, _TYPES_BY_MANIFEST[manifest])


def _descriptor(serializer_id: int) -> SerializerDescriptor:
    manifests = frozenset(_MANIFESTS.values())
    return SerializerDescriptor(
        serializer_id=serializer_id,
        name="movie-cluster-control",
        protocol_major=1,
        protocol_minor=0,
        readable_manifests=manifests,
        writable_manifests=manifests,
    )


def _bindings() -> tuple[tuple[type[object], str], ...]:
    return tuple(_MANIFESTS.items())


def _augment_registry(registry: SerializerRegistry, serializer_id: int) -> SerializerRegistry:
    builder = (
        SerializerRegistryBuilder()
        .include(registry)
        .register(_descriptor(serializer_id), _ClusterSerializer())
    )
    for message_type, manifest in _bindings():
        builder.bind(message_type, serializer_id, manifest)
    return builder.build()
