import json
from dataclasses import FrozenInstanceError, replace
from uuid import UUID

import pytest

from movie.cluster._protocol import (
    _MANIFESTS,
    _bindings,
    _ClusterSerializer,
    _descriptor,
    _Heartbeat,
    _HeartbeatAck,
    _JoinAccepted,
    _JoinConfirm,
    _JoinRequest,
    _Leave,
    _LeaveAck,
    _MembershipUpdate,
    _WireMember,
)
from movie.remoting.errors import ProtocolValidationError

SOURCE_UID = UUID("11111111-1111-4111-8111-111111111111")
TARGET_UID = UUID("22222222-2222-4222-8222-222222222222")
REQUEST_ID = UUID("33333333-3333-4333-8333-333333333333")
MEMBERSHIP_TOKEN = UUID("44444444-4444-4444-8444-444444444444")

MEMBERS = (
    _WireMember("alpha", SOURCE_UID, "127.0.0.1", 2551, "up", "reachable"),
    _WireMember("beta", TARGET_UID, "example.test", 2552, "joining", "unreachable"),
)

MESSAGES = (
    _JoinRequest(
        "production",
        "a" * 64,
        "alpha",
        SOURCE_UID,
        "127.0.0.1",
        2551,
        MEMBERSHIP_TOKEN,
        TARGET_UID,
        REQUEST_ID,
    ),
    _JoinAccepted(
        "production",
        "alpha",
        SOURCE_UID,
        TARGET_UID,
        REQUEST_ID,
        MEMBERSHIP_TOKEN,
        7,
        MEMBERS,
    ),
    _JoinConfirm(
        "production",
        "alpha",
        SOURCE_UID,
        TARGET_UID,
        REQUEST_ID,
        MEMBERSHIP_TOKEN,
    ),
    _Heartbeat(
        "production",
        "alpha",
        SOURCE_UID,
        TARGET_UID,
        MEMBERSHIP_TOKEN,
        8,
    ),
    _HeartbeatAck(
        "production",
        "alpha",
        SOURCE_UID,
        TARGET_UID,
        MEMBERSHIP_TOKEN,
        8,
        9,
        MEMBERS,
    ),
    _MembershipUpdate(
        "production",
        "alpha",
        SOURCE_UID,
        TARGET_UID,
        MEMBERSHIP_TOKEN,
        9,
        MEMBERS,
    ),
    _Leave(
        "production",
        "alpha",
        SOURCE_UID,
        TARGET_UID,
        MEMBERSHIP_TOKEN,
        REQUEST_ID,
    ),
    _LeaveAck(
        "production",
        "alpha",
        SOURCE_UID,
        TARGET_UID,
        REQUEST_ID,
        10,
        MEMBERS,
    ),
)


@pytest.mark.parametrize("message", MESSAGES)
def test_cluster_records_round_trip(message):
    serializer = _ClusterSerializer()
    manifest = _MANIFESTS[type(message)]

    payload = serializer.serialize(message, manifest, 0)

    assert serializer.deserialize(payload, manifest, 0) == message


def test_join_request_has_canonical_deterministic_json():
    serializer = _ClusterSerializer()
    message = MESSAGES[0]
    expected = (
        b'{"cluster_name":"production","config_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        b'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","request_id":'
        b'"33333333-3333-4333-8333-333333333333",'
        b'"source_control_actor_uid":"44444444-4444-4444-8444-444444444444",'
        b'"source_host":"127.0.0.1","source_incarnation_uid":'
        b'"11111111-1111-4111-8111-111111111111","source_port":2551,'
        b'"source_system_name":"alpha","target_incarnation_uid":'
        b'"22222222-2222-4222-8222-222222222222"}'
    )

    assert serializer.serialize(message, _MANIFESTS[_JoinRequest], 0) == expected
    assert serializer.serialize(message, _MANIFESTS[_JoinRequest], 0) == expected


def test_descriptor_and_bindings_cover_exact_cluster_contracts():
    descriptor = _descriptor(71)

    assert descriptor.serializer_id == 71
    assert descriptor.name == "movie-cluster-control"
    assert (descriptor.protocol_major, descriptor.protocol_minor) == (1, 0)
    assert descriptor.readable_manifests == frozenset(_MANIFESTS.values())
    assert descriptor.writable_manifests == frozenset(_MANIFESTS.values())
    assert _bindings() == tuple(_MANIFESTS.items())
    assert __import__("movie.cluster._protocol", fromlist=["__all__"]).__all__ == []


def test_records_are_frozen_and_slotted():
    member = MEMBERS[0]

    with pytest.raises(FrozenInstanceError):
        member.status = "left"
    with pytest.raises(AttributeError):
        member.extra = "value"


@pytest.mark.parametrize(
    "payload",
    [
        b"{",
        b"\xff",
        b'{"cluster_name":"first","cluster_name":"second"}',
        b"NaN",
    ],
)
def test_deserialization_rejects_malformed_json(payload):
    with pytest.raises(ProtocolValidationError):
        _ClusterSerializer().deserialize(payload, _MANIFESTS[_JoinRequest], 0)


@pytest.mark.parametrize("mutation", ["missing", "extra"])
def test_deserialization_requires_exact_top_level_keys(mutation):
    data = json.loads(_ClusterSerializer().serialize(MESSAGES[0], _MANIFESTS[_JoinRequest], 0))
    if mutation == "missing":
        del data["request_id"]
    else:
        data["unexpected"] = "value"

    with pytest.raises(ProtocolValidationError, match="missing or extra"):
        _ClusterSerializer().deserialize(
            json.dumps(data).encode(), _MANIFESTS[_JoinRequest], 0
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("cluster_name", 1),
        ("config_fingerprint", "A" * 64),
        ("source_port", True),
        ("source_port", 0),
        ("source_port", 65_536),
        ("source_incarnation_uid", 1),
        ("request_id", "33333333333343338333333333333333"),
        ("target_incarnation_uid", "22222222-2222-4222-8222-22222222222Z"),
    ],
)
def test_deserialization_rejects_wrong_types_ranges_and_uuids(field, value):
    serializer = _ClusterSerializer()
    data = json.loads(serializer.serialize(MESSAGES[0], _MANIFESTS[_JoinRequest], 0))
    data[field] = value

    with pytest.raises(ProtocolValidationError):
        serializer.deserialize(json.dumps(data).encode(), _MANIFESTS[_JoinRequest], 0)


def test_deserialization_requires_canonical_uuid_text():
    serializer = _ClusterSerializer()
    data = json.loads(serializer.serialize(MESSAGES[0], _MANIFESTS[_JoinRequest], 0))
    data["source_incarnation_uid"] = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".upper()

    with pytest.raises(ProtocolValidationError, match="canonical UUID"):
        serializer.deserialize(json.dumps(data).encode(), _MANIFESTS[_JoinRequest], 0)


def test_serializer_rejects_unknown_and_mismatched_manifests():
    serializer = _ClusterSerializer()

    with pytest.raises(ProtocolValidationError, match="unknown cluster manifest"):
        serializer.serialize(MESSAGES[0], "movie-cluster/unknown/v1", 0)
    with pytest.raises(ProtocolValidationError, match="requires exact type"):
        serializer.serialize(MESSAGES[0], _MANIFESTS[_Heartbeat], 0)
    with pytest.raises(ProtocolValidationError, match="unknown cluster manifest"):
        serializer.deserialize(b"{}", "movie-cluster/unknown/v1", 0)


def test_member_objects_and_wire_objects_require_exact_keys():
    serializer = _ClusterSerializer()
    data = json.loads(serializer.serialize(MESSAGES[1], _MANIFESTS[_JoinAccepted], 0))
    data["members"][0]["unexpected"] = True

    with pytest.raises(ProtocolValidationError, match="missing or extra"):
        serializer.deserialize(json.dumps(data).encode(), _MANIFESTS[_JoinAccepted], 0)


def test_member_snapshots_are_bounded_and_unique():
    repeated = (MEMBERS[0],) * 2
    too_many = tuple(
        _WireMember("alpha", UUID(int=index + 1), "127.0.0.1", 2551, "up", "reachable")
        for index in range(257)
    )

    with pytest.raises(ProtocolValidationError, match="unique"):
        replace(MESSAGES[1], members=repeated)
    with pytest.raises(ProtocolValidationError, match="at most 256"):
        replace(MESSAGES[1], members=too_many)


def test_member_snapshots_reject_multiple_active_incarnations_for_one_name():
    duplicate_name = replace(MEMBERS[1], system_name="alpha")

    with pytest.raises(ProtocolValidationError, match="multiple active incarnations"):
        replace(MESSAGES[1], members=(MEMBERS[0], duplicate_name))


def test_deserialization_rejects_oversized_member_snapshot():
    serializer = _ClusterSerializer()
    data = json.loads(serializer.serialize(MESSAGES[1], _MANIFESTS[_JoinAccepted], 0))
    data["members"] = [data["members"][0]] * 257

    with pytest.raises(ProtocolValidationError, match="at most 256"):
        serializer.deserialize(json.dumps(data).encode(), _MANIFESTS[_JoinAccepted], 0)


@pytest.mark.parametrize(
    "member",
    [
        lambda: _WireMember("", SOURCE_UID, "127.0.0.1", 2551, "up", "reachable"),
        lambda: _WireMember("alpha", UUID(int=0), "127.0.0.1", 2551, "up", "reachable"),
        lambda: _WireMember("alpha", SOURCE_UID, "", 2551, "up", "reachable"),
        lambda: _WireMember("alpha", SOURCE_UID, "127.0.0.1", True, "up", "reachable"),
        lambda: _WireMember("alpha", SOURCE_UID, "127.0.0.1", 2551, "down", "reachable"),
        lambda: _WireMember("alpha", SOURCE_UID, "127.0.0.1", 2551, "up", "unknown"),
    ],
)
def test_wire_member_validates_every_field(member):
    with pytest.raises(ProtocolValidationError):
        member()


def test_nonnegative_integer_fields_reject_bools_and_negative_values():
    with pytest.raises(ProtocolValidationError):
        replace(MESSAGES[3], sequence=True)
    with pytest.raises(ProtocolValidationError):
        replace(MESSAGES[4], revision=-1)


def test_deserialization_rejects_payload_over_one_mibibyte():
    payload = b"{" + b" " * (1 << 20)

    with pytest.raises(ProtocolValidationError, match="exceeds 1 MiB"):
        _ClusterSerializer().deserialize(payload, _MANIFESTS[_JoinRequest], 0)
