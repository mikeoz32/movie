from dataclasses import FrozenInstanceError, replace
from uuid import UUID

import pytest

from movie.cluster.config import ClusterConfig, SeedContact, _compatibility_fingerprint
from movie.cluster.model import (
    ClusterMember,
    MemberIdentity,
    MembershipSnapshot,
    MemberStatus,
    Reachability,
)
from movie.remoting.transport import Endpoint


def identity(system_name: str, value: int) -> MemberIdentity:
    return MemberIdentity(system_name, UUID(int=value))


def member(system_name: str, value: int, port: int) -> ClusterMember:
    return ClusterMember(
        identity(system_name, value),
        Endpoint("127.0.0.1", port),
        MemberStatus.UP,
        Reachability.REACHABLE,
    )


def test_cluster_enums_have_stable_wire_values() -> None:
    assert [status.value for status in MemberStatus] == ["joining", "up", "leaving", "left"]
    assert [reachability.value for reachability in Reachability] == [
        "reachable",
        "unreachable",
    ]


@pytest.mark.parametrize(
    "system_name",
    ["", "bad/name", "caf\N{LATIN SMALL LETTER E WITH ACUTE}", "a" * 256],
)
def test_member_identity_validates_names(system_name: str) -> None:
    with pytest.raises(ValueError, match="1-255 ASCII"):
        MemberIdentity(system_name, UUID(int=1))


def test_member_identity_is_immutable_and_requires_a_nonzero_uuid() -> None:
    value = identity("member_1~west", 1)

    with pytest.raises(FrozenInstanceError):
        value.system_name = "other"  # type: ignore[misc]
    assert not hasattr(value, "__dict__")
    with pytest.raises(ValueError, match="nonzero UUID"):
        MemberIdentity("member", UUID(int=0))
    with pytest.raises(ValueError, match="nonzero UUID"):
        MemberIdentity("member", "not-a-uuid")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("identity", object()),
        ("endpoint", object()),
        ("status", "up"),
        ("reachability", "reachable"),
    ],
)
def test_cluster_member_requires_exact_domain_types(field: str, value: object) -> None:
    values = {
        "identity": identity("member", 1),
        "endpoint": Endpoint("127.0.0.1", 7001),
        "status": MemberStatus.UP,
        "reachability": Reachability.REACHABLE,
    }
    values[field] = value

    with pytest.raises(ValueError, match=field):
        ClusterMember(**values)  # type: ignore[arg-type]


def test_cluster_member_requires_a_positive_endpoint_port() -> None:
    with pytest.raises(ValueError, match="port must be positive"):
        ClusterMember(
            identity("member", 1),
            Endpoint("127.0.0.1", 0),
            MemberStatus.JOINING,
            Reachability.REACHABLE,
        )


def test_membership_snapshot_sorts_members_and_is_immutable() -> None:
    alpha_high = replace(member("alpha", 2, 7002), status=MemberStatus.LEFT)
    zeta = member("zeta", 3, 7003)
    alpha_low = member("alpha", 1, 7001)

    snapshot = MembershipSnapshot("cluster.one", 7, zeta.identity, (zeta, alpha_high, alpha_low))

    assert snapshot.members == (alpha_low, alpha_high, zeta)
    with pytest.raises(FrozenInstanceError):
        snapshot.revision = 8  # type: ignore[misc]


def test_membership_snapshot_validates_revision_members_and_self() -> None:
    local = member("local", 1, 7001)

    with pytest.raises(ValueError, match="nonnegative integer"):
        MembershipSnapshot("cluster", -1, local.identity, (local,))
    with pytest.raises(ValueError, match="nonnegative integer"):
        MembershipSnapshot("cluster", True, local.identity, (local,))
    with pytest.raises(ValueError, match="tuple"):
        MembershipSnapshot("cluster", 0, local.identity, [local])  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="unique"):
        MembershipSnapshot("cluster", 0, local.identity, (local, local))
    with pytest.raises(ValueError, match="self identity"):
        MembershipSnapshot("cluster", 0, identity("missing", 2), (local,))


def test_membership_snapshot_allows_only_one_active_incarnation_per_name() -> None:
    first = member("member", 1, 7001)
    second = member("member", 2, 7002)

    with pytest.raises(ValueError, match="multiple active incarnations"):
        MembershipSnapshot("cluster", 1, first.identity, (first, second))

    left = replace(second, status=MemberStatus.LEFT)
    assert MembershipSnapshot("cluster", 2, first.identity, (first, left)).members == (
        first,
        left,
    )


def test_seed_and_cluster_config_defaults_are_immutable() -> None:
    seed = SeedContact("coordinator", Endpoint("127.0.0.1", 7000))
    config = ClusterConfig("production", seed)

    assert config == ClusterConfig(
        name="production",
        seed=seed,
        heartbeat_interval=0.25,
        unreachable_timeout=1.0,
        join_timeout=5.0,
        reassociation_timeout=0.25,
        member_limit=256,
        retired_identity_limit=4_096,
        control_queue_capacity=1_024,
        event_capacity=1_000,
        event_max_subscriptions=1_000,
        serializer_id=0x4D4F5601,
    )
    assert not hasattr(seed, "__dict__")
    with pytest.raises(FrozenInstanceError):
        config.name = "other"  # type: ignore[misc]


def test_seed_contact_validates_name_endpoint_and_port() -> None:
    with pytest.raises(ValueError, match="1-255 ASCII"):
        SeedContact("bad/name", Endpoint("127.0.0.1", 7000))
    with pytest.raises(ValueError, match="Endpoint"):
        SeedContact("seed", object())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="positive"):
        SeedContact("seed", Endpoint("127.0.0.1", 0))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("heartbeat_interval", 0),
        ("heartbeat_interval", True),
        ("unreachable_timeout", float("inf")),
        ("join_timeout", float("nan")),
        ("reassociation_timeout", 0),
    ],
)
def test_cluster_config_validates_finite_positive_timings(field: str, value: object) -> None:
    config = ClusterConfig("cluster", SeedContact("seed", Endpoint("127.0.0.1", 7000)))

    with pytest.raises(ValueError, match="finite positive"):
        replace(config, **{field: value})


def test_cluster_config_requires_unreachable_timeout_after_heartbeat() -> None:
    seed = SeedContact("seed", Endpoint("127.0.0.1", 7000))

    with pytest.raises(ValueError, match="greater than heartbeat"):
        ClusterConfig("cluster", seed, heartbeat_interval=1.0, unreachable_timeout=1.0)


@pytest.mark.parametrize(
    "field",
    [
        "member_limit",
        "retired_identity_limit",
        "control_queue_capacity",
        "event_capacity",
        "event_max_subscriptions",
    ],
)
@pytest.mark.parametrize("value", [0, True, 1.5])
def test_cluster_config_validates_positive_integer_capacities(
    field: str,
    value: object,
) -> None:
    config = ClusterConfig("cluster", SeedContact("seed", Endpoint("127.0.0.1", 7000)))

    with pytest.raises(ValueError, match="positive integer"):
        replace(config, **{field: value})


def test_cluster_config_bounds_members_to_the_v1_wire_limit() -> None:
    seed = SeedContact("seed", Endpoint("127.0.0.1", 7000))

    with pytest.raises(ValueError, match="protocol limit"):
        ClusterConfig("cluster", seed, member_limit=257)


@pytest.mark.parametrize("serializer_id", [0, True, 1 << 32])
def test_cluster_config_validates_serializer_id(serializer_id: object) -> None:
    seed = SeedContact("seed", Endpoint("127.0.0.1", 7000))

    with pytest.raises(ValueError, match="serializer ID"):
        ClusterConfig("cluster", seed, serializer_id=serializer_id)  # type: ignore[arg-type]


def test_cluster_compatibility_fingerprint_covers_shared_protocol_settings() -> None:
    seed = SeedContact("seed", Endpoint("127.0.0.1", 7000))
    config = ClusterConfig("cluster", seed)

    assert _compatibility_fingerprint(config) == _compatibility_fingerprint(config)
    assert len(_compatibility_fingerprint(config)) == 64
    for changed in (
        replace(config, heartbeat_interval=0.5),
        replace(config, unreachable_timeout=2.0),
        replace(config, member_limit=128),
        replace(config, retired_identity_limit=2_048),
        replace(config, serializer_id=config.serializer_id + 1),
    ):
        assert _compatibility_fingerprint(changed) != _compatibility_fingerprint(config)
