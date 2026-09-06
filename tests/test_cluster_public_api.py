import socket
from dataclasses import dataclass
from threading import Event

import pytest

import movie.cluster as cluster_api
from movie.actor import ActorSystem, Behaviors
from movie.actor.impl.system import ActorSystemImpl
from movie.cluster import (
    CLUSTER,
    ClusterConfig,
    ClusterExtension,
    SeedContact,
)
from movie.remoting import (
    REMOTING,
    Endpoint,
    RemotingConfig,
    RemotingExtension,
    SerializerDescriptor,
    SerializerRegistryBuilder,
    SerializerRegistryError,
    TcpTransport,
)


@dataclass(frozen=True, slots=True)
class AppMessage:
    value: str


class AppSerializer:
    def serialize(self, value: object, manifest: str, protocol_minor: int) -> bytes:
        if type(value) is not AppMessage or manifest != "app-message/v1":
            raise ValueError("unsupported application message")
        return value.value.encode("ascii")

    def deserialize(self, payload: bytes, manifest: str, protocol_minor: int) -> object:
        if manifest != "app-message/v1":
            raise ValueError("unsupported application manifest")
        return AppMessage(payload.decode("ascii"))


def endpoint() -> Endpoint:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return Endpoint("127.0.0.1", sock.getsockname()[1])
    finally:
        sock.close()


def app_registry():
    descriptor = SerializerDescriptor(
        17,
        "application-messages",
        1,
        0,
        frozenset({"app-message/v1"}),
        frozenset({"app-message/v1"}),
    )
    return (
        SerializerRegistryBuilder()
        .register(descriptor, AppSerializer())
        .bind(AppMessage, 17, "app-message/v1")
        .build()
    )


def test_cluster_package_exports_only_public_api() -> None:
    assert set(cluster_api.__all__) == {
        "CLUSTER",
        "ClusterConfig",
        "ClusterError",
        "ClusterEvent",
        "ClusterEventKind",
        "ClusterEventSubscription",
        "ClusterEvents",
        "ClusterExtension",
        "ClusterJoinError",
        "ClusterMember",
        "ClusterRuntime",
        "ClusterShutdownError",
        "MemberIdentity",
        "MembershipSnapshot",
        "MemberStatus",
        "Reachability",
        "SeedContact",
    }
    assert not hasattr(cluster_api, "_JoinRequest")


def test_actor_system_exposes_none_when_cluster_is_disabled() -> None:
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "local-without-cluster",
    )
    try:
        assert system.cluster is None
    finally:
        system.stop()


def test_remoting_and_cluster_are_actor_system_extensions(monkeypatch) -> None:
    local = endpoint()
    system = ActorSystemImpl(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "extension-seed",
        remoting=RemotingConfig(
            local,
            {},
            SerializerRegistryBuilder().build(),
            transport=TcpTransport(),
        ),
        cluster=ClusterConfig(
            "extension-cluster",
            SeedContact("extension-seed", local),
        ),
    )
    remoting = system.remoting
    cluster = system.cluster
    assert isinstance(remoting, RemotingExtension)
    assert isinstance(cluster, ClusterExtension)
    assert REMOTING.get(system) is remoting
    assert CLUSTER.get(system) is cluster
    events = []

    remoting_start = remoting.start
    cluster_start = cluster.start
    remoting_prepare = remoting.prepare_stop
    cluster_prepare = cluster.prepare_stop
    remoting_stop = remoting.stop
    cluster_stop = cluster.stop

    def start_remoting() -> None:
        events.append("start:remoting")
        remoting_start()

    def start_cluster() -> None:
        events.append("start:cluster")
        cluster_start()

    def prepare_cluster(timeout: float) -> None:
        events.append("prepare:cluster")
        assert remoting.is_healthy
        cluster_prepare(timeout)

    def prepare_remoting(timeout: float) -> None:
        events.append("prepare:remoting")
        remoting_prepare(timeout)

    def stop_cluster(timeout: float) -> None:
        events.append("stop:cluster")
        cluster_stop(timeout)

    def stop_remoting(timeout: float) -> None:
        events.append("stop:remoting")
        remoting_stop(timeout)

    monkeypatch.setattr(remoting, "start", start_remoting)
    monkeypatch.setattr(cluster, "start", start_cluster)
    monkeypatch.setattr(cluster, "prepare_stop", prepare_cluster)
    monkeypatch.setattr(remoting, "prepare_stop", prepare_remoting)
    monkeypatch.setattr(cluster, "stop", stop_cluster)
    monkeypatch.setattr(remoting, "stop", stop_remoting)

    try:
        system.start()
    finally:
        system.stop()

    assert events == [
        "start:remoting",
        "start:cluster",
        "prepare:cluster",
        "prepare:remoting",
        "stop:cluster",
        "stop:remoting",
    ]
    assert system.remoting is remoting
    assert system.cluster is cluster


def test_cluster_extension_rejects_start_from_actor_setup(
    monkeypatch,
) -> None:
    local = endpoint()
    events = []

    def setup(context):
        CLUSTER.get(context.get_system())
        return Behaviors.receive(lambda inner_context, message: Behaviors.same)

    system = ActorSystemImpl(
        Behaviors.setup(setup),
        "dependency-seed",
        remoting=RemotingConfig(
            local,
            {},
            SerializerRegistryBuilder().build(),
            transport=TcpTransport(),
        ),
        cluster=ClusterConfig(
            "dependency-cluster",
            SeedContact("dependency-seed", local),
        ),
    )
    remoting = system.remoting
    cluster = system.cluster
    assert remoting is not None
    assert cluster is not None
    remoting_start = remoting.start

    def start_remoting() -> None:
        events.append("remoting")
        remoting_start()

    monkeypatch.setattr(remoting, "start", start_remoting)

    with pytest.raises(RuntimeError, match="Root actor failed during startup"):
        system.start()

    assert events == []


def test_cluster_serializer_composes_with_application_serializers() -> None:
    seed_endpoint = endpoint()
    member_endpoint = endpoint()
    serializers = app_registry()
    cluster = ClusterConfig(
        "application-cluster",
        SeedContact("application-seed", seed_endpoint),
        heartbeat_interval=0.02,
        unreachable_timeout=0.15,
        join_timeout=2.0,
    )
    delivered = Event()
    received = []

    def receive(_context, message):
        received.append(message)
        delivered.set()
        return Behaviors.same

    seed = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "application-seed",
        remoting=RemotingConfig(
            seed_endpoint,
            {"application-member": member_endpoint},
            serializers,
            transport=TcpTransport(),
            association_timeout=1.0,
        ),
        cluster=cluster,
    )
    member = None
    try:
        member = ActorSystem.create(
            Behaviors.receive(receive),
            "application-member",
            remoting=RemotingConfig(
                member_endpoint,
                {"application-seed": seed_endpoint},
                serializers,
                transport=TcpTransport(),
                association_timeout=1.0,
            ),
            cluster=cluster,
        )
        remote = seed.remoting.resolve(
            f"movie://application-member@{member_endpoint.host}:"
            f"{member_endpoint.port}{member.path.remote_path}"
        )

        remote.tell(AppMessage("still-routable"))

        assert delivered.wait(1.0)
        assert received == [AppMessage("still-routable")]
    finally:
        if member is not None:
            member.stop()
        seed.stop()


def test_cluster_serializer_id_collision_fails_before_startup() -> None:
    seed_endpoint = endpoint()
    cluster = ClusterConfig(
        "collision-cluster",
        SeedContact("collision-seed", seed_endpoint),
    )
    conflicting = SerializerDescriptor(
        cluster.serializer_id,
        "application-reserved-id",
        1,
        0,
        frozenset(),
        frozenset(),
    )
    serializers = (
        SerializerRegistryBuilder()
        .register(conflicting, AppSerializer())
        .build()
    )

    with pytest.raises(SerializerRegistryError, match="already registered"):
        ActorSystem.create(
            Behaviors.receive(lambda context, message: Behaviors.same),
            "collision-seed",
            remoting=RemotingConfig(
                seed_endpoint,
                {},
                serializers,
                transport=TcpTransport(),
            ),
            cluster=cluster,
        )


def test_invalid_cluster_configuration_does_not_claim_the_transport() -> None:
    local = endpoint()
    wrong_seed_endpoint = endpoint()
    transport = TcpTransport()
    serializers = SerializerRegistryBuilder().build()
    invalid_cluster = ClusterConfig(
        "validation-cluster",
        SeedContact("validation-seed", wrong_seed_endpoint),
    )
    remoting = RemotingConfig(
        local,
        {},
        serializers,
        transport=transport,
    )

    with pytest.raises(ValueError, match="coordinator endpoint"):
        ActorSystem.create(
            Behaviors.receive(lambda context, message: Behaviors.same),
            "validation-seed",
            remoting=remoting,
            cluster=invalid_cluster,
        )

    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "transport-reuse",
        remoting=remoting,
    )
    system.stop()
