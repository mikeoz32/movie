import socket
from dataclasses import dataclass, replace
from threading import Barrier, Thread

import pytest

from movie.actor import ActorSystem, Behaviors
from movie.remoting import (
    AssociationState,
    Endpoint,
    HandshakeError,
    ProtocolValidationError,
    RemotingConfig,
    SerializerDescriptor,
    SerializerRegistryBuilder,
    TcpTransport,
    TransportLimits,
)


@dataclass(frozen=True)
class TextMessage:
    text: str


class TextSerializer:
    def serialize(self, value: object, manifest: str, protocol_minor: int) -> bytes:
        if not isinstance(value, TextMessage) or manifest != "text/v1":
            raise ValueError("unsupported message")
        return value.text.encode("utf-8")

    def deserialize(self, payload: bytes, manifest: str, protocol_minor: int) -> object:
        if manifest != "text/v1":
            raise ValueError("unsupported manifest")
        return TextMessage(payload.decode("utf-8"))


def endpoint() -> Endpoint:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return Endpoint("127.0.0.1", sock.getsockname()[1])
    finally:
        sock.close()


def registry(*, name: str = "text"):
    descriptor = SerializerDescriptor(
        7,
        name,
        1,
        0,
        frozenset({"text/v1"}),
        frozenset({"text/v1"}),
    )
    return (
        SerializerRegistryBuilder()
        .register(descriptor, TextSerializer())
        .bind(TextMessage, 7, "text/v1")
        .build()
    )


def remoting(local: Endpoint, peer_name: str, peer: Endpoint, **changes):
    values = {
        "local": local,
        "peers": {peer_name: peer},
        "serializers": registry(),
        "association_timeout": 2.0,
    }
    values.update(changes)
    return RemotingConfig(**values)


def behavior():
    return Behaviors.receive(lambda context, message: Behaviors.same)


def test_remoting_config_is_immutable_and_validates_remote_bounds() -> None:
    local = endpoint()
    peer = endpoint()
    config = remoting(local, "peer", peer)

    with pytest.raises(TypeError):
        config.peers["other"] = endpoint()  # type: ignore[index]
    with pytest.raises(ProtocolValidationError, match="actor-system name"):
        remoting(local, "bad/name", peer)
    with pytest.raises(ProtocolValidationError, match="unique"):
        RemotingConfig(local, {"first": peer, "second": peer}, registry())
    with pytest.raises(ProtocolValidationError, match="positive"):
        replace(
            config,
            limits=TransportLimits(1024, 1, 1024, 0, 1024),
        )
    with pytest.raises(ProtocolValidationError, match="lane count"):
        replace(config, lane_count=0)
    with pytest.raises(ProtocolValidationError, match="advertised"):
        replace(config, local=Endpoint("0.0.0.0", 7000))
    positional_transport = TcpTransport()
    positional = RemotingConfig(
        local,
        {"peer": peer},
        registry(),
        positional_transport,
        config.limits,
        4,
        2.0,
        64,
        64,
    )
    assert positional.transport is positional_transport
    assert positional.transport_backend == "tcp"
    with pytest.raises(ProtocolValidationError, match="transport backend"):
        replace(config, transport_backend="unknown")


def test_serializer_conflict_rejects_the_handshake() -> None:
    first_endpoint = endpoint()
    second_endpoint = endpoint()
    first = ActorSystem.create(
        behavior(),
        "serializer-first",
        remoting=remoting(first_endpoint, "serializer-second", second_endpoint),
    )
    second = ActorSystem.create(
        behavior(),
        "serializer-second",
        remoting=remoting(
            second_endpoint,
            "serializer-first",
            first_endpoint,
            serializers=registry(name="conflicting-name"),
        ),
    )
    try:
        with pytest.raises(HandshakeError):
            first.remoting.associate("serializer-second")
    finally:
        first.stop()
        second.stop()


@pytest.mark.parametrize("maximum_frame_bytes", [32, 4096])
def test_bootstrap_handshake_is_reserved_from_small_negotiated_queues(
    maximum_frame_bytes,
) -> None:
    first_endpoint = endpoint()
    second_endpoint = endpoint()
    small = TransportLimits(maximum_frame_bytes, 1, 128, 1, 128)
    first = ActorSystem.create(
        behavior(),
        "small-first",
        remoting=remoting(
            first_endpoint,
            "small-second",
            second_endpoint,
            limits=small,
        ),
    )
    second = ActorSystem.create(
        behavior(),
        "small-second",
        remoting=remoting(
            second_endpoint,
            "small-first",
            first_endpoint,
            limits=small,
        ),
    )
    try:
        association = first.remoting.associate("small-second")

        assert association.state is AssociationState.ACTIVE
        assert association.snapshot().outbound_message_limit == 1
        assert association.snapshot().outbound_byte_limit == 128
    finally:
        first.stop()
        second.stop()


def test_configured_hostname_is_preserved_as_the_advertised_endpoint() -> None:
    first_port = endpoint().port
    second_port = endpoint().port
    first_endpoint = Endpoint("localhost", first_port)
    second_endpoint = Endpoint("localhost", second_port)
    first = ActorSystem.create(
        behavior(),
        "hostname-first",
        remoting=remoting(first_endpoint, "hostname-second", second_endpoint),
    )
    second = ActorSystem.create(
        behavior(),
        "hostname-second",
        remoting=remoting(second_endpoint, "hostname-first", first_endpoint),
    )
    try:
        association = first.remoting.associate("hostname-second")

        assert association.peer_endpoint == second_endpoint
        assert second.remoting.endpoint == second_endpoint
    finally:
        first.stop()
        second.stop()


def test_simultaneous_associations_retain_the_same_canonical_connection() -> None:
    first_endpoint = endpoint()
    second_endpoint = endpoint()
    first = ActorSystem.create(
        behavior(),
        "duplicate-first",
        remoting=remoting(first_endpoint, "duplicate-second", second_endpoint),
    )
    second = ActorSystem.create(
        behavior(),
        "duplicate-second",
        remoting=remoting(second_endpoint, "duplicate-first", first_endpoint),
    )
    barrier = Barrier(2)
    associations = []
    errors = []

    def associate(system, peer_name: str) -> None:
        try:
            barrier.wait()
            associations.append(system.remoting.associate(peer_name))
        except BaseException as error:
            errors.append(error)

    threads = [
        Thread(target=associate, args=(first, "duplicate-second")),
        Thread(target=associate, args=(second, "duplicate-first")),
    ]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(3.0)

        assert all(not thread.is_alive() for thread in threads)
        assert errors == []
        assert len(associations) == 2
        assert associations[0].association_uid == associations[1].association_uid
        assert associations[0].state is AssociationState.ACTIVE
        assert associations[1].state is AssociationState.ACTIVE
    finally:
        first.stop()
        second.stop()
