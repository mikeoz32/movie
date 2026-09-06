import socket
import time
from dataclasses import dataclass, replace
from threading import Barrier, Event, Thread
from uuid import UUID, uuid4

import pytest

from movie.actor import ActorSystem, Behaviors
from movie.remoting import (
    BOOTSTRAP_MAX_FRAME_BYTES,
    COMMON_HEADER_SIZE,
    CONTROL_LANE_ID,
    HEADER_VERSION,
    PROTOCOL_MAJOR,
    PROTOCOL_MINOR,
    AssociationRole,
    AssociationState,
    Endpoint,
    FrameTooLargeError,
    FrameType,
    GoAway,
    HandshakeError,
    Hello,
    HelloAccept,
    HelloReject,
    LogicalChannel,
    ProtocolValidationError,
    ReasonCode,
    RemotingCapacityError,
    RemotingConfig,
    SerializerDescriptor,
    SerializerRegistryBuilder,
    StreamKind,
    StreamPreamble,
    TcpTransport,
    TransportLimits,
    TransportRecord,
    UserMessage,
    decode_frame,
    encode_common_header,
    encode_frame,
    encode_preamble,
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


def wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("condition was not met before the deadline")
        time.sleep(0.005)


def recv_exact(sock: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            pytest.fail("raw peer closed before a complete frame arrived")
        data.extend(chunk)
    return bytes(data)


def recv_frame(sock: socket.socket):
    header = recv_exact(sock, COMMON_HEADER_SIZE)
    total_length = int.from_bytes(header[:4], "big") + 4
    payload = header + recv_exact(sock, total_length - COMMON_HEADER_SIZE)
    return payload, decode_frame(payload, stream_kind=StreamKind.MULTIPLEXED)


def peer_hello(
    system,
    peer_endpoint: Endpoint,
    association_uid: UUID,
    peer_incarnation_uid: UUID,
    *,
    peer_name: str = "raw-peer",
    protocol_minor: int = PROTOCOL_MINOR,
    maximum_frame_bytes: int = BOOTSTRAP_MAX_FRAME_BYTES,
    outbound_byte_limit: int = 65_536,
) -> Hello:
    return Hello(
        PROTOCOL_MAJOR,
        protocol_minor,
        AssociationRole.INITIATOR,
        peer_name,
        peer_incarnation_uid,
        association_uid,
        peer_endpoint.host,
        peer_endpoint.port,
        maximum_frame_bytes,
        4,
        16,
        outbound_byte_limit,
        16,
        65_536,
        serializers=system.remoting.config.serializers.descriptors,
    )


def open_raw_peer(system, association_uid: UUID) -> socket.socket:
    raw = socket.create_connection(
        (system.remoting.endpoint.host, system.remoting.endpoint.port),
        timeout=1.0,
    )
    raw.settimeout(1.0)
    raw.sendall(
        encode_preamble(
            StreamPreamble(
                StreamKind.MULTIPLEXED,
                association_uid,
                CONTROL_LANE_ID,
            )
        )
    )
    return raw


def complete_raw_handshake(system, raw: socket.socket, hello: Hello) -> HelloAccept:
    raw.sendall(encode_frame(hello, stream_kind=StreamKind.MULTIPLEXED))
    _, local_hello = recv_frame(raw)
    assert isinstance(local_hello, Hello)
    accept_payload, accept = recv_frame(raw)
    assert isinstance(accept, HelloAccept)
    raw.sendall(accept_payload)
    wait_until(
        lambda: any(
            snapshot.association_uid == hello.association_uid
            and snapshot.state is AssociationState.ACTIVE
            for snapshot in system.remoting.associations
        )
    )
    return accept


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
    with pytest.raises(ProtocolValidationError, match="health event"):
        replace(config, health_event_capacity=0)
    with pytest.raises(ProtocolValidationError, match="health event"):
        replace(config, health_event_max_subscriptions=0)
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


@pytest.mark.parametrize("version_field", ["protocol_major", "header_version"])
def test_live_incompatible_protocol_version_is_rejected(version_field: str) -> None:
    local = endpoint()
    advertised_peer = endpoint()
    system = ActorSystem.create(
        behavior(),
        "version-target",
        remoting=remoting(local, "raw-peer", advertised_peer),
    )
    association_uid = uuid4()
    raw = open_raw_peer(system, association_uid)
    try:
        hello = peer_hello(system, advertised_peer, association_uid, uuid4())
        payload = bytearray(
            encode_frame(hello, stream_kind=StreamKind.MULTIPLEXED)
        )
        if version_field == "protocol_major":
            payload = bytearray(
                encode_frame(
                    replace(hello, protocol_major=PROTOCOL_MAJOR + 1),
                    stream_kind=StreamKind.MULTIPLEXED,
                )
            )
        else:
            payload[6:8] = (HEADER_VERSION + 1).to_bytes(2, "big")
        raw.sendall(payload)

        _, response = recv_frame(raw)
        assert isinstance(response, HelloReject)
        assert response.reason is ReasonCode.INCOMPATIBLE_VERSION
        wait_until(lambda: bool(system.remoting.association_history))
        assert (
            system.remoting.association_history[-1].close_reason
            is ReasonCode.INCOMPATIBLE_VERSION
        )
        assert system.remoting.is_healthy
    finally:
        raw.close()
        system.stop()


def test_peer_frame_limit_must_encode_a_minimum_goaway() -> None:
    local = endpoint()
    advertised_peer = endpoint()
    system = ActorSystem.create(
        behavior(),
        "terminal-limit-target",
        remoting=remoting(local, "raw-peer", advertised_peer),
    )
    association_uid = uuid4()
    raw = open_raw_peer(system, association_uid)
    try:
        raw.sendall(
            encode_frame(
                peer_hello(
                    system,
                    advertised_peer,
                    association_uid,
                    uuid4(),
                    maximum_frame_bytes=19,
                ),
                stream_kind=StreamKind.MULTIPLEXED,
            )
        )

        _, response = recv_frame(raw)
        assert isinstance(response, HelloReject)
        assert response.reason is ReasonCode.PROTOCOL_VIOLATION
        wait_until(lambda: bool(system.remoting.association_history))
        assert not system.remoting.associations
        assert system.remoting.is_healthy
    finally:
        raw.close()
        system.stop()


def test_newer_peer_minor_negotiates_the_current_v1_minor() -> None:
    local = endpoint()
    advertised_peer = endpoint()
    system = ActorSystem.create(
        behavior(),
        "minor-version-target",
        remoting=remoting(local, "raw-peer", advertised_peer),
    )
    association_uid = uuid4()
    raw = open_raw_peer(system, association_uid)
    try:
        accept = complete_raw_handshake(
            system,
            raw,
            peer_hello(
                system,
                advertised_peer,
                association_uid,
                uuid4(),
                protocol_minor=7,
            ),
        )

        assert accept.protocol_minor == PROTOCOL_MINOR
        assert system.remoting.associations[0].state is AssociationState.ACTIVE
    finally:
        raw.close()
        system.stop()


def test_minimum_negotiated_frame_limit_preserves_terminal_goaway() -> None:
    local = endpoint()
    advertised_peer = endpoint()
    minimum_limits = TransportLimits(20, 16, 65_536, 16, 65_536)
    system = ActorSystem.create(
        behavior(),
        "minimum-goaway-target",
        remoting=remoting(
            local,
            "raw-peer",
            advertised_peer,
            limits=minimum_limits,
        ),
    )
    association_uid = uuid4()
    raw = open_raw_peer(system, association_uid)
    try:
        accept = complete_raw_handshake(
            system,
            raw,
            peer_hello(
                system,
                advertised_peer,
                association_uid,
                uuid4(),
                maximum_frame_bytes=20,
            ),
        )
        assert accept.maximum_frame_bytes == 20
        raw.sendall(
            encode_common_header(
                FrameType.USER_MESSAGE,
                5,
                0,
            )
        )

        payload, response = recv_frame(raw)
        assert len(payload) == 20
        assert response == GoAway(ReasonCode.FLOW_CONTROL_VIOLATION)
        wait_until(lambda: bool(system.remoting.association_history))
        assert (
            system.remoting.association_history[-1].close_reason
            is ReasonCode.FLOW_CONTROL_VIOLATION
        )
    finally:
        raw.close()
        system.stop()


def test_negotiated_inbound_byte_limit_rejects_before_reading_the_body() -> None:
    local = endpoint()
    advertised_peer = endpoint()
    constrained = TransportLimits(4_096, 16, 65_536, 16, 100)
    system = ActorSystem.create(
        behavior(),
        "inbound-byte-limit-target",
        remoting=remoting(
            local,
            "raw-peer",
            advertised_peer,
            limits=constrained,
        ),
    )
    association_uid = uuid4()
    raw = open_raw_peer(system, association_uid)
    try:
        accept = complete_raw_handshake(
            system,
            raw,
            peer_hello(
                system,
                advertised_peer,
                association_uid,
                uuid4(),
                maximum_frame_bytes=4_096,
                outbound_byte_limit=100,
            ),
        )
        assert accept.outbound_byte_limit == 100
        raw.sendall(
            encode_common_header(
                FrameType.USER_MESSAGE,
                85,
                0,
            )
        )

        _, response = recv_frame(raw)
        assert isinstance(response, GoAway)
        assert response.reason is ReasonCode.FLOW_CONTROL_VIOLATION
        wait_until(lambda: bool(system.remoting.association_history))
        assert (
            system.remoting.association_history[-1].close_reason
            is ReasonCode.FLOW_CONTROL_VIOLATION
        )
    finally:
        raw.close()
        system.stop()


def test_active_incarnation_rejects_a_late_different_incarnation() -> None:
    target_endpoint = endpoint()
    peer_endpoint = endpoint()
    target = ActorSystem.create(
        behavior(),
        "incarnation-target",
        remoting=remoting(target_endpoint, "incarnation-peer", peer_endpoint),
    )
    peer = ActorSystem.create(
        behavior(),
        "incarnation-peer",
        remoting=remoting(peer_endpoint, "incarnation-target", target_endpoint),
    )
    retained = target.remoting.associate("incarnation-peer")
    association_uid = uuid4()
    raw = open_raw_peer(target, association_uid)
    try:
        raw.sendall(
            encode_frame(
                peer_hello(
                    target,
                    peer_endpoint,
                    association_uid,
                    uuid4(),
                    peer_name="incarnation-peer",
                ),
                stream_kind=StreamKind.MULTIPLEXED,
            )
        )
        _, local_hello = recv_frame(raw)
        assert isinstance(local_hello, Hello)
        accept_payload, accept = recv_frame(raw)
        assert isinstance(accept, HelloAccept)
        raw.sendall(accept_payload)

        _, response = recv_frame(raw)
        assert response == GoAway(
            ReasonCode.DUPLICATE_ASSOCIATION,
            "a canonical duplicate association was retained",
        )
        wait_until(
            lambda: any(
                snapshot.association_uid == association_uid
                for snapshot in target.remoting.association_history
            )
        )
        assert len(target.remoting.associations) == 1
        assert target.remoting.associations[0].association_uid == retained.association_uid
        assert target.remoting.associations[0].state is AssociationState.ACTIVE
    finally:
        raw.close()
        target.stop()
        peer.stop()


def test_peer_goaway_moves_association_to_closing_before_transport_cleanup(
    monkeypatch,
) -> None:
    first_endpoint = endpoint()
    second_endpoint = endpoint()
    first = ActorSystem.create(
        behavior(),
        "goaway-first",
        remoting=remoting(first_endpoint, "goaway-second", second_endpoint),
    )
    second = ActorSystem.create(
        behavior(),
        "goaway-second",
        remoting=remoting(second_endpoint, "goaway-first", first_endpoint),
    )
    entered_close = Event()
    release_close = Event()
    try:
        initiator = first.remoting.associate("goaway-second")
        responder = second.remoting._active["goaway-first"]
        original_close = responder._connection.close

        def blocked_close(timeout=None) -> None:
            entered_close.set()
            release_close.wait(1.0)
            original_close(timeout)

        monkeypatch.setattr(responder._connection, "close", blocked_close)
        initiator._connection.send_terminal(
            TransportRecord(
                LogicalChannel(StreamKind.CONTROL, CONTROL_LANE_ID),
                encode_frame(
                    GoAway(ReasonCode.NORMAL_SHUTDOWN, "peer shutdown"),
                    stream_kind=StreamKind.CONTROL,
                ),
            )
        )

        assert entered_close.wait(1.0)
        assert second.remoting.associations[0].state is AssociationState.CLOSING
        assert (
            second.remoting.associations[0].close_reason
            is ReasonCode.NORMAL_SHUTDOWN
        )
    finally:
        release_close.set()
        first.stop()
        second.stop()


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


def test_default_asyncio_association_negotiates_asymmetric_limits_by_role() -> None:
    first_endpoint = endpoint()
    second_endpoint = endpoint()
    first_limits = TransportLimits(8_192, 11, 80_000, 33, 60_000)
    second_limits = TransportLimits(4_096, 44, 50_000, 22, 30_000)
    first = ActorSystem.create(
        behavior(),
        "asymmetric-first",
        remoting=remoting(
            first_endpoint,
            "asymmetric-second",
            second_endpoint,
            limits=first_limits,
            lane_count=7,
        ),
    )
    second = ActorSystem.create(
        behavior(),
        "asymmetric-second",
        remoting=remoting(
            second_endpoint,
            "asymmetric-first",
            first_endpoint,
            limits=second_limits,
            lane_count=3,
        ),
    )
    try:
        initiator = first.remoting.associate("asymmetric-second")
        responder = second.remoting._active["asymmetric-first"]
        initiator_snapshot = initiator.snapshot()
        responder_snapshot = responder.snapshot()

        assert (
            initiator_snapshot.lane_count,
            initiator_snapshot.outbound_message_limit,
            initiator_snapshot.outbound_byte_limit,
            initiator_snapshot.inbound_message_limit,
            initiator_snapshot.inbound_byte_limit,
        ) == (3, 11, 30_000, 33, 50_000)
        assert (
            responder_snapshot.lane_count,
            responder_snapshot.outbound_message_limit,
            responder_snapshot.outbound_byte_limit,
            responder_snapshot.inbound_message_limit,
            responder_snapshot.inbound_byte_limit,
        ) == (3, 33, 50_000, 11, 30_000)
        assert initiator_snapshot.maximum_frame_bytes == 4_096
        assert responder_snapshot.maximum_frame_bytes == 4_096
    finally:
        first.stop()
        second.stop()


def test_oversized_outbound_attempt_does_not_advance_the_delivery_lane() -> None:
    first_endpoint = endpoint()
    second_endpoint = endpoint()
    constrained = TransportLimits(256, 16, 65_536, 16, 65_536)
    received = []
    delivered = Event()
    first = ActorSystem.create(
        behavior(),
        "oversized-first",
        remoting=remoting(
            first_endpoint,
            "oversized-second",
            second_endpoint,
            limits=constrained,
        ),
    )
    second = ActorSystem.create(
        Behaviors.receive(
            lambda context, message: (
                received.append(message),
                delivered.set(),
                Behaviors.same,
            )[2]
        ),
        "oversized-second",
        remoting=remoting(
            second_endpoint,
            "oversized-first",
            first_endpoint,
            limits=constrained,
        ),
    )
    try:
        association = first.remoting.associate("oversized-second")
        remote = first.remoting.resolve(
            f"movie://oversized-second@{second_endpoint.host}:"
            f"{second_endpoint.port}{second.path.remote_path}"
        )

        with pytest.raises(FrameTooLargeError):
            remote.tell(TextMessage("x" * 512))
        remote.tell(TextMessage("small"))

        assert delivered.wait(1.0)
        assert received == [TextMessage("small")]
        assert association.state is AssociationState.ACTIVE
        assert association.snapshot().metrics.rejected_delivery_attempts == 1
        assert first.remoting.metrics.rejected_delivery_attempts == 1
        assert second.remoting.metrics.sequence_violations == 0
    finally:
        first.stop()
        second.stop()


@pytest.mark.parametrize(
    ("sequences", "expected_detail", "accepted_prefix"),
    [
        ((1,), "delivery lane 0 expected sequence 0, got 1", ()),
        ((0, 0), "delivery lane 0 expected sequence 1, got 0", (0,)),
        ((0, 1, 0), "delivery lane 0 expected sequence 2, got 0", (0, 1)),
        ((0, 2), "delivery lane 0 expected sequence 1, got 2", (0,)),
    ],
    ids=("initial-gap", "duplicate", "regression", "gap"),
)
def test_live_lane_sequence_violation_closes_after_the_valid_prefix(
    sequences,
    expected_detail,
    accepted_prefix,
) -> None:
    first_endpoint = endpoint()
    second_endpoint = endpoint()
    received = []
    first = ActorSystem.create(
        behavior(),
        "sequence-first",
        remoting=remoting(first_endpoint, "sequence-second", second_endpoint),
    )
    second = ActorSystem.create(
        Behaviors.receive(
            lambda context, message: (received.append(message), Behaviors.same)[1]
        ),
        "sequence-second",
        remoting=remoting(second_endpoint, "sequence-first", first_endpoint),
    )
    try:
        initiator = first.remoting.associate("sequence-second")
        responder = second.remoting._active["sequence-first"]
        for sequence in sequences:
            frame = UserMessage(
                initiator.association_uid,
                0,
                sequence,
                first.incarnation_uid,
                second.incarnation_uid,
                second.id,
                7,
                "text/v1",
                str(sequence).encode("ascii"),
            )
            initiator._connection.send_active(
                TransportRecord(
                    LogicalChannel(StreamKind.DELIVERY_LANE, 0),
                    encode_frame(frame, stream_kind=StreamKind.DELIVERY_LANE),
                )
            )

        responder._thread.join(1.0)
        initiator._thread.join(1.0)

        assert not responder._thread.is_alive()
        assert not initiator._thread.is_alive()
        responder_failure = second.remoting.association_history[-1]
        initiator_failure = first.remoting.association_history[-1]
        assert received == [TextMessage(str(sequence)) for sequence in accepted_prefix]
        assert responder_failure.close_reason is ReasonCode.PROTOCOL_VIOLATION
        assert responder_failure.close_detail == expected_detail
        assert responder_failure.metrics.sequence_violations == 1
        assert responder_failure.sequence_violations_by_lane == (1, 0, 0, 0)
        assert initiator_failure.close_reason is ReasonCode.PROTOCOL_VIOLATION
    finally:
        first.stop()
        second.stop()


def test_outbound_lane_sequence_exhaustion_rejects_without_wraparound() -> None:
    first_endpoint = endpoint()
    second_endpoint = endpoint()
    received = []
    first = ActorSystem.create(
        behavior(),
        "exhaustion-first",
        remoting=remoting(first_endpoint, "exhaustion-second", second_endpoint),
    )
    second = ActorSystem.create(
        Behaviors.receive(
            lambda context, message: (received.append(message), Behaviors.same)[1]
        ),
        "exhaustion-second",
        remoting=remoting(second_endpoint, "exhaustion-first", first_endpoint),
    )
    try:
        association = first.remoting.associate("exhaustion-second")
        remote = first.remoting.resolve(
            f"movie://exhaustion-second@{second_endpoint.host}:"
            f"{second_endpoint.port}{second.path.remote_path}"
        )
        association._outbound_sequences = [1 << 64] * 4

        with pytest.raises(RemotingCapacityError, match="sequence is exhausted"):
            remote.tell(TextMessage("not sent"))

        wait_until(lambda: not first.remoting.associations)
        assert received == []
        failure = first.remoting.association_history[-1]
        assert failure.close_reason is ReasonCode.PROTOCOL_VIOLATION
        assert failure.metrics.rejected_delivery_attempts == 1
        assert first.remoting.metrics.rejected_delivery_attempts == 1
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
