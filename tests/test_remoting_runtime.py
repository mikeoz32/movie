import socket
import struct
import threading
import time
from dataclasses import dataclass
from threading import Event, Lock, Thread
from uuid import uuid4

import pytest

from movie.actor import (
    AbstractBehavior,
    ActorContext,
    ActorSystem,
    Behaviors,
    DeadLetterReason,
)
from movie.actor.impl.system import ActorSystemImpl
from movie.config import Config
from movie.remoting import (
    CONTROL_LANE_ID,
    Association,
    AssociationState,
    AsyncioTcpTransport,
    ConnectionState,
    Endpoint,
    HandshakeError,
    NoAssociationError,
    ReasonCode,
    RemotingCapacityError,
    RemotingConfig,
    RemotingHealthEventKind,
    RemotingShutdownError,
    ResolutionError,
    SerializerDescriptor,
    SerializerRegistryBuilder,
    StaleIncarnationError,
    StreamKind,
    StreamPreamble,
    TcpTransport,
    TransportCapacityError,
    TransportConnectionSnapshot,
    TransportListenError,
    UnknownSerializerError,
    encode_preamble,
)


@dataclass(frozen=True)
class OrderedMessage:
    producer: int
    sequence: int


class OrderedSerializer:
    def serialize(self, value: object, manifest: str, protocol_minor: int) -> bytes:
        if not isinstance(value, OrderedMessage) or manifest != "ordered/v1":
            raise ValueError("unsupported message")
        return f"{value.producer}:{value.sequence}".encode("ascii")

    def deserialize(self, payload: bytes, manifest: str, protocol_minor: int) -> object:
        if manifest != "ordered/v1":
            raise ValueError("unsupported manifest")
        producer, sequence = payload.decode("ascii").split(":", 1)
        if producer == "-1":
            raise ValueError("deliberately malformed payload")
        return OrderedMessage(int(producer), int(sequence))


def endpoint() -> Endpoint:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return Endpoint("127.0.0.1", sock.getsockname()[1])
    finally:
        sock.close()


def registry():
    descriptor = SerializerDescriptor(
        11,
        "ordered-text",
        1,
        0,
        frozenset({"ordered/v1"}),
        frozenset({"ordered/v1"}),
    )
    return (
        SerializerRegistryBuilder()
        .register(descriptor, OrderedSerializer())
        .bind(OrderedMessage, 11, "ordered/v1")
        .build()
    )


def remoting(
    local: Endpoint,
    peer_name: str,
    peer: Endpoint,
) -> RemotingConfig:
    return RemotingConfig(
        local,
        {peer_name: peer},
        registry(),
        lane_count=4,
        association_timeout=2.0,
    )


def wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("condition was not met before the deadline")
        time.sleep(0.005)


def start_pair(receive):
    sender_endpoint = endpoint()
    receiver_endpoint = endpoint()
    sender = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "runtime-sender",
        remoting=remoting(
            sender_endpoint,
            "runtime-receiver",
            receiver_endpoint,
        ),
    )
    receiver = ActorSystem.create(
        Behaviors.receive(receive),
        "runtime-receiver",
        remoting=remoting(
            receiver_endpoint,
            "runtime-sender",
            sender_endpoint,
        ),
    )
    return sender, receiver


def locator(system, actor=None) -> str:
    endpoint = system.remoting.endpoint
    target = system if actor is None else actor
    return f"movie://{system.name}@{endpoint.host}:{endpoint.port}{target.path.remote_path}"


def test_associate_resolve_tell_and_concurrent_producer_ordering() -> None:
    received: list[OrderedMessage] = []
    received_lock = Lock()
    completed = Event()

    def receive(context: ActorContext, message: OrderedMessage):
        with received_lock:
            received.append(message)
            if len(received) == 400:
                completed.set()
        return Behaviors.same

    sender, receiver = start_pair(receive)
    try:
        sender.remoting.associate("runtime-receiver")
        remote = sender.remoting.resolve(locator(receiver))
        producers = [
            Thread(
                target=lambda producer=producer: [
                    remote.tell(OrderedMessage(producer, sequence))
                    for sequence in range(100)
                ]
            )
            for producer in range(4)
        ]
        for producer in producers:
            producer.start()
        for producer in producers:
            producer.join(2.0)

        assert all(not producer.is_alive() for producer in producers)
        assert completed.wait(3.0)
        for producer in range(4):
            assert [
                message.sequence for message in received if message.producer == producer
            ] == list(range(100))
        assert sender.remoting.metrics.accepted_delivery_attempts == 400
        assert receiver.remoting.metrics.accepted_delivery_attempts == 400
    finally:
        sender.stop()
        receiver.stop()


def test_remote_reference_does_not_follow_a_reused_actor_path() -> None:
    received = []
    delivered = Event()

    def receive(context: ActorContext, message: OrderedMessage):
        received.append(message)
        delivered.set()
        return Behaviors.same

    sender, receiver = start_pair(lambda context, message: Behaviors.same)
    first = receiver.spawn(Behaviors.receive(receive), "replaceable")
    letters = sender.dead_letters.subscribe()
    observed = []
    try:
        sender.remoting.associate("runtime-receiver")
        old_remote = sender.remoting.resolve(locator(receiver, first))

        stopped = receiver.actor_stop_future(first)
        receiver.terminate(first)
        stopped.result(timeout=1.0)
        replacement = receiver.spawn(Behaviors.receive(receive), "replaceable")
        new_remote = sender.remoting.resolve(locator(receiver, replacement))

        assert new_remote.path == old_remote.path
        assert new_remote.id != old_remote.id

        old_remote.tell(OrderedMessage(1, 1))
        wait_until(
            lambda: (
                observed.extend(letters.drain())
                or any(
                    letter.reason is DeadLetterReason.ACTOR_NOT_FOUND
                    for letter in observed
                )
            )
        )
        assert received == []

        new_remote.tell(OrderedMessage(2, 1))
        assert delivered.wait(1.0)
        assert received == [OrderedMessage(2, 1)]
    finally:
        sender.stop()
        receiver.stop()


def test_unregistered_outbound_type_does_not_advance_the_delivery_lane() -> None:
    received = []
    delivered = Event()

    def receive(context: ActorContext, message: OrderedMessage):
        received.append(message)
        delivered.set()
        return Behaviors.same

    sender, receiver = start_pair(receive)
    try:
        association = sender.remoting.associate("runtime-receiver")
        remote = sender.remoting.resolve(locator(receiver))
        before = association.snapshot()

        with pytest.raises(UnknownSerializerError, match="exact-type binding"):
            remote.tell("unregistered")

        rejected = association.snapshot()
        assert rejected.state is AssociationState.ACTIVE
        assert (
            rejected.metrics.serialization_rejections
            == before.metrics.serialization_rejections + 1
        )
        assert (
            rejected.metrics.accepted_delivery_attempts
            == before.metrics.accepted_delivery_attempts
        )

        remote.tell(OrderedMessage(1, 1))
        assert delivered.wait(1.0)
        assert received == [OrderedMessage(1, 1)]
        assert association.state is AssociationState.ACTIVE
    finally:
        sender.stop()
        receiver.stop()


def test_disconnecting_one_peer_preserves_the_other_ordered_association() -> None:
    sender_endpoint = endpoint()
    disconnected_endpoint = endpoint()
    receiver_endpoint = endpoint()
    received = []
    completed = Event()

    def receive(context: ActorContext, message: OrderedMessage):
        received.append(message)
        if len(received) == 16:
            completed.set()
        return Behaviors.same

    sender = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "multi-sender",
        remoting=RemotingConfig(
            sender_endpoint,
            {
                "multi-disconnected": disconnected_endpoint,
                "multi-receiver": receiver_endpoint,
            },
            registry(),
            lane_count=4,
            association_timeout=2.0,
        ),
    )
    disconnected = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "multi-disconnected",
        remoting=remoting(
            disconnected_endpoint,
            "multi-sender",
            sender_endpoint,
        ),
    )
    receiver = ActorSystem.create(
        Behaviors.receive(receive),
        "multi-receiver",
        remoting=remoting(receiver_endpoint, "multi-sender", sender_endpoint),
    )
    disconnected_stopped = False
    try:
        sender.remoting.associate("multi-disconnected")
        surviving = sender.remoting.associate("multi-receiver")
        remote = sender.remoting.resolve(locator(receiver))
        remote.tell(OrderedMessage(1, 0))
        wait_until(lambda: received == [OrderedMessage(1, 0)])

        disconnected.stop()
        disconnected_stopped = True
        wait_until(
            lambda: {
                snapshot.peer_system_name for snapshot in sender.remoting.associations
            }
            == {"multi-receiver"}
        )
        snapshot = sender.remoting.associations[0]
        assert snapshot.association_uid == surviving.association_uid
        assert snapshot.state is AssociationState.ACTIVE

        for sequence in range(1, 16):
            remote.tell(OrderedMessage(1, sequence))

        assert completed.wait(1.0)
        assert received == [OrderedMessage(1, sequence) for sequence in range(16)]
    finally:
        sender.stop()
        if not disconnected_stopped:
            disconnected.stop()
        receiver.stop()


def test_asyncio_tcp_transport_is_the_default_remoting_backend() -> None:
    received = []
    completed = Event()

    def receive(context: ActorContext, message: OrderedMessage):
        received.append(message)
        if len(received) == 100:
            completed.set()
        return Behaviors.same

    sender, receiver = start_pair(receive)
    try:
        assert isinstance(sender.remoting._transport, AsyncioTcpTransport)
        assert isinstance(receiver.remoting._transport, AsyncioTcpTransport)
        sender.remoting.associate("runtime-receiver")
        remote = sender.remoting.resolve(locator(receiver))
        for sequence in range(100):
            remote.tell(OrderedMessage(1, sequence))

        assert completed.wait(3.0)
        assert received == [OrderedMessage(1, sequence) for sequence in range(100)]
        assert any(
            thread.name == "movie-asyncio-io-runtime-sender"
            for thread in threading.enumerate()
        )
        assert not any(
            thread.name.startswith("movie-tcp-connection-")
            for thread in threading.enumerate()
        )
    finally:
        sender.stop()
        receiver.stop()


def test_legacy_tcp_transport_remains_explicitly_injectable() -> None:
    sender_endpoint = endpoint()
    receiver_endpoint = endpoint()
    received = []
    completed = Event()

    def receive(context: ActorContext, message: OrderedMessage):
        received.append(message)
        completed.set()
        return Behaviors.same

    sender = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "legacy-runtime-sender",
        remoting=RemotingConfig(
            sender_endpoint,
            {"legacy-runtime-receiver": receiver_endpoint},
            registry(),
            transport=TcpTransport(),
        ),
    )
    receiver = ActorSystem.create(
        Behaviors.receive(receive),
        "legacy-runtime-receiver",
        remoting=RemotingConfig(
            receiver_endpoint,
            {"legacy-runtime-sender": sender_endpoint},
            registry(),
            transport=TcpTransport(),
        ),
    )
    try:
        assert isinstance(sender.remoting._transport, TcpTransport)
        assert isinstance(receiver.remoting._transport, TcpTransport)
        sender.remoting.associate("legacy-runtime-receiver")
        sender.remoting.resolve(locator(receiver)).tell(OrderedMessage(1, 1))

        assert completed.wait(3.0)
        assert received == [OrderedMessage(1, 1)]
    finally:
        sender.stop()
        receiver.stop()


def test_asyncio_listener_failure_makes_remoting_deterministically_unavailable(
    monkeypatch,
) -> None:
    sender, receiver = start_pair(lambda context, message: Behaviors.same)
    health_events = sender.remoting.health_events.subscribe()
    try:
        association = sender.remoting.associate("runtime-receiver")
        remote = sender.remoting.resolve(locator(receiver))
        assert sender.remoting.is_healthy
        assert sender.remoting.failure is None

        send_entered = Event()
        send_errors = []
        original_send = association.send_user_message

        def blocked_send(identity, message) -> None:
            send_entered.set()
            original_send(identity, message)

        monkeypatch.setattr(association, "send_user_message", blocked_send)
        association._send_lock.acquire()
        try:
            sender_thread = Thread(
                target=lambda: _capture_error(
                    lambda: remote.tell(OrderedMessage(1, 1)),
                    send_errors,
                )
            )
            sender_thread.start()
            assert send_entered.wait(1.0)
            sender.remoting._listener._record_failure(
                OSError("deliberate accept failure")
            )
        finally:
            association._send_lock.release()
        sender_thread.join(1.0)

        assert not sender_thread.is_alive()
        assert len(send_errors) == 1
        wait_until(lambda: not sender.remoting.is_healthy)
        wait_until(lambda: not sender.remoting.associations)
        observed_health_events = []
        wait_until(
            lambda: (
                observed_health_events.extend(health_events.drain())
                or len(observed_health_events) >= 3
            )
        )

        failure = sender.remoting.failure
        assert isinstance(failure, TransportListenError)
        assert "deliberate accept failure" in str(failure)
        assert [event.kind for event in observed_health_events] == [
            RemotingHealthEventKind.ASSOCIATION_ACTIVATED,
            RemotingHealthEventKind.LISTENER_FAILED,
            RemotingHealthEventKind.ASSOCIATION_CLOSED,
        ]
        assert observed_health_events[1].failure is failure
        assert isinstance(send_errors[0], RemotingShutdownError)
        assert send_errors[0].__cause__ is failure
        with pytest.raises(RemotingShutdownError, match="listener failed") as associate:
            sender.remoting.associate("runtime-receiver")
        assert associate.value.__cause__ is failure
        with pytest.raises(RemotingShutdownError, match="listener failed") as resolve:
            sender.remoting.resolve(locator(receiver))
        assert resolve.value.__cause__ is failure
        with pytest.raises(RemotingShutdownError, match="listener failed") as tell:
            remote.tell(OrderedMessage(1, 1))
        assert tell.value.__cause__ is failure
        assert sender.actor_count == 2
    finally:
        health_events.close()
        sender.stop()
        receiver.stop()


def test_unknown_recipient_and_malformed_payload_publish_advisory_dead_letters() -> None:
    sender, receiver = start_pair(lambda context, message: Behaviors.same)
    sender_letters = sender.dead_letters.subscribe()
    receiver_letters = receiver.dead_letters.subscribe()
    try:
        association = sender.remoting.associate("runtime-receiver")
        remote = sender.remoting.resolve(locator(receiver))

        remote.tell(OrderedMessage(-1, 1))
        sender_observed = []
        receiver_observed = []
        wait_until(
            lambda: (
                sender_observed.extend(sender_letters.drain())
                or any(
                    letter.reason is DeadLetterReason.DESERIALIZATION_REJECTED
                    for letter in sender_observed
                )
            )
        )
        receiver_observed.extend(receiver_letters.drain())
        assert any(
            letter.reason is DeadLetterReason.DESERIALIZATION_REJECTED
            for letter in receiver_observed
        )
        assert all(letter.message is None for letter in receiver_observed)

        receiver.terminate(receiver._root_ref)
        receiver.actor_stop_future(receiver._root_ref).result(timeout=1.0)

        remote.tell(OrderedMessage(1, 1))
        wait_until(
            lambda: (
                sender_observed.extend(sender_letters.drain())
                or any(
                    letter.reason is DeadLetterReason.ACTOR_NOT_FOUND
                    for letter in sender_observed
                )
            )
        )
        receiver_observed.extend(receiver_letters.drain())
        assert any(
            letter.reason is DeadLetterReason.ACTOR_NOT_FOUND
            for letter in receiver_observed
        )
        assert association.state.name == "ACTIVE"
        assert sender.remoting.metrics.accepted_delivery_attempts == 2
        assert sender.remoting.metrics.dead_letter_count == 2
        assert receiver.remoting.metrics.rejected_delivery_attempts == 2
        assert receiver.remoting.metrics.dead_letter_count == 2
        assert receiver.remoting.metrics.deserialization_rejections == 1
    finally:
        sender.stop()
        receiver.stop()


def test_no_association_reconnect_and_stale_incarnation() -> None:
    received = []
    delivered = Event()

    def receive(context: ActorContext, message: OrderedMessage):
        received.append(message)
        delivered.set()
        return Behaviors.same

    sender, receiver = start_pair(receive)
    letters = sender.dead_letters.subscribe()
    try:
        association = sender.remoting.associate("runtime-receiver")
        remote = sender.remoting.resolve(locator(receiver))
        association.close(1.0, detail="test reconnect")
        wait_until(lambda: not sender.remoting.associations)
        assert sender.remoting.association_history[-1].close_reason.name == "NORMAL_SHUTDOWN"

        with pytest.raises(NoAssociationError):
            remote.tell(OrderedMessage(1, 1))
        assert letters.drain()[-1].reason is DeadLetterReason.NO_ASSOCIATION

        reconnected = sender.remoting.associate("runtime-receiver")
        assert reconnected.snapshot().metrics.reconnect_count == 1
        remote.tell(OrderedMessage(1, 2))
        assert delivered.wait(1.0)
        assert received == [OrderedMessage(1, 2)]

        receiver_endpoint = receiver.remoting.endpoint
        receiver.stop()
        wait_until(lambda: not sender.remoting.associations)
        receiver = ActorSystem.create(
            Behaviors.receive(lambda context, message: Behaviors.same),
            "runtime-receiver",
            remoting=remoting(
                receiver_endpoint,
                "runtime-sender",
                sender.remoting.endpoint,
            ),
        )
        sender.remoting.associate("runtime-receiver")
        with pytest.raises(StaleIncarnationError):
            remote.tell(OrderedMessage(1, 3))
        assert letters.drain()[-1].reason is DeadLetterReason.STALE_INCARNATION
    finally:
        sender.stop()
        receiver.stop()


def test_repeated_reassociation_preserves_identity_order_and_bounded_cleanup() -> None:
    before = {thread.ident for thread in threading.enumerate()}
    received = []

    def receive(context: ActorContext, message: OrderedMessage):
        received.append(message)
        return Behaviors.same

    sender, receiver = start_pair(receive)
    remote = None
    association_uids = []
    try:
        for sequence in range(12):
            association = sender.remoting.associate("runtime-receiver")
            association_uids.append(association.association_uid)
            if remote is None:
                remote = sender.remoting.resolve(locator(receiver))
            remote.tell(OrderedMessage(1, sequence))
            wait_until(lambda: len(received) == sequence + 1)
            association.close(1.0, detail="lifecycle soak reconnect")
            wait_until(lambda: not sender.remoting.associations)
            wait_until(lambda: not receiver.remoting.associations)

        assert len(set(association_uids)) == 12
        assert received == [OrderedMessage(1, sequence) for sequence in range(12)]
        assert sender.remoting.metrics.accepted_delivery_attempts == 12
        assert sender.remoting.metrics.reconnect_count == 11
        assert sender.remoting.is_healthy
        assert receiver.remoting.is_healthy
    finally:
        sender.stop()
        receiver.stop()

    wait_until(
        lambda: not any(
            thread.ident not in before
            and thread.is_alive()
            and thread.name.startswith(("movie-remoting-", "movie-asyncio-"))
            for thread in threading.enumerate()
        )
    )


def test_incomplete_inbound_handshake_releases_pending_capacity() -> None:
    limited_endpoint = endpoint()
    peer_endpoint = endpoint()
    limited = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "pending-limited",
        remoting=RemotingConfig(
            limited_endpoint,
            {"pending-peer": peer_endpoint},
            registry(),
            association_timeout=0.2,
            pending_association_limit=1,
        ),
    )
    peer = None
    partial = socket.create_connection(
        (limited.remoting.endpoint.host, limited.remoting.endpoint.port),
        timeout=1.0,
    )
    try:
        partial.sendall(
            encode_preamble(
                StreamPreamble(StreamKind.MULTIPLEXED, uuid4(), CONTROL_LANE_ID)
            )
        )
        wait_until(
            lambda: any(
                snapshot.peer_system_name is None
                for snapshot in limited.remoting.associations
            )
        )
        peer = ActorSystem.create(
            Behaviors.receive(lambda context, message: Behaviors.same),
            "pending-peer",
            remoting=RemotingConfig(
                peer_endpoint,
                {"pending-limited": limited_endpoint},
                registry(),
                association_timeout=0.5,
                pending_association_limit=1,
            ),
        )

        with pytest.raises(HandshakeError):
            peer.remoting.associate("pending-limited")

        wait_until(lambda: not limited.remoting.associations)
        wait_until(lambda: not peer.remoting.associations)
        association = peer.remoting.associate("pending-limited")

        assert association.state is AssociationState.ACTIVE
        assert limited.remoting.is_healthy
    finally:
        partial.close()
        if peer is not None:
            peer.stop()
        limited.stop()


def test_resolve_rejects_a_non_allowlisted_locator_endpoint() -> None:
    sender, receiver = start_pair(lambda context, message: Behaviors.same)
    try:
        sender.remoting.associate("runtime-receiver")
        wrong = endpoint()
        with pytest.raises(ResolutionError, match="allowlist"):
            sender.remoting.resolve(
                f"movie://runtime-receiver@{wrong.host}:{wrong.port}/runtime-receiver"
            )
    finally:
        sender.stop()
        receiver.stop()


def test_effective_outbound_capacity_rejects_without_advancing_the_lane(
    monkeypatch,
) -> None:
    received = []
    completed = Event()

    def receive(context: ActorContext, message: OrderedMessage):
        received.append(message)
        completed.set()
        return Behaviors.same

    sender, receiver = start_pair(receive)
    try:
        association = sender.remoting.associate("runtime-receiver")
        remote = sender.remoting.resolve(locator(receiver))
        connection = association._connection
        original_send = connection.send_active

        def reject_send(*_args) -> None:
            raise TransportCapacityError("outbound transport capacity is full")

        monkeypatch.setattr(
            connection,
            "send_active",
            reject_send,
        )
        with pytest.raises(RemotingCapacityError):
            remote.tell(OrderedMessage(1, 1))

        monkeypatch.setattr(connection, "send_active", original_send)
        remote.tell(OrderedMessage(1, 2))
        assert completed.wait(1.0)
        assert received == [OrderedMessage(1, 2)]
    finally:
        sender.stop()
        receiver.stop()


def test_mailbox_full_advisory_does_not_invoke_the_actor() -> None:
    sender_endpoint = endpoint()
    receiver_endpoint = endpoint()
    received = []
    sender = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "full-sender",
        remoting=remoting(sender_endpoint, "full-receiver", receiver_endpoint),
    )
    receiver = ActorSystem.create(
        Behaviors.receive(
            lambda context, message: (received.append(message), Behaviors.same)[1]
        ),
        "full-receiver",
        config=Config(
            {
                "movie": {
                    "mailbox": {"default": {"capacity": 1}},
                    "dispatcher": {
                        "default-dispatcher": {
                            "type": "movie.dispatch.worker_pool.WorkerPoolDispatcherImpl",
                            "workers": 1,
                        }
                    },
                }
            }
        ),
        remoting=remoting(receiver_endpoint, "full-sender", sender_endpoint),
    )
    entered = Event()
    release = Event()
    receiver._dispatchers.default_dispatcher.dispatch(
        lambda: (entered.set(), release.wait(3.0))
    )
    assert entered.wait(1.0)
    letters = sender.dead_letters.subscribe()
    observed = []
    try:
        sender.remoting.associate("full-receiver")
        remote = sender.remoting.resolve(locator(receiver))
        remote.tell(OrderedMessage(1, 1))
        remote.tell(OrderedMessage(1, 2))

        wait_until(
            lambda: (
                observed.extend(letters.drain())
                or any(
                    letter.reason is DeadLetterReason.MAILBOX_FULL
                    for letter in observed
                )
            )
        )
        release.set()
        wait_until(lambda: len(received) == 1)
        assert received == [OrderedMessage(1, 1)]
    finally:
        release.set()
        sender.stop()
        receiver.stop()


def test_stopping_recipient_returns_an_advisory() -> None:
    sender_endpoint = endpoint()
    receiver_endpoint = endpoint()
    child_entered = Event()
    child_release = Event()
    child_ref = []

    def block_child(context: ActorContext, message: OrderedMessage):
        child_entered.set()
        child_release.wait(3.0)
        return Behaviors.same

    def parent(context: ActorContext) -> AbstractBehavior:
        child_ref.append(context.spawn(Behaviors.receive(block_child), "blocking-child"))
        return Behaviors.receive(lambda child_context, message: Behaviors.same)

    sender = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "stopping-sender",
        remoting=remoting(sender_endpoint, "stopping-receiver", receiver_endpoint),
    )
    receiver = ActorSystem.create(
        Behaviors.setup(parent),
        "stopping-receiver",
        remoting=remoting(receiver_endpoint, "stopping-sender", sender_endpoint),
    )
    letters = sender.dead_letters.subscribe()
    observed = []
    try:
        sender.remoting.associate("stopping-receiver")
        remote = sender.remoting.resolve(locator(receiver))
        child_ref[0].tell(OrderedMessage(1, 0))
        assert child_entered.wait(1.0)
        receiver.terminate(receiver._root_ref)
        context = receiver.get_context(receiver._root_ref)
        wait_until(lambda: context.state.name == "STOPPING")

        remote.tell(OrderedMessage(1, 1))
        wait_until(
            lambda: (
                observed.extend(letters.drain())
                or any(
                    letter.reason is DeadLetterReason.ACTOR_STOPPING
                    for letter in observed
                )
            )
        )
    finally:
        child_release.set()
        sender.stop()
        receiver.stop()


def test_port_zero_exposes_and_advertises_the_bound_endpoint() -> None:
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "port-zero-system",
        remoting=RemotingConfig(Endpoint("127.0.0.1", 0), {}, registry()),
    )
    try:
        assert system.remoting.endpoint.host == "127.0.0.1"
        assert system.remoting.endpoint.port > 0
    finally:
        system.stop()


def test_shutdown_leaves_no_remoting_or_tcp_threads() -> None:
    before = {thread.ident for thread in threading.enumerate()}
    sender, receiver = start_pair(lambda context, message: Behaviors.same)
    sender.remoting.associate("runtime-receiver")

    sender.stop()
    receiver.stop()
    wait_until(
        lambda: not any(
            thread.ident not in before
            and thread.is_alive()
            and thread.name.startswith(
                ("movie-remoting-", "movie-tcp-", "movie-asyncio-")
            )
            for thread in threading.enumerate()
        )
    )


def test_protocol_rejection_goaway_reaches_the_peer_before_close() -> None:
    sender, receiver = start_pair(lambda context, message: Behaviors.same)
    try:
        association = sender.remoting.associate("runtime-receiver")
        unknown_frame = struct.pack(">IBBHQ", 12, 0xFF, 0, 1, 0)

        association._connection._socket.sendall(unknown_frame)

        wait_until(
            lambda: sender.remoting.association_history
            and receiver.remoting.association_history
        )
        assert (
            sender.remoting.association_history[-1].close_reason
            is ReasonCode.UNSUPPORTED_FRAME
        )
        assert (
            receiver.remoting.association_history[-1].close_reason
            is ReasonCode.UNSUPPORTED_FRAME
        )
    finally:
        sender.stop()
        receiver.stop()


def test_unidentified_incoming_handshake_does_not_block_outbound_association() -> None:
    sender, receiver = start_pair(lambda context, message: Behaviors.same)
    raw = socket.create_connection(
        (sender.remoting.endpoint.host, sender.remoting.endpoint.port),
        timeout=1.0,
    )
    try:
        raw.sendall(
            encode_preamble(
                StreamPreamble(StreamKind.MULTIPLEXED, uuid4(), CONTROL_LANE_ID)
            )
        )
        wait_until(
            lambda: any(
                snapshot.peer_system_name is None
                for snapshot in sender.remoting.associations
            )
        )

        started = time.monotonic()
        association = sender.remoting.associate("runtime-receiver", timeout=1.0)

        assert association.state.name == "ACTIVE"
        assert time.monotonic() - started < 0.75
    finally:
        raw.close()
        sender.stop()
        receiver.stop()


class _TestListener:
    def __init__(self, endpoint: Endpoint, *, fail_first_close: bool = False) -> None:
        self.endpoint = endpoint
        self.fail_first_close = fail_first_close
        self.close_calls = 0
        self.closed = Event()

    def close(self, timeout=None) -> None:
        self.close_calls += 1
        if self.fail_first_close and self.close_calls == 1:
            raise TimeoutError("deliberate listener close timeout")
        self.closed.set()


class _HandoffOnCloseListener(_TestListener):
    def __init__(self, bound_endpoint: Endpoint, connection) -> None:
        super().__init__(bound_endpoint)
        self.connection = connection
        self.on_connection = None

    def close(self, timeout=None) -> None:
        self.close_calls += 1
        if self.on_connection is not None:
            on_connection = self.on_connection
            self.on_connection = None
            on_connection(self.connection)
        self.closed.set()


class _TestTransport:
    def __init__(self, listener: _TestListener, *, block_listen: bool = False) -> None:
        self.listener = listener
        self.block_listen = block_listen
        self.listen_entered = Event()
        self.release_listen = Event()

    def listen(self, endpoint, limits, on_connection):
        self.listen_entered.set()
        if self.block_listen:
            self.release_listen.wait(2.0)
        return self.listener

    def connect(self, endpoint, limits, association_uid, timeout=None):
        raise AssertionError("test transport does not connect")

    def close(self, timeout=None) -> None:
        pass


class _HandoffOnCloseTransport(_TestTransport):
    def listen(self, endpoint, limits, on_connection):
        self.listener.on_connection = on_connection
        return super().listen(endpoint, limits, on_connection)


class _UnhashableTestTransport(_TestTransport):
    __hash__ = None


class _NonWeakReferenceTransport:
    __slots__ = ()

    def listen(self, endpoint, limits, on_connection):
        raise AssertionError("non-weak-referenceable transport must be rejected")

    def connect(self, endpoint, limits, association_uid, timeout=None):
        raise AssertionError("non-weak-referenceable transport must be rejected")

    def close(self, timeout=None) -> None:
        raise AssertionError("non-weak-referenceable transport must be rejected")


class _EventuallyClosingConnection:
    def __init__(self) -> None:
        self.allow_close = Event()
        self._lock = Lock()
        self._close_calls = 0
        self.association_uid = uuid4()

    @property
    def close_calls(self) -> int:
        with self._lock:
            return self._close_calls

    def close(self, timeout=None) -> None:
        with self._lock:
            self._close_calls += 1
        if not self.allow_close.is_set():
            raise TimeoutError("deliberate connection close timeout")

    def snapshot(self) -> TransportConnectionSnapshot:
        return TransportConnectionSnapshot(
            ConnectionState.CLOSED if self.allow_close.is_set() else ConnectionState.OPEN,
            None,
            1024,
            0,
            0,
            0,
            0,
        )


class _TimeoutBlockingConnection(_EventuallyClosingConnection):
    def close(self, timeout=None) -> None:
        with self._lock:
            self._close_calls += 1
        if self.allow_close.is_set():
            return
        time.sleep(timeout if timeout is not None else 1.0)
        raise TimeoutError("deliberate connection close timeout")


class _SecondAttemptClosingConnection(_EventuallyClosingConnection):
    def close(self, timeout=None) -> None:
        with self._lock:
            self._close_calls += 1
            close_calls = self._close_calls
        if close_calls == 1:
            raise TimeoutError("deliberate first connection close timeout")
        self.allow_close.set()


class _ConnectingTestTransport(_TestTransport):
    def __init__(self, listener: _TestListener, connection) -> None:
        super().__init__(listener)
        self.connection = connection

    def connect(self, endpoint, limits, association_uid, timeout=None):
        self.connection.association_uid = association_uid
        return self.connection


class _BlockingConnectTransport(_ConnectingTestTransport):
    def __init__(self, listener: _TestListener, connection) -> None:
        super().__init__(listener, connection)
        self.connect_entered = Event()
        self.release_connect = Event()

    def connect(self, endpoint, limits, association_uid, timeout=None):
        self.connect_entered.set()
        self.release_connect.wait(1.0)
        return super().connect(endpoint, limits, association_uid, timeout)


def test_transport_object_belongs_to_one_actor_system_incarnation() -> None:
    listener = _TestListener(endpoint())
    transport = _UnhashableTestTransport(listener)

    def create_system(name: str):
        return ActorSystem.create(
            Behaviors.receive(lambda context, message: Behaviors.same),
            name,
            remoting=RemotingConfig(
                listener.endpoint,
                {},
                registry(),
                transport=transport,
            ),
        )

    first = create_system("transport-owner-first")
    second = None
    try:
        with pytest.raises(ValueError, match="one actor-system incarnation"):
            second = create_system("transport-owner-second")
    finally:
        if second is not None:
            second.stop()
        first.stop()

    third = None
    try:
        with pytest.raises(ValueError, match="one actor-system incarnation"):
            third = create_system("transport-owner-third")
    finally:
        if third is not None:
            third.stop()


def test_transport_object_must_support_weak_references() -> None:
    with pytest.raises(ValueError, match="support weak references"):
        ActorSystem.create(
            Behaviors.receive(lambda context, message: Behaviors.same),
            "non-weak-reference-transport",
            remoting=RemotingConfig(
                endpoint(),
                {},
                registry(),
                transport=_NonWeakReferenceTransport(),
            ),
        )


def test_concurrent_associate_reports_the_retained_listener_failure() -> None:
    listener = _TestListener(endpoint())
    connection = _EventuallyClosingConnection()
    transport = _BlockingConnectTransport(listener, connection)
    peer = endpoint()
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "listener-failure-during-connect",
        remoting=RemotingConfig(
            listener.endpoint,
            {"peer": peer},
            registry(),
            transport=transport,
        ),
    )
    errors = []
    associator = Thread(
        target=lambda: _capture_error(
            lambda: system.remoting.associate("peer"),
            errors,
        )
    )
    failure = TransportListenError("deliberate concurrent listener failure")
    try:
        associator.start()
        assert transport.connect_entered.wait(1.0)
        system.remoting._listener_failed(failure)
        transport.release_connect.set()
        associator.join(1.0)

        assert not associator.is_alive()
        assert len(errors) == 1
        assert isinstance(errors[0], RemotingShutdownError)
        assert errors[0].__cause__ is failure
    finally:
        transport.release_connect.set()
        associator.join(1.0)
        connection.allow_close.set()
        system.stop()


def test_shutdown_waits_for_concurrent_remoting_startup_to_settle() -> None:
    listener = _TestListener(endpoint())
    transport = _TestTransport(listener, block_listen=True)
    system = ActorSystemImpl(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "startup-shutdown-race",
        remoting=RemotingConfig(
            listener.endpoint,
            {},
            registry(),
            transport=transport,
            association_timeout=1.0,
        ),
    )
    start_errors = []
    stop_errors = []
    starter = Thread(target=lambda: _capture_error(system.start, start_errors))
    stopper = Thread(target=lambda: _capture_error(lambda: system.stop(2.0), stop_errors))

    starter.start()
    assert transport.listen_entered.wait(1.0)
    stopper.start()
    time.sleep(0.02)
    assert stopper.is_alive()
    transport.release_listen.set()
    starter.join(2.0)
    stopper.join(2.0)

    assert not starter.is_alive()
    assert not stopper.is_alive()
    assert stop_errors == []
    assert len(start_errors) == 1
    assert listener.closed.is_set()
    assert system.actor_count == 0


def test_shutdown_retries_a_listener_that_did_not_close() -> None:
    listener = _TestListener(endpoint(), fail_first_close=True)
    transport = _TestTransport(listener)
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "listener-close-retry",
        remoting=RemotingConfig(
            listener.endpoint,
            {},
            registry(),
            transport=transport,
            association_timeout=1.0,
        ),
    )

    with pytest.raises(TimeoutError, match="listener close"):
        system.stop(1.0)
    assert system.actor_count == 0
    system.stop(1.0)

    assert listener.close_calls == 2
    assert listener.closed.is_set()
    assert system.actor_count == 0


def test_shutdown_drains_a_connection_handed_off_while_listener_stops() -> None:
    connection = _SecondAttemptClosingConnection()
    listener = _HandoffOnCloseListener(endpoint(), connection)
    transport = _HandoffOnCloseTransport(listener)
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "listener-shutdown-handoff",
        remoting=RemotingConfig(
            listener.endpoint,
            {},
            registry(),
            transport=transport,
        ),
    )

    system.stop(1.0)

    assert connection.close_calls == 2
    assert connection.snapshot().state is ConnectionState.CLOSED
    assert not system.remoting._closing_connections


def test_timed_out_connection_closes_are_retried_with_bounded_ownership() -> None:
    listener = _TestListener(endpoint())
    transport = _TestTransport(listener)
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "connection-close-retry",
        remoting=RemotingConfig(
            listener.endpoint,
            {},
            registry(),
            transport=transport,
            association_timeout=0.1,
            pending_association_limit=1,
        ),
    )
    first = _EventuallyClosingConnection()
    second = _EventuallyClosingConnection()
    try:
        system.remoting._close_connection_quickly(first)
        with pytest.raises(TimeoutError, match="capacity"):
            system.remoting._close_connection_quickly(second)
        assert len(system.remoting._closing_connections) == 1

        first.allow_close.set()
        wait_until(lambda: not system.remoting._closing_connections)
    finally:
        first.allow_close.set()
        second.allow_close.set()
        system.stop()

    assert first.close_calls >= 2


def test_shutdown_stops_background_connection_close_retries_at_deadline() -> None:
    listener = _TestListener(endpoint())
    transport = _TestTransport(listener)
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "connection-close-deadline",
        remoting=RemotingConfig(
            listener.endpoint,
            {},
            registry(),
            transport=transport,
            association_timeout=0.02,
        ),
    )
    connection = _EventuallyClosingConnection()
    try:
        system.remoting._close_connection_quickly(connection)
        wait_until(lambda: connection.close_calls >= 2)
        time.sleep(0.05)
        attempts_after_cleanup_deadline = connection.close_calls
        time.sleep(0.1)
        assert connection.close_calls == attempts_after_cleanup_deadline

        shutdown_deadline = time.monotonic() + 0.1
        with pytest.raises(TimeoutError):
            system.stop(0.1)
        time.sleep(max(0.0, shutdown_deadline - time.monotonic()) + 0.02)
        attempts_at_deadline = connection.close_calls
        time.sleep(0.1)

        assert connection.close_calls == attempts_at_deadline
    finally:
        connection.allow_close.set()
        system.stop(1.0)


def test_shutdown_bounds_an_in_progress_background_close_attempt() -> None:
    listener = _TestListener(endpoint())
    transport = _TestTransport(listener)
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "connection-close-in-progress",
        remoting=RemotingConfig(
            listener.endpoint,
            {},
            registry(),
            transport=transport,
            association_timeout=0.2,
        ),
    )
    connection = _TimeoutBlockingConnection()
    try:
        system.remoting._close_connection_quickly(connection)
        wait_until(lambda: connection.close_calls >= 2)

        started = time.monotonic()
        with pytest.raises(TimeoutError):
            system.stop(0.02)
        assert time.monotonic() - started < 0.1

        time.sleep(0.1)
        attempts_after_deadline = connection.close_calls
        time.sleep(0.1)
        assert connection.close_calls == attempts_after_deadline
    finally:
        connection.allow_close.set()
        system.stop(1.0)


def test_shutdown_stops_association_close_retries_at_deadline(monkeypatch) -> None:
    listener = _TestListener(endpoint())
    connection = _EventuallyClosingConnection()
    transport = _ConnectingTestTransport(listener, connection)
    peer = endpoint()
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "association-close-deadline",
        remoting=RemotingConfig(
            listener.endpoint,
            {"peer": peer},
            registry(),
            transport=transport,
            association_timeout=0.02,
        ),
    )

    def fail_handshake(_association) -> None:
        raise TimeoutError("deliberate handshake failure")

    monkeypatch.setattr(Association, "_handshake", fail_handshake)
    try:
        with pytest.raises(HandshakeError):
            system.remoting.associate("peer")
        wait_until(lambda: connection.close_calls >= 1)
        wait_until(lambda: not system.remoting.associations)
        assert system.remoting.association_history[-1].state is AssociationState.CLOSED
        assert len(system.remoting._closing_connections) == 1
        time.sleep(0.05)
        attempts_after_association_deadline = connection.close_calls
        time.sleep(0.1)
        assert connection.close_calls == attempts_after_association_deadline

        shutdown_deadline = time.monotonic() + 0.1
        with pytest.raises(TimeoutError):
            system.stop(0.1)
        time.sleep(max(0.0, shutdown_deadline - time.monotonic()) + 0.02)
        attempts_at_deadline = connection.close_calls
        time.sleep(0.1)

        assert connection.close_calls == attempts_at_deadline
    finally:
        connection.allow_close.set()
        system.stop(1.0)


def test_outbound_start_failure_closes_the_connected_transport(monkeypatch) -> None:
    listener = _TestListener(endpoint())
    connection = _EventuallyClosingConnection()
    connection.allow_close.set()
    transport = _ConnectingTestTransport(listener, connection)
    peer = endpoint()
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "outbound-start-failure",
        remoting=RemotingConfig(
            listener.endpoint,
            {"peer": peer},
            registry(),
            transport=transport,
            association_timeout=0.1,
        ),
    )

    def fail_start(self) -> None:
        raise RuntimeError("injected association start failure")

    monkeypatch.setattr(Association, "start", fail_start)
    try:
        with pytest.raises(RuntimeError, match="injected association start failure"):
            system.remoting.associate("peer")
        wait_until(lambda: connection.close_calls >= 1)
        assert not system.remoting._associations
    finally:
        system.stop()


def _capture_error(operation, errors) -> None:
    try:
        operation()
    except BaseException as error:
        errors.append(error)
