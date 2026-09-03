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
    Endpoint,
    NoAssociationError,
    ReasonCode,
    RemotingCapacityError,
    RemotingConfig,
    ResolutionError,
    SerializerDescriptor,
    SerializerRegistryBuilder,
    StaleIncarnationError,
    StreamKind,
    StreamPreamble,
    TransportCapacityError,
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
    *,
    transport_backend: str = "tcp",
) -> RemotingConfig:
    return RemotingConfig(
        local,
        {peer_name: peer},
        registry(),
        transport_backend=transport_backend,
        lane_count=4,
        association_timeout=2.0,
    )


def wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("condition was not met before the deadline")
        time.sleep(0.005)


def start_pair(receive, *, transport_backend: str = "tcp"):
    sender_endpoint = endpoint()
    receiver_endpoint = endpoint()
    sender = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "runtime-sender",
        remoting=remoting(
            sender_endpoint,
            "runtime-receiver",
            receiver_endpoint,
            transport_backend=transport_backend,
        ),
    )
    receiver = ActorSystem.create(
        Behaviors.receive(receive),
        "runtime-receiver",
        remoting=remoting(
            receiver_endpoint,
            "runtime-sender",
            sender_endpoint,
            transport_backend=transport_backend,
        ),
    )
    return sender, receiver


def locator(system) -> str:
    endpoint = system.remoting.endpoint
    return f"movie://{system.name}@{endpoint.host}:{endpoint.port}/{system.name}"


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
    finally:
        sender.stop()
        receiver.stop()


def test_asyncio_transport_backend_integrates_with_remoting_runtime() -> None:
    received = []
    completed = Event()

    def receive(context: ActorContext, message: OrderedMessage):
        received.append(message)
        if len(received) == 100:
            completed.set()
        return Behaviors.same

    sender, receiver = start_pair(receive, transport_backend="asyncio")
    try:
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
    finally:
        sender.stop()
        receiver.stop()


def test_no_association_reconnect_and_stale_incarnation() -> None:
    sender, receiver = start_pair(lambda context, message: Behaviors.same)
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
            and thread.name.startswith(("movie-remoting-", "movie-tcp-"))
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


class _EventuallyClosingConnection:
    def __init__(self) -> None:
        self.allow_close = Event()
        self.close_calls = 0
        self.association_uid = uuid4()

    def close(self, timeout=None) -> None:
        self.close_calls += 1
        if not self.allow_close.is_set():
            raise TimeoutError("deliberate connection close timeout")


class _ConnectingTestTransport(_TestTransport):
    def __init__(self, listener: _TestListener, connection) -> None:
        super().__init__(listener)
        self.connection = connection

    def connect(self, endpoint, limits, association_uid, timeout=None):
        self.connection.association_uid = association_uid
        return self.connection


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
