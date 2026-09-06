import socket
import time
from collections.abc import Callable
from dataclasses import dataclass
from queue import Queue
from threading import Event, Lock, Thread
from uuid import UUID

from movie.actor import ActorSystem, Behaviors
from movie.remoting import (
    AssociationState,
    ConnectionState,
    Endpoint,
    FrameType,
    RemotingConfig,
    SerializerDescriptor,
    SerializerRegistryBuilder,
    TcpTransport,
    TransportClosedError,
    TransportConnection,
    TransportConnectionSnapshot,
    TransportLimits,
    TransportListener,
    TransportRecord,
    decode_common_header,
)


@dataclass(frozen=True, slots=True)
class _Message:
    value: str


class _MessageSerializer:
    def serialize(self, value: object, manifest: str, protocol_minor: int) -> bytes:
        if not isinstance(value, _Message) or manifest != "failover-metrics/v1":
            raise ValueError("unsupported message")
        return value.value.encode("ascii")

    def deserialize(self, payload: bytes, manifest: str, protocol_minor: int) -> object:
        if manifest != "failover-metrics/v1":
            raise ValueError("unsupported manifest")
        return _Message(payload.decode("ascii"))


class _ObservedConnection:
    def __init__(
        self,
        connection: TransportConnection,
        transport: "_GatedTcpTransport",
        *,
        outbound: bool,
    ) -> None:
        self._connection = connection
        self._transport = transport
        self._outbound = outbound

    @property
    def association_uid(self) -> UUID:
        return self._connection.association_uid

    def send(self, record: TransportRecord) -> None:
        if self._outbound:
            self._transport.hold_handshake(record)
        self._connection.send(record)

    def send_prevalidated(
        self,
        record: TransportRecord,
        message_limit: int,
        byte_limit: int,
    ) -> None:
        self._connection.send_prevalidated(record, message_limit, byte_limit)

    def send_active(self, record: TransportRecord) -> None:
        is_user_message = decode_common_header(record.payload).frame_type is FrameType.USER_MESSAGE
        if self._outbound and is_user_message:
            self._transport.hold_user_message()
        try:
            self._connection.send_active(record)
        except TransportClosedError:
            if self._outbound and is_user_message:
                self._transport.closed_user_send.set()
            raise
        else:
            if is_user_message:
                self._transport.record_user_admission()

    def send_terminal(self, record: TransportRecord) -> None:
        self._connection.send_terminal(record)

    def receive(self, timeout: float | None = None) -> TransportRecord:
        return self._connection.receive(timeout)

    def receive_many(
        self,
        max_records: int,
        timeout: float | None = None,
    ) -> list[TransportRecord]:
        return self._connection.receive_many(max_records, timeout)

    def activate(self, limits: TransportLimits) -> None:
        self._transport.hold_activation()
        self._connection.activate(limits)
        if not self._outbound:
            self._transport.inbound_activated.set()

    def set_maximum_record_bytes(self, maximum_record_bytes: int) -> None:
        self._connection.set_maximum_record_bytes(maximum_record_bytes)

    def snapshot(self) -> TransportConnectionSnapshot:
        return self._connection.snapshot()

    def close(self, timeout: float | None = None) -> None:
        self._connection.close(timeout)


class _GatedTcpTransport:
    def __init__(self) -> None:
        self._backend = TcpTransport()
        self._lock = Lock()
        self._outbound_association_uid: UUID | None = None
        self._outbound_connection: TransportConnection | None = None
        self._held_frame_types: set[FrameType] = set()
        self._user_message_held = False
        self._activation_held = False
        self._admitted_user_messages = 0
        self.gate_hello = Event()
        self.hello_held = Event()
        self.release_hello = Event()
        self.gate_accept = Event()
        self.accept_held = Event()
        self.release_accept = Event()
        self.gate_user = Event()
        self.user_held = Event()
        self.release_user = Event()
        self.gate_activation = Event()
        self.activation_held = Event()
        self.release_activation = Event()
        self.closed_user_send = Event()
        self.inbound_activated = Event()

    @property
    def outbound_association_uid(self) -> UUID | None:
        with self._lock:
            return self._outbound_association_uid

    @property
    def admitted_user_messages(self) -> int:
        with self._lock:
            return self._admitted_user_messages

    @property
    def outbound_connection_state(self) -> ConnectionState | None:
        with self._lock:
            connection = self._outbound_connection
        return connection.snapshot().state if connection is not None else None

    def listen(
        self,
        endpoint: Endpoint,
        limits: TransportLimits,
        on_connection: Callable[[TransportConnection], None],
    ) -> TransportListener:
        def observe(connection: TransportConnection) -> None:
            on_connection(_ObservedConnection(connection, self, outbound=False))

        return self._backend.listen(endpoint, limits, observe)

    def connect(
        self,
        endpoint: Endpoint,
        limits: TransportLimits,
        association_uid: UUID,
        timeout: float | None = None,
    ) -> TransportConnection:
        connection = self._backend.connect(endpoint, limits, association_uid, timeout)
        with self._lock:
            self._outbound_association_uid = association_uid
            self._outbound_connection = connection
        return _ObservedConnection(connection, self, outbound=True)

    def close(self, timeout: float | None = None) -> None:
        self._backend.close(timeout)

    def hold_handshake(self, record: TransportRecord) -> None:
        frame_type = decode_common_header(record.payload).frame_type
        gate, held, release = {
            FrameType.HELLO: (self.gate_hello, self.hello_held, self.release_hello),
            FrameType.HELLO_ACCEPT: (
                self.gate_accept,
                self.accept_held,
                self.release_accept,
            ),
        }.get(frame_type, (None, None, None))
        if gate is None or not gate.is_set():
            return
        with self._lock:
            if frame_type in self._held_frame_types:
                return
            self._held_frame_types.add(frame_type)
        held.set()
        if not release.wait(3.0):
            raise TimeoutError(f"test did not release {frame_type.name}")

    def hold_user_message(self) -> None:
        if not self.gate_user.is_set():
            return
        with self._lock:
            if self._user_message_held:
                return
            self._user_message_held = True
        self.user_held.set()
        if not self.release_user.wait(3.0):
            raise TimeoutError("test did not release the old USER_MESSAGE send")

    def record_user_admission(self) -> None:
        with self._lock:
            self._admitted_user_messages += 1

    def hold_activation(self) -> None:
        if not self.gate_activation.is_set():
            return
        with self._lock:
            if self._activation_held:
                return
            self._activation_held = True
        self.activation_held.set()
        if not self.release_activation.wait(3.0):
            raise TimeoutError("test did not release canonical transport activation")


def _endpoint() -> Endpoint:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return Endpoint("127.0.0.1", sock.getsockname()[1])
    finally:
        sock.close()


def _registry():
    descriptor = SerializerDescriptor(
        51,
        "failover-metrics",
        1,
        0,
        frozenset({"failover-metrics/v1"}),
        frozenset({"failover-metrics/v1"}),
    )
    return (
        SerializerRegistryBuilder()
        .register(descriptor, _MessageSerializer())
        .bind(_Message, 51, "failover-metrics/v1")
        .build()
    )


def _wait_until(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("condition was not met before the deadline")
        time.sleep(0.005)


def test_retry_on_canonical_replacement_counts_one_accepted_delivery_attempt() -> None:
    first_endpoint = _endpoint()
    second_endpoint = _endpoint()
    first_transport = _GatedTcpTransport()
    second_transport = _GatedTcpTransport()
    serializers = _registry()
    received: list[_Message] = []
    delivered = Event()

    def receive(_context, message: _Message):
        received.append(message)
        delivered.set()
        return Behaviors.same

    first = ActorSystem.create(
        Behaviors.receive(receive),
        "failover-metrics-first",
        remoting=RemotingConfig(
            first_endpoint,
            {"failover-metrics-second": second_endpoint},
            serializers,
            transport=first_transport,
            association_timeout=3.0,
        ),
    )
    second = ActorSystem.create(
        Behaviors.receive(receive),
        "failover-metrics-second",
        remoting=RemotingConfig(
            second_endpoint,
            {"failover-metrics-first": first_endpoint},
            serializers,
            transport=second_transport,
            association_timeout=3.0,
        ),
    )
    systems = (
        (first, first_transport, second.name),
        (second, second_transport, first.name),
    )
    lower, upper = sorted(systems, key=lambda item: item[0].incarnation_uid.bytes)
    lower_system, lower_transport, lower_peer_name = lower
    upper_system, upper_transport, upper_peer_name = upper
    upper_transport.gate_accept.set()
    lower_transport.gate_hello.set()
    errors: Queue[BaseException] = Queue()

    def associate(system, peer_name: str) -> None:
        try:
            system.remoting.associate(peer_name)
        except BaseException as error:
            errors.put_nowait(error)

    old_thread = Thread(target=associate, args=(upper_system, upper_peer_name))
    canonical_thread = Thread(target=associate, args=(lower_system, lower_peer_name))
    tell_errors: Queue[BaseException] = Queue()

    try:
        old_thread.start()
        assert upper_transport.accept_held.wait(1.0)

        canonical_thread.start()
        assert lower_transport.hello_held.wait(1.0)
        old_uid = upper_transport.outbound_association_uid
        canonical_uid = lower_transport.outbound_association_uid
        assert old_uid is not None
        assert canonical_uid is not None
        assert old_uid != canonical_uid

        upper_transport.release_accept.set()
        old_thread.join(2.0)
        assert not old_thread.is_alive()
        assert errors.empty()
        _wait_until(
            lambda: all(
                any(
                    snapshot.association_uid == old_uid
                    and snapshot.state is AssociationState.ACTIVE
                    for snapshot in system.remoting.associations
                )
                for system in (lower_system, upper_system)
            )
        )

        target = lower_system.remoting.endpoint
        remote = upper_system.remoting.resolve(
            f"movie://{lower_system.name}@{target.host}:{target.port}"
            f"{lower_system.path.remote_path}"
        )
        upper_transport.gate_activation.set()
        lower_transport.gate_activation.set()
        lower_transport.release_hello.set()
        assert upper_transport.activation_held.wait(2.0)
        assert lower_transport.activation_held.wait(2.0)
        upper_transport.gate_user.set()

        def tell() -> None:
            try:
                remote.tell(_Message("once"))
            except BaseException as error:
                tell_errors.put_nowait(error)

        tell_thread = Thread(target=tell)
        tell_thread.start()
        assert upper_transport.user_held.wait(1.0)

        upper_transport.release_activation.set()
        assert upper_transport.inbound_activated.wait(2.0)
        lower_transport.release_activation.set()
        _wait_until(
            lambda: any(
                snapshot.association_uid == canonical_uid
                and snapshot.state is AssociationState.ACTIVE
                for snapshot in lower_system.remoting.associations
            )
        )
        _wait_until(lambda: upper_transport.outbound_connection_state is ConnectionState.CLOSED)

        upper_transport.release_user.set()
        assert upper_transport.closed_user_send.wait(1.0)
        tell_thread.join(2.0)
        canonical_thread.join(2.0)
        assert not tell_thread.is_alive()
        assert not canonical_thread.is_alive()
        assert tell_errors.empty()
        assert errors.empty()
        assert delivered.wait(2.0)
        assert received == [_Message("once")]
        assert upper_transport.admitted_user_messages == 1

        metrics = upper_system.remoting.metrics
        assert metrics.accepted_delivery_attempts == 1
        assert metrics.rejected_delivery_attempts == 0
        assert metrics.reconnect_count == 0
    finally:
        upper_transport.release_accept.set()
        upper_transport.release_user.set()
        upper_transport.release_activation.set()
        lower_transport.release_hello.set()
        lower_transport.release_activation.set()
        for thread in (old_thread, canonical_thread):
            if thread.ident is not None:
                thread.join(1.0)
        first.stop()
        second.stop()
