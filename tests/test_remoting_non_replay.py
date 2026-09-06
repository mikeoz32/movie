import socket
from collections.abc import Callable
from dataclasses import dataclass
from queue import Empty, Queue
from threading import Event, Lock
from uuid import UUID

import pytest

from movie.actor import ActorSystem, Behaviors
from movie.remoting import (
    Endpoint,
    FrameType,
    RemotingConfig,
    SerializerDescriptor,
    SerializerRegistryBuilder,
    TcpTransport,
    TransportConnection,
    TransportConnectionSnapshot,
    TransportLimits,
    TransportListener,
    TransportRecord,
    UserMessage,
    decode_common_header,
    decode_frame,
)


@dataclass(frozen=True, slots=True)
class _Message:
    value: str


class _MessageSerializer:
    def serialize(self, value: object, manifest: str, protocol_minor: int) -> bytes:
        if not isinstance(value, _Message) or manifest != "non-replay/v1":
            raise ValueError("unsupported message")
        return value.value.encode("ascii")

    def deserialize(self, payload: bytes, manifest: str, protocol_minor: int) -> object:
        if manifest != "non-replay/v1":
            raise ValueError("unsupported manifest")
        return _Message(payload.decode("ascii"))


def _decode_user_message(record: TransportRecord) -> UserMessage | None:
    if decode_common_header(record.payload).frame_type is not FrameType.USER_MESSAGE:
        return None
    frame = decode_frame(record.payload, stream_kind=record.channel.kind)
    assert isinstance(frame, UserMessage)
    return frame


class _HoldingConnection:
    def __init__(
        self,
        connection: TransportConnection,
        transport: "_HoldingTcpTransport",
        *,
        first: bool,
    ) -> None:
        self._connection = connection
        self._transport = transport
        self._first = first
        self._held: Queue[TransportRecord] = Queue(maxsize=1)
        self._lock = Lock()
        self._closed = False

    @property
    def association_uid(self) -> UUID:
        return self._connection.association_uid

    def send(self, record: TransportRecord) -> None:
        self._connection.send(record)

    def send_prevalidated(
        self,
        record: TransportRecord,
        message_limit: int,
        byte_limit: int,
    ) -> None:
        if not self._intercept_user_message(record):
            self._connection.send_prevalidated(record, message_limit, byte_limit)

    def send_active(self, record: TransportRecord) -> None:
        if not self._intercept_user_message(record):
            self._connection.send_active(record)

    def _intercept_user_message(self, record: TransportRecord) -> bool:
        frame = _decode_user_message(record)
        if frame is None:
            return False
        with self._lock:
            if self._first and not self._closed and self._held.empty():
                self._held.put_nowait(record)
                self._transport.first_user_message_held.set()
                return True
        if not self._first:
            self._transport.successor_user_messages.put_nowait(frame)
        return False

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
        self._connection.activate(limits)

    def set_maximum_record_bytes(self, maximum_record_bytes: int) -> None:
        self._connection.set_maximum_record_bytes(maximum_record_bytes)

    def snapshot(self) -> TransportConnectionSnapshot:
        return self._connection.snapshot()

    def close(self, timeout: float | None = None) -> None:
        discarded = None
        with self._lock:
            if not self._closed:
                self._closed = True
                try:
                    discarded = self._held.get_nowait()
                except Empty:
                    pass
        if discarded is not None:
            frame = _decode_user_message(discarded)
            assert frame is not None
            self._transport.discarded_user_messages.put_nowait(frame)
            self._transport.first_user_message_discarded.set()
        self._connection.close(timeout)


class _HoldingTcpTransport:
    def __init__(self) -> None:
        self._backend = TcpTransport()
        self._lock = Lock()
        self._connection_count = 0
        self.first_user_message_held = Event()
        self.first_user_message_discarded = Event()
        self.discarded_user_messages: Queue[UserMessage] = Queue(maxsize=1)
        self.successor_user_messages: Queue[UserMessage] = Queue(maxsize=2)

    def listen(
        self,
        endpoint: Endpoint,
        limits: TransportLimits,
        on_connection: Callable[[TransportConnection], None],
    ) -> TransportListener:
        return self._backend.listen(endpoint, limits, on_connection)

    def connect(
        self,
        endpoint: Endpoint,
        limits: TransportLimits,
        association_uid: UUID,
        timeout: float | None = None,
    ) -> TransportConnection:
        connection = self._backend.connect(endpoint, limits, association_uid, timeout)
        with self._lock:
            first = self._connection_count == 0
            self._connection_count += 1
        return _HoldingConnection(connection, self, first=first)

    def close(self, timeout: float | None = None) -> None:
        self._backend.close(timeout)


def _endpoint() -> Endpoint:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return Endpoint("127.0.0.1", sock.getsockname()[1])
    finally:
        sock.close()


def _registry():
    descriptor = SerializerDescriptor(
        41,
        "non-replay",
        1,
        0,
        frozenset({"non-replay/v1"}),
        frozenset({"non-replay/v1"}),
    )
    return (
        SerializerRegistryBuilder()
        .register(descriptor, _MessageSerializer())
        .bind(_Message, 41, "non-replay/v1")
        .build()
    )


def test_locally_accepted_message_is_not_replayed_on_reassociation() -> None:
    sender_endpoint = _endpoint()
    receiver_endpoint = _endpoint()
    sender_transport = _HoldingTcpTransport()
    serializers = _registry()
    received: Queue[_Message] = Queue(maxsize=2)
    new_message_delivered = Event()

    def receive(_context, message: _Message):
        received.put_nowait(message)
        if message == _Message("new"):
            new_message_delivered.set()
        return Behaviors.same

    sender = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "non-replay-sender",
        remoting=RemotingConfig(
            sender_endpoint,
            {"non-replay-receiver": receiver_endpoint},
            serializers,
            transport=sender_transport,
            association_timeout=2.0,
        ),
    )
    receiver = ActorSystem.create(
        Behaviors.receive(receive),
        "non-replay-receiver",
        remoting=RemotingConfig(
            receiver_endpoint,
            {"non-replay-sender": sender_endpoint},
            serializers,
            transport=TcpTransport(),
            association_timeout=2.0,
        ),
    )
    try:
        first = sender.remoting.associate(receiver.name)
        remote = sender.remoting.resolve(
            f"movie://{receiver.name}@{receiver_endpoint.host}:"
            f"{receiver_endpoint.port}{receiver.path.remote_path}"
        )

        remote.tell(_Message("old"))
        assert sender_transport.first_user_message_held.wait(1.0)
        assert first.snapshot().metrics.accepted_delivery_attempts == 1
        with pytest.raises(Empty):
            received.get_nowait()

        first.close(2.0, detail="discard the first connection")
        assert sender_transport.first_user_message_discarded.wait(1.0)
        discarded = sender_transport.discarded_user_messages.get_nowait()
        assert discarded.association_uid == first.association_uid
        assert discarded.payload == b"old"

        successor = sender.remoting.associate(receiver.name)
        assert successor.association_uid != first.association_uid
        assert successor.peer_incarnation_uid == first.peer_incarnation_uid
        assert successor.peer_incarnation_uid == receiver.incarnation_uid

        remote.tell(_Message("new"))
        assert new_message_delivered.wait(2.0)
        assert received.get_nowait() == _Message("new")
        with pytest.raises(Empty):
            received.get_nowait()

        submitted = sender_transport.successor_user_messages.get_nowait()
        assert submitted.association_uid == successor.association_uid
        assert submitted.lane_sequence == 0
        assert submitted.payload == b"new"
        assert submitted != discarded
        with pytest.raises(Empty):
            sender_transport.successor_user_messages.get_nowait()
    finally:
        sender.stop()
        receiver.stop()
