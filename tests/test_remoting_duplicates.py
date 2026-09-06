import socket
from collections.abc import Callable
from threading import Event, Lock, Thread
from uuid import UUID

from movie.actor import ActorSystem, Behaviors
from movie.remoting import (
    AssociationState,
    Endpoint,
    FrameType,
    RemotingConfig,
    SerializerRegistryBuilder,
    TcpTransport,
    TransportConnection,
    TransportConnectionSnapshot,
    TransportLimits,
    TransportListener,
    TransportRecord,
    decode_common_header,
)


class _ObservedOutboundConnection:
    def __init__(
        self,
        connection: TransportConnection,
        gate_hello: Event,
        hello_held: Event,
        release_hello: Event,
    ) -> None:
        self._connection = connection
        self._gate_hello = gate_hello
        self._hello_held = hello_held
        self._release_hello = release_hello

    @property
    def association_uid(self) -> UUID:
        return self._connection.association_uid

    def send(self, record: TransportRecord) -> None:
        header = decode_common_header(record.payload)
        if self._gate_hello.is_set() and header.frame_type is FrameType.HELLO:
            self._hello_held.set()
            if not self._release_hello.wait(2.0):
                raise TimeoutError("test did not release the outbound HELLO")
        self._connection.send(record)

    def send_prevalidated(
        self,
        record: TransportRecord,
        message_limit: int,
        byte_limit: int,
    ) -> None:
        self._connection.send_prevalidated(record, message_limit, byte_limit)

    def send_active(self, record: TransportRecord) -> None:
        self._connection.send_active(record)

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
        self._connection.close(timeout)


class _GatedTcpTransport:
    def __init__(self) -> None:
        self._backend = TcpTransport()
        self._lock = Lock()
        self._outbound_association_uid: UUID | None = None
        self.gate_hello = Event()
        self.hello_held = Event()
        self.release_hello = Event()
        self.gate_handoff = Event()
        self.handoff_held = Event()
        self.release_handoff = Event()

    @property
    def outbound_association_uid(self) -> UUID | None:
        with self._lock:
            return self._outbound_association_uid

    def listen(
        self,
        endpoint: Endpoint,
        limits: TransportLimits,
        on_connection: Callable[[TransportConnection], None],
    ) -> TransportListener:
        def gated_handoff(connection: TransportConnection) -> None:
            if self.gate_handoff.is_set():
                self.handoff_held.set()
                if not self.release_handoff.wait(2.0):
                    raise TimeoutError("test did not release the connection handoff")
            on_connection(connection)

        return self._backend.listen(endpoint, limits, gated_handoff)

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
        return _ObservedOutboundConnection(
            connection,
            self.gate_hello,
            self.hello_held,
            self.release_hello,
        )

    def close(self, timeout: float | None = None) -> None:
        self._backend.close(timeout)


def _endpoint() -> Endpoint:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return Endpoint("127.0.0.1", sock.getsockname()[1])
    finally:
        sock.close()


def test_delayed_reciprocal_duplicate_returns_stable_canonical_association() -> None:
    first_endpoint = _endpoint()
    second_endpoint = _endpoint()
    first_transport = _GatedTcpTransport()
    second_transport = _GatedTcpTransport()
    serializers = SerializerRegistryBuilder().build()
    behavior = Behaviors.receive(lambda context, message: Behaviors.same)
    first = ActorSystem.create(
        behavior,
        "delayed-duplicate-first",
        remoting=RemotingConfig(
            first_endpoint,
            {"delayed-duplicate-second": second_endpoint},
            serializers,
            transport=first_transport,
            association_timeout=2.0,
        ),
    )
    second = ActorSystem.create(
        behavior,
        "delayed-duplicate-second",
        remoting=RemotingConfig(
            second_endpoint,
            {"delayed-duplicate-first": first_endpoint},
            serializers,
            transport=second_transport,
            association_timeout=2.0,
        ),
    )
    systems = (
        (first, first_transport, second.name),
        (second, second_transport, first.name),
    )
    lower, upper = sorted(systems, key=lambda item: item[0].incarnation_uid.bytes)
    lower_system, lower_transport, lower_peer_name = lower
    upper_system, upper_transport, upper_peer_name = upper
    lower_transport.gate_hello.set()

    result_lock = Lock()
    results = {}
    errors = []
    completed = {first.name: Event(), second.name: Event()}

    def associate(system, peer_name: str) -> None:
        try:
            association = system.remoting.associate(peer_name)
            with result_lock:
                results[system.name] = association
        except BaseException as error:
            with result_lock:
                errors.append(error)
        finally:
            completed[system.name].set()

    threads = (
        Thread(target=associate, args=(lower_system, lower_peer_name)),
        Thread(target=associate, args=(upper_system, upper_peer_name)),
    )
    try:
        upper_transport.gate_handoff.set()
        threads[0].start()
        try:
            assert lower_transport.hello_held.wait(1.0)
            assert upper_transport.handoff_held.wait(1.0)
            threads[1].start()
            assert not completed[lower_system.name].wait(0.1)
            assert not completed[upper_system.name].wait(0.1)
            lower_transport.release_hello.set()
            assert not completed[upper_system.name].wait(0.1)
        finally:
            lower_transport.release_hello.set()
            upper_transport.release_handoff.set()

        for thread in threads:
            thread.join(0.5)
        assert all(not thread.is_alive() for thread in threads)
        assert errors == []

        canonical_uid = lower_transport.outbound_association_uid
        assert canonical_uid is not None
        assert results[lower_system.name].association_uid == canonical_uid
        assert results[upper_system.name].association_uid == canonical_uid
        assert all(association.state is AssociationState.ACTIVE for association in results.values())
        for system in (first, second):
            active = [
                association
                for association in system.remoting.associations
                if association.state is AssociationState.ACTIVE
            ]
            assert len(active) == 1
            assert active[0].association_uid == canonical_uid
    finally:
        lower_transport.release_hello.set()
        upper_transport.release_handoff.set()
        for thread in threads:
            if thread.ident is not None:
                thread.join(1.0)
        first.stop()
        second.stop()
