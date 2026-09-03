import socket
import struct
import threading
import time
from dataclasses import FrozenInstanceError, replace
from queue import Queue
from uuid import UUID

import pytest

import movie.remoting.tcp as tcp_module
from movie.remoting import (
    COMMON_HEADER_SIZE,
    CONTROL_LANE_ID,
    ConnectionState,
    Endpoint,
    FrameType,
    GoAway,
    HelloAccept,
    LogicalChannel,
    ReasonCode,
    StreamKind,
    StreamPreamble,
    TcpConnection,
    TcpTransport,
    TransportCapacityError,
    TransportClosedError,
    TransportConnectError,
    TransportFlowControlError,
    TransportLimits,
    TransportProtocolError,
    TransportRecord,
    encode_common_header,
    encode_frame,
    encode_preamble,
)

ASSOCIATION_UID = UUID("00112233-4455-6677-8899-aabbccddeeff")
CONTROL = LogicalChannel(StreamKind.CONTROL, CONTROL_LANE_ID)
MULTIPLEXED = LogicalChannel(StreamKind.MULTIPLEXED, CONTROL_LANE_ID)
LANE_1 = LogicalChannel(StreamKind.DELIVERY_LANE, 1)
LANE_2 = LogicalChannel(StreamKind.DELIVERY_LANE, 2)


def limits(**changes) -> TransportLimits:
    defaults = TransportLimits(
        maximum_record_bytes=4096,
        outbound_message_limit=16,
        outbound_byte_limit=64 * 1024,
        inbound_message_limit=16,
        inbound_byte_limit=64 * 1024,
    )
    return replace(defaults, **changes)


def control_frame(detail: str) -> bytes:
    return encode_frame(GoAway(ReasonCode.PROTOCOL_VIOLATION, detail))


def hello_accept_frame(maximum_frame_bytes: int) -> bytes:
    return encode_frame(
        HelloAccept(
            ASSOCIATION_UID,
            0,
            maximum_frame_bytes,
            1,
            4,
            16 * 1024,
            4,
            16 * 1024,
        )
    )


def delivery_frame(marker: int) -> bytes:
    return encode_common_header(FrameType.USER_MESSAGE, 1, 0) + bytes((marker,))


def wait_until(predicate, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("condition was not met before the deadline")
        time.sleep(0.005)


def _capture_error(operation, errors: Queue) -> None:
    try:
        operation()
    except BaseException as error:
        errors.put(error)


def tcp_socket_pair() -> tuple[socket.socket, socket.socket]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    peer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        peer.connect(listener.getsockname())
        local, _ = listener.accept()
        return local, peer
    except BaseException:
        peer.close()
        raise
    finally:
        listener.close()


def recv_exact(sock: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            pytest.fail("socket closed before the expected bytes were received")
        data.extend(chunk)
    return bytes(data)


class ControlledSocket:
    """Socket test double that can hold the first writer call in progress."""

    def __init__(self) -> None:
        self.send_started = threading.Event()
        self.release_first_send = threading.Event()
        self.closed = threading.Event()
        self.sent: list[bytes] = []
        self._send_count = 0
        self._lock = threading.Lock()

    def setsockopt(self, level, option, value) -> None:
        pass

    def settimeout(self, timeout) -> None:
        pass

    def sendall(self, data: bytes) -> None:
        with self._lock:
            self.sent.append(data)
            self._send_count += 1
            send_count = self._send_count
        if send_count == 1:
            self.send_started.set()
            self.release_first_send.wait()

    def recv(self, size: int) -> bytes:
        if self.closed.wait(0.01):
            return b""
        raise TimeoutError

    def shutdown(self, how) -> None:
        self.closed.set()

    def close(self) -> None:
        self.closed.set()


def test_transport_values_are_immutable_and_validate_channel_forms() -> None:
    endpoint = Endpoint("127.0.0.1", 0)

    with pytest.raises(FrozenInstanceError):
        endpoint.port = 1
    with pytest.raises(TransportProtocolError):
        LogicalChannel(StreamKind.CONTROL, 1)
    with pytest.raises(TransportProtocolError):
        LogicalChannel(StreamKind.DELIVERY_LANE, CONTROL_LANE_ID)
    with pytest.raises(TransportProtocolError):
        TransportLimits(COMMON_HEADER_SIZE - 1, 1, 1, 1, 1)


def test_reader_restores_a_fragmented_record() -> None:
    local, peer = tcp_socket_pair()
    connection = TcpConnection(local, limits(), ASSOCIATION_UID)
    frame = control_frame("fragmented")
    try:
        for offset in range(0, len(frame), 3):
            peer.sendall(frame[offset : offset + 3])

        assert connection.receive(timeout=1.0) == TransportRecord(MULTIPLEXED, frame)
    finally:
        connection.close(timeout=1.0)
        peer.close()


def test_partial_header_bytes_are_accounted_and_cleared_on_close() -> None:
    local, peer = tcp_socket_pair()
    connection = TcpConnection(local, limits(), ASSOCIATION_UID)
    frame = control_frame("partial-header")
    try:
        peer.sendall(frame[:8])
        wait_until(lambda: connection.snapshot().pending_inbound_bytes == 8)

        snapshot = connection.snapshot()
        assert snapshot.pending_inbound_messages == 0
        assert connection._reading_header_bytes == 8

        connection.close(timeout=1.0)
        assert connection.snapshot().pending_inbound_bytes == 0
        assert connection._receive_buffer == bytearray()
    finally:
        connection.close(timeout=1.0)
        peer.close()


def test_reader_splits_multiple_concatenated_records() -> None:
    local, peer = tcp_socket_pair()
    connection = TcpConnection(local, limits(), ASSOCIATION_UID)
    frames = [control_frame("first"), control_frame("second"), control_frame("third")]
    try:
        peer.sendall(b"".join(frames))

        assert [connection.receive(timeout=1.0).payload for _ in frames] == frames
    finally:
        connection.close(timeout=1.0)
        peer.close()


def test_receive_many_drains_only_the_ready_bounded_prefix() -> None:
    local, peer = tcp_socket_pair()
    connection = TcpConnection(local, limits(), ASSOCIATION_UID)
    frames = [control_frame("first"), control_frame("second"), control_frame("third")]
    try:
        peer.sendall(b"".join(frames))
        wait_until(lambda: connection.snapshot().pending_inbound_messages == 3)

        assert [record.payload for record in connection.receive_many(2, timeout=1.0)] == (
            frames[:2]
        )
        snapshot = connection.snapshot()
        assert snapshot.pending_inbound_messages == 1
        assert snapshot.pending_inbound_bytes == len(frames[2])
        assert [record.payload for record in connection.receive_many(128, timeout=1.0)] == [
            frames[2]
        ]
    finally:
        connection.close(timeout=1.0)
        peer.close()


def test_receive_many_validates_its_bound() -> None:
    local, peer = tcp_socket_pair()
    connection = TcpConnection(local, limits(), ASSOCIATION_UID)
    try:
        with pytest.raises(TransportProtocolError, match="batch size"):
            connection.receive_many(0, timeout=0.0)
    finally:
        connection.close(timeout=1.0)
        peer.close()


@pytest.mark.parametrize("negotiated_maximum", [128, 4096])
def test_reader_holds_pipelined_active_frames_until_limits_activate(
    negotiated_maximum,
) -> None:
    local, peer = tcp_socket_pair()
    bootstrap = limits(maximum_record_bytes=1024)
    connection = TcpConnection(local, bootstrap, ASSOCIATION_UID)
    accept = hello_accept_frame(negotiated_maximum)
    active = control_frame("x" * 256)
    assert len(accept) < 128 < len(active) < 1024
    peer.sendall(accept + active)
    try:
        assert connection.receive(timeout=1.0).payload == accept
        with pytest.raises(TimeoutError):
            connection.receive(timeout=0.02)

        connection.activate(
            limits(
                maximum_record_bytes=negotiated_maximum,
                inbound_byte_limit=4096,
            )
        )
        if negotiated_maximum > len(active):
            assert connection.receive(timeout=1.0).payload == active
        else:
            with pytest.raises(TransportFlowControlError):
                connection.receive(timeout=1.0)
    finally:
        connection.close(timeout=1.0)
        peer.close()


def test_reader_enforces_bootstrap_maximum_before_allocating_a_body() -> None:
    local, peer = tcp_socket_pair()
    connection = TcpConnection(
        local,
        limits(maximum_record_bytes=COMMON_HEADER_SIZE),
        ASSOCIATION_UID,
    )
    boundary = encode_common_header(FrameType.GOAWAY, 0, 0)
    oversized_header = encode_common_header(FrameType.GOAWAY, 1, 0)
    try:
        peer.sendall(boundary + oversized_header)

        assert connection.receive(timeout=1.0) == TransportRecord(MULTIPLEXED, boundary)
        with pytest.raises(TransportFlowControlError) as failure:
            connection.receive(timeout=1.0)
        snapshot = connection.snapshot()
        assert snapshot.state is ConnectionState.READ_FAILED
        assert snapshot.cause is failure.value
    finally:
        connection.close(timeout=1.0)
        peer.close()


def test_reader_delivers_unknown_common_header_fields_to_the_wire_codec() -> None:
    local, peer = tcp_socket_pair()
    connection = TcpConnection(local, limits(), ASSOCIATION_UID)
    unknown_header = struct.pack(">IBBHQ", COMMON_HEADER_SIZE - 4, 0xFF, 0x80, 0xBEEF, 123)
    try:
        peer.sendall(unknown_header)

        assert connection.receive(timeout=1.0) == TransportRecord(
            MULTIPLEXED,
            unknown_header,
        )
        assert connection.snapshot().state is ConnectionState.OPEN
    finally:
        connection.close(timeout=1.0)
        peer.close()


@pytest.mark.parametrize("capacity", ["messages", "bytes"])
def test_outbound_admission_counts_the_record_blocked_in_sendall(capacity) -> None:
    sock = ControlledSocket()
    first = TransportRecord(CONTROL, control_frame("first"))
    second = TransportRecord(CONTROL, control_frame("second"))
    connection_limits = (
        limits(outbound_message_limit=1)
        if capacity == "messages"
        else limits(
            outbound_message_limit=2,
            outbound_byte_limit=len(first.payload) + len(second.payload) - 1,
        )
    )
    connection = TcpConnection(
        sock,
        connection_limits,
        ASSOCIATION_UID,
    )
    try:
        connection.send(first)
        assert sock.send_started.wait(1.0)

        snapshot = connection.snapshot()
        assert snapshot.pending_outbound_messages == 1
        assert snapshot.pending_outbound_bytes == len(first.payload)
        with pytest.raises(TransportCapacityError):
            connection.send(second)

        sock.release_first_send.set()
        wait_until(lambda: connection.snapshot().pending_outbound_messages == 0)
    finally:
        sock.release_first_send.set()
        connection.close(timeout=1.0)


@pytest.mark.parametrize("capacity", ["messages", "bytes"])
@pytest.mark.parametrize("configured_is_stricter", [False, True])
def test_prevalidated_admission_enforces_effective_and_configured_capacity(
    capacity,
    configured_is_stricter,
) -> None:
    sock = ControlledSocket()
    first = TransportRecord(CONTROL, control_frame("first"))
    second = TransportRecord(CONTROL, control_frame("second"))
    configured_message_limit = 1 if configured_is_stricter and capacity == "messages" else 2
    configured_byte_limit = (
        len(first.payload) + len(second.payload) - 1
        if configured_is_stricter and capacity == "bytes"
        else 64 * 1024
    )
    effective_message_limit = 1 if capacity == "messages" else 2
    effective_byte_limit = (
        len(first.payload) + len(second.payload) - 1
        if capacity == "bytes"
        else 64 * 1024
    )
    connection = TcpConnection(
        sock,
        limits(
            outbound_message_limit=configured_message_limit,
            outbound_byte_limit=configured_byte_limit,
        ),
        ASSOCIATION_UID,
    )
    try:
        connection.send_prevalidated(
            first,
            effective_message_limit,
            effective_byte_limit,
        )
        assert sock.send_started.wait(1.0)

        with pytest.raises(TransportCapacityError):
            connection.send_prevalidated(
                second,
                effective_message_limit,
                effective_byte_limit,
            )
    finally:
        sock.release_first_send.set()
        connection.close(timeout=1.0)


def test_prevalidated_admission_enforces_maximum_record_bytes() -> None:
    sock = ControlledSocket()
    record = TransportRecord(CONTROL, control_frame("too-large"))
    connection = TcpConnection(
        sock,
        limits(maximum_record_bytes=len(record.payload) - 1),
        ASSOCIATION_UID,
    )
    try:
        with pytest.raises(TransportProtocolError, match="exceeds maximum"):
            connection.send_prevalidated(record, 1, len(record.payload))
    finally:
        sock.release_first_send.set()
        connection.close(timeout=1.0)


@pytest.mark.parametrize(
    ("message_limit", "byte_limit"),
    [
        (float("nan"), 4096),
        (1, float("nan")),
        (-1, 4096),
        (1, -1),
        (True, 4096),
        (1, True),
    ],
)
def test_prevalidated_admission_rejects_invalid_effective_limits(
    message_limit,
    byte_limit,
) -> None:
    sock = ControlledSocket()
    connection = TcpConnection(sock, limits(), ASSOCIATION_UID)
    record = TransportRecord(CONTROL, control_frame("invalid-limits"))
    try:
        with pytest.raises(TransportProtocolError, match="nonnegative integer"):
            connection.send_prevalidated(record, message_limit, byte_limit)
    finally:
        sock.release_first_send.set()
        connection.close(timeout=1.0)


def test_terminal_control_record_has_one_reserved_outbound_slot() -> None:
    sock = ControlledSocket()
    connection = TcpConnection(sock, limits(), ASSOCIATION_UID)
    connection.activate(limits(outbound_message_limit=1, outbound_byte_limit=4096))
    ordinary = TransportRecord(CONTROL, control_frame("ordinary"))
    terminal = TransportRecord(
        CONTROL,
        encode_frame(GoAway(ReasonCode.NORMAL_SHUTDOWN)),
    )
    try:
        connection.send(ordinary)
        assert sock.send_started.wait(1.0)

        connection.send_terminal(terminal)
        with pytest.raises(TransportCapacityError, match="already in use"):
            connection.send_terminal(terminal)

        sock.release_first_send.set()
        wait_until(lambda: len(sock.sent) == 2)
        assert sock.sent == [ordinary.payload, terminal.payload]
    finally:
        sock.release_first_send.set()
        connection.close(timeout=1.0)


def test_terminal_record_follows_all_previously_admitted_channels() -> None:
    sock = ControlledSocket()
    connection = TcpConnection(sock, limits(), ASSOCIATION_UID)
    first = TransportRecord(CONTROL, control_frame("first"))
    lane_one = TransportRecord(LANE_1, delivery_frame(1))
    lane_two = TransportRecord(LANE_2, delivery_frame(2))
    terminal = TransportRecord(
        CONTROL,
        encode_frame(GoAway(ReasonCode.NORMAL_SHUTDOWN)),
    )
    try:
        connection.send(first)
        assert sock.send_started.wait(1.0)
        connection.send(lane_one)
        connection.send(lane_two)
        connection.send_terminal(terminal)

        with pytest.raises(TransportClosedError, match="terminal"):
            connection.send(TransportRecord(LANE_1, delivery_frame(3)))

        sock.release_first_send.set()
        wait_until(lambda: connection.snapshot().pending_outbound_messages == 0)
        assert b"".join(sock.sent) == b"".join(
            (first.payload, lane_one.payload, lane_two.payload, terminal.payload)
        )
    finally:
        sock.release_first_send.set()
        connection.close(timeout=1.0)


def test_terminal_reserve_survives_tightened_ordinary_limits() -> None:
    sock = ControlledSocket()
    connection = TcpConnection(sock, limits(), ASSOCIATION_UID)
    ordinary = TransportRecord(CONTROL, control_frame("ordinary-before-tightening"))
    terminal = TransportRecord(
        CONTROL,
        encode_frame(GoAway(ReasonCode.NORMAL_SHUTDOWN)),
    )
    try:
        connection.send(ordinary)
        assert sock.send_started.wait(1.0)
        connection.activate(
            limits(outbound_message_limit=0, outbound_byte_limit=0)
        )

        connection.send_terminal(terminal)
        sock.release_first_send.set()
        wait_until(lambda: connection.snapshot().pending_outbound_messages == 0)
        assert b"".join(sock.sent) == ordinary.payload + terminal.payload
    finally:
        sock.release_first_send.set()
        connection.close(timeout=1.0)


def test_terminal_record_identity_cannot_livelock_the_writer() -> None:
    sock = ControlledSocket()
    connection = TcpConnection(sock, limits(), ASSOCIATION_UID)
    blocker = TransportRecord(CONTROL, control_frame("blocker"))
    reused = TransportRecord(
        CONTROL,
        encode_frame(GoAway(ReasonCode.NORMAL_SHUTDOWN)),
    )
    try:
        connection.send(blocker)
        assert sock.send_started.wait(1.0)
        connection.send(reused)
        connection.send_terminal(reused)

        sock.release_first_send.set()
        wait_until(lambda: connection.snapshot().pending_outbound_messages == 0)
        assert b"".join(sock.sent) == blocker.payload + reused.payload + reused.payload
    finally:
        sock.release_first_send.set()
        connection.close(timeout=1.0)


def test_terminal_admission_revalidates_a_tightened_maximum(monkeypatch) -> None:
    sock = ControlledSocket()
    terminal = TransportRecord(
        CONTROL,
        encode_frame(GoAway(ReasonCode.NORMAL_SHUTDOWN, "too-large-after-tightening")),
    )
    connection = TcpConnection(
        sock,
        limits(maximum_record_bytes=len(terminal.payload)),
        ASSOCIATION_UID,
    )
    validated = threading.Event()
    release = threading.Event()
    errors = Queue()
    original_validate = connection._validate_record
    calls = 0

    def validate(record, maximum_record_bytes):
        nonlocal calls
        header = original_validate(record, maximum_record_bytes)
        calls += 1
        if calls == 1:
            validated.set()
            release.wait(1.0)
        return header

    monkeypatch.setattr(connection, "_validate_record", validate)
    sender = threading.Thread(
        target=lambda: _capture_error(lambda: connection.send_terminal(terminal), errors)
    )
    try:
        sender.start()
        assert validated.wait(1.0)
        connection.set_maximum_record_bytes(COMMON_HEADER_SIZE)
        release.set()
        sender.join(1.0)

        assert not sender.is_alive()
        assert isinstance(errors.get_nowait(), TransportProtocolError)
        assert connection.snapshot().pending_outbound_messages == 0
    finally:
        release.set()
        sender.join(1.0)
        sock.release_first_send.set()
        connection.close(timeout=1.0)


def test_writer_batch_construction_failure_closes_the_connection(monkeypatch) -> None:
    sock = ControlledSocket()
    connection = TcpConnection(sock, limits(), ASSOCIATION_UID)

    def fail_batch(_records):
        raise MemoryError("injected batch allocation failure")

    monkeypatch.setattr(tcp_module, "_batch_payload", fail_batch)
    try:
        connection.send(TransportRecord(CONTROL, control_frame("batch-failure")))
        wait_until(lambda: connection.snapshot().state is ConnectionState.CLOSED)

        snapshot = connection.snapshot()
        assert isinstance(snapshot.cause, TransportClosedError)
        assert snapshot.pending_outbound_messages == 0
        assert snapshot.pending_outbound_bytes == 0
    finally:
        sock.release_first_send.set()
        connection.close(timeout=1.0)


def test_outbound_scheduler_preserves_channel_fifo_and_is_fair() -> None:
    sock = ControlledSocket()
    connection = TcpConnection(sock, limits(), ASSOCIATION_UID)
    records = [
        TransportRecord(CONTROL, control_frame("control-1")),
        TransportRecord(CONTROL, control_frame("control-2")),
        TransportRecord(CONTROL, control_frame("control-3")),
        TransportRecord(LANE_1, delivery_frame(1)),
        TransportRecord(LANE_1, delivery_frame(2)),
        TransportRecord(LANE_2, delivery_frame(3)),
    ]
    try:
        connection.send(records[0])
        assert sock.send_started.wait(1.0)
        for record in records[1:]:
            connection.send(record)

        sock.release_first_send.set()
        wait_until(lambda: connection.snapshot().pending_outbound_messages == 0)

        assert b"".join(sock.sent) == b"".join(
            (
                records[0].payload,
                records[1].payload,
                records[3].payload,
                records[5].payload,
                records[2].payload,
                records[4].payload,
            )
        )
        assert len(sock.sent) < len(records)
    finally:
        sock.release_first_send.set()
        connection.close(timeout=1.0)


@pytest.mark.parametrize("capacity", ["messages", "bytes"])
def test_inbound_capacity_applies_backpressure_until_a_queued_record_is_received(
    capacity,
) -> None:
    local, peer = tcp_socket_pair()
    first = delivery_frame(1)
    second = delivery_frame(2)
    connection_limits = (
        limits(inbound_message_limit=1)
        if capacity == "messages"
        else limits(
            inbound_message_limit=2,
            inbound_byte_limit=max(len(first), len(second)),
        )
    )
    connection = TcpConnection(
        local,
        connection_limits,
        ASSOCIATION_UID,
    )
    connection.activate(connection_limits)
    try:
        peer.sendall(first + second)
        wait_until(lambda: connection.snapshot().pending_inbound_messages == 1)
        time.sleep(0.05)

        snapshot = connection.snapshot()
        assert snapshot.state is ConnectionState.OPEN
        assert snapshot.pending_inbound_messages == 1
        assert snapshot.pending_inbound_bytes == (
            len(first)
            + connection._reading_header_bytes
            + connection._unassigned_receive_bytes
        )
        assert (
            snapshot.pending_inbound_bytes
            <= connection_limits.inbound_byte_limit + COMMON_HEADER_SIZE
        )
        assert connection.receive(timeout=1.0).payload == first
        assert connection.receive(timeout=1.0).payload == second
    finally:
        connection.close(timeout=1.0)
        peer.close()


def test_tightened_maximum_rejects_an_in_progress_inbound_record() -> None:
    local, peer = tcp_socket_pair()
    connection = TcpConnection(local, limits(), ASSOCIATION_UID)
    frame = encode_common_header(FrameType.USER_MESSAGE, 200, 0) + b"x" * 200
    try:
        peer.sendall(frame[:COMMON_HEADER_SIZE])
        wait_until(lambda: connection.snapshot().pending_inbound_messages == 1)

        connection.set_maximum_record_bytes(COMMON_HEADER_SIZE)
        peer.sendall(frame[COMMON_HEADER_SIZE:])

        with pytest.raises(TransportFlowControlError, match="maximum record bytes"):
            connection.receive(timeout=1.0)
    finally:
        connection.close(timeout=1.0)
        peer.close()


def test_tightened_maximum_rejects_a_header_waiting_for_capacity() -> None:
    local, peer = tcp_socket_pair()
    connection_limits = limits(inbound_message_limit=1)
    connection = TcpConnection(local, connection_limits, ASSOCIATION_UID)
    connection.activate(connection_limits)
    first = delivery_frame(1)
    second = encode_common_header(FrameType.USER_MESSAGE, 200, 0) + b"x" * 200
    try:
        peer.sendall(first + second[:COMMON_HEADER_SIZE])
        wait_until(
            lambda: connection.snapshot().pending_inbound_messages == 1
            and connection._reading_header_bytes == COMMON_HEADER_SIZE
        )

        connection.set_maximum_record_bytes(COMMON_HEADER_SIZE)
        assert connection.receive(timeout=1.0).payload == first
        with pytest.raises(TransportFlowControlError, match="maximum record bytes"):
            connection.receive(timeout=1.0)
    finally:
        connection.close(timeout=1.0)
        peer.close()


@pytest.mark.parametrize("capacity", ["messages", "bytes"])
def test_activation_rechecks_in_progress_inbound_capacity(capacity) -> None:
    local, peer = tcp_socket_pair()
    connection = TcpConnection(local, limits(), ASSOCIATION_UID)
    frame = encode_common_header(FrameType.USER_MESSAGE, 200, 0) + b"x" * 200
    tightened = (
        limits(inbound_message_limit=0)
        if capacity == "messages"
        else limits(inbound_byte_limit=COMMON_HEADER_SIZE)
    )
    try:
        peer.sendall(frame[:COMMON_HEADER_SIZE])
        wait_until(lambda: connection.snapshot().pending_inbound_messages == 1)

        connection.activate(tightened)
        peer.sendall(frame[COMMON_HEADER_SIZE:])

        with pytest.raises(TransportFlowControlError, match="current inbound capacity"):
            connection.receive(timeout=1.0)
        assert connection.snapshot().pending_inbound_bytes == 0
    finally:
        connection.close(timeout=1.0)
        peer.close()


def test_tightened_maximum_rechecks_terminal_read_ahead_capacity() -> None:
    local, peer = tcp_socket_pair()
    initial = limits(
        maximum_record_bytes=4096,
        inbound_message_limit=0,
        inbound_byte_limit=0,
    )
    connection = TcpConnection(local, initial, ASSOCIATION_UID)
    connection.activate(initial)
    terminal = encode_frame(
        GoAway(ReasonCode.NORMAL_SHUTDOWN, "x" * 64),
        maximum_frame_bytes=4096,
    )
    tightened_maximum = len(terminal) + 16
    try:
        peer.sendall(terminal[:COMMON_HEADER_SIZE])
        wait_until(lambda: connection.snapshot().pending_inbound_messages == 1)

        connection.set_maximum_record_bytes(tightened_maximum)
        peer.sendall(terminal[COMMON_HEADER_SIZE:] + b"x" * 1024)

        with pytest.raises(TransportFlowControlError, match="current inbound capacity"):
            connection.receive(timeout=1.0)
        assert connection.snapshot().pending_inbound_bytes == 0
    finally:
        connection.close(timeout=1.0)
        peer.close()


def test_inbound_single_record_byte_violation_is_typed_and_keeps_send_available() -> None:
    local, peer = tcp_socket_pair()
    inbound = encode_common_header(FrameType.USER_MESSAGE, 64, 0) + b"x" * 64
    outbound = control_frame("flow-control GOAWAY")
    connection = TcpConnection(
        local,
        limits(inbound_byte_limit=len(inbound) - 1),
        ASSOCIATION_UID,
    )
    try:
        peer.sendall(inbound)

        with pytest.raises(TransportFlowControlError) as failure:
            connection.receive(timeout=1.0)
        snapshot = connection.snapshot()
        assert snapshot.state is ConnectionState.READ_FAILED
        assert snapshot.cause is failure.value
        assert snapshot.pending_inbound_messages == 0
        assert snapshot.pending_inbound_bytes == 0

        connection.send_terminal(TransportRecord(CONTROL, outbound))
        peer.settimeout(1.0)
        assert recv_exact(peer, len(outbound)) == outbound
    finally:
        connection.close(timeout=1.0)
        peer.close()


def test_inbound_terminal_reserve_does_not_admit_ordinary_records() -> None:
    local, peer = tcp_socket_pair()
    connection = TcpConnection(local, limits(), ASSOCIATION_UID)
    connection.activate(
        limits(
            maximum_record_bytes=4096,
            inbound_message_limit=1,
            inbound_byte_limit=128,
        )
    )
    ordinary = encode_common_header(FrameType.USER_MESSAGE, 200, 0) + b"x" * 200
    try:
        peer.sendall(ordinary)
        with pytest.raises(TransportFlowControlError):
            connection.receive(timeout=1.0)
    finally:
        connection.close(timeout=1.0)
        peer.close()

    local, peer = tcp_socket_pair()
    connection = TcpConnection(local, limits(), ASSOCIATION_UID)
    connection.activate(
        limits(
            maximum_record_bytes=4096,
            inbound_message_limit=1,
            inbound_byte_limit=128,
        )
    )
    terminal = encode_frame(GoAway(ReasonCode.NORMAL_SHUTDOWN, "x" * 200))
    try:
        peer.sendall(terminal)
        assert connection.receive(timeout=1.0).payload == terminal
    finally:
        connection.close(timeout=1.0)
        peer.close()


def test_send_validates_bytes_complete_length_and_tightened_limit() -> None:
    sock = ControlledSocket()
    connection = TcpConnection(sock, limits(), ASSOCIATION_UID)
    frame = control_frame("larger record")
    try:
        with pytest.raises(TransportProtocolError, match="bytes"):
            connection.send(TransportRecord(CONTROL, bytearray(frame)))
        with pytest.raises(TransportProtocolError, match="declared"):
            connection.send(TransportRecord(CONTROL, frame[:-1]))

        connection.set_maximum_record_bytes(len(frame) - 1)
        with pytest.raises(TransportProtocolError, match="limit"):
            connection.send(TransportRecord(CONTROL, frame))
        with pytest.raises(TransportProtocolError, match="tighten"):
            connection.set_maximum_record_bytes(len(frame))
    finally:
        sock.release_first_send.set()
        connection.close(timeout=1.0)


def test_activate_can_increase_the_maximum_and_is_one_shot() -> None:
    sock = ControlledSocket()
    frame = control_frame("negotiated increase")
    connection = TcpConnection(
        sock,
        limits(maximum_record_bytes=len(frame) - 1),
        ASSOCIATION_UID,
    )
    record = TransportRecord(CONTROL, frame)
    try:
        with pytest.raises(TransportProtocolError, match="limit"):
            connection.send(record)

        connection.activate(limits(maximum_record_bytes=len(frame)))
        assert connection.snapshot().maximum_record_bytes == len(frame)
        connection.send(record)
        assert sock.send_started.wait(1.0)

        with pytest.raises(TransportProtocolError, match="already active"):
            connection.activate(limits(maximum_record_bytes=len(frame) + 1))
        connection.set_maximum_record_bytes(len(frame) - 1)
        with pytest.raises(TransportProtocolError, match="tighten"):
            connection.set_maximum_record_bytes(len(frame))
    finally:
        sock.release_first_send.set()
        connection.close(timeout=1.0)


def test_activate_can_decrease_limits_while_admitted_outbound_work_drains() -> None:
    sock = ControlledSocket()
    existing = TransportRecord(CONTROL, control_frame("admitted under bootstrap limits"))
    smallest = TransportRecord(CONTROL, encode_common_header(FrameType.GOAWAY, 0, 0))
    connection = TcpConnection(sock, limits(), ASSOCIATION_UID)
    try:
        connection.send(existing)
        assert sock.send_started.wait(1.0)

        connection.activate(
            limits(
                maximum_record_bytes=COMMON_HEADER_SIZE,
                outbound_message_limit=1,
                outbound_byte_limit=COMMON_HEADER_SIZE,
            )
        )
        snapshot = connection.snapshot()
        assert snapshot.maximum_record_bytes == COMMON_HEADER_SIZE
        assert snapshot.pending_outbound_messages == 1
        assert snapshot.pending_outbound_bytes == len(existing.payload)
        with pytest.raises(TransportProtocolError, match="limit"):
            connection.send(existing)
        with pytest.raises(TransportCapacityError):
            connection.send(smallest)

        sock.release_first_send.set()
        wait_until(lambda: connection.snapshot().pending_outbound_messages == 0)
        connection.send(smallest)
        wait_until(lambda: len(sock.sent) == 2)
    finally:
        sock.release_first_send.set()
        connection.close(timeout=1.0)


def test_port_zero_loopback_exchanges_multiplexed_records() -> None:
    accepted: Queue = Queue()
    transport = TcpTransport(handshake_workers=2, accepted_socket_limit=4)
    listener = transport.listen(Endpoint("127.0.0.1", 0), limits(), accepted.put)
    client = None
    server = None
    try:
        assert listener.endpoint.port != 0
        client = transport.connect(listener.endpoint, limits(), ASSOCIATION_UID)
        server = accepted.get(timeout=1.0)
        assert server.association_uid == ASSOCIATION_UID

        first = TransportRecord(CONTROL, control_frame("client"))
        second = TransportRecord(CONTROL, control_frame("server"))
        client.send(first)
        server.send(second)

        assert server.receive(timeout=1.0) == TransportRecord(MULTIPLEXED, first.payload)
        assert client.receive(timeout=1.0) == TransportRecord(MULTIPLEXED, second.payload)
    finally:
        if client is not None:
            client.close(timeout=1.0)
        if server is not None:
            server.close(timeout=1.0)
        listener.close(timeout=1.0)


@pytest.mark.parametrize("timeout", [-1.0, float("inf"), float("nan"), True, "1"])
def test_connect_rejects_invalid_per_call_timeout(timeout) -> None:
    transport = TcpTransport()

    with pytest.raises(ValueError, match="TCP connect timeout"):
        transport.connect(Endpoint("127.0.0.1", 0), limits(), ASSOCIATION_UID, timeout=timeout)


def test_listener_strictly_rejects_a_non_multiplexed_preamble() -> None:
    called = threading.Event()
    transport = TcpTransport(handshake_workers=1, accepted_socket_limit=1)
    listener = transport.listen(Endpoint("127.0.0.1", 0), limits(), lambda _: called.set())
    raw = socket.create_connection((listener.endpoint.host, listener.endpoint.port), timeout=1.0)
    try:
        raw.settimeout(1.0)
        raw.sendall(
            encode_preamble(StreamPreamble(StreamKind.CONTROL, ASSOCIATION_UID, CONTROL_LANE_ID))
        )

        assert raw.recv(1) == b""
        assert not called.is_set()
    finally:
        raw.close()
        listener.close(timeout=1.0)


def test_listener_does_not_consume_frame_bytes_after_the_preamble() -> None:
    accepted: Queue = Queue()
    transport = TcpTransport(handshake_workers=1, accepted_socket_limit=1)
    listener = transport.listen(Endpoint("127.0.0.1", 0), limits(), accepted.put)
    raw = socket.create_connection((listener.endpoint.host, listener.endpoint.port), timeout=1.0)
    server = None
    frame = control_frame("already buffered")
    try:
        raw.sendall(
            encode_preamble(
                StreamPreamble(StreamKind.MULTIPLEXED, ASSOCIATION_UID, CONTROL_LANE_ID)
            )
            + frame
        )

        server = accepted.get(timeout=1.0)
        assert server.receive(timeout=1.0) == TransportRecord(MULTIPLEXED, frame)
    finally:
        raw.close()
        if server is not None:
            server.close(timeout=1.0)
        listener.close(timeout=1.0)


def test_callback_failure_closes_the_new_connection() -> None:
    called = threading.Event()

    def fail_callback(connection) -> None:
        called.set()
        raise RuntimeError("callback failed")

    transport = TcpTransport(handshake_workers=1, accepted_socket_limit=1)
    listener = transport.listen(Endpoint("127.0.0.1", 0), limits(), fail_callback)
    client = transport.connect(listener.endpoint, limits(), ASSOCIATION_UID)
    try:
        assert called.wait(1.0)
        with pytest.raises(TransportClosedError):
            client.receive(timeout=1.0)
    finally:
        client.close(timeout=1.0)
        listener.close(timeout=1.0)


def test_eof_wakes_receive_and_close_has_a_bounded_join() -> None:
    before = {thread.ident for thread in threading.enumerate()}
    local, peer = tcp_socket_pair()
    connection = TcpConnection(local, limits(), ASSOCIATION_UID)
    with pytest.raises(TimeoutError):
        connection.receive(timeout=0.01)
    peer.close()

    with pytest.raises(TransportClosedError):
        connection.receive(timeout=1.0)
    connection.close(timeout=1.0)

    wait_until(
        lambda: (
            not any(
                thread.ident not in before
                and thread.name.startswith("movie-tcp-connection-")
                and thread.is_alive()
                for thread in threading.enumerate()
            )
        )
    )
    assert all(
        thread.daemon for thread in threading.enumerate() if thread.name.startswith("movie-tcp-")
    )


def test_close_times_out_if_a_socket_call_ignores_shutdown() -> None:
    sock = ControlledSocket()
    connection = TcpConnection(sock, limits(), ASSOCIATION_UID)
    connection.send(TransportRecord(CONTROL, control_frame("blocked")))
    assert sock.send_started.wait(1.0)

    started = time.monotonic()
    with pytest.raises(TimeoutError):
        connection.close(timeout=0.02)
    assert time.monotonic() - started < 0.5
    assert connection.snapshot().state is ConnectionState.CLOSED

    sock.release_first_send.set()
    connection.close(timeout=1.0)


def test_listener_close_interrupts_an_incomplete_handshake_and_joins_workers() -> None:
    before = {thread.ident for thread in threading.enumerate()}
    transport = TcpTransport(
        handshake_workers=1,
        accepted_socket_limit=1,
        handshake_timeout=10.0,
    )
    listener = transport.listen(Endpoint("127.0.0.1", 0), limits(), lambda _: None)
    raw = socket.create_connection((listener.endpoint.host, listener.endpoint.port), timeout=1.0)
    try:
        raw.sendall(b"partial")
        listener.close(timeout=1.0)

        wait_until(
            lambda: (
                not any(
                    thread.ident not in before
                    and thread.name.startswith("movie-tcp-listener-")
                    and thread.is_alive()
                    for thread in threading.enumerate()
                )
            )
        )
    finally:
        raw.close()
        listener.close(timeout=1.0)


def test_connect_timeout_includes_endpoint_resolution(monkeypatch) -> None:
    entered = threading.Event()
    release = threading.Event()
    original = TcpTransport._resolve

    def blocked_resolution(endpoint, *, passive):
        entered.set()
        release.wait(1.0)
        return original(endpoint, passive=passive)

    monkeypatch.setattr(TcpTransport, "_resolve", staticmethod(blocked_resolution))
    transport = TcpTransport(connect_timeout=1.0)
    started = time.monotonic()
    try:
        with pytest.raises(TransportConnectError):
            transport.connect(
                Endpoint("localhost", 1),
                limits(),
                ASSOCIATION_UID,
                timeout=0.02,
            )
        assert entered.is_set()
        assert time.monotonic() - started < 0.5
        with pytest.raises(TimeoutError, match="resolver threads"):
            transport.close(timeout=0.01)
    finally:
        release.set()
        transport.close(timeout=1.0)


def test_transient_resolution_failure_is_not_cached(monkeypatch) -> None:
    calls = 0
    expected = (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, ("127.0.0.1", 1))

    def flaky_resolution(endpoint, *, passive):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("transient resolution failure")
        return expected

    monkeypatch.setattr(TcpTransport, "_resolve", staticmethod(flaky_resolution))
    transport = TcpTransport()
    target = Endpoint("transient.test", 1)

    with pytest.raises(OSError, match="transient"):
        transport._resolve_with_timeout(target, passive=False, timeout=1.0)
    assert transport._resolve_with_timeout(target, passive=False, timeout=1.0) == expected
    assert calls == 2
    transport.close(timeout=1.0)
