import asyncio
import socket
import threading
import time
from dataclasses import replace
from queue import Empty, Queue
from uuid import UUID

import pytest

import movie.remoting.asyncio_tcp as asyncio_tcp_module
from movie.actor import ActorSystem, Behaviors
from movie.io import ASYNCIO_IO, AsyncioIOStateError
from movie.remoting import (
    COMMON_HEADER_SIZE,
    CONTROL_LANE_ID,
    AsyncioTcpConnection,
    AsyncioTcpTransport,
    ConnectionState,
    Endpoint,
    FrameType,
    GoAway,
    HelloAccept,
    LogicalChannel,
    ReasonCode,
    StreamKind,
    StreamPreamble,
    TransportCapacityError,
    TransportClosedError,
    TransportFlowControlError,
    TransportLimits,
    TransportListenError,
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


def delivery_frame(marker: int) -> bytes:
    return encode_common_header(FrameType.USER_MESSAGE, 1, 0) + bytes((marker,))


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


def wait_until(predicate, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("condition was not met before the deadline")
        time.sleep(0.005)


def pause_loop(extension):
    entered = threading.Event()
    release = threading.Event()

    def block() -> None:
        entered.set()
        release.wait(2.0)

    completed = extension.schedule(block)
    assert entered.wait(1.0)
    return release, completed


def connect_pair(extension, connection_limits=None):
    connection_limits = limits() if connection_limits is None else connection_limits
    accepted: Queue = Queue()
    transport = AsyncioTcpTransport(extension)
    listener = transport.listen(
        Endpoint("127.0.0.1", 0),
        connection_limits,
        accepted.put,
    )
    client = transport.connect(listener.endpoint, connection_limits, ASSOCIATION_UID)
    server = accepted.get(timeout=1.0)
    return transport, listener, client, server


def close_pair(transport, listener, client, server) -> None:
    client.close(timeout=1.0)
    server.close(timeout=1.0)
    listener.close(timeout=1.0)
    transport.close(timeout=1.0)


def open_raw_peer(extension, connection_limits=None):
    connection_limits = limits() if connection_limits is None else connection_limits
    accepted: Queue = Queue()
    transport = AsyncioTcpTransport(extension)
    listener = transport.listen(
        Endpoint("127.0.0.1", 0),
        connection_limits,
        accepted.put,
    )
    raw = socket.create_connection(
        (listener.endpoint.host, listener.endpoint.port),
        timeout=1.0,
    )
    return transport, listener, accepted, raw


def multiplexed_preamble() -> bytes:
    return encode_preamble(
        StreamPreamble(StreamKind.MULTIPLEXED, ASSOCIATION_UID, CONTROL_LANE_ID)
    )


@pytest.fixture
def io_extension():
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "asyncio-remoting-transport",
    )
    extension = ASYNCIO_IO.get(system)
    try:
        yield extension
    finally:
        system.stop()


def test_loopback_transport_exchanges_records_without_connection_threads(io_extension) -> None:
    before = {thread.ident for thread in threading.enumerate()}
    accepted: Queue = Queue()
    transport = AsyncioTcpTransport(io_extension)
    listener = transport.listen(
        Endpoint("127.0.0.1", 0),
        limits(),
        lambda connection: accepted.put((connection, threading.get_ident())),
    )
    client = transport.connect(listener.endpoint, limits(), ASSOCIATION_UID)
    server, callback_thread_id = accepted.get(timeout=1.0)
    frame = control_frame("loopback")
    try:
        client.send(TransportRecord(CONTROL, frame))

        assert server.association_uid == ASSOCIATION_UID
        assert server.receive(timeout=1.0) == TransportRecord(MULTIPLEXED, frame)
        assert callback_thread_id != io_extension.thread.ident
        assert not any(
            thread.ident not in before and "asyncio-tcp-connection" in thread.name
            for thread in threading.enumerate()
        )
    finally:
        client.close(timeout=1.0)
        server.close(timeout=1.0)
        listener.close(timeout=1.0)
        transport.close(timeout=1.0)


def test_listener_rejects_a_non_multiplexed_preamble_and_recovers(io_extension) -> None:
    transport, listener, accepted, raw = open_raw_peer(io_extension)
    valid = None
    server = None
    try:
        raw.sendall(
            encode_preamble(
                StreamPreamble(StreamKind.CONTROL, ASSOCIATION_UID, CONTROL_LANE_ID)
            )
        )
        raw.settimeout(1.0)

        assert raw.recv(1) == b""
        with pytest.raises(Empty):
            accepted.get_nowait()

        valid = socket.create_connection(
            (listener.endpoint.host, listener.endpoint.port),
            timeout=1.0,
        )
        valid.sendall(multiplexed_preamble())
        server = accepted.get(timeout=1.0)
        frame = control_frame("after invalid preamble")
        valid.sendall(frame)

        assert server.receive(timeout=1.0) == TransportRecord(MULTIPLEXED, frame)
    finally:
        raw.close()
        if valid is not None:
            valid.close()
        if server is not None:
            server.close(timeout=1.0)
        listener.close(timeout=1.0)
        transport.close(timeout=1.0)


def test_partial_preamble_timeout_releases_listener_capacity(
    io_extension,
    monkeypatch,
) -> None:
    preamble_started = threading.Event()
    original_read_preamble = asyncio_tcp_module.AsyncioTcpListener._read_preamble

    async def observe_read_preamble(listener, accepted_socket):
        preamble_started.set()
        return await original_read_preamble(listener, accepted_socket)

    monkeypatch.setattr(
        asyncio_tcp_module.AsyncioTcpListener,
        "_read_preamble",
        observe_read_preamble,
    )
    accepted: Queue = Queue()
    transport = AsyncioTcpTransport(
        io_extension,
        handshake_workers=1,
        accepted_socket_limit=1,
        handshake_timeout=0.5,
    )
    listener = transport.listen(
        Endpoint("127.0.0.1", 0),
        limits(),
        accepted.put,
    )
    partial = socket.create_connection(
        (listener.endpoint.host, listener.endpoint.port),
        timeout=1.0,
    )
    valid = None
    server = None
    try:
        partial.sendall(multiplexed_preamble()[:-1])
        assert preamble_started.wait(1.0)

        valid = socket.create_connection(
            (listener.endpoint.host, listener.endpoint.port),
            timeout=1.0,
        )
        valid.sendall(multiplexed_preamble())

        async def wait_for_accepted_capacity() -> None:
            async with asyncio.timeout(0.25):
                while not listener._accepted.full():
                    await asyncio.sleep(0)

        io_extension.run_coroutine(wait_for_accepted_capacity, timeout=1.0)
        partial.settimeout(1.0)
        assert partial.recv(1) == b""

        server = accepted.get(timeout=1.0)
        frame = control_frame("after preamble timeout")
        valid.sendall(frame)
        assert server.receive(timeout=1.0) == TransportRecord(MULTIPLEXED, frame)
        assert not listener._closed
    finally:
        partial.close()
        if valid is not None:
            valid.close()
        if server is not None:
            server.close(timeout=1.0)
        listener.close(timeout=1.0)
        transport.close(timeout=1.0)


def test_listener_restores_fragmented_and_concatenated_frames(io_extension) -> None:
    transport, listener, accepted, raw = open_raw_peer(io_extension)
    server = None
    frames = [control_frame("first"), control_frame("second"), control_frame("third")]
    try:
        preamble = multiplexed_preamble()
        for byte in preamble:
            raw.sendall(bytes((byte,)))
        server = accepted.get(timeout=1.0)

        for offset in range(0, len(frames[0]), 3):
            raw.sendall(frames[0][offset : offset + 3])
        raw.sendall(frames[1] + frames[2])

        assert [server.receive(timeout=1.0).payload for _ in frames] == frames
    finally:
        raw.close()
        if server is not None:
            server.close(timeout=1.0)
        listener.close(timeout=1.0)
        transport.close(timeout=1.0)


def test_receive_many_drains_only_the_ready_bounded_prefix(io_extension) -> None:
    transport, listener, client, server = connect_pair(io_extension)
    frames = [control_frame("first"), control_frame("second"), control_frame("third")]
    try:
        for frame in frames:
            client.send(TransportRecord(CONTROL, frame))
        wait_until(lambda: server.snapshot().pending_inbound_messages == 3)

        assert [record.payload for record in server.receive_many(2, timeout=1.0)] == frames[:2]
        snapshot = server.snapshot()
        assert snapshot.pending_inbound_messages == 1
        assert snapshot.pending_inbound_bytes == len(frames[2])
        assert [record.payload for record in server.receive_many(128, timeout=1.0)] == [
            frames[2]
        ]
    finally:
        close_pair(transport, listener, client, server)


def test_receive_many_validates_its_bound(io_extension) -> None:
    transport, listener, client, server = connect_pair(io_extension)
    try:
        with pytest.raises(TransportProtocolError, match="batch size"):
            server.receive_many(0, timeout=0.0)
    finally:
        close_pair(transport, listener, client, server)


def test_blocked_writer_does_not_block_another_connection(io_extension) -> None:
    connection_limits = limits(
        maximum_record_bytes=64 * 1024,
        outbound_message_limit=1024,
        outbound_byte_limit=70 * 1024 * 1024,
        inbound_message_limit=1,
        inbound_byte_limit=64 * 1024,
    )
    accepted: Queue = Queue()
    transport = AsyncioTcpTransport(io_extension)
    listener = transport.listen(
        Endpoint("127.0.0.1", 0),
        connection_limits,
        accepted.put,
    )
    first_client = transport.connect(
        listener.endpoint,
        connection_limits,
        ASSOCIATION_UID,
    )
    first_server = accepted.get(timeout=1.0)
    second_client = transport.connect(
        listener.endpoint,
        connection_limits,
        UUID("10112233-4455-6677-8899-aabbccddeeff"),
    )
    second_server = accepted.get(timeout=1.0)
    large_payload = (
        encode_common_header(FrameType.USER_MESSAGE, 60_000, 0)
        + b"x" * 60_000
    )
    try:
        first_client._socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        for _ in range(1024):
            first_client.send(TransportRecord(LANE_1, large_payload))
        wait_until(lambda: first_server.snapshot().pending_inbound_messages == 1)
        time.sleep(0.05)
        assert first_client.snapshot().pending_outbound_messages > 0

        ready_payload = delivery_frame(7)
        second_client.send(TransportRecord(LANE_2, ready_payload))

        assert second_server.receive(timeout=1.0).payload == ready_payload
    finally:
        transport.close(timeout=2.0)


@pytest.mark.parametrize("capacity", ["messages", "bytes"])
def test_outbound_admission_is_exact_while_the_loop_is_busy(io_extension, capacity) -> None:
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
    transport, listener, client, server = connect_pair(io_extension, connection_limits)
    release, completed = pause_loop(io_extension)
    try:
        client.send(first)
        snapshot = client.snapshot()
        assert snapshot.pending_outbound_messages == 1
        assert snapshot.pending_outbound_bytes == len(first.payload)
        with pytest.raises(TransportCapacityError):
            client.send(second)

        release.set()
        completed.result(1.0)
        assert server.receive(timeout=1.0).payload == first.payload
        wait_until(lambda: client.snapshot().pending_outbound_messages == 0)
    finally:
        release.set()
        completed.result(1.0)
        close_pair(transport, listener, client, server)


@pytest.mark.parametrize("capacity", ["messages", "bytes"])
def test_inbound_capacity_backpressures_with_one_header_reserve(io_extension, capacity) -> None:
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
    transport, listener, client, server = connect_pair(io_extension, connection_limits)
    server.activate(connection_limits)
    try:
        client.send(TransportRecord(LANE_1, first))
        client.send(TransportRecord(LANE_1, second))
        wait_until(lambda: server.snapshot().pending_inbound_messages == 1)
        time.sleep(0.03)

        snapshot = server.snapshot()
        assert snapshot.state is ConnectionState.OPEN
        assert snapshot.pending_inbound_messages == 1
        assert snapshot.pending_inbound_bytes <= (
            connection_limits.inbound_byte_limit + COMMON_HEADER_SIZE
        )
        assert server.receive(timeout=1.0).payload == first
        assert server.receive(timeout=1.0).payload == second
    finally:
        close_pair(transport, listener, client, server)


def test_outbound_scheduler_preserves_fifo_across_batch_boundaries(io_extension) -> None:
    connection_limits = limits(outbound_message_limit=100)
    transport, listener, client, server = connect_pair(io_extension, connection_limits)
    lane_one = [TransportRecord(LANE_1, delivery_frame(marker)) for marker in range(40)]
    lane_two = [TransportRecord(LANE_2, delivery_frame(marker)) for marker in range(40, 80)]
    release, completed = pause_loop(io_extension)
    try:
        for record in (*lane_one, *lane_two):
            client.send(record)
        release.set()
        completed.result(1.0)

        expected = [item for pair in zip(lane_one, lane_two, strict=True) for item in pair]
        assert [server.receive(timeout=1.0).payload for _ in expected] == [
            record.payload for record in expected
        ]
        wait_until(lambda: client.snapshot().pending_outbound_messages == 0)
    finally:
        release.set()
        completed.result(1.0)
        close_pair(transport, listener, client, server)


def test_terminal_reserve_is_final_and_accepts_reused_record_identity(io_extension) -> None:
    connection_limits = limits(outbound_message_limit=2)
    transport, listener, client, server = connect_pair(io_extension, connection_limits)
    blocker = TransportRecord(CONTROL, control_frame("blocker"))
    reused = TransportRecord(
        CONTROL,
        encode_frame(GoAway(ReasonCode.NORMAL_SHUTDOWN)),
    )
    release, completed = pause_loop(io_extension)
    try:
        client.send(blocker)
        client.send(reused)
        client.send_terminal(reused)
        with pytest.raises(TransportClosedError, match="terminal"):
            client.send(TransportRecord(CONTROL, control_frame("too late")))
        with pytest.raises(TransportCapacityError, match="already in use"):
            client.send_terminal(reused)

        release.set()
        completed.result(1.0)
        assert [server.receive(timeout=1.0).payload for _ in range(3)] == [
            blocker.payload,
            reused.payload,
            reused.payload,
        ]
        wait_until(lambda: client.snapshot().pending_outbound_messages == 0)
    finally:
        release.set()
        completed.result(1.0)
        close_pair(transport, listener, client, server)


def test_terminal_reserve_survives_tightened_ordinary_limits(io_extension) -> None:
    transport, listener, client, server = connect_pair(io_extension)
    ordinary = TransportRecord(CONTROL, control_frame("ordinary-before-tightening"))
    terminal = TransportRecord(
        CONTROL,
        encode_frame(GoAway(ReasonCode.NORMAL_SHUTDOWN)),
    )
    release, completed = pause_loop(io_extension)
    try:
        client.send(ordinary)
        client.activate(limits(outbound_message_limit=0, outbound_byte_limit=0))

        client.send_terminal(terminal)
        release.set()
        completed.result(1.0)
        assert [server.receive(timeout=1.0).payload for _ in range(2)] == [
            ordinary.payload,
            terminal.payload,
        ]
    finally:
        release.set()
        completed.result(1.0)
        close_pair(transport, listener, client, server)


def test_hello_accept_holds_pipelined_frames_until_activation(io_extension) -> None:
    transport, listener, client, server = connect_pair(io_extension)
    accept = hello_accept_frame(4096)
    active = control_frame("active")
    try:
        client.send(TransportRecord(CONTROL, accept))
        client.send(TransportRecord(CONTROL, active))

        assert server.receive(timeout=1.0).payload == accept
        with pytest.raises(TimeoutError):
            server.receive(timeout=0.03)
        server.activate(limits())
        assert server.receive(timeout=1.0).payload == active
    finally:
        close_pair(transport, listener, client, server)


def test_current_limits_reject_an_in_progress_record(io_extension) -> None:
    transport, listener, accepted, raw = open_raw_peer(io_extension)
    server = None
    frame = encode_common_header(FrameType.USER_MESSAGE, 200, 0) + b"x" * 200
    try:
        raw.sendall(multiplexed_preamble())
        server = accepted.get(timeout=1.0)
        raw.sendall(frame[:COMMON_HEADER_SIZE])
        wait_until(lambda: server.snapshot().pending_inbound_messages == 1)

        server.activate(limits(inbound_message_limit=0))
        raw.sendall(frame[COMMON_HEADER_SIZE:])
        with pytest.raises(TransportFlowControlError, match="current inbound capacity"):
            server.receive(timeout=1.0)
        assert server.snapshot().state is ConnectionState.READ_FAILED
        assert server.snapshot().pending_inbound_bytes == 0
    finally:
        raw.close()
        if server is not None:
            server.close(timeout=1.0)
        listener.close(timeout=1.0)
        transport.close(timeout=1.0)


def test_tightened_maximum_rejects_an_in_progress_record(io_extension) -> None:
    transport, listener, accepted, raw = open_raw_peer(io_extension)
    server = None
    frame = encode_common_header(FrameType.USER_MESSAGE, 200, 0) + b"x" * 200
    try:
        raw.sendall(multiplexed_preamble())
        server = accepted.get(timeout=1.0)
        raw.sendall(frame[:COMMON_HEADER_SIZE])
        wait_until(lambda: server.snapshot().pending_inbound_messages == 1)

        server.set_maximum_record_bytes(COMMON_HEADER_SIZE)
        with pytest.raises(TransportProtocolError, match="tighten"):
            server.set_maximum_record_bytes(COMMON_HEADER_SIZE + 1)
        raw.sendall(frame[COMMON_HEADER_SIZE:])
        with pytest.raises(TransportFlowControlError, match="maximum record bytes"):
            server.receive(timeout=1.0)
    finally:
        raw.close()
        if server is not None:
            server.close(timeout=1.0)
        listener.close(timeout=1.0)
        transport.close(timeout=1.0)


def test_read_failure_keeps_terminal_send_available(io_extension) -> None:
    connection_limits = limits(inbound_byte_limit=COMMON_HEADER_SIZE)
    transport, listener, accepted, raw = open_raw_peer(io_extension, connection_limits)
    server = None
    oversized = encode_common_header(FrameType.USER_MESSAGE, 1, 0) + b"x"
    terminal = encode_frame(GoAway(ReasonCode.NORMAL_SHUTDOWN))
    try:
        raw.sendall(multiplexed_preamble())
        server = accepted.get(timeout=1.0)
        raw.sendall(oversized)
        with pytest.raises(TransportFlowControlError):
            server.receive(timeout=1.0)

        server.send_terminal(TransportRecord(CONTROL, terminal))
        raw.settimeout(1.0)
        assert raw.recv(len(terminal)) == terminal
    finally:
        raw.close()
        if server is not None:
            server.close(timeout=1.0)
        listener.close(timeout=1.0)
        transport.close(timeout=1.0)


def test_eof_wakes_receive_and_transport_close_leaves_extension_running(io_extension) -> None:
    transport, listener, accepted, raw = open_raw_peer(io_extension)
    server = None
    try:
        raw.sendall(multiplexed_preamble())
        server = accepted.get(timeout=1.0)
        raw.close()
        with pytest.raises(TransportClosedError):
            server.receive(timeout=1.0)

        transport.close(timeout=1.0)

        async def loop_is_running() -> bool:
            return True

        assert io_extension.run_coroutine(loop_is_running, timeout=1.0)
    finally:
        raw.close()
        if server is not None:
            server.close(timeout=1.0)
        listener.close(timeout=1.0)
        transport.close(timeout=1.0)


def test_transport_close_closes_live_listeners_and_connections(io_extension) -> None:
    transport, listener, client, server = connect_pair(io_extension)

    transport.close(timeout=1.0)

    assert client.snapshot().state is ConnectionState.CLOSED
    assert server.snapshot().state is ConnectionState.CLOSED
    with pytest.raises(TransportClosedError):
        client.send(TransportRecord(CONTROL, control_frame("closed")))
    with pytest.raises(OSError):
        socket.create_connection(
            (listener.endpoint.host, listener.endpoint.port),
            timeout=0.1,
        )

    async def loop_is_running() -> bool:
        return True

    assert io_extension.run_coroutine(loop_is_running, timeout=1.0)


def test_connection_created_after_transport_close_is_closed_on_its_owning_loop(
    io_extension,
    monkeypatch,
) -> None:
    transport = AsyncioTcpTransport(io_extension)
    connection_socket, peer_socket = socket.socketpair()
    connection_socket.setblocking(False)
    peer_socket.settimeout(1.0)
    monkeypatch.setattr(transport, "_track_connection", lambda connection: False)

    async def reject_connection() -> None:
        with pytest.raises(TransportClosedError):
            transport._create_connection_on_loop(
                connection_socket,
                limits(),
                ASSOCIATION_UID,
            )
        async with asyncio.timeout(1.0):
            while connection_socket.fileno() != -1:
                await asyncio.sleep(0.005)

    try:
        io_extension.run_coroutine(reject_connection, timeout=2.0)
        assert peer_socket.recv(1) == b""
    finally:
        connection_socket.close()
        peer_socket.close()
        transport.close(timeout=1.0)


def test_transport_close_converges_after_asyncio_extension_stops(io_extension) -> None:
    transport, listener, client, server = connect_pair(io_extension)

    io_extension.stop(1.0)
    transport.close(timeout=1.0)
    transport.close(timeout=1.0)

    assert client.snapshot().state is ConnectionState.CLOSED
    assert server.snapshot().state is ConnectionState.CLOSED
    assert not listener._handoff_thread.is_alive()


def test_transport_close_retry_retains_a_blocked_handoff(io_extension) -> None:
    entered = threading.Event()
    release = threading.Event()

    def block_handoff(connection) -> None:
        entered.set()
        release.wait(1.0)

    transport = AsyncioTcpTransport(io_extension)
    listener = transport.listen(Endpoint("127.0.0.1", 0), limits(), block_handoff)
    client = transport.connect(listener.endpoint, limits(), ASSOCIATION_UID)
    try:
        assert entered.wait(1.0)
        with pytest.raises(TimeoutError, match="handoff"):
            transport.close(timeout=0.05)

        release.set()
        transport.close(timeout=1.0)
        assert not listener._handoff_thread.is_alive()
    finally:
        release.set()
        client.close(timeout=1.0)
        transport.close(timeout=1.0)


def test_failed_handoff_does_not_retry_connection_close_forever(io_extension) -> None:
    attempts: Queue[float | None] = Queue()
    allow_close = threading.Event()

    def reject_handoff(connection) -> None:
        original_close = connection.close

        def controlled_close(timeout=None) -> None:
            attempts.put(timeout)
            if not allow_close.is_set():
                raise TimeoutError("deliberate handoff close timeout")
            original_close(timeout)

        connection.close = controlled_close
        raise RuntimeError("deliberate handoff rejection")

    transport = AsyncioTcpTransport(io_extension, close_timeout=1.0)
    listener = transport.listen(Endpoint("127.0.0.1", 0), limits(), reject_handoff)
    client = transport.connect(listener.endpoint, limits(), ASSOCIATION_UID)
    try:
        assert attempts.get(timeout=1.0) == pytest.approx(0.05)
        with pytest.raises(Empty):
            attempts.get(timeout=0.1)
    finally:
        allow_close.set()
        client.close(timeout=1.0)
        transport.close(timeout=1.0)


def test_unexpected_accept_failure_is_reported(io_extension, monkeypatch) -> None:
    async def current_loop():
        return asyncio.get_running_loop()

    async def fail_accept(_socket):
        raise RuntimeError("deliberate accept task failure")

    loop = io_extension.run_coroutine(current_loop, timeout=1.0)
    monkeypatch.setattr(loop, "sock_accept", fail_accept)
    transport = AsyncioTcpTransport(io_extension)
    listener = transport.listen(Endpoint("127.0.0.1", 0), limits(), lambda _: None)
    failures: Queue[BaseException] = Queue()
    try:
        listener.set_failure_callback(failures.put)

        failure = failures.get(timeout=1.0)
        assert isinstance(failure, TransportListenError)
        assert "deliberate accept task failure" in str(failure)
        assert isinstance(failure.__cause__, RuntimeError)
    finally:
        transport.close(timeout=1.0)


@pytest.mark.parametrize(
    ("task_name", "detail"),
    [
        ("accept", "accept task was cancelled"),
        ("handshake", "handshake task was cancelled"),
    ],
)
def test_unexpected_listener_task_cancellation_is_reported(
    io_extension,
    task_name,
    detail,
) -> None:
    transport = AsyncioTcpTransport(io_extension)
    listener = transport.listen(Endpoint("127.0.0.1", 0), limits(), lambda _: None)
    failures: Queue[BaseException] = Queue()
    listener.set_failure_callback(failures.put)
    task = (
        listener._accept_task
        if task_name == "accept"
        else listener._handshake_tasks[0]
    )
    try:
        listener._loop.call_soon_threadsafe(task.cancel)

        failure = failures.get(timeout=1.0)
        assert isinstance(failure, TransportListenError)
        assert detail in str(failure)
    finally:
        transport.close(timeout=1.0)


def test_connection_timeout_retains_transport_ownership_until_cleanup(
    io_extension,
) -> None:
    transport, listener, client, server = connect_pair(io_extension)
    original_close = client._perform_close_on_loop
    close_calls = 0

    async def delayed_close() -> None:
        nonlocal close_calls
        close_calls += 1
        await asyncio.sleep(0.05)
        await original_close()

    client._perform_close_on_loop = delayed_close
    try:
        with pytest.raises(TimeoutError, match="tasks did not stop"):
            client.close(timeout=0.001)
        assert client in transport._connections
        with pytest.raises(TimeoutError, match="tasks did not stop"):
            client.close(timeout=0.001)
        assert close_calls == 1

        wait_until(lambda: client not in transport._connections)
        assert client._socket.fileno() == -1
    finally:
        close_pair(transport, listener, client, server)


def test_partial_connection_construction_cancels_tasks_and_closes_socket(
    io_extension,
    monkeypatch,
) -> None:
    original_create_task = asyncio_tcp_module._create_task
    calls = 0

    def fail_second_task(loop, coroutine):
        nonlocal calls
        calls += 1
        if calls == 2:
            coroutine.close()
            raise RuntimeError("injected task creation failure")
        return original_create_task(loop, coroutine)

    async def construct() -> None:
        left, right = socket.socketpair()
        try:
            with pytest.raises(
                RuntimeError,
                match="injected task creation failure",
            ) as startup_error:
                AsyncioTcpConnection(
                    io_extension,
                    left,
                    limits(),
                    ASSOCIATION_UID,
                )
            await asyncio.shield(startup_error.value.settled)
            assert left.fileno() == -1
        finally:
            right.close()

    monkeypatch.setattr(asyncio_tcp_module, "_create_task", fail_second_task)
    io_extension.run_coroutine(construct, timeout=1.0)


def test_partial_listener_construction_does_not_start_handoff_thread(
    io_extension,
    monkeypatch,
) -> None:
    original_create_task = asyncio_tcp_module._create_task
    calls = 0

    def fail_second_task(loop, coroutine):
        nonlocal calls
        calls += 1
        if calls == 2:
            coroutine.close()
            raise RuntimeError("injected task creation failure")
        return original_create_task(loop, coroutine)

    before = {thread.ident for thread in threading.enumerate()}
    transport = AsyncioTcpTransport(io_extension)
    monkeypatch.setattr(asyncio_tcp_module, "_create_task", fail_second_task)
    try:
        with pytest.raises(TransportListenError, match="could not listen"):
            transport.listen(Endpoint("127.0.0.1", 0), limits(), lambda connection: None)
        assert all(
            thread.ident in before
            for thread in threading.enumerate()
            if thread.name.startswith("movie-asyncio-tcp-listener-")
        )
    finally:
        transport.close(timeout=1.0)


def test_connection_close_waits_for_stopping_proactor_loop(
    io_extension,
    monkeypatch,
) -> None:
    transport, listener, client, server = connect_pair(io_extension)

    async def current_loop():
        return asyncio.get_running_loop()

    loop = io_extension.run_coroutine(current_loop, timeout=1.0)
    entered = threading.Event()
    release = threading.Event()
    stop_errors = []

    async def delayed_executor_shutdown() -> None:
        entered.set()
        while not release.is_set():
            await asyncio.sleep(0.005)

    monkeypatch.setattr(loop, "shutdown_default_executor", delayed_executor_shutdown)

    def stop_extension() -> None:
        try:
            io_extension.stop(1.0)
        except BaseException as error:
            stop_errors.append(error)

    stopper = threading.Thread(target=stop_extension)
    try:
        stopper.start()
        assert entered.wait(1.0)

        with pytest.raises(TimeoutError, match="I/O loop did not stop"):
            client.close(timeout=0.01)
        assert client._socket.fileno() != -1

        release.set()
        stopper.join(1.0)
        assert not stopper.is_alive()
        assert not stop_errors

        close_pair(transport, listener, client, server)
    finally:
        release.set()
        stopper.join(1.0)
        transport.close(timeout=1.0)


def test_synchronous_close_is_rejected_on_the_asyncio_loop(io_extension) -> None:
    transport, listener, client, server = connect_pair(io_extension)

    async def attempt_closes() -> None:
        with pytest.raises(AsyncioIOStateError, match="cannot synchronously close"):
            client.close(timeout=0.1)
        with pytest.raises(AsyncioIOStateError, match="cannot synchronously close"):
            listener.close(timeout=0.1)
        with pytest.raises(AsyncioIOStateError, match="cannot synchronously close"):
            transport.close(timeout=0.1)

    try:
        io_extension.run_coroutine(attempt_closes, timeout=1.0)
        assert client.snapshot().state is ConnectionState.OPEN
        assert not listener._closed
        assert not transport._closed
    finally:
        close_pair(transport, listener, client, server)


def test_cleanup_claim_recovers_when_task_is_cancelled_before_start(
    io_extension,
) -> None:
    transport, listener, client, server = connect_pair(io_extension)

    def schedule_cleanup_then_stop_loop() -> None:
        client._mark_closed(None)
        assert client._claim_cleanup()
        assert client._schedule_close_on_loop()
        client._loop.call_soon(client._loop.stop)

    completed = io_extension.schedule(schedule_cleanup_then_stop_loop)
    completed.result(1.0)
    assert io_extension.wait_stopped(1.0)

    client.close(timeout=1.0)
    assert client._socket.fileno() == -1
    close_pair(transport, listener, client, server)
