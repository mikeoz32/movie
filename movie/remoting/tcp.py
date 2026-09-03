"""Bounded threaded TCP backend for the remoting transport SPI."""

from __future__ import annotations

import math
import os
import socket
from collections import deque
from collections.abc import Callable
from queue import Empty, Full, Queue
from threading import Condition, Event, Lock, Thread, current_thread
from time import monotonic, sleep
from uuid import UUID

from movie.remoting.errors import ProtocolValidationError, WireCodecError
from movie.remoting.transport import (
    ConnectionState,
    Endpoint,
    LogicalChannel,
    TransportCapacityError,
    TransportClosedError,
    TransportConnectError,
    TransportConnection,
    TransportConnectionSnapshot,
    TransportFlowControlError,
    TransportLimits,
    TransportListenError,
    TransportProtocolError,
    TransportRecord,
)
from movie.remoting.wire import (
    COMMON_HEADER_SIZE,
    CONTROL_LANE_ID,
    PREAMBLE_SIZE,
    FrameType,
    StreamKind,
    StreamPreamble,
    decode_common_header,
    decode_preamble,
    encode_preamble,
)

_RECEIVE_CHUNK_BYTES = 64 * 1024
_WRITE_BATCH_BYTES = 256 * 1024
_WRITE_BATCH_RECORDS = 64
_RESOLUTION_TTL_SECONDS = 60.0
_CONNECTION_IDS = iter(range(1, 1 << 63))
_CONNECTION_ID_LOCK = Lock()
_LISTENER_IDS = iter(range(1, 1 << 63))
_LISTENER_ID_LOCK = Lock()


class _ConnectionStopped(Exception):
    pass


class _PeerEOF(Exception):
    def __init__(self, *, partial: bool, field: str) -> None:
        super().__init__(field)
        self.partial = partial
        self.field = field


class _Resolution:
    def __init__(self) -> None:
        self.completed = Event()
        self.value = None
        self.error: BaseException | None = None
        self.completed_at: float | None = None
        self.thread: Thread | None = None


def _next_id(ids, lock: Lock) -> int:
    with lock:
        return next(ids)


def _validate_uuid(value: UUID) -> UUID:
    if not isinstance(value, UUID) or not value.int:
        raise TransportProtocolError("association UID must be a nonzero UUID")
    return value


def _timeout(value: float | None, field: str, *, allow_none: bool = True) -> float | None:
    if value is None:
        if allow_none:
            return None
        raise TransportProtocolError(f"{field} must be a finite positive number")
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError(f"{field} must be a finite nonnegative number")
    return float(value)


def _positive_timeout(value: float, field: str) -> float:
    result = _timeout(value, field, allow_none=False)
    assert result is not None
    if result == 0:
        raise TransportProtocolError(f"{field} must be positive")
    return result


def _positive_int(value: int, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise TransportProtocolError(f"{field} must be a positive integer")
    return value


def _nonnegative_int(value: int, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise TransportProtocolError(f"{field} must be a nonnegative integer")
    return value


def _deadline(timeout: float | None) -> float | None:
    return None if timeout is None else monotonic() + timeout


def _remaining(deadline: float | None) -> float | None:
    return None if deadline is None else max(0.0, deadline - monotonic())


def _batch_payload(records: list[TransportRecord]) -> bytes:
    return (
        records[0].payload
        if len(records) == 1
        else b"".join(record.payload for record in records)
    )


def _configure_socket(sock, timeout: float) -> None:
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.settimeout(timeout)


def _close_socket(sock) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass


class TcpConnection:
    """One multiplexed TCP stream with bounded, independently driven I/O."""

    def __init__(
        self,
        sock,
        limits: TransportLimits,
        association_uid: UUID,
        *,
        io_timeout: float = 0.2,
    ) -> None:
        if not isinstance(limits, TransportLimits):
            raise TransportProtocolError("limits must be a TransportLimits value")
        self._association_uid = _validate_uuid(association_uid)
        self._io_timeout = _positive_timeout(io_timeout, "socket I/O timeout")

        try:
            _configure_socket(sock, self._io_timeout)
        except OSError as error:
            _close_socket(sock)
            raise TransportConnectError("could not configure TCP connection") from error

        self._socket = sock
        self._limits = limits
        self._activated = False
        self._peer_accept_seen = False
        self._condition = Condition(Lock())
        self._writer_ready = Event()
        self._state = ConnectionState.OPEN
        self._cause = None
        self._maximum_record_bytes = limits.maximum_record_bytes

        self._outbound: dict[LogicalChannel, deque[TransportRecord]] = {}
        self._ready_channels: deque[LogicalChannel] = deque()
        self._writing_channel: LogicalChannel | None = None
        self._outbound_messages = 0
        self._outbound_bytes = 0
        self._terminal_outbound_record: TransportRecord | None = None
        self._terminal_outbound_pending = False

        self._inbound: deque[TransportRecord] = deque()
        self._inbound_messages = 0
        self._inbound_bytes = 0
        self._reading_record_size: int | None = None
        self._reader_backpressured = False
        self._inbound_waiters = 0
        self._receive_buffer = bytearray()
        self._receive_offset = 0
        self._unassigned_receive_bytes = 0
        self._reading_header_bytes = 0

        connection_id = _next_id(_CONNECTION_IDS, _CONNECTION_ID_LOCK)
        self._writer_thread = Thread(
            target=self._writer_loop,
            name=f"movie-tcp-connection-{connection_id}-writer",
            daemon=True,
        )
        self._reader_thread = Thread(
            target=self._reader_loop,
            name=f"movie-tcp-connection-{connection_id}-reader",
            daemon=True,
        )
        started: list[Thread] = []
        try:
            self._writer_thread.start()
            started.append(self._writer_thread)
            self._reader_thread.start()
            started.append(self._reader_thread)
        except BaseException:
            self._terminate(TransportClosedError("connection I/O threads did not start"))
            for thread in started:
                thread.join(self._io_timeout)
            raise

    @property
    def association_uid(self) -> UUID:
        return self._association_uid

    @property
    def state(self) -> ConnectionState:
        with self._condition:
            return self._state

    @property
    def cause(self):
        with self._condition:
            return self._cause

    def send(self, record: TransportRecord) -> None:
        if not isinstance(record, TransportRecord):
            raise TransportProtocolError("send requires a TransportRecord")

        with self._condition:
            maximum_record_bytes = self._maximum_record_bytes
        self._validate_record(record, maximum_record_bytes)

        record_bytes = len(record.payload)
        with self._condition:
            if self._state is ConnectionState.CLOSED:
                self._raise_closed_locked()
            if self._terminal_outbound_record is not None:
                raise TransportClosedError("terminal transport record is already admitted")
            if maximum_record_bytes != self._maximum_record_bytes:
                self._validate_record(record, self._maximum_record_bytes)
            if (
                self._outbound_messages >= self._limits.outbound_message_limit
                or self._outbound_bytes + record_bytes > self._limits.outbound_byte_limit
            ):
                raise TransportCapacityError("outbound transport capacity is full")
            self._enqueue_outbound_locked(record, record_bytes)

    def send_prevalidated(
        self,
        record: TransportRecord,
        message_limit: int,
        byte_limit: int,
    ) -> None:
        message_limit = _nonnegative_int(message_limit, "outbound message limit")
        byte_limit = _nonnegative_int(byte_limit, "outbound byte limit")
        record_bytes = len(record.payload)
        with self._condition:
            if self._state is ConnectionState.CLOSED:
                self._raise_closed_locked()
            if self._terminal_outbound_record is not None:
                raise TransportClosedError("terminal transport record is already admitted")
            if record_bytes > self._maximum_record_bytes:
                raise TransportProtocolError(
                    "prevalidated record exceeds maximum record bytes"
                )
            message_limit = min(message_limit, self._limits.outbound_message_limit)
            byte_limit = min(byte_limit, self._limits.outbound_byte_limit)
            if (
                self._outbound_messages >= message_limit
                or self._outbound_bytes + record_bytes > byte_limit
            ):
                raise TransportCapacityError("outbound transport capacity is full")
            self._enqueue_outbound_locked(record, record_bytes)

    def send_active(self, record: TransportRecord) -> None:
        record_bytes = len(record.payload)
        with self._condition:
            if self._state is ConnectionState.CLOSED:
                self._raise_closed_locked()
            if self._terminal_outbound_record is not None:
                raise TransportClosedError("terminal transport record is already admitted")
            if record_bytes > self._maximum_record_bytes:
                raise TransportProtocolError(
                    "prevalidated record exceeds maximum record bytes"
                )
            if (
                self._outbound_messages >= self._limits.outbound_message_limit
                or self._outbound_bytes + record_bytes > self._limits.outbound_byte_limit
            ):
                raise TransportCapacityError("outbound transport capacity is full")
            self._enqueue_outbound_locked(record, record_bytes)

    def send_terminal(self, record: TransportRecord) -> None:
        if not isinstance(record, TransportRecord):
            raise TransportProtocolError("send_terminal requires a TransportRecord")
        with self._condition:
            maximum_record_bytes = self._maximum_record_bytes
        header = self._validate_record(record, maximum_record_bytes)
        if (
            header.frame_type is not FrameType.GOAWAY
            or record.channel.kind is not StreamKind.CONTROL
        ):
            raise TransportProtocolError("terminal transport reserve accepts only GOAWAY")

        record_bytes = len(record.payload)
        with self._condition:
            if self._state is ConnectionState.CLOSED:
                self._raise_closed_locked()
            if self._terminal_outbound_record is not None:
                raise TransportCapacityError("terminal transport reserve is already in use")
            if maximum_record_bytes != self._maximum_record_bytes:
                header = self._validate_record(record, self._maximum_record_bytes)
                if (
                    header.frame_type is not FrameType.GOAWAY
                    or record.channel.kind is not StreamKind.CONTROL
                ):
                    raise TransportProtocolError(
                        "terminal transport reserve accepts only GOAWAY"
                    )
            self._terminal_outbound_record = record
            self._terminal_outbound_pending = True
            self._outbound_messages += 1
            self._outbound_bytes += record_bytes
            self._writer_ready.set()

    def _enqueue_outbound_locked(
        self,
        record: TransportRecord,
        record_bytes: int,
    ) -> None:
        writer_needs_signal = not self._ready_channels
        queue = self._outbound.get(record.channel)
        if queue is None:
            queue = deque()
            self._outbound[record.channel] = queue
            self._ready_channels.append(record.channel)
        queue.append(record)
        self._outbound_messages += 1
        self._outbound_bytes += record_bytes
        if writer_needs_signal:
            self._writer_ready.set()

    def receive(self, timeout: float | None = None) -> TransportRecord:
        return self.receive_many(1, timeout)[0]

    def receive_many(
        self,
        max_records: int,
        timeout: float | None = None,
    ) -> list[TransportRecord]:
        max_records = _positive_int(max_records, "receive batch size")
        wait_timeout = _timeout(timeout, "receive timeout")
        deadline = _deadline(wait_timeout)
        with self._condition:
            while True:
                if self._inbound:
                    count = min(max_records, len(self._inbound))
                    records = [self._inbound.popleft() for _ in range(count)]
                    self._inbound_messages -= count
                    self._inbound_bytes -= sum(len(record.payload) for record in records)
                    if self._reader_backpressured:
                        self._condition.notify_all()
                    return records
                if self._state is ConnectionState.READ_FAILED:
                    assert isinstance(
                        self._cause,
                        (TransportFlowControlError, TransportProtocolError),
                    )
                    raise self._cause
                if self._state is ConnectionState.CLOSED:
                    self._raise_closed_locked()
                remaining = _remaining(deadline)
                if remaining == 0:
                    raise TimeoutError("no transport record was received before the deadline")
                self._inbound_waiters += 1
                try:
                    self._condition.wait(remaining)
                finally:
                    self._inbound_waiters -= 1

    def activate(self, limits: TransportLimits) -> None:
        if not isinstance(limits, TransportLimits):
            raise TransportProtocolError("limits must be a TransportLimits value")

        with self._condition:
            if self._state is ConnectionState.CLOSED:
                self._raise_closed_locked()
            if self._activated:
                raise TransportProtocolError("transport connection limits are already active")
            self._limits = limits
            self._maximum_record_bytes = limits.maximum_record_bytes
            self._activated = True
            self._condition.notify_all()

    def set_maximum_record_bytes(self, maximum_record_bytes: int) -> None:
        if (
            not isinstance(maximum_record_bytes, int)
            or isinstance(maximum_record_bytes, bool)
            or maximum_record_bytes < COMMON_HEADER_SIZE
            or maximum_record_bytes > (1 << 32) - 1
        ):
            raise TransportProtocolError(
                f"maximum record bytes must be between {COMMON_HEADER_SIZE} and {(1 << 32) - 1}"
            )

        with self._condition:
            if self._state is ConnectionState.CLOSED:
                self._raise_closed_locked()
            if maximum_record_bytes > self._maximum_record_bytes:
                raise TransportProtocolError("maximum record bytes can only tighten")
            self._maximum_record_bytes = maximum_record_bytes
            self._condition.notify_all()

    def snapshot(self) -> TransportConnectionSnapshot:
        with self._condition:
            return TransportConnectionSnapshot(
                state=self._state,
                cause=self._cause,
                maximum_record_bytes=self._maximum_record_bytes,
                pending_outbound_messages=self._outbound_messages,
                pending_outbound_bytes=self._outbound_bytes,
                pending_inbound_messages=self._inbound_messages,
                pending_inbound_bytes=self._inbound_bytes,
            )

    def close(self, timeout: float | None = None) -> None:
        close_timeout = _timeout(timeout, "connection close timeout")
        deadline = _deadline(close_timeout)
        self._terminate(None)

        this_thread = current_thread()
        threads = (self._writer_thread, self._reader_thread)
        for thread in threads:
            if thread is this_thread:
                continue
            thread.join(_remaining(deadline))
        if any(thread is not this_thread and thread.is_alive() for thread in threads):
            raise TimeoutError("TCP connection I/O threads did not stop before the deadline")

    def _validate_record(self, record: TransportRecord, maximum_record_bytes: int):
        try:
            header = decode_common_header(
                record.payload,
                maximum_frame_bytes=maximum_record_bytes,
            )
        except (WireCodecError, ProtocolValidationError) as error:
            raise TransportProtocolError(f"invalid transport record: {error}") from error
        if len(record.payload) != header.total_length:
            raise TransportProtocolError(
                f"record has {len(record.payload)} bytes but declared {header.total_length}"
            )
        if (
            record.channel.kind is StreamKind.CONTROL
            and header.frame_type is FrameType.USER_MESSAGE
        ):
            raise TransportProtocolError("USER_MESSAGE cannot use a CONTROL logical channel")
        if (
            record.channel.kind is StreamKind.DELIVERY_LANE
            and header.frame_type is not FrameType.USER_MESSAGE
        ):
            raise TransportProtocolError(
                f"{header.frame_type.name} cannot use a DELIVERY_LANE logical channel"
            )
        return header

    def _raise_closed_locked(self) -> None:
        error = TransportClosedError(cause=self._cause)
        if self._cause is None:
            raise error
        raise error from self._cause

    def _terminate(self, cause) -> bool:
        with self._condition:
            if self._state is ConnectionState.CLOSED:
                return False
            self._state = ConnectionState.CLOSED
            if cause is not None or self._cause is None:
                self._cause = cause
            self._outbound.clear()
            self._ready_channels.clear()
            self._writing_channel = None
            self._outbound_messages = 0
            self._outbound_bytes = 0
            self._terminal_outbound_record = None
            self._terminal_outbound_pending = False
            self._inbound.clear()
            self._inbound_messages = 0
            self._inbound_bytes = 0
            self._reading_record_size = None
            self._unassigned_receive_bytes = 0
            self._reading_header_bytes = 0
            self._receive_buffer.clear()
            self._receive_offset = 0
            self._condition.notify_all()
            self._writer_ready.set()
        _close_socket(self._socket)
        return True

    def _fail_read(self, cause: TransportFlowControlError | TransportProtocolError) -> bool:
        with self._condition:
            if self._state is ConnectionState.CLOSED:
                return False
            self._state = ConnectionState.READ_FAILED
            self._cause = cause
            self._inbound_bytes -= (
                self._unassigned_receive_bytes + self._reading_header_bytes
            )
            self._unassigned_receive_bytes = 0
            self._reading_header_bytes = 0
            self._receive_buffer.clear()
            self._receive_offset = 0
            self._condition.notify_all()
            return True

    def _writer_loop(self) -> None:
        try:
            while True:
                self._writer_ready.wait()
                with self._condition:
                    if self._state is ConnectionState.CLOSED:
                        return

                    records: list[TransportRecord] = []
                    batch_bytes = 0
                    first_channel: LogicalChannel | None = None
                    while self._ready_channels and len(records) < _WRITE_BATCH_RECORDS:
                        channel = self._ready_channels[0]
                        record = self._outbound[channel][0]
                        if records and batch_bytes + len(record.payload) > _WRITE_BATCH_BYTES:
                            break
                        self._ready_channels.popleft()
                        queue = self._outbound[channel]
                        record = queue.popleft()
                        if queue:
                            self._ready_channels.append(channel)
                        else:
                            del self._outbound[channel]
                        if first_channel is None:
                            first_channel = channel
                        records.append(record)
                        batch_bytes += len(record.payload)

                    terminal = self._terminal_outbound_record
                    if (
                        not self._ready_channels
                        and self._terminal_outbound_pending
                        and terminal is not None
                        and len(records) < _WRITE_BATCH_RECORDS
                        and (
                            not records
                            or batch_bytes + len(terminal.payload) <= _WRITE_BATCH_BYTES
                        )
                    ):
                        records.append(terminal)
                        batch_bytes += len(terminal.payload)
                        self._terminal_outbound_pending = False

                    self._writing_channel = first_channel
                    if not self._ready_channels and not self._terminal_outbound_pending:
                        self._writer_ready.clear()
                    if not records:
                        continue

                payload = _batch_payload(records)
                self._socket.sendall(payload)
                with self._condition:
                    if self._state is ConnectionState.CLOSED:
                        return
                    self._writing_channel = None
                    self._outbound_messages -= len(records)
                    self._outbound_bytes -= batch_bytes
        except OSError as error:
            self._terminate(TransportClosedError("TCP record write failed", cause=error))
        except BaseException as error:
            self._terminate(
                TransportClosedError("TCP writer stopped unexpectedly", cause=error)
            )

    def _reader_loop(self) -> None:
        try:
            while True:
                with self._condition:
                    while (
                        self._peer_accept_seen
                        and not self._activated
                        and self._state is not ConnectionState.CLOSED
                    ):
                        self._condition.wait()
                    if self._state is ConnectionState.CLOSED:
                        return
                header_bytes = self._recv_exact(
                    COMMON_HEADER_SIZE,
                    "common frame header",
                    track_header_bytes=True,
                )
                with self._condition:
                    if self._state is ConnectionState.CLOSED:
                        raise _ConnectionStopped
                    assert self._reading_header_bytes == COMMON_HEADER_SIZE
                frame_length = int.from_bytes(header_bytes[:4], "big")
                if frame_length < COMMON_HEADER_SIZE - 4:
                    raise TransportProtocolError(
                        "inbound frame length is smaller than the common header"
                    )
                total_length = frame_length + 4
                terminal_record = header_bytes[4] == FrameType.GOAWAY

                with self._condition:
                    if self._state is ConnectionState.CLOSED:
                        return
                    if total_length > self._maximum_record_bytes:
                        raise TransportFlowControlError(
                            "inbound record exceeds maximum record bytes"
                        )
                    while True:
                        if total_length > self._maximum_record_bytes:
                            raise TransportFlowControlError(
                                "inbound record exceeds maximum record bytes"
                            )
                        admission_limits = self._limits
                        message_limit = admission_limits.inbound_message_limit + int(
                            terminal_record
                        )
                        byte_limit = admission_limits.inbound_byte_limit + (
                            self._maximum_record_bytes if terminal_record else 0
                        )
                        if total_length > byte_limit:
                            raise TransportFlowControlError(
                                "inbound record exceeds inbound byte limit"
                            )
                        if message_limit == 0:
                            raise TransportFlowControlError(
                                "inbound message limit does not admit records"
                            )
                        body_length = total_length - COMMON_HEADER_SIZE
                        prepaid_body_bytes = min(
                            body_length,
                            self._unassigned_receive_bytes,
                        )
                        additional_record_bytes = body_length - prepaid_body_bytes
                        if (
                            self._inbound_messages < message_limit
                            and self._inbound_bytes + additional_record_bytes
                            <= byte_limit
                        ):
                            break
                        if self._state is ConnectionState.CLOSED:
                            raise _ConnectionStopped
                        self._reader_backpressured = True
                        try:
                            self._condition.wait()
                        finally:
                            self._reader_backpressured = False
                    self._reading_record_size = total_length
                    self._reading_header_bytes = 0
                    self._inbound_messages += 1
                    self._inbound_bytes += additional_record_bytes
                    read_ahead_bytes = (
                        byte_limit - self._inbound_bytes if self._activated else 0
                    )

                enqueued = False
                try:
                    body = self._recv_exact(
                        total_length - COMMON_HEADER_SIZE,
                        "frame body",
                        read_ahead_bytes=read_ahead_bytes,
                    )
                    payload = header_bytes + body
                    record = TransportRecord(
                        LogicalChannel(StreamKind.MULTIPLEXED, CONTROL_LANE_ID),
                        payload,
                    )
                    with self._condition:
                        if self._state is ConnectionState.CLOSED:
                            raise _ConnectionStopped
                        if total_length > self._maximum_record_bytes:
                            raise TransportFlowControlError(
                                "inbound record exceeds maximum record bytes"
                            )
                        current_limits = self._limits
                        current_message_limit = (
                            current_limits.inbound_message_limit
                            + int(terminal_record)
                        )
                        current_byte_limit = current_limits.inbound_byte_limit + (
                            self._maximum_record_bytes if terminal_record else 0
                        )
                        if (
                            self._inbound_messages > current_message_limit
                            or self._inbound_bytes > current_byte_limit
                        ):
                            raise TransportFlowControlError(
                                "inbound record exceeds current inbound capacity"
                            )
                        self._reading_record_size = None
                        self._inbound.append(record)
                        if header_bytes[4] == FrameType.HELLO_ACCEPT:
                            self._peer_accept_seen = True
                        enqueued = True
                        if self._inbound_waiters:
                            self._condition.notify_all()
                finally:
                    if not enqueued:
                        with self._condition:
                            if self._reading_record_size is not None:
                                self._reading_record_size = None
                                self._inbound_messages -= 1
                                self._inbound_bytes -= total_length
                                self._condition.notify_all()
        except _ConnectionStopped:
            return
        except _PeerEOF as error:
            if error.partial or error.field != "common frame header":
                cause = TransportProtocolError(f"peer closed during {error.field}")
            else:
                cause = TransportClosedError("peer closed the TCP connection")
            self._terminate(cause)
        except TransportProtocolError as error:
            self._fail_read(error)
        except TransportFlowControlError as error:
            self._fail_read(error)
        except OSError as error:
            self._terminate(TransportClosedError("TCP record read failed", cause=error))
        except BaseException as error:
            self._terminate(TransportClosedError("TCP reader stopped unexpectedly", cause=error))

    def _recv_exact(
        self,
        size: int,
        field: str,
        *,
        read_ahead_bytes: int = 0,
        track_header_bytes: bool = False,
    ) -> bytes:
        with self._condition:
            prepaid_bytes = min(size, self._unassigned_receive_bytes)
            self._unassigned_receive_bytes -= prepaid_bytes
            if track_header_bytes:
                self._reading_header_bytes += prepaid_bytes
        while len(self._receive_buffer) - self._receive_offset < size:
            with self._condition:
                if self._state is ConnectionState.CLOSED:
                    raise _ConnectionStopped
            try:
                remaining = size - (
                    len(self._receive_buffer) - self._receive_offset
                )
                requested = min(
                    _RECEIVE_CHUNK_BYTES,
                    remaining + read_ahead_bytes,
                )
                chunk = self._socket.recv(requested)
            except TimeoutError:
                continue
            except OSError:
                with self._condition:
                    if self._state is ConnectionState.CLOSED:
                        raise _ConnectionStopped from None
                raise
            if not chunk:
                raise _PeerEOF(
                    partial=len(self._receive_buffer) > self._receive_offset,
                    field=field,
                )
            extra_bytes = max(0, len(chunk) - remaining)
            with self._condition:
                if self._state is ConnectionState.CLOSED:
                    raise _ConnectionStopped
                if track_header_bytes:
                    self._reading_header_bytes += len(chunk) - extra_bytes
                    self._inbound_bytes += len(chunk) - extra_bytes
                if extra_bytes:
                    self._unassigned_receive_bytes += extra_bytes
                    self._inbound_bytes += extra_bytes
                    read_ahead_bytes -= extra_bytes
                self._receive_buffer.extend(chunk)

        with self._condition:
            if self._state is ConnectionState.CLOSED:
                raise _ConnectionStopped
            start = self._receive_offset
            self._receive_offset += size
            data = bytes(self._receive_buffer[start : self._receive_offset])
            if self._receive_offset >= len(self._receive_buffer) // 2:
                del self._receive_buffer[: self._receive_offset]
                self._receive_offset = 0
            return data

class TcpListener:
    """TCP acceptor with a fixed handshake pool and bounded socket handoff."""

    def __init__(
        self,
        sock,
        endpoint: Endpoint,
        limits: TransportLimits,
        on_connection: Callable[[TransportConnection], None],
        *,
        handshake_workers: int,
        accepted_socket_limit: int,
        handshake_timeout: float,
        io_timeout: float,
    ) -> None:
        self._socket = sock
        self._endpoint = endpoint
        self._limits = limits
        self._on_connection = on_connection
        self._handshake_timeout = handshake_timeout
        self._io_timeout = io_timeout
        self._accepted: Queue = Queue(maxsize=accepted_socket_limit)
        self._stopping = Event()
        self._lock = Lock()
        self._active_sockets: set = set()

        listener_id = _next_id(_LISTENER_IDS, _LISTENER_ID_LOCK)
        self._accept_thread = Thread(
            target=self._accept_loop,
            name=f"movie-tcp-listener-{listener_id}-accept",
            daemon=True,
        )
        self._handshake_threads = tuple(
            Thread(
                target=self._handshake_loop,
                name=f"movie-tcp-listener-{listener_id}-handshake-{index}",
                daemon=True,
            )
            for index in range(handshake_workers)
        )

        started: list[Thread] = []
        try:
            for thread in self._handshake_threads:
                thread.start()
                started.append(thread)
            self._accept_thread.start()
            started.append(self._accept_thread)
        except BaseException:
            self._request_stop()
            for thread in started:
                thread.join(self._io_timeout)
            raise

    @property
    def endpoint(self) -> Endpoint:
        return self._endpoint

    @property
    def bound_endpoint(self) -> Endpoint:
        return self._endpoint

    def close(self, timeout: float | None = None) -> None:
        close_timeout = _timeout(timeout, "listener close timeout")
        deadline = _deadline(close_timeout)
        self._request_stop()

        this_thread = current_thread()
        threads = (self._accept_thread, *self._handshake_threads)
        for thread in threads:
            if thread is this_thread:
                continue
            thread.join(_remaining(deadline))
        if any(thread is not this_thread and thread.is_alive() for thread in threads):
            raise TimeoutError("TCP listener threads did not stop before the deadline")

    def _request_stop(self) -> None:
        with self._lock:
            self._stopping.set()
            active = tuple(self._active_sockets)
        _close_socket(self._socket)
        while True:
            try:
                accepted = self._accepted.get_nowait()
            except Empty:
                break
            _close_socket(accepted)
        for active_socket in active:
            _close_socket(active_socket)

    def _accept_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                accepted, _ = self._socket.accept()
            except TimeoutError:
                continue
            except OSError:
                if not self._stopping.is_set():
                    self._request_stop()
                return

            with self._lock:
                if self._stopping.is_set():
                    admit = False
                else:
                    try:
                        self._accepted.put_nowait(accepted)
                    except Full:
                        admit = False
                    else:
                        admit = True
            if not admit:
                _close_socket(accepted)

    def _handshake_loop(self) -> None:
        while True:
            if self._stopping.is_set() and self._accepted.empty():
                return
            try:
                accepted = self._accepted.get(timeout=self._io_timeout)
            except Empty:
                continue

            with self._lock:
                if self._stopping.is_set():
                    active = False
                else:
                    self._active_sockets.add(accepted)
                    active = True
            if not active:
                _close_socket(accepted)
                continue

            connection = None
            transferred = False
            try:
                _configure_socket(accepted, self._io_timeout)
                preamble = self._read_preamble(accepted)
                connection = TcpConnection(
                    accepted,
                    self._limits,
                    preamble.association_uid,
                    io_timeout=self._io_timeout,
                )
                self._on_connection(connection)
                transferred = True
            except BaseException:
                if connection is not None:
                    while True:
                        try:
                            connection.close(timeout=max(self._io_timeout * 4, 0.1))
                        except TimeoutError:
                            sleep(0.05)
                        else:
                            break
                else:
                    _close_socket(accepted)
            finally:
                with self._lock:
                    self._active_sockets.discard(accepted)
                if not transferred and connection is None:
                    _close_socket(accepted)

    def _read_preamble(self, accepted) -> StreamPreamble:
        deadline = monotonic() + self._handshake_timeout
        data = bytearray()
        while len(data) < PREAMBLE_SIZE:
            if self._stopping.is_set():
                raise TransportClosedError("TCP listener is closing")
            if monotonic() >= deadline:
                raise TransportProtocolError("TCP stream preamble timed out")
            try:
                chunk = accepted.recv(PREAMBLE_SIZE - len(data))
            except TimeoutError:
                continue
            if not chunk:
                raise TransportProtocolError("peer closed during TCP stream preamble")
            data.extend(chunk)
        try:
            return decode_preamble(
                bytes(data),
                expected_kind=StreamKind.MULTIPLEXED,
            )
        except WireCodecError as error:
            raise TransportProtocolError(f"invalid TCP stream preamble: {error}") from error


class TcpTransport:
    """Factory for bounded multiplexed TCP listeners and connections."""

    def __init__(
        self,
        *,
        handshake_workers: int = 4,
        accepted_socket_limit: int = 64,
        connect_timeout: float = 5.0,
        handshake_timeout: float = 5.0,
        io_timeout: float = 0.2,
    ) -> None:
        self._handshake_workers = _positive_int(handshake_workers, "handshake worker count")
        self._accepted_socket_limit = _positive_int(
            accepted_socket_limit,
            "accepted socket limit",
        )
        self._connect_timeout = _positive_timeout(connect_timeout, "TCP connect timeout")
        self._handshake_timeout = _positive_timeout(
            handshake_timeout,
            "TCP handshake timeout",
        )
        self._io_timeout = _positive_timeout(io_timeout, "socket I/O timeout")
        self._resolution_lock = Lock()
        self._resolutions: dict[tuple[Endpoint, bool], _Resolution] = {}
        self._closed = False

    def listen(
        self,
        endpoint: Endpoint,
        limits: TransportLimits,
        on_connection: Callable[[TransportConnection], None],
    ) -> TcpListener:
        self._ensure_open()
        self._validate_common(endpoint, limits)
        if not callable(on_connection):
            raise TransportProtocolError("on_connection must be callable")

        try:
            address = self._resolve_with_timeout(
                endpoint,
                passive=True,
                timeout=self._connect_timeout,
            )
            family, socktype, protocol, sockaddr = address
            listener_socket = socket.socket(family, socktype, protocol)
        except Exception as error:
            raise TransportListenError(
                f"could not resolve or create listener for {endpoint}"
            ) from error

        try:
            if os.name == "nt" and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                listener_socket.setsockopt(
                    socket.SOL_SOCKET,
                    socket.SO_EXCLUSIVEADDRUSE,
                    1,
                )
            else:
                listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener_socket.bind(sockaddr)
            listener_socket.listen(self._accepted_socket_limit + self._handshake_workers)
            listener_socket.settimeout(self._io_timeout)
            bound = listener_socket.getsockname()
            bound_endpoint = Endpoint(str(bound[0]), int(bound[1]))
            return TcpListener(
                listener_socket,
                bound_endpoint,
                limits,
                on_connection,
                handshake_workers=self._handshake_workers,
                accepted_socket_limit=self._accepted_socket_limit,
                handshake_timeout=self._handshake_timeout,
                io_timeout=self._io_timeout,
            )
        except (OSError, RuntimeError) as error:
            _close_socket(listener_socket)
            raise TransportListenError(f"could not listen at {endpoint}") from error

    def connect(
        self,
        endpoint: Endpoint,
        limits: TransportLimits,
        association_uid: UUID,
        timeout: float | None = None,
    ) -> TcpConnection:
        self._ensure_open()
        self._validate_common(endpoint, limits)
        association_uid = _validate_uuid(association_uid)
        requested_timeout = _timeout(timeout, "TCP connect timeout")
        connect_timeout = (
            self._connect_timeout
            if requested_timeout is None
            else min(requested_timeout, self._connect_timeout)
        )
        if endpoint.port == 0:
            raise TransportConnectError("cannot connect to endpoint port zero")

        deadline = monotonic() + connect_timeout
        try:
            family, socktype, protocol, sockaddr = self._resolve_with_timeout(
                endpoint,
                passive=False,
                timeout=max(0.0, deadline - monotonic()),
            )
            connection_socket = socket.socket(family, socktype, protocol)
        except Exception as error:
            if isinstance(error, TransportConnectError):
                raise
            raise TransportConnectError(
                f"could not resolve or create connection to {endpoint}"
            ) from error

        try:
            remaining = _remaining(deadline)
            if remaining == 0:
                raise TimeoutError("TCP endpoint resolution timed out")
            assert remaining is not None
            _configure_socket(connection_socket, remaining)
            connection_socket.connect(sockaddr)
            remaining = _remaining(deadline)
            if remaining == 0:
                raise TimeoutError("TCP connect and preamble send timed out")
            connection_socket.settimeout(remaining)
            connection_socket.sendall(
                encode_preamble(
                    StreamPreamble(
                        StreamKind.MULTIPLEXED,
                        association_uid,
                        CONTROL_LANE_ID,
                    )
                )
            )
            connection = TcpConnection(
                connection_socket,
                limits,
                association_uid,
                io_timeout=self._io_timeout,
            )
        except Exception as error:
            _close_socket(connection_socket)
            with self._resolution_lock:
                self._resolutions.pop((endpoint, False), None)
            if isinstance(error, TransportConnectError):
                raise
            raise TransportConnectError(f"could not connect to {endpoint}") from error
        return connection

    def _resolve_with_timeout(
        self,
        endpoint: Endpoint,
        *,
        passive: bool,
        timeout: float,
    ):
        key = (endpoint, passive)
        with self._resolution_lock:
            if self._closed:
                raise TransportClosedError("TCP transport is closed")
            resolution = self._resolutions.get(key)
            if (
                resolution is not None
                and resolution.completed.is_set()
                and (
                    resolution.error is not None
                    or resolution.completed_at is None
                    or monotonic() - resolution.completed_at > _RESOLUTION_TTL_SECONDS
                )
            ):
                self._resolutions.pop(key, None)
                resolution = None
            if resolution is None:
                if len(self._resolutions) >= 1_024:
                    completed = next(
                        (
                            cached_key
                            for cached_key, cached in self._resolutions.items()
                            if cached.completed.is_set()
                        ),
                        None,
                    )
                    if completed is None:
                        raise TransportConnectError(
                            "TCP resolution cache capacity is full"
                        )
                    self._resolutions.pop(completed, None)
                resolution = _Resolution()
                self._resolutions[key] = resolution
                thread = Thread(
                    target=self._resolve_endpoint,
                    args=(resolution, endpoint, passive),
                    name=f"movie-tcp-resolver-{_next_id(_CONNECTION_IDS, _CONNECTION_ID_LOCK)}",
                    daemon=True,
                )
                resolution.thread = thread
                try:
                    thread.start()
                except BaseException:
                    self._resolutions.pop(key, None)
                    raise
        if not resolution.completed.wait(timeout):
            raise TimeoutError("TCP endpoint resolution timed out")
        if resolution.error is not None:
            raise resolution.error
        return resolution.value

    def close(self, timeout: float | None = None) -> None:
        close_timeout = _timeout(timeout, "TCP transport close timeout")
        deadline = _deadline(close_timeout)
        with self._resolution_lock:
            self._closed = True
            threads = tuple(
                resolution.thread
                for resolution in self._resolutions.values()
                if resolution.thread is not None
            )
        this_thread = current_thread()
        for thread in threads:
            if thread is not this_thread:
                thread.join(_remaining(deadline))
        if any(thread is not this_thread and thread.is_alive() for thread in threads):
            raise TimeoutError("TCP resolver threads did not stop before the deadline")

    def _ensure_open(self) -> None:
        with self._resolution_lock:
            if self._closed:
                raise TransportClosedError("TCP transport is closed")

    @staticmethod
    def _resolve_endpoint(
        resolution: _Resolution,
        endpoint: Endpoint,
        passive: bool,
    ) -> None:
        try:
            resolution.value = TcpTransport._resolve(endpoint, passive=passive)
        except BaseException as error:
            resolution.error = error
        finally:
            resolution.completed_at = monotonic()
            resolution.completed.set()

    @staticmethod
    def _validate_common(endpoint: Endpoint, limits: TransportLimits) -> None:
        if not isinstance(endpoint, Endpoint):
            raise TransportProtocolError("endpoint must be an Endpoint value")
        if not isinstance(limits, TransportLimits):
            raise TransportProtocolError("limits must be a TransportLimits value")

    @staticmethod
    def _resolve(endpoint: Endpoint, *, passive: bool):
        flags = socket.AI_PASSIVE if passive else 0
        addresses = socket.getaddrinfo(
            None if passive and endpoint.host == "" else endpoint.host,
            endpoint.port,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
            proto=socket.IPPROTO_TCP,
            flags=flags,
        )
        if not addresses:
            raise OSError(f"no TCP address resolved for {endpoint.host}")
        family, socktype, protocol, _, sockaddr = addresses[0]
        return family, socktype, protocol, sockaddr


__all__ = ["TcpConnection", "TcpListener", "TcpTransport"]
