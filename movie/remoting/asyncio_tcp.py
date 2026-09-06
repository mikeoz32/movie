"""Bounded asyncio TCP backend for the synchronous remoting transport SPI."""

from __future__ import annotations

import asyncio
import math
import os
import socket
from collections import deque
from collections.abc import Callable, Coroutine
from queue import Empty, Full, Queue
from threading import Condition, Event, Lock, Thread, current_thread
from time import monotonic
from typing import Any
from uuid import UUID

from movie.io.asyncio import (
    AsyncioIOCapacityError,
    AsyncioIOExtension,
    AsyncioIOStateError,
)
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
_HANDOFF_CLOSE_ATTEMPT_SECONDS = 0.05


class _ConnectionStopped(Exception):
    pass


class _PeerEOF(Exception):
    def __init__(self, *, partial: bool, field: str) -> None:
        super().__init__(field)
        self.partial = partial
        self.field = field


def _validate_uuid(value: UUID) -> UUID:
    if not isinstance(value, UUID) or not value.int:
        raise TransportProtocolError("association UID must be a nonzero UUID")
    return value


def _timeout(value: float | None, field: str) -> float | None:
    if value is None:
        return None
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError(f"{field} must be a finite nonnegative number")
    return float(value)


def _positive_timeout(value: float, field: str) -> float:
    result = _timeout(value, field)
    if result is None or result == 0:
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


def _configure_connection_socket(sock: socket.socket) -> None:
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setblocking(False)


def _close_socket(sock: socket.socket) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass


def _create_task(
    loop: asyncio.AbstractEventLoop,
    coroutine: Coroutine[Any, Any, None],
) -> asyncio.Task[None]:
    try:
        return loop.create_task(coroutine)
    except BaseException:
        coroutine.close()
        raise


class _TaskStartupError(RuntimeError):
    def __init__(self, settled: asyncio.Future[None], cause: BaseException) -> None:
        super().__init__(str(cause))
        self.settled = settled


def _cancel_tasks_then_close(
    loop: asyncio.AbstractEventLoop,
    tasks: list[asyncio.Task[None]],
    sock: socket.socket,
) -> asyncio.Future[None]:
    settled = loop.create_future()
    remaining = len(tasks)
    if not tasks:
        _close_socket(sock)
        settled.set_result(None)
        return settled

    def task_done(task: asyncio.Task[None]) -> None:
        nonlocal remaining
        remaining -= 1
        if remaining == 0:
            _close_socket(sock)
            settled.set_result(None)

    for task in tasks:
        task.add_done_callback(task_done)
        task.cancel()
    return settled


class AsyncioTcpConnection:
    """One bounded TCP stream driven by two tasks on a shared asyncio loop."""

    def __init__(
        self,
        extension: AsyncioIOExtension,
        sock: socket.socket,
        limits: TransportLimits,
        association_uid: UUID,
        *,
        close_timeout: float = 5.0,
        on_close: Callable[[AsyncioTcpConnection], None] | None = None,
    ) -> None:
        if not isinstance(extension, AsyncioIOExtension):
            raise TransportProtocolError("extension must be an AsyncioIOExtension")
        if not isinstance(limits, TransportLimits):
            raise TransportProtocolError("limits must be a TransportLimits value")
        self._association_uid = _validate_uuid(association_uid)
        self._close_timeout = _positive_timeout(close_timeout, "Asyncio TCP close timeout")
        try:
            loop = asyncio.get_running_loop()
            _configure_connection_socket(sock)
        except (RuntimeError, OSError) as error:
            _close_socket(sock)
            raise TransportConnectError("could not configure Asyncio TCP connection") from error

        self._extension = extension
        self._loop = loop
        self._socket = sock
        self._on_close = on_close
        self._condition = Condition(Lock())
        self._unregistered = False
        self._cleanup_started = False
        self._cleanup_complete = Event()
        self._state = ConnectionState.OPEN
        self._cause = None
        self._limits = limits
        self._activated = False
        self._peer_accept_seen = False
        self._maximum_record_bytes = limits.maximum_record_bytes

        self._outbound: dict[LogicalChannel, deque[TransportRecord]] = {}
        self._ready_channels: deque[LogicalChannel] = deque()
        self._outbound_messages = 0
        self._outbound_bytes = 0
        self._terminal_outbound_record: TransportRecord | None = None
        self._terminal_outbound_pending = False
        self._writer_wake_scheduled = False

        self._inbound: deque[TransportRecord] = deque()
        self._inbound_messages = 0
        self._inbound_bytes = 0
        self._reading_record_size: int | None = None
        self._reading_header_bytes = 0
        self._reader_backpressured = False
        self._inbound_waiters = 0

        self._writer_ready = asyncio.Event()
        self._reader_ready = asyncio.Event()
        self._activation_ready = asyncio.Event()
        self._cleanup_complete_on_loop = asyncio.Event()
        self._writer_task = _create_task(loop, self._writer_loop())
        try:
            self._reader_task = _create_task(loop, self._reader_loop())
        except BaseException as error:
            settled = _cancel_tasks_then_close(loop, [self._writer_task], sock)
            raise _TaskStartupError(settled, error) from error

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
            if not self._ready_channels:
                self._wake_writer_locked()
            self._terminal_outbound_record = record
            self._terminal_outbound_pending = True
            self._outbound_messages += 1
            self._outbound_bytes += record_bytes

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
                    if self._reader_backpressured:
                        self._wake_loop_event_locked(self._reader_ready)
                    count = min(max_records, len(self._inbound))
                    records = [self._inbound.popleft() for _ in range(count)]
                    self._inbound_messages -= count
                    self._inbound_bytes -= sum(len(record.payload) for record in records)
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
            self._wake_loop_event_locked(self._activation_ready)
            if self._reader_backpressured:
                self._wake_loop_event_locked(self._reader_ready)

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
            if self._reader_backpressured:
                self._wake_loop_event_locked(self._reader_ready)

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
        requested_timeout = _timeout(timeout, "connection close timeout")
        close_timeout = self._close_timeout if requested_timeout is None else requested_timeout
        if self._extension.owns_current_thread():
            raise AsyncioIOStateError(
                "Asyncio TCP connection cannot synchronously close on the I/O loop"
            )
        self._mark_closed(None)
        if not self._claim_cleanup():
            if not self._wait_for_cleanup(close_timeout):
                raise TimeoutError(
                    "Asyncio TCP connection tasks did not stop before the deadline"
                )
            return
        try:
            self._extension.run_coroutine(
                self._perform_close_on_loop,
                timeout=close_timeout,
                cancel_on_timeout=False,
            )
        except TimeoutError as error:
            raise TimeoutError(
                "Asyncio TCP connection tasks did not stop before the deadline"
            ) from error
        except AsyncioIOCapacityError as error:
            if not self._schedule_close_on_loop():
                self._abandon_cleanup()
            raise TransportCapacityError(
                "Asyncio I/O control capacity is full"
            ) from error
        except AsyncioIOStateError as error:
            self._abandon_cleanup()
            if not self._extension.wait_stopped(close_timeout):
                raise TimeoutError(
                    "Asyncio I/O loop did not stop before the connection close deadline"
                ) from error
            self._close_without_loop(None)
            return

    def _enqueue_outbound_locked(
        self,
        record: TransportRecord,
        record_bytes: int,
    ) -> None:
        queue = self._outbound.get(record.channel)
        if queue is None:
            if not self._ready_channels and not self._terminal_outbound_pending:
                self._wake_writer_locked()
            queue = deque()
            self._outbound[record.channel] = queue
            self._ready_channels.append(record.channel)
        queue.append(record)
        self._outbound_messages += 1
        self._outbound_bytes += record_bytes

    def _wake_writer_locked(self) -> None:
        if self._writer_wake_scheduled:
            return
        self._writer_wake_scheduled = True
        try:
            self._loop.call_soon_threadsafe(self._writer_ready.set)
        except RuntimeError as error:
            self._writer_wake_scheduled = False
            raise TransportClosedError(
                "Asyncio I/O extension loop is not available",
                cause=error,
            ) from error

    def _wake_loop_event_locked(self, event: asyncio.Event) -> None:
        try:
            self._loop.call_soon_threadsafe(event.set)
        except RuntimeError as error:
            raise TransportClosedError(
                "Asyncio I/O extension loop is not available",
                cause=error,
            ) from error

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

    def _mark_closed(self, cause) -> bool:
        with self._condition:
            if self._state is ConnectionState.CLOSED:
                return False
            self._state = ConnectionState.CLOSED
            if cause is not None or self._cause is None:
                self._cause = cause
            self._outbound.clear()
            self._ready_channels.clear()
            self._outbound_messages = 0
            self._outbound_bytes = 0
            self._terminal_outbound_record = None
            self._terminal_outbound_pending = False
            self._writer_wake_scheduled = False
            self._inbound.clear()
            self._inbound_messages = 0
            self._inbound_bytes = 0
            self._reading_record_size = None
            self._reading_header_bytes = 0
            self._reader_backpressured = False
            self._condition.notify_all()
        return True

    def _finish_registration(self) -> None:
        with self._condition:
            if self._unregistered:
                return
            self._unregistered = True
        if self._on_close is not None:
            self._on_close(self)

    def _close_without_loop(self, cause: TransportClosedError | None) -> None:
        self._mark_closed(cause)
        _close_socket(self._socket)
        self._finish_registration()
        self._cleanup_complete.set()

    def _claim_cleanup(self) -> bool:
        with self._condition:
            if self._cleanup_complete.is_set() or self._cleanup_started:
                return False
            self._cleanup_started = True
            return True

    def _abandon_cleanup(self) -> None:
        with self._condition:
            if not self._cleanup_complete.is_set():
                self._cleanup_started = False
                self._condition.notify_all()

    def _wait_for_cleanup(self, timeout: float) -> bool:
        deadline = monotonic() + timeout
        while not self._cleanup_complete.is_set():
            if self._extension.wait_stopped(0.0):
                self._abandon_cleanup()
                self._close_without_loop(None)
                return True
            remaining = deadline - monotonic()
            if remaining <= 0:
                return False
            self._cleanup_complete.wait(min(0.01, remaining))
        return True

    def _fail_read(
        self,
        cause: TransportFlowControlError | TransportProtocolError,
    ) -> bool:
        with self._condition:
            if self._state is ConnectionState.CLOSED:
                return False
            self._state = ConnectionState.READ_FAILED
            self._cause = cause
            self._inbound_bytes -= self._reading_header_bytes
            self._reading_header_bytes = 0
            self._reader_backpressured = False
            self._condition.notify_all()
            return True

    async def _close_on_loop(self) -> None:
        self._mark_closed(None)
        if not self._claim_cleanup():
            await self._cleanup_complete_on_loop.wait()
            return
        await self._perform_close_on_loop()

    async def _perform_close_on_loop(self) -> None:
        try:
            current = asyncio.current_task()
            tasks = tuple(
                task
                for task in (self._reader_task, self._writer_task)
                if task is not current and not task.done()
            )
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            _close_socket(self._socket)
            self._finish_registration()
            self._cleanup_complete.set()
            self._cleanup_complete_on_loop.set()

    def _schedule_close_on_loop(self) -> bool:
        def start_cleanup() -> None:
            try:
                _create_task(self._loop, self._perform_close_on_loop())
            except BaseException:
                self._abandon_cleanup()

        try:
            self._loop.call_soon_threadsafe(start_cleanup)
        except RuntimeError:
            return False
        return True

    async def _close_from_loop(self, cause: TransportClosedError) -> None:
        self._mark_closed(cause)
        if not self._claim_cleanup():
            await self._cleanup_complete_on_loop.wait()
            return
        await self._perform_close_on_loop()

    async def _writer_loop(self) -> None:
        try:
            while True:
                await self._writer_ready.wait()
                self._writer_ready.clear()
                while True:
                    with self._condition:
                        if self._state is ConnectionState.CLOSED:
                            return
                        records: list[TransportRecord] = []
                        batch_bytes = 0
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
                        if not records:
                            self._writer_wake_scheduled = False
                            break

                    await self._loop.sock_sendall(self._socket, _batch_payload(records))
                    with self._condition:
                        if self._state is ConnectionState.CLOSED:
                            return
                        self._outbound_messages -= len(records)
                        self._outbound_bytes -= batch_bytes
        except asyncio.CancelledError:
            raise
        except OSError as error:
            await self._close_from_loop(
                TransportClosedError("Asyncio TCP record write failed", cause=error)
            )
        except BaseException as error:
            await self._close_from_loop(
                TransportClosedError("Asyncio TCP writer stopped unexpectedly", cause=error)
            )

    async def _reader_loop(self) -> None:
        try:
            while True:
                await self._wait_for_activation()
                header_bytes = await self._recv_exact(
                    COMMON_HEADER_SIZE,
                    "common frame header",
                    track_header_bytes=True,
                )
                frame_length = int.from_bytes(header_bytes[:4], "big")
                if frame_length < COMMON_HEADER_SIZE - 4:
                    raise TransportProtocolError(
                        "inbound frame length is smaller than the common header"
                    )
                total_length = frame_length + 4
                terminal_record = header_bytes[4] == FrameType.GOAWAY
                await self._reserve_inbound(total_length, terminal_record)

                enqueued = False
                try:
                    body = await self._recv_exact(
                        total_length - COMMON_HEADER_SIZE,
                        "frame body",
                    )
                    record = TransportRecord(
                        LogicalChannel(StreamKind.MULTIPLEXED, CONTROL_LANE_ID),
                        header_bytes + body,
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
                            current_limits.inbound_message_limit + int(terminal_record)
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
                        if header_bytes[4] == FrameType.HELLO_ACCEPT:
                            self._peer_accept_seen = True
                        self._inbound.append(record)
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
        except asyncio.CancelledError:
            raise
        except _ConnectionStopped:
            return
        except _PeerEOF as error:
            if error.partial or error.field != "common frame header":
                cause = TransportProtocolError(f"peer closed during {error.field}")
            else:
                cause = TransportClosedError("peer closed the Asyncio TCP connection")
            await self._close_from_loop(cause)
        except TransportProtocolError as error:
            self._fail_read(error)
        except TransportFlowControlError as error:
            self._fail_read(error)
        except OSError as error:
            await self._close_from_loop(
                TransportClosedError("Asyncio TCP record read failed", cause=error)
            )
        except BaseException as error:
            await self._close_from_loop(
                TransportClosedError("Asyncio TCP reader stopped unexpectedly", cause=error)
            )

    async def _wait_for_activation(self) -> None:
        while True:
            with self._condition:
                if self._state is ConnectionState.CLOSED:
                    raise _ConnectionStopped
                if not self._peer_accept_seen or self._activated:
                    return
                self._activation_ready.clear()
            await self._activation_ready.wait()

    async def _reserve_inbound(self, total_length: int, terminal_record: bool) -> None:
        while True:
            with self._condition:
                if self._state is ConnectionState.CLOSED:
                    raise _ConnectionStopped
                if total_length > self._maximum_record_bytes:
                    raise TransportFlowControlError(
                        "inbound record exceeds maximum record bytes"
                    )
                admission_limits = self._limits
                message_limit = admission_limits.inbound_message_limit + int(terminal_record)
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
                if (
                    self._inbound_messages < message_limit
                    and self._inbound_bytes + body_length <= byte_limit
                ):
                    self._reading_record_size = total_length
                    self._reading_header_bytes = 0
                    self._inbound_messages += 1
                    self._inbound_bytes += body_length
                    return
                self._reader_backpressured = True
                self._reader_ready.clear()
            await self._reader_ready.wait()
            with self._condition:
                self._reader_backpressured = False

    async def _recv_exact(
        self,
        size: int,
        field: str,
        *,
        track_header_bytes: bool = False,
    ) -> bytes:
        data = bytearray()
        while len(data) < size:
            with self._condition:
                if self._state is ConnectionState.CLOSED:
                    raise _ConnectionStopped
            chunk = await self._loop.sock_recv(
                self._socket,
                min(_RECEIVE_CHUNK_BYTES, size - len(data)),
            )
            if not chunk:
                raise _PeerEOF(partial=bool(data), field=field)
            if track_header_bytes:
                with self._condition:
                    if self._state is ConnectionState.CLOSED:
                        raise _ConnectionStopped
                    self._reading_header_bytes += len(chunk)
                    self._inbound_bytes += len(chunk)
            data.extend(chunk)
        return bytes(data)


class AsyncioTcpListener:
    """Bounded raw-socket acceptor with an off-loop callback handoff."""

    def __init__(
        self,
        extension: AsyncioIOExtension,
        sock: socket.socket,
        endpoint: Endpoint,
        limits: TransportLimits,
        on_connection: Callable[[TransportConnection], None],
        *,
        handshake_workers: int,
        accepted_socket_limit: int,
        handshake_timeout: float,
        close_timeout: float,
        create_connection: Callable[[socket.socket, TransportLimits, UUID], AsyncioTcpConnection],
        on_close: Callable[[AsyncioTcpListener], None],
    ) -> None:
        self._extension = extension
        self._loop = asyncio.get_running_loop()
        self._socket = sock
        self._endpoint = endpoint
        self._limits = limits
        self._on_connection = on_connection
        self._handshake_timeout = handshake_timeout
        self._close_timeout = close_timeout
        self._create_connection = create_connection
        self._on_close = on_close
        self._accepted: asyncio.Queue[socket.socket] = asyncio.Queue(
            maxsize=accepted_socket_limit
        )
        self._active_sockets: set[socket.socket] = set()
        self._handoff: Queue[AsyncioTcpConnection] = Queue(maxsize=accepted_socket_limit)
        self._handoff_stopping = Event()
        self._state_lock = Lock()
        self._handoff_deadline: float | None = None
        self._closed = False
        self._unregistered = False
        self._cleanup_started = False
        self._cleanup_complete = Event()
        self._cleanup_complete_on_loop = asyncio.Event()
        self._failure: TransportListenError | None = None
        self._failure_callback: Callable[[BaseException], None] | None = None
        self._handoff_thread = Thread(
            target=self._handoff_loop,
            name=f"movie-asyncio-tcp-listener-{endpoint.port}-handoff",
            daemon=True,
        )
        tasks: list[asyncio.Task[None]] = []
        try:
            self._accept_task = _create_task(self._loop, self._accept_loop())
            tasks.append(self._accept_task)
            handshake_tasks: list[asyncio.Task[None]] = []
            for _ in range(handshake_workers):
                task = _create_task(self._loop, self._handshake_loop())
                handshake_tasks.append(task)
                tasks.append(task)
            self._handshake_tasks = tuple(handshake_tasks)
            self._handoff_thread.start()
        except BaseException as error:
            self._request_handoff_stop()
            settled = _cancel_tasks_then_close(self._loop, tasks, sock)
            raise _TaskStartupError(settled, error) from error

    @property
    def endpoint(self) -> Endpoint:
        return self._endpoint

    @property
    def bound_endpoint(self) -> Endpoint:
        return self._endpoint

    def close(self, timeout: float | None = None) -> None:
        requested_timeout = _timeout(timeout, "listener close timeout")
        close_timeout = self._close_timeout if requested_timeout is None else requested_timeout
        if self._extension.owns_current_thread():
            raise AsyncioIOStateError(
                "Asyncio TCP listener cannot synchronously close on the I/O loop"
            )
        deadline = _deadline(close_timeout)
        self._request_handoff_stop(deadline)
        self._mark_closed()
        if self._claim_cleanup():
            try:
                self._extension.run_coroutine(
                    self._perform_close_on_loop,
                    timeout=_remaining(deadline),
                    cancel_on_timeout=False,
                )
            except TimeoutError as error:
                raise TimeoutError(
                    "Asyncio TCP listener tasks did not stop before the deadline"
                ) from error
            except AsyncioIOCapacityError as error:
                if not self._schedule_close_on_loop():
                    self._abandon_cleanup()
                raise TransportCapacityError(
                    "Asyncio I/O control capacity is full"
                ) from error
            except AsyncioIOStateError as error:
                self._abandon_cleanup()
                if not self._extension.wait_stopped(_remaining(deadline)):
                    raise TimeoutError(
                        "Asyncio I/O loop did not stop before the listener close deadline"
                    ) from error
                self._close_without_loop()
        elif not self._wait_for_cleanup(_remaining(deadline)):
            raise TimeoutError(
                "Asyncio TCP listener tasks did not stop before the deadline"
            )
        self._join_handoff(_remaining(deadline))
        self._finish_registration()

    def set_failure_callback(
        self,
        callback: Callable[[BaseException], None],
    ) -> None:
        if not callable(callback):
            raise TransportProtocolError("listener failure callback must be callable")
        with self._state_lock:
            self._failure_callback = callback
            failure = self._failure
        if failure is not None:
            callback(failure)

    def _record_failure(self, error: BaseException) -> None:
        failure = TransportListenError(
            f"Asyncio TCP listener failed at {self._endpoint}: {error}"
        )
        failure.__cause__ = error
        with self._state_lock:
            if self._closed or self._failure is not None:
                return
            self._failure = failure
            callback = self._failure_callback
        if callback is not None:
            try:
                callback(failure)
            except BaseException:
                pass

    def _request_handoff_stop(self, deadline: float | None = None) -> None:
        if deadline is not None:
            with self._state_lock:
                if (
                    self._handoff_deadline is None
                    or self._handoff_deadline <= monotonic()
                ):
                    self._handoff_deadline = deadline
                else:
                    self._handoff_deadline = min(self._handoff_deadline, deadline)
        self._handoff_stopping.set()

    def _join_handoff(self, timeout: float | None) -> None:
        if self._handoff_thread is current_thread():
            return
        self._handoff_thread.join(timeout)
        if self._handoff_thread.is_alive():
            raise TimeoutError("Asyncio TCP listener handoff did not stop before the deadline")

    def _mark_closed(self) -> bool:
        with self._state_lock:
            if self._closed:
                return False
            self._closed = True
        self._request_handoff_stop()
        return True

    def _finish_registration(self) -> None:
        with self._state_lock:
            if self._unregistered:
                return
            self._unregistered = True
        self._on_close(self)

    def _close_without_loop(self) -> None:
        self._mark_closed()
        _close_socket(self._socket)
        while True:
            try:
                accepted = self._accepted.get_nowait()
            except asyncio.QueueEmpty:
                break
            _close_socket(accepted)
        for active in tuple(self._active_sockets):
            _close_socket(active)
        while True:
            try:
                connection = self._handoff.get_nowait()
            except Empty:
                break
            connection._close_without_loop(
                TransportClosedError("Asyncio TCP listener is closed")
            )
        self._cleanup_complete.set()

    def _claim_cleanup(self) -> bool:
        with self._state_lock:
            if self._cleanup_complete.is_set() or self._cleanup_started:
                return False
            self._cleanup_started = True
            return True

    def _abandon_cleanup(self) -> None:
        with self._state_lock:
            if not self._cleanup_complete.is_set():
                self._cleanup_started = False

    def _wait_for_cleanup(self, timeout: float) -> bool:
        deadline = monotonic() + timeout
        while not self._cleanup_complete.is_set():
            if self._extension.wait_stopped(0.0):
                self._abandon_cleanup()
                self._close_without_loop()
                return True
            remaining = deadline - monotonic()
            if remaining <= 0:
                return False
            self._cleanup_complete.wait(min(0.01, remaining))
        return True

    async def _close_on_loop(self) -> None:
        self._mark_closed()
        if not self._claim_cleanup():
            await self._cleanup_complete_on_loop.wait()
            return
        await self._perform_close_on_loop()

    async def _perform_close_on_loop(self) -> None:
        try:
            current = asyncio.current_task()
            tasks = tuple(
                task
                for task in (self._accept_task, *self._handshake_tasks)
                if task is not current and not task.done()
            )
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            _close_socket(self._socket)
            while True:
                try:
                    accepted = self._accepted.get_nowait()
                except asyncio.QueueEmpty:
                    break
                _close_socket(accepted)
            for active in tuple(self._active_sockets):
                _close_socket(active)
            while True:
                try:
                    connection = self._handoff.get_nowait()
                except Empty:
                    break
                await connection._close_from_loop(
                    TransportClosedError("Asyncio TCP listener is closed")
                )
        finally:
            _close_socket(self._socket)
            self._cleanup_complete.set()
            self._cleanup_complete_on_loop.set()

    def _schedule_close_on_loop(self) -> bool:
        def start_cleanup() -> None:
            try:
                _create_task(self._loop, self._perform_close_on_loop())
            except BaseException:
                self._abandon_cleanup()

        try:
            self._loop.call_soon_threadsafe(start_cleanup)
        except RuntimeError:
            return False
        return True

    async def _close_from_loop(self) -> None:
        await self._close_on_loop()

    async def _accept_loop(self) -> None:
        try:
            while True:
                accepted, _ = await self._loop.sock_accept(self._socket)
                try:
                    _configure_connection_socket(accepted)
                    self._accepted.put_nowait(accepted)
                except (OSError, asyncio.QueueFull):
                    _close_socket(accepted)
        except asyncio.CancelledError:
            with self._state_lock:
                closing = self._closed
            if not closing:
                self._record_failure(
                    RuntimeError("Asyncio TCP listener accept task was cancelled")
                )
                await self._close_from_loop()
            raise
        except BaseException as error:
            self._record_failure(error)
            await self._close_from_loop()

    async def _handshake_loop(self) -> None:
        try:
            while True:
                accepted = await self._accepted.get()
                self._active_sockets.add(accepted)
                connection = None
                try:
                    async with asyncio.timeout(self._handshake_timeout):
                        preamble = await self._read_preamble(accepted)
                    connection = self._create_connection(
                        accepted,
                        self._limits,
                        preamble.association_uid,
                    )
                    self._handoff.put_nowait(connection)
                    connection = None
                except (TimeoutError, TransportProtocolError, Full, OSError):
                    if connection is not None:
                        await connection._close_from_loop(
                            TransportClosedError("Asyncio TCP listener handoff is full")
                        )
                    else:
                        _close_socket(accepted)
                except _TaskStartupError as error:
                    await asyncio.shield(error.settled)
                except asyncio.CancelledError:
                    if connection is not None:
                        await connection._close_from_loop(
                            TransportClosedError("Asyncio TCP listener is closing")
                        )
                    else:
                        _close_socket(accepted)
                    raise
                finally:
                    self._active_sockets.discard(accepted)
        except asyncio.CancelledError:
            with self._state_lock:
                closing = self._closed
            if not closing:
                self._record_failure(
                    RuntimeError("Asyncio TCP listener handshake task was cancelled")
                )
                await self._close_from_loop()
            raise
        except BaseException as error:
            self._record_failure(error)
            await self._close_from_loop()

    async def _read_preamble(self, accepted: socket.socket) -> StreamPreamble:
        data = bytearray()
        while len(data) < PREAMBLE_SIZE:
            chunk = await self._loop.sock_recv(accepted, PREAMBLE_SIZE - len(data))
            if not chunk:
                raise TransportProtocolError("peer closed during Asyncio TCP stream preamble")
            data.extend(chunk)
        try:
            return decode_preamble(
                bytes(data),
                expected_kind=StreamKind.MULTIPLEXED,
            )
        except WireCodecError as error:
            raise TransportProtocolError(
                f"invalid Asyncio TCP stream preamble: {error}"
            ) from error

    def _handoff_loop(self) -> None:
        while not self._handoff_stopping.is_set() or not self._handoff.empty():
            try:
                connection = self._handoff.get(timeout=0.05)
            except Empty:
                continue
            if self._handoff_stopping.is_set():
                self._close_handoff_connection(connection)
                continue
            try:
                self._on_connection(connection)
            except BaseException:
                self._close_handoff_connection(connection)

    def _close_handoff_connection(self, connection: AsyncioTcpConnection) -> None:
        with self._state_lock:
            deadline = self._handoff_deadline
        close_timeout = min(self._close_timeout, _HANDOFF_CLOSE_ATTEMPT_SECONDS)
        if deadline is not None:
            remaining = deadline - monotonic()
            if remaining <= 0:
                return
            close_timeout = min(close_timeout, remaining)
        try:
            connection.close(timeout=close_timeout)
        except (TransportCapacityError, TransportClosedError, TimeoutError):
            pass


class AsyncioTcpTransport:
    """Raw-socket TCP transport using an actor system's asyncio extension."""

    def __init__(
        self,
        extension: AsyncioIOExtension,
        *,
        handshake_workers: int = 4,
        accepted_socket_limit: int = 64,
        connect_timeout: float = 5.0,
        handshake_timeout: float = 5.0,
        close_timeout: float = 5.0,
    ) -> None:
        if not isinstance(extension, AsyncioIOExtension):
            raise TransportProtocolError("extension must be an AsyncioIOExtension")
        self._extension = extension
        self._handshake_workers = _positive_int(handshake_workers, "handshake worker count")
        self._accepted_socket_limit = _positive_int(
            accepted_socket_limit,
            "accepted socket limit",
        )
        self._connect_timeout = _positive_timeout(
            connect_timeout,
            "Asyncio TCP connect timeout",
        )
        self._handshake_timeout = _positive_timeout(
            handshake_timeout,
            "Asyncio TCP handshake timeout",
        )
        self._close_timeout = _positive_timeout(
            close_timeout,
            "Asyncio TCP close timeout",
        )
        self._lock = Lock()
        self._closed = False
        self._listeners: set[AsyncioTcpListener] = set()
        self._connections: set[AsyncioTcpConnection] = set()
        self._cleanup_started = False
        self._cleanup_complete = Event()

    def listen(
        self,
        endpoint: Endpoint,
        limits: TransportLimits,
        on_connection: Callable[[TransportConnection], None],
    ) -> AsyncioTcpListener:
        self._ensure_open()
        self._validate_common(endpoint, limits)
        if not callable(on_connection):
            raise TransportProtocolError("on_connection must be callable")
        try:
            return self._extension.run_coroutine(
                lambda: self._listen_on_loop(endpoint, limits, on_connection),
                timeout=self._connect_timeout + 1.0,
            )
        except (TransportClosedError, TransportListenError):
            raise
        except BaseException as error:
            raise TransportListenError(f"could not listen at {endpoint}") from error

    def connect(
        self,
        endpoint: Endpoint,
        limits: TransportLimits,
        association_uid: UUID,
        timeout: float | None = None,
    ) -> AsyncioTcpConnection:
        self._ensure_open()
        self._validate_common(endpoint, limits)
        association_uid = _validate_uuid(association_uid)
        requested_timeout = _timeout(timeout, "Asyncio TCP connect timeout")
        connect_timeout = (
            self._connect_timeout
            if requested_timeout is None
            else min(requested_timeout, self._connect_timeout)
        )
        if endpoint.port == 0:
            raise TransportConnectError("cannot connect to endpoint port zero")
        try:
            return self._extension.run_coroutine(
                lambda: self._connect_on_loop(
                    endpoint,
                    limits,
                    association_uid,
                    connect_timeout,
                ),
                timeout=connect_timeout + 1.0,
            )
        except (TransportClosedError, TransportConnectError):
            raise
        except BaseException as error:
            raise TransportConnectError(f"could not connect to {endpoint}") from error

    def close(self, timeout: float | None = None) -> None:
        requested_timeout = _timeout(timeout, "transport close timeout")
        close_timeout = self._close_timeout if requested_timeout is None else requested_timeout
        if self._extension.owns_current_thread():
            raise AsyncioIOStateError(
                "Asyncio TCP transport cannot synchronously close on the I/O loop"
            )
        deadline = _deadline(close_timeout)
        with self._lock:
            if self._closed:
                listeners = tuple(self._listeners)
                connections = tuple(self._connections)
            else:
                self._closed = True
                listeners = tuple(self._listeners)
                connections = tuple(self._connections)
            cleanup_owner = not self._cleanup_started and not self._cleanup_complete.is_set()
            if cleanup_owner:
                self._cleanup_started = True
        for listener in listeners:
            listener._request_handoff_stop(deadline)
        if cleanup_owner:
            try:
                self._extension.run_coroutine(
                    lambda: self._perform_close_all_on_loop(listeners, connections),
                    timeout=_remaining(deadline),
                    cancel_on_timeout=False,
                )
            except TimeoutError as error:
                raise TimeoutError(
                    "Asyncio TCP transport resources did not stop before the deadline"
                ) from error
            except AsyncioIOCapacityError as error:
                if not self._schedule_close_on_loop(listeners, connections):
                    self._abandon_cleanup()
                raise TransportCapacityError(
                    "Asyncio I/O control capacity is full"
                ) from error
            except AsyncioIOStateError as error:
                self._abandon_cleanup()
                if not self._extension.wait_stopped(_remaining(deadline)):
                    raise TimeoutError(
                        "Asyncio I/O loop did not stop before the transport close deadline"
                    ) from error
                for listener in listeners:
                    listener._close_without_loop()
                for connection in connections:
                    connection._close_without_loop(
                        TransportClosedError(
                            "Asyncio I/O extension is not running",
                            cause=error,
                        )
                    )
                self._cleanup_complete.set()
        elif not self._wait_for_cleanup(
            listeners,
            connections,
            _remaining(deadline),
        ):
            raise TimeoutError(
                "Asyncio TCP transport resources did not stop before the deadline"
            )
        for listener in listeners:
            listener._join_handoff(_remaining(deadline))
            listener._finish_registration()

    async def _listen_on_loop(
        self,
        endpoint: Endpoint,
        limits: TransportLimits,
        on_connection: Callable[[TransportConnection], None],
    ) -> AsyncioTcpListener:
        listener_socket = None
        listener = None
        try:
            async with asyncio.timeout(self._connect_timeout):
                family, socktype, protocol, sockaddr = await self._resolve(
                    endpoint,
                    passive=True,
                )
                listener_socket = socket.socket(family, socktype, protocol)
                if os.name == "nt" and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                    listener_socket.setsockopt(
                        socket.SOL_SOCKET,
                        socket.SO_EXCLUSIVEADDRUSE,
                        1,
                    )
                else:
                    listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener_socket.setblocking(False)
                listener_socket.bind(sockaddr)
                listener_socket.listen(
                    self._accepted_socket_limit + self._handshake_workers
                )
                bound = listener_socket.getsockname()
                bound_endpoint = Endpoint(str(bound[0]), int(bound[1]))
                listener = AsyncioTcpListener(
                    self._extension,
                    listener_socket,
                    bound_endpoint,
                    limits,
                    on_connection,
                    handshake_workers=self._handshake_workers,
                    accepted_socket_limit=self._accepted_socket_limit,
                    handshake_timeout=self._handshake_timeout,
                    close_timeout=self._close_timeout,
                    create_connection=self._create_connection_on_loop,
                    on_close=self._forget_listener,
                )
                listener_socket = None
                if not self._track_listener(listener):
                    await listener._close_on_loop()
                    raise TransportClosedError("Asyncio TCP transport is closed")
                return listener
        except TransportClosedError:
            raise
        except _TaskStartupError as error:
            listener_socket = None
            await asyncio.shield(error.settled)
            raise TransportListenError(f"could not listen at {endpoint}") from error
        except BaseException as error:
            if listener is not None:
                await listener._close_on_loop()
            if listener_socket is not None:
                _close_socket(listener_socket)
            raise TransportListenError(f"could not listen at {endpoint}") from error

    async def _connect_on_loop(
        self,
        endpoint: Endpoint,
        limits: TransportLimits,
        association_uid: UUID,
        timeout: float,
    ) -> AsyncioTcpConnection:
        connection_socket = None
        connection = None
        try:
            async with asyncio.timeout(timeout):
                family, socktype, protocol, sockaddr = await self._resolve(
                    endpoint,
                    passive=False,
                )
                connection_socket = socket.socket(family, socktype, protocol)
                _configure_connection_socket(connection_socket)
                loop = asyncio.get_running_loop()
                await loop.sock_connect(connection_socket, sockaddr)
                await loop.sock_sendall(
                    connection_socket,
                    encode_preamble(
                        StreamPreamble(
                            StreamKind.MULTIPLEXED,
                            association_uid,
                            CONTROL_LANE_ID,
                        )
                    ),
                )
                connection = self._create_connection_on_loop(
                    connection_socket,
                    limits,
                    association_uid,
                )
                connection_socket = None
                return connection
        except BaseException as error:
            if isinstance(error, _TaskStartupError):
                connection_socket = None
                await asyncio.shield(error.settled)
            if connection is not None:
                await connection._close_on_loop()
            if connection_socket is not None:
                _close_socket(connection_socket)
            if isinstance(error, (TransportClosedError, TransportConnectError)):
                raise
            raise TransportConnectError(f"could not connect to {endpoint}") from error

    def _create_connection_on_loop(
        self,
        sock: socket.socket,
        limits: TransportLimits,
        association_uid: UUID,
    ) -> AsyncioTcpConnection:
        connection = AsyncioTcpConnection(
            self._extension,
            sock,
            limits,
            association_uid,
            close_timeout=self._close_timeout,
            on_close=self._forget_connection,
        )
        if not self._track_connection(connection):
            asyncio.get_running_loop().create_task(
                connection._close_from_loop(
                    TransportClosedError("Asyncio TCP transport is closed")
                )
            )
            raise TransportClosedError("Asyncio TCP transport is closed")
        return connection

    async def _perform_close_all_on_loop(
        self,
        listeners: tuple[AsyncioTcpListener, ...],
        connections: tuple[AsyncioTcpConnection, ...],
    ) -> None:
        try:
            await asyncio.gather(
                *(listener._close_on_loop() for listener in listeners),
                *(connection._close_on_loop() for connection in connections),
                return_exceptions=True,
            )
        finally:
            self._cleanup_complete.set()

    def _abandon_cleanup(self) -> None:
        with self._lock:
            if not self._cleanup_complete.is_set():
                self._cleanup_started = False

    def _wait_for_cleanup(
        self,
        listeners: tuple[AsyncioTcpListener, ...],
        connections: tuple[AsyncioTcpConnection, ...],
        timeout: float,
    ) -> bool:
        deadline = monotonic() + timeout
        while not self._cleanup_complete.is_set():
            if self._extension.wait_stopped(0.0):
                self._abandon_cleanup()
                for listener in listeners:
                    listener._close_without_loop()
                for connection in connections:
                    connection._close_without_loop(
                        TransportClosedError(
                            "Asyncio I/O extension loop is not available"
                        )
                    )
                self._cleanup_complete.set()
                return True
            remaining = deadline - monotonic()
            if remaining <= 0:
                return False
            self._cleanup_complete.wait(min(0.01, remaining))
        return True

    def _schedule_close_on_loop(
        self,
        listeners: tuple[AsyncioTcpListener, ...],
        connections: tuple[AsyncioTcpConnection, ...],
    ) -> bool:
        try:
            loop = listeners[0]._loop if listeners else connections[0]._loop
        except IndexError:
            self._cleanup_complete.set()
            return True

        def start_cleanup() -> None:
            try:
                _create_task(
                    loop,
                    self._perform_close_all_on_loop(listeners, connections),
                )
            except BaseException:
                self._abandon_cleanup()

        try:
            loop.call_soon_threadsafe(start_cleanup)
        except RuntimeError:
            return False
        return True

    async def _resolve(self, endpoint: Endpoint, *, passive: bool):
        loop = asyncio.get_running_loop()
        flags = socket.AI_PASSIVE if passive else 0
        addresses = await loop.getaddrinfo(
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

    def _track_listener(self, listener: AsyncioTcpListener) -> bool:
        with self._lock:
            if self._closed:
                return False
            self._listeners.add(listener)
            return True

    def _forget_listener(self, listener: AsyncioTcpListener) -> None:
        with self._lock:
            self._listeners.discard(listener)

    def _track_connection(self, connection: AsyncioTcpConnection) -> bool:
        with self._lock:
            if self._closed:
                return False
            self._connections.add(connection)
            return True

    def _forget_connection(self, connection: AsyncioTcpConnection) -> None:
        with self._lock:
            self._connections.discard(connection)

    def _ensure_open(self) -> None:
        with self._lock:
            if self._closed:
                raise TransportClosedError("Asyncio TCP transport is closed")

    @staticmethod
    def _validate_common(endpoint: Endpoint, limits: TransportLimits) -> None:
        if not isinstance(endpoint, Endpoint):
            raise TransportProtocolError("endpoint must be an Endpoint value")
        if not isinstance(limits, TransportLimits):
            raise TransportProtocolError("limits must be a TransportLimits value")


__all__ = ["AsyncioTcpConnection", "AsyncioTcpListener", "AsyncioTcpTransport"]
