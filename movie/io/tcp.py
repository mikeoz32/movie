from __future__ import annotations

import asyncio
import os
import socket
from collections import deque
from collections.abc import Callable, Coroutine
from concurrent.futures import Future
from dataclasses import dataclass
from threading import Lock
from time import monotonic, sleep
from typing import Any

from movie.actor.behaviour import AbstractBehavior, Behaviors
from movie.actor.context import ActorContext
from movie.actor.extension import ExtensionId
from movie.actor.ref import ActorRef
from movie.actor.system import ActorSystem, ExtendedActorSystem
from movie.io.asyncio import (
    ASYNCIO_IO,
    AsyncioIOCapacityError,
    AsyncioIOStateError,
    AsyncioIOWorker,
)

_DEFAULT_WRITE_BATCH_MESSAGES = 64
_DEFAULT_WRITE_BATCH_BYTES = 256 * 1024
_DEFAULT_WRITE_BATCH_DELAY_MS = 1
_DEFAULT_OPERATION_LIMIT = 1024


@dataclass(frozen=True, slots=True)
class TcpEndpoint:
    host: str
    port: int

    def __post_init__(self) -> None:
        if not isinstance(self.host, str):
            raise TypeError("TCP endpoint host must be a string")
        if not isinstance(self.port, int) or isinstance(self.port, bool):
            raise TypeError("TCP endpoint port must be an integer")
        if not 0 <= self.port <= 65535:
            raise ValueError("TCP endpoint port must be between 0 and 65535")


@dataclass(frozen=True, slots=True)
class Bind:
    """Bind a listener whose lifetime is controlled by its returned actor ref."""

    local: TcpEndpoint
    handler: ActorRef[Any]
    reply_to: ActorRef[Any]


@dataclass(frozen=True, slots=True)
class Connect:
    """Open a connection that must be registered before reads begin."""

    remote: TcpEndpoint
    reply_to: ActorRef[Any]


@dataclass(frozen=True, slots=True)
class Register:
    handler: ActorRef[Any]
    pull_mode: bool = False


@dataclass(frozen=True, slots=True)
class Read:
    """Permit one socket read when registered in pull mode."""


@dataclass(frozen=True, slots=True)
class Write:
    data: bytes
    reply_to: ActorRef[Any] | None = None
    completion_to: ActorRef[Any] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.data, bytes):
            raise TypeError("TCP write data must be bytes")
        if not self.data:
            raise ValueError("TCP write data must not be empty")


@dataclass(frozen=True, slots=True)
class Close:
    """Discard writes still queued in the runtime and close the connection."""

    reply_to: ActorRef[Any]


@dataclass(frozen=True, slots=True)
class Unbind:
    reply_to: ActorRef[Any]


@dataclass(frozen=True, slots=True)
class Bound:
    local: TcpEndpoint
    listener: ActorRef[Any]


@dataclass(frozen=True, slots=True)
class Connected:
    connection: ActorRef[Any]
    local: TcpEndpoint
    remote: TcpEndpoint


@dataclass(frozen=True, slots=True)
class Received:
    connection: ActorRef[Any]
    data: bytes


@dataclass(frozen=True, slots=True)
class PeerClosed:
    """Reports peer write-side EOF while pull-mode writes remain available."""

    connection: ActorRef[Any]


@dataclass(frozen=True, slots=True)
class WriteAccepted:
    """Confirms bounded runtime-queue admission, not network delivery."""

    connection: ActorRef[Any]
    byte_count: int


@dataclass(frozen=True, slots=True)
class WriteCompleted:
    """Confirms that one logical write completed all socket write calls."""

    connection: ActorRef[Any]
    byte_count: int


@dataclass(frozen=True, slots=True)
class Closed:
    connection: ActorRef[Any]


@dataclass(frozen=True, slots=True)
class ConnectionClosed:
    connection: ActorRef[Any]
    cause: str | None = None


@dataclass(frozen=True, slots=True)
class ListenerClosed:
    listener: ActorRef[Any]
    cause: str


@dataclass(frozen=True, slots=True)
class Unbound:
    local: TcpEndpoint


@dataclass(frozen=True, slots=True)
class CommandFailed:
    command: object
    detail: str


TcpManagerCommand = Bind | Connect
TcpListenerCommand = Unbind
TcpConnectionCommand = Register | Read | Write | Close
TcpEvent = (
    Bound
    | Connected
    | Received
    | PeerClosed
    | WriteAccepted
    | WriteCompleted
    | Closed
    | ConnectionClosed
    | ListenerClosed
    | Unbound
    | CommandFailed
)


class TcpIOClosedError(RuntimeError):
    pass


class TcpIOCapacityError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class _PendingWrite:
    data: bytes
    completion_to: ActorRef[Any] | None


@dataclass(slots=True)
class _AcceptedSetup:
    sock: socket.socket
    delivered: bool = False


class _BindFutureReply:
    def __init__(self, completed: Future[Bound]) -> None:
        self._completed = completed

    def tell(self, message: Bound | CommandFailed) -> None:
        if self._completed.done():
            return
        if isinstance(message, Bound):
            self._completed.set_result(message)
        else:
            self._completed.set_exception(TcpIOClosedError(message.detail))


def _positive_int(value: int | None, default: int, field: str) -> int:
    resolved = default if value is None else value
    if not isinstance(resolved, int) or isinstance(resolved, bool) or resolved <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return resolved


def _nonnegative_int(value: int | None, default: int, field: str) -> int:
    resolved = default if value is None else value
    if not isinstance(resolved, int) or isinstance(resolved, bool) or resolved < 0:
        raise ValueError(f"{field} must be a nonnegative integer")
    return resolved


def _close_socket(sock: socket.socket) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass


def _endpoint(address) -> TcpEndpoint:
    return TcpEndpoint(str(address[0]), int(address[1]))


def _configure_socket(sock: socket.socket) -> None:
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setblocking(False)


def _create_task(coroutine: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
    try:
        return asyncio.create_task(coroutine)
    except BaseException:
        coroutine.close()
        raise


def _after_tasks_settle(
    tasks: tuple[asyncio.Task[None], ...],
    callback: Callable[[], None],
) -> None:
    pending = tuple(task for task in tasks if not task.done())
    if not pending:
        callback()
        return
    remaining = len(pending)

    def task_done(task: asyncio.Task[None]) -> None:
        nonlocal remaining
        if not task.cancelled():
            task.exception()
        remaining -= 1
        if remaining == 0:
            callback()

    for task in pending:
        task.add_done_callback(task_done)
        task.cancel()


def _observe(future) -> None:
    def completed(result) -> None:
        if not result.cancelled():
            result.exception()

    future.add_done_callback(completed)


class TcpExtension:
    """Actor-facing raw TCP service backed by one actor-system asyncio loop."""

    def __init__(self, system: ExtendedActorSystem) -> None:
        self._system = system
        self._io = ASYNCIO_IO.get(system)
        config = system.config
        self._write_message_limit = _positive_int(
            config.get_int("movie.io.tcp.write-message-limit", 1024),
            1024,
            "TCP write message limit",
        )
        self._write_byte_limit = _positive_int(
            config.get_int("movie.io.tcp.write-byte-limit", 4 * 1024 * 1024),
            4 * 1024 * 1024,
            "TCP write byte limit",
        )
        self._write_batch_message_limit = _positive_int(
            config.get_int(
                "movie.io.tcp.write-batch-message-limit",
                _DEFAULT_WRITE_BATCH_MESSAGES,
            ),
            _DEFAULT_WRITE_BATCH_MESSAGES,
            "TCP write batch message limit",
        )
        self._write_batch_byte_limit = _positive_int(
            config.get_int(
                "movie.io.tcp.write-batch-byte-limit",
                _DEFAULT_WRITE_BATCH_BYTES,
            ),
            _DEFAULT_WRITE_BATCH_BYTES,
            "TCP write batch byte limit",
        )
        self._write_batch_delay = (
            _nonnegative_int(
                config.get_int(
                    "movie.io.tcp.write-batch-delay-ms",
                    _DEFAULT_WRITE_BATCH_DELAY_MS,
                ),
                _DEFAULT_WRITE_BATCH_DELAY_MS,
                "TCP write batch delay",
            )
            / 1_000
        )
        self._read_chunk_bytes = _positive_int(
            config.get_int("movie.io.tcp.read-chunk-bytes", 64 * 1024),
            64 * 1024,
            "TCP read chunk bytes",
        )
        self._backlog = _positive_int(
            config.get_int("movie.io.tcp.backlog", 128),
            128,
            "TCP listen backlog",
        )
        self._connect_timeout = float(
            _positive_int(
                config.get_int("movie.io.tcp.connect-timeout", 10),
                10,
                "TCP connect timeout",
            )
        )
        self._operation_limit = _positive_int(
            config.get_int("movie.io.tcp.operation-limit", _DEFAULT_OPERATION_LIMIT),
            _DEFAULT_OPERATION_LIMIT,
            "TCP operation limit",
        )
        self._lock = Lock()
        self._closed = False
        self._manager: ActorRef[TcpManagerCommand] | None = None
        self._listeners: set[_TcpListener] = set()
        self._listeners_by_ref: dict[ActorRef[Any], _TcpListener] = {}
        self._connections: set[_TcpConnection] = set()
        self._connections_by_ref: dict[ActorRef[Any], _TcpConnection] = {}
        self._operations: dict[asyncio.Task[None], AsyncioIOWorker] = {}
        self._pending_operations = 0
        self._next_actor_id = 1

    @property
    def manager(self) -> ActorRef[TcpManagerCommand]:
        with self._lock:
            manager = self._manager
        if manager is None:
            raise TcpIOClosedError("TCP extension has not started")
        return manager

    def start(self) -> None:
        manager = self._system.spawn(
            Behaviors.setup(lambda context: _TcpManagerBehavior(context, self)),
            f"tcp-manager-{self._system.incarnation_uid.hex}",
        )
        with self._lock:
            if self._closed or self._manager is not None:
                raise TcpIOClosedError("TCP extension cannot start")
            self._manager = manager

    def stop(self, timeout: float) -> None:
        if timeout < 0:
            raise ValueError("TCP shutdown timeout must be nonnegative")
        with self._lock:
            self._closed = True
        deadline = monotonic() + timeout
        for worker in self._io.workers:
            while True:
                remaining = max(0.0, deadline - monotonic())
                try:
                    worker.run_coroutine(
                        lambda worker=worker: self._close_all_on_worker(worker),
                        timeout=remaining,
                        cancel_on_timeout=False,
                    )
                    break
                except AsyncioIOCapacityError as error:
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        raise TimeoutError(
                            "TCP cleanup admission did not succeed before the deadline"
                        ) from error
                    sleep(min(0.01, remaining))
                except AsyncioIOStateError as error:
                    remaining = max(0.0, deadline - monotonic())
                    if not worker.wait_stopped(remaining):
                        raise TimeoutError(
                            "Asyncio I/O worker did not stop before TCP shutdown"
                        ) from error
                    self._close_worker_without_loop(worker)
                    break

    def bind(self, command: Bind) -> None:
        worker = self._io.select_worker()
        self._submit_operation(
            worker,
            lambda: self._bind_on_loop(command, worker),
            lambda: self._fail(command.reply_to, command, "TCP extension is stopping"),
        )

    def bind_endpoint(
        self,
        endpoint: TcpEndpoint,
        handler: ActorRef[Any],
    ) -> Future[Bound]:
        completed: Future[Bound] = Future()
        reply = _BindFutureReply(completed)
        try:
            self.bind(Bind(endpoint, handler, reply))
        except BaseException as error:
            completed.set_exception(error)
        return completed

    def connect(self, command: Connect) -> None:
        worker = self._io.select_worker()
        self._submit_operation(
            worker,
            lambda: self._connect_on_loop(command, worker),
            lambda: self._fail(command.reply_to, command, "TCP extension is stopping"),
        )

    def unbind_listener(
        self,
        listener: ActorRef[Any],
        reply_to: ActorRef[Any],
    ) -> None:
        with self._lock:
            implementation = self._listeners_by_ref.get(listener)
        if implementation is None:
            raise TcpIOClosedError("TCP listener is closed")
        implementation.request_close(reply_to)

    def close_listener(self, listener: ActorRef[Any]) -> Future[None]:
        with self._lock:
            implementation = self._listeners_by_ref.get(listener)
        if implementation is None:
            completed: Future[None] = Future()
            completed.set_result(None)
            return completed
        implementation.request_close(None)
        return implementation.closed_future

    def close_connection(
        self,
        connection: ActorRef[Any],
        reply_to: ActorRef[Any] | None = None,
        cause: str | None = None,
    ) -> None:
        with self._lock:
            implementation = self._connections_by_ref.get(connection)
        if implementation is None:
            if reply_to is not None:
                try:
                    reply_to.tell(Closed(connection))
                except BaseException:
                    pass
            return
        implementation.request_close(reply_to, cause)

    def _submit_operation(
        self,
        worker: AsyncioIOWorker,
        factory: Callable[[], Coroutine[Any, Any, None]],
        rejected: Callable[[], None],
    ) -> None:
        with self._lock:
            if self._closed:
                raise TcpIOClosedError("TCP extension is stopping")
            if self._pending_operations + len(self._operations) >= self._operation_limit:
                raise TcpIOCapacityError("TCP operation capacity is full")
            self._pending_operations += 1
        try:
            worker.schedule(self._start_operation_on_loop, worker, factory, rejected)
        except BaseException:
            with self._lock:
                self._pending_operations -= 1
            raise

    def _start_operation_on_loop(
        self,
        worker: AsyncioIOWorker,
        factory: Callable[[], Coroutine[Any, Any, None]],
        rejected: Callable[[], None],
    ) -> None:
        try:
            task = _create_task(factory())
        except BaseException:
            with self._lock:
                self._pending_operations -= 1
            rejected()
            return
        with self._lock:
            self._pending_operations -= 1
            accepted = not self._closed
            if accepted:
                self._operations[task] = worker
        task.add_done_callback(self._operation_done)
        if not accepted:
            task.cancel()
            rejected()

    def _operation_done(self, task: asyncio.Task[None]) -> None:
        with self._lock:
            self._operations.pop(task, None)
        if not task.cancelled():
            task.exception()

    async def _bind_on_loop(self, command: Bind, worker: AsyncioIOWorker) -> None:
        listener_socket = None
        listener = None
        try:
            addresses = await self._resolve(
                command.local,
                passive=True,
            )
            last_error = None
            for family, socktype, protocol, sockaddr in addresses:
                candidate = None
                try:
                    candidate = socket.socket(family, socktype, protocol)
                    if os.name == "nt" and hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                        candidate.setsockopt(
                            socket.SOL_SOCKET,
                            socket.SO_EXCLUSIVEADDRUSE,
                            1,
                        )
                    else:
                        candidate.setsockopt(
                            socket.SOL_SOCKET,
                            socket.SO_REUSEADDR,
                            1,
                        )
                    candidate.setblocking(False)
                    candidate.bind(sockaddr)
                    candidate.listen(self._backlog)
                except OSError as error:
                    last_error = error
                    if candidate is not None:
                        _close_socket(candidate)
                    continue
                listener_socket = candidate
                break
            if listener_socket is None:
                raise OSError("no resolved TCP address could be bound") from last_error
            local = _endpoint(listener_socket.getsockname())
            listener = _TcpListener(
                self,
                worker,
                listener_socket,
                local,
                command.handler,
            )
            listener_socket = None
            listener_ref = self._system.spawn(
                Behaviors.setup(lambda context: _TcpListenerBehavior(context, listener)),
                self._next_name("tcp-listener"),
            )
            if not self._track_listener(listener, listener_ref):
                await listener.close_on_loop("TCP extension is stopping")
                self._system.terminate(listener_ref)
                raise TcpIOClosedError("TCP extension is stopping")
            listener.attach(listener_ref)
            command.reply_to.tell(Bound(local, listener_ref))
        except asyncio.CancelledError:
            if listener is not None:
                await listener.close_on_loop("TCP bind was cancelled")
            if listener_socket is not None:
                _close_socket(listener_socket)
            raise
        except BaseException as error:
            if listener is not None:
                await listener.close_on_loop(str(error))
            if listener_socket is not None:
                _close_socket(listener_socket)
            self._fail(command.reply_to, command, f"TCP bind failed: {error}")

    async def _connect_on_loop(
        self,
        command: Connect,
        worker: AsyncioIOWorker,
    ) -> None:
        connection_socket = None
        candidate = None
        connection = None
        try:
            async with asyncio.timeout(self._connect_timeout):
                addresses = await self._resolve(
                    command.remote,
                    passive=False,
                )
                loop = asyncio.get_running_loop()
                last_error = None
                candidate = None
                for family, socktype, protocol, sockaddr in addresses:
                    try:
                        candidate = socket.socket(family, socktype, protocol)
                        _configure_socket(candidate)
                        await loop.sock_connect(candidate, sockaddr)
                    except asyncio.CancelledError:
                        _close_socket(candidate)
                        candidate = None
                        raise
                    except OSError as error:
                        last_error = error
                        if candidate is not None:
                            _close_socket(candidate)
                        candidate = None
                        continue
                    connection_socket = candidate
                    candidate = None
                    break
                if connection_socket is None:
                    raise OSError("no resolved TCP address could be connected") from last_error
            connection, connection_ref = await self._create_connection_on_loop(
                connection_socket,
                worker,
            )
            connection_socket = None
            command.reply_to.tell(
                Connected(
                    connection_ref,
                    connection.local,
                    connection.remote,
                )
            )
        except asyncio.CancelledError:
            if connection is not None:
                await connection.close_on_loop("TCP connect was cancelled")
            if connection_socket is not None:
                _close_socket(connection_socket)
            if candidate is not None:
                _close_socket(candidate)
            raise
        except BaseException as error:
            if connection is not None:
                await connection.close_on_loop(str(error))
            if connection_socket is not None:
                _close_socket(connection_socket)
            if candidate is not None:
                _close_socket(candidate)
            self._fail(command.reply_to, command, f"TCP connect failed: {error}")

    async def _create_connection_on_loop(
        self,
        sock: socket.socket,
        worker: AsyncioIOWorker,
    ) -> tuple[_TcpConnection, ActorRef[TcpConnectionCommand]]:
        connection = _TcpConnection(self, worker, sock)
        try:
            connection_ref = self._system.spawn(
                Behaviors.setup(lambda context: _TcpConnectionBehavior(context, connection)),
                self._next_name("tcp-connection"),
            )
        except BaseException:
            _close_socket(sock)
            raise
        if not self._track_connection(connection, connection_ref):
            connection.close_without_loop("TCP extension is stopping")
            self._system.terminate(connection_ref)
            raise TcpIOClosedError("TCP extension is stopping")
        try:
            await connection.attach(connection_ref)
        except BaseException as error:
            await connection.close_on_loop(str(error))
            self._system.terminate(connection_ref)
            raise
        return connection, connection_ref

    async def _accept_connection_on_loop(
        self,
        sock: socket.socket,
        worker: AsyncioIOWorker,
        listener: _TcpListener,
        accepted_setup: _AcceptedSetup,
    ) -> None:
        connection = None
        try:
            connection, connection_ref = await self._create_connection_on_loop(sock, worker)
            delivered = listener._deliver_connected(
                Connected(
                    connection_ref,
                    connection.local,
                    connection.remote,
                ),
                accepted_setup,
            )
            if not delivered:
                await connection.close_on_loop("TCP listener is closing")
                self._system.terminate(connection_ref)
        except asyncio.CancelledError:
            if connection is None:
                _close_socket(sock)
            else:
                await connection.close_on_loop("TCP listener is closing")
            raise
        except BaseException as error:
            if connection is None:
                _close_socket(sock)
            else:
                await connection.close_on_loop(str(error))
            raise

    async def _close_all_on_worker(self, worker: AsyncioIOWorker) -> None:
        with self._lock:
            operations = tuple(task for task, owner in self._operations.items() if owner is worker)
            listeners = tuple(listener for listener in self._listeners if listener.worker is worker)
            connections = tuple(
                connection for connection in self._connections if connection.worker is worker
            )
        current = asyncio.current_task()
        pending_operations = tuple(
            task for task in operations if task is not current and not task.done()
        )
        for task in pending_operations:
            task.cancel()
        if pending_operations:
            await asyncio.gather(*pending_operations, return_exceptions=True)
        await asyncio.gather(
            *(listener.close_on_loop("TCP extension stopped") for listener in listeners),
            *(connection.close_on_loop("TCP extension stopped") for connection in connections),
            return_exceptions=True,
        )

    def _close_worker_without_loop(self, worker: AsyncioIOWorker) -> None:
        with self._lock:
            listeners = tuple(listener for listener in self._listeners if listener.worker is worker)
            connections = tuple(
                connection for connection in self._connections if connection.worker is worker
            )
        for listener in listeners:
            listener.close_without_loop()
        for connection in connections:
            connection.close_without_loop("Asyncio I/O worker is not running")

    async def _resolve(self, endpoint: TcpEndpoint, *, passive: bool):
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
        return tuple(
            (family, socktype, protocol, sockaddr)
            for family, socktype, protocol, _, sockaddr in addresses
        )

    def _next_name(self, prefix: str) -> str:
        with self._lock:
            actor_id = self._next_actor_id
            self._next_actor_id += 1
        return f"{prefix}-{self._system.incarnation_uid.hex}-{actor_id}"

    def _track_listener(
        self,
        listener: _TcpListener,
        listener_ref: ActorRef[Any],
    ) -> bool:
        with self._lock:
            if self._closed:
                return False
            self._listeners.add(listener)
            self._listeners_by_ref[listener_ref] = listener
            return True

    def _forget_listener(self, listener: _TcpListener) -> None:
        with self._lock:
            self._listeners.discard(listener)
            for listener_ref, candidate in tuple(self._listeners_by_ref.items()):
                if candidate is listener:
                    self._listeners_by_ref.pop(listener_ref, None)

    def _track_connection(
        self,
        connection: _TcpConnection,
        connection_ref: ActorRef[Any],
    ) -> bool:
        with self._lock:
            if self._closed:
                return False
            self._connections.add(connection)
            self._connections_by_ref[connection_ref] = connection
            return True

    def _forget_connection(self, connection: _TcpConnection) -> None:
        with self._lock:
            self._connections.discard(connection)
            for connection_ref, candidate in tuple(self._connections_by_ref.items()):
                if candidate is connection:
                    self._connections_by_ref.pop(connection_ref, None)

    @staticmethod
    def _fail(reply_to: ActorRef[Any], command: object, detail: str) -> None:
        try:
            reply_to.tell(CommandFailed(command, detail))
        except BaseException:
            pass


class _TcpManagerBehavior(AbstractBehavior[TcpManagerCommand]):
    def __init__(self, context: ActorContext, extension: TcpExtension) -> None:
        super().__init__(context)
        self._extension = extension

    def receive(self, context: ActorContext, message: TcpManagerCommand):
        try:
            if isinstance(message, Bind):
                self._extension.bind(message)
            elif isinstance(message, Connect):
                self._extension.connect(message)
        except (
            AsyncioIOCapacityError,
            AsyncioIOStateError,
            TcpIOCapacityError,
            TcpIOClosedError,
        ) as error:
            self._extension._fail(message.reply_to, message, str(error))
        return Behaviors.same


class _TcpListenerBehavior(AbstractBehavior[TcpListenerCommand]):
    def __init__(self, context: ActorContext, listener: _TcpListener) -> None:
        super().__init__(context)
        self._listener = listener

    def receive(self, context: ActorContext, message: TcpListenerCommand):
        if isinstance(message, Unbind):
            try:
                self._listener.request_close(message.reply_to)
            except (AsyncioIOCapacityError, AsyncioIOStateError) as error:
                self._listener.extension._fail(
                    message.reply_to,
                    message,
                    str(error),
                )
        return Behaviors.same

    def on_signal(self, context: ActorContext, message: ActorSystem.SystemMessage) -> None:
        if isinstance(message, ActorSystem.PostStop):
            try:
                self._listener.request_close(None)
            except AsyncioIOCapacityError, AsyncioIOStateError:
                pass


class _TcpConnectionBehavior(AbstractBehavior[TcpConnectionCommand]):
    def __init__(self, context: ActorContext, connection: _TcpConnection) -> None:
        super().__init__(context)
        self._connection = connection

    def receive(self, context: ActorContext, message: TcpConnectionCommand):
        if isinstance(message, Register):
            try:
                self._connection.register(message.handler, message.pull_mode)
            except TcpIOClosedError:
                pass
        elif isinstance(message, Read):
            try:
                self._connection.request_read()
            except TcpIOClosedError:
                pass
        elif isinstance(message, Write):
            wake_writer = False
            try:
                wake_writer = self._connection.write(message.data, message.completion_to)
            except (TcpIOCapacityError, TcpIOClosedError) as error:
                failure_to = message.reply_to or message.completion_to
                if failure_to is not None:
                    self._connection.extension._fail(
                        failure_to,
                        message,
                        str(error),
                    )
            else:
                if message.reply_to is not None:
                    try:
                        message.reply_to.tell(
                            WriteAccepted(self._connection.ref, len(message.data))
                        )
                    except BaseException:
                        pass
                if wake_writer:
                    self._connection.wake_writer()
        elif isinstance(message, Close):
            try:
                self._connection.request_close(message.reply_to, None)
            except (AsyncioIOCapacityError, AsyncioIOStateError) as error:
                self._connection.extension._fail(
                    message.reply_to,
                    message,
                    str(error),
                )
        return Behaviors.same

    def on_signal(self, context: ActorContext, message: ActorSystem.SystemMessage) -> None:
        if isinstance(message, ActorSystem.PostStop):
            try:
                self._connection.request_close(None, "TCP connection actor stopped")
            except AsyncioIOCapacityError, AsyncioIOStateError:
                pass


class _TcpListener:
    def __init__(
        self,
        extension: TcpExtension,
        worker: AsyncioIOWorker,
        sock: socket.socket,
        local: TcpEndpoint,
        handler: ActorRef[Any],
    ) -> None:
        self.extension = extension
        self.worker = worker
        self._socket = sock
        self.local = local
        self._handler = handler
        self._loop = asyncio.get_running_loop()
        self._lock = Lock()
        self._ref: ActorRef[TcpListenerCommand] | None = None
        self._accept_task: asyncio.Task[None] | None = None
        self._accept_setups: dict[Future[None], _AcceptedSetup] = {}
        self._close_task: asyncio.Task[None] | None = None
        self._close_replies: list[ActorRef[Any]] = []
        self._closing = False
        self._close_complete = False
        self._close_cause: str | None = None
        self._closed_future: Future[None] = Future()

    @property
    def closed_future(self) -> Future[None]:
        return self._closed_future

    def attach(self, ref: ActorRef[TcpListenerCommand]) -> None:
        self._ref = ref
        self._accept_task = _create_task(self._accept_loop())

    def request_close(self, reply_to: ActorRef[Any] | None) -> None:
        with self._lock:
            if self._close_complete:
                replay = True
            else:
                replay = False
                self._closing = True
                scheduled = self.worker._schedule_control(
                    self._request_close_on_loop,
                    reply_to,
                    None,
                )
        if replay:
            if reply_to is not None:
                try:
                    reply_to.tell(Unbound(self.local))
                except BaseException:
                    pass
        else:
            _observe(scheduled)

    async def close_on_loop(self, cause: str | None = None) -> None:
        with self._lock:
            self._closing = True
            if cause is not None and self._close_cause is None:
                self._close_cause = cause
        self._start_close_on_loop()
        close_task = self._close_task
        if close_task is not None and close_task is not asyncio.current_task():
            await asyncio.shield(close_task)

    def close_without_loop(self, cause: str | None = None) -> None:
        with self._lock:
            self._closing = True
            if cause is not None and self._close_cause is None:
                self._close_cause = cause
        self._abort_accept_setups()
        self._finalize_close()

    def _request_close_on_loop(
        self,
        reply_to: ActorRef[Any] | None,
        cause: str | None,
    ) -> None:
        with self._lock:
            self._closing = True
            complete = self._close_complete
            if reply_to is not None and not complete:
                self._close_replies.append(reply_to)
            if cause is not None and self._close_cause is None:
                self._close_cause = cause
        if complete:
            if reply_to is not None:
                try:
                    reply_to.tell(Unbound(self.local))
                except BaseException:
                    pass
            return
        self._start_close_on_loop()

    def _start_close_on_loop(self) -> None:
        with self._lock:
            self._closing = True
            if self._close_complete:
                return
        if self._close_task is None or self._close_task.done():
            try:
                self._close_task = _create_task(self._finish_close())
            except BaseException:
                accept_task = self._accept_task
                tasks = () if accept_task is None else (accept_task,)
                self._abort_accept_setups()
                _after_tasks_settle(tasks, self._finalize_close)
                return
            self._close_task.add_done_callback(self._close_done)

    def _close_done(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled():
            task.exception()
        with self._lock:
            if not self._close_complete and self._close_task is task:
                self._close_task = None

    async def _finish_close(self) -> None:
        try:
            accept_task = self._accept_task
            if accept_task is not None and accept_task is not asyncio.current_task():
                accept_task.cancel()
                await asyncio.gather(accept_task, return_exceptions=True)
            setups = tuple(self._accept_setups)
            if setups:
                await asyncio.gather(
                    *(asyncio.wrap_future(setup) for setup in setups),
                    return_exceptions=True,
                )
            for setup in setups:
                self._finish_accept_setup(setup)
        finally:
            self._finalize_close()

    def _abort_accept_setups(self) -> None:
        for setup, accepted_setup in tuple(self._accept_setups.items()):
            setup.cancel()
            self._close_runtime_owned_setup(accepted_setup)

    def _deliver_connected(
        self,
        message: Connected,
        accepted_setup: _AcceptedSetup,
    ) -> bool:
        with self._lock:
            if self._closing or self._close_complete:
                return False
            self._handler.tell(message)
            accepted_setup.delivered = True
            return True

    def _close_runtime_owned_setup(self, accepted_setup: _AcceptedSetup) -> None:
        with self._lock:
            runtime_owned = not accepted_setup.delivered
        if runtime_owned:
            _close_socket(accepted_setup.sock)

    def _accept_setup_completed(
        self,
        setup: Future[None],
        accepted_setup: _AcceptedSetup,
    ) -> None:
        try:
            self._loop.call_soon_threadsafe(self._finish_accept_setup, setup)
        except RuntimeError:
            if setup.cancelled() or setup.exception() is not None:
                self._close_runtime_owned_setup(accepted_setup)

    def _finish_accept_setup(self, setup: Future[None]) -> None:
        accepted_setup = self._accept_setups.pop(setup, None)
        failed = setup.cancelled()
        if not failed:
            failed = setup.exception() is not None
        if failed and accepted_setup is not None:
            self._close_runtime_owned_setup(accepted_setup)

    def _finalize_close(self) -> None:
        with self._lock:
            if self._close_complete:
                return
            _close_socket(self._socket)
            self._close_complete = True
            replies = tuple(self._close_replies)
            self._close_replies.clear()
            cause = self._close_cause
            ref = self._ref
        self.extension._forget_listener(self)
        if not self._closed_future.done():
            self._closed_future.set_result(None)
        for reply_to in replies:
            try:
                reply_to.tell(Unbound(self.local))
            except BaseException:
                pass
        if ref is not None:
            if cause is not None:
                try:
                    self._handler.tell(ListenerClosed(ref, cause))
                except BaseException:
                    pass
            self.extension._system.terminate(ref)

    async def _accept_loop(self) -> None:
        try:
            while True:
                accepted, _ = await self._loop.sock_accept(self._socket)
                try:
                    _configure_socket(accepted)
                    worker = self.extension._io.select_worker()
                    accepted_setup = _AcceptedSetup(accepted)
                    setup = worker.submit_coroutine(
                        lambda: self.extension._accept_connection_on_loop(
                            accepted,
                            worker,
                            self,
                            accepted_setup,
                        )
                    )
                    self._accept_setups[setup] = accepted_setup
                    setup.add_done_callback(
                        lambda completed, state=accepted_setup: self._accept_setup_completed(
                            completed,
                            state,
                        )
                    )
                except BaseException:
                    _close_socket(accepted)
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise
        except OSError as error:
            self._request_close_on_loop(
                None,
                f"TCP listener accept failed: {error}",
            )


class _TcpConnection:
    def __init__(
        self,
        extension: TcpExtension,
        worker: AsyncioIOWorker,
        sock: socket.socket,
    ) -> None:
        self.extension = extension
        self.worker = worker
        self._socket = sock
        self._handler: ActorRef[Any] | None = None
        self._pull_mode = False
        self._read_requested = False
        self._loop = asyncio.get_running_loop()
        self.local = _endpoint(sock.getsockname())
        self.remote = _endpoint(sock.getpeername())
        self._lock = Lock()
        self._ref: ActorRef[TcpConnectionCommand] | None = None
        self._open = True
        self._outbound: deque[_PendingWrite] = deque()
        self._outbound_messages = 0
        self._outbound_bytes = 0
        self._writer_wake_scheduled = False
        self._writer_ready = asyncio.Event()
        self._registered_ready = asyncio.Event()
        self._read_ready = asyncio.Event()
        self._reader_task: asyncio.Task[None] | None = None
        self._writer_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._close_replies: list[ActorRef[Any]] = []
        self._close_cause: str | None = None
        self._close_complete = False

    @property
    def ref(self) -> ActorRef[TcpConnectionCommand]:
        ref = self._ref
        if ref is None:
            raise TcpIOClosedError("TCP connection actor is not attached")
        return ref

    async def attach(self, ref: ActorRef[TcpConnectionCommand]) -> None:
        tasks: list[asyncio.Task[None]] = []
        self._ref = ref
        try:
            self._reader_task = _create_task(self._reader_loop())
            tasks.append(self._reader_task)
            self._writer_task = _create_task(self._writer_loop())
            tasks.append(self._writer_task)
        except BaseException:
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            _close_socket(self._socket)
            self._ref = None
            raise

    def register(self, handler: ActorRef[Any], pull_mode: bool) -> None:
        with self._lock:
            if self._handler is not None:
                return
            if not self._open:
                raise TcpIOClosedError("TCP connection is closed")
            self._handler = handler
            self._pull_mode = pull_mode
            try:
                self._loop.call_soon_threadsafe(self._registered_ready.set)
            except RuntimeError as error:
                self._handler = None
                self._pull_mode = False
                raise TcpIOClosedError("Asyncio I/O worker is not running") from error

    def request_read(self) -> None:
        with self._lock:
            if not self._open:
                raise TcpIOClosedError("TCP connection is closed")
            if not self._pull_mode or self._read_requested:
                return
            self._read_requested = True
            try:
                self._loop.call_soon_threadsafe(self._read_ready.set)
            except RuntimeError as error:
                self._read_requested = False
                raise TcpIOClosedError("Asyncio I/O worker is not running") from error

    def write(self, data: bytes, completion_to: ActorRef[Any] | None) -> bool:
        with self._lock:
            if not self._open:
                raise TcpIOClosedError("TCP connection is closed")
            if (
                self._outbound_messages >= self.extension._write_message_limit
                or self._outbound_bytes + len(data) > self.extension._write_byte_limit
            ):
                raise TcpIOCapacityError("TCP write capacity is full")
            wake = not self._outbound
            self._outbound.append(_PendingWrite(data, completion_to))
            self._outbound_messages += 1
            self._outbound_bytes += len(data)
            if wake and not self._writer_wake_scheduled:
                self._writer_wake_scheduled = True
                return True
            return False

    def wake_writer(self) -> None:
        try:
            self._loop.call_soon_threadsafe(self._writer_ready.set)
        except RuntimeError as error:
            raise TcpIOClosedError("Asyncio I/O loop is not running") from error

    def request_close(
        self,
        reply_to: ActorRef[Any] | None,
        cause: str | None,
    ) -> None:
        with self._lock:
            if self._close_complete:
                replay = True
            else:
                replay = False
                scheduled = self.worker._schedule_control(
                    self._request_close_on_loop,
                    reply_to,
                    cause,
                )
                self._open = False
                if cause is not None and self._close_cause is None:
                    self._close_cause = cause
        if replay:
            if reply_to is not None:
                try:
                    reply_to.tell(Closed(self.ref))
                except BaseException:
                    pass
        else:
            _observe(scheduled)

    async def close_on_loop(self, cause: str | None) -> None:
        with self._lock:
            self._open = False
            if cause is not None and self._close_cause is None:
                self._close_cause = cause
        self._start_close_on_loop()
        close_task = self._close_task
        if close_task is not None and close_task is not asyncio.current_task():
            await asyncio.shield(close_task)

    def close_without_loop(self, cause: str | None) -> None:
        with self._lock:
            self._open = False
            if cause is not None and self._close_cause is None:
                self._close_cause = cause
        self._finalize_close()

    def _request_close_on_loop(
        self,
        reply_to: ActorRef[Any] | None,
        cause: str | None,
    ) -> None:
        with self._lock:
            complete = self._close_complete
            if reply_to is not None and not complete:
                self._close_replies.append(reply_to)
            if cause is not None and self._close_cause is None:
                self._close_cause = cause
            self._open = False
        if complete:
            if reply_to is not None:
                try:
                    reply_to.tell(Closed(self.ref))
                except BaseException:
                    pass
            return
        self._start_close_on_loop()

    def _start_close_on_loop(self) -> None:
        if self._close_complete:
            return
        if self._close_task is None or self._close_task.done():
            try:
                self._close_task = _create_task(self._finish_close())
            except BaseException:
                tasks = tuple(
                    task for task in (self._reader_task, self._writer_task) if task is not None
                )
                _after_tasks_settle(tasks, self._finalize_close)
                return
            self._close_task.add_done_callback(self._close_done)

    def _close_done(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled():
            task.exception()
        with self._lock:
            if not self._close_complete and self._close_task is task:
                self._close_task = None

    async def _finish_close(self) -> None:
        try:
            current = asyncio.current_task()
            tasks = tuple(
                task
                for task in (self._reader_task, self._writer_task)
                if task is not None and task is not current and not task.done()
            )
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            self._finalize_close()

    def _finalize_close(self) -> None:
        with self._lock:
            if self._close_complete:
                return
            self._open = False
            self._outbound.clear()
            self._outbound_messages = 0
            self._outbound_bytes = 0
            self._writer_wake_scheduled = False
            _close_socket(self._socket)
            self._close_complete = True
            cause = self._close_cause
            replies = tuple(self._close_replies)
            self._close_replies.clear()
            ref = self._ref
        self.extension._forget_connection(self)
        if ref is not None:
            handler = self._handler
            if handler is not None:
                try:
                    handler.tell(ConnectionClosed(ref, cause))
                except BaseException:
                    pass
            for reply_to in replies:
                try:
                    reply_to.tell(Closed(ref))
                except BaseException:
                    pass
            self.extension._system.terminate(ref)

    async def _reader_loop(self) -> None:
        try:
            await self._registered_ready.wait()
            while True:
                if self._pull_mode:
                    await self._read_ready.wait()
                    self._read_ready.clear()
                    with self._lock:
                        if not self._open:
                            return
                        if not self._read_requested:
                            continue
                        self._read_requested = False
                data = await self._loop.sock_recv(
                    self._socket,
                    self.extension._read_chunk_bytes,
                )
                if not data:
                    if self._pull_mode:
                        handler = self._handler
                        if handler is not None:
                            handler.tell(PeerClosed(self.ref))
                        return
                    with self._lock:
                        self._open = False
                        if self._close_cause is None:
                            self._close_cause = "TCP peer closed the connection"
                    self._start_close_on_loop()
                    return
                handler = self._handler
                if handler is not None:
                    handler.tell(Received(self.ref, data))
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            with self._lock:
                self._open = False
                if self._close_cause is None:
                    self._close_cause = f"TCP read failed: {error}"
            self._start_close_on_loop()

    async def _writer_loop(self) -> None:
        pending: list[bytes] = []
        pending_completions: list[tuple[ActorRef[Any] | None, int]] = []
        pending_bytes = 0
        deadline = 0.0
        try:
            while True:
                if pending:
                    timeout = max(0.0, deadline - self._loop.time())
                    try:
                        await asyncio.wait_for(self._writer_ready.wait(), timeout)
                    except TimeoutError:
                        pass
                else:
                    await self._writer_ready.wait()
                self._writer_ready.clear()
                while True:
                    with self._lock:
                        if not self._open:
                            return
                        submissions: list[_PendingWrite] = []
                        while (
                            self._outbound
                            and len(submissions) < self.extension._write_batch_message_limit
                        ):
                            submissions.append(self._outbound.popleft())
                        if not submissions:
                            self._writer_wake_scheduled = False
                            break
                    for submission in submissions:
                        data = submission.data
                        offset = 0
                        while offset < len(data):
                            if pending and (
                                len(pending_completions)
                                >= self.extension._write_batch_message_limit
                                or pending_bytes >= self.extension._write_batch_byte_limit
                            ):
                                await self._send_batch(
                                    pending,
                                    pending_completions,
                                    pending_bytes,
                                )
                                pending = []
                                pending_completions = []
                                pending_bytes = 0
                            if not pending:
                                deadline = self._loop.time() + self.extension._write_batch_delay
                            available = self.extension._write_batch_byte_limit - pending_bytes
                            chunk_size = min(available, len(data) - offset)
                            pending.append(data[offset : offset + chunk_size])
                            pending_bytes += chunk_size
                            offset += chunk_size
                            if offset == len(data):
                                pending_completions.append((submission.completion_to, len(data)))
                            if (
                                len(pending_completions)
                                >= self.extension._write_batch_message_limit
                                or pending_bytes >= self.extension._write_batch_byte_limit
                                or self.extension._write_batch_delay == 0
                            ):
                                await self._send_batch(
                                    pending,
                                    pending_completions,
                                    pending_bytes,
                                )
                                pending = []
                                pending_completions = []
                                pending_bytes = 0
                if pending and deadline <= self._loop.time():
                    await self._send_batch(pending, pending_completions, pending_bytes)
                    pending = []
                    pending_completions = []
                    pending_bytes = 0
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            with self._lock:
                self._open = False
                if self._close_cause is None:
                    self._close_cause = f"TCP write failed: {error}"
            self._start_close_on_loop()

    async def _send_batch(
        self,
        chunks: list[bytes],
        completions: list[tuple[ActorRef[Any] | None, int]],
        batch_bytes: int,
    ) -> None:
        payload = chunks[0] if len(chunks) == 1 else b"".join(chunks)
        await self._loop.sock_sendall(self._socket, payload)
        with self._lock:
            self._outbound_messages -= len(completions)
            self._outbound_bytes -= batch_bytes
        for completion_to, byte_count in completions:
            if completion_to is not None:
                completion_to.tell(WriteCompleted(self.ref, byte_count))


TCP: ExtensionId[TcpExtension] = ExtensionId("tcp", TcpExtension)


__all__ = [
    "TCP",
    "Bind",
    "Bound",
    "Close",
    "Closed",
    "CommandFailed",
    "Connect",
    "Connected",
    "ConnectionClosed",
    "ListenerClosed",
    "PeerClosed",
    "Read",
    "Received",
    "Register",
    "TcpConnectionCommand",
    "TcpEndpoint",
    "TcpEvent",
    "TcpExtension",
    "TcpIOCapacityError",
    "TcpIOClosedError",
    "TcpListenerCommand",
    "TcpManagerCommand",
    "Unbind",
    "Unbound",
    "Write",
    "WriteAccepted",
    "WriteCompleted",
]
