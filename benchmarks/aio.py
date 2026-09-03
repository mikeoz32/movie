from __future__ import annotations

import argparse
import asyncio
import multiprocessing
import socket
import statistics
import struct
import sys
import traceback
from collections import deque
from dataclasses import dataclass
from itertools import count
from multiprocessing.connection import Connection
from queue import Queue
from threading import Event, Lock, Thread
from time import monotonic, perf_counter

from movie.actor.behaviour import AbstractBehavior, Behaviors
from movie.actor.context import ActorContext
from movie.actor.extension import ExtensionId, ManagedExtension
from movie.actor.ref import ActorRef
from movie.actor.system import ActorSystem, ExtendedActorSystem
from movie.config import Config

_WRITE_MESSAGE_LIMIT = 1_024
_WRITE_BYTE_LIMIT = 4 * 1024 * 1024
_OUTBOUND_DRAIN_RECORDS = 64
_FRAME_HEADER = struct.Struct("!I")


@dataclass(frozen=True, slots=True)
class Bind:
    host: str
    port: int
    accepted_to: ActorRef[TCPEvent]
    reply_to: ActorRef[Bound | CommandFailed]


@dataclass(frozen=True, slots=True)
class Bound:
    host: str
    port: int
    listener: ActorRef[Unbind]


@dataclass(frozen=True, slots=True)
class Unbind:
    reply_to: ActorRef[Unbound | CommandFailed]


@dataclass(frozen=True, slots=True)
class Unbound:
    host: str
    port: int


@dataclass(frozen=True, slots=True)
class CommandFailed:
    command: object
    detail: str


@dataclass(frozen=True, slots=True)
class Register:
    handler: ActorRef[Received | ConnectionClosed]


@dataclass(frozen=True, slots=True)
class Write:
    data: bytes
    reply_to: ActorRef[CommandFailed] | None = None


@dataclass(frozen=True, slots=True)
class Close:
    pass


@dataclass(frozen=True, slots=True)
class Connected:
    connection: ActorRef[TCPConnectionCommand]
    local_host: str
    local_port: int
    remote_host: str
    remote_port: int


@dataclass(frozen=True, slots=True)
class Received:
    connection: ActorRef[TCPConnectionCommand]
    payloads: tuple[bytes, ...]


@dataclass(frozen=True, slots=True)
class ConnectionClosed:
    connection: ActorRef[TCPConnectionCommand]


TCPEvent = Connected | Received | ConnectionClosed
TCPConnectionCommand = Register | Write | Close


TCPListenerMessage = Bind | Unbind


class AsyncIOExtension(ManagedExtension):
    def __init__(self, system: ExtendedActorSystem) -> None:
        self.system = system
        self.worker_count = system.config.get_int("benchmark.asyncio.loops", 1) or 1
        self.maximum_records = system.config.get_int("benchmark.asyncio.maximum-records", 64) or 64
        self.maximum_bytes = (
            system.config.get_int("benchmark.asyncio.maximum-bytes", 256 * 1024) or 256 * 1024
        )
        self.maximum_delay = (
            system.config.get_int("benchmark.asyncio.maximum-delay-ms", 1) or 0
        ) / 1_000
        self.loops: list[asyncio.AbstractEventLoop | None] = [None] * self.worker_count
        self.threads: list[Thread] = []
        self.ready = [Event() for _ in range(self.worker_count)]
        self.next_worker = 0
        self.assigned_connections = [0] * self.worker_count
        self.scheduled_tasks = [0] * self.worker_count
        self.metrics_lock = Lock()

    def start(self) -> None:
        for worker in range(self.worker_count):
            thread = Thread(
                target=self.event_loop,
                args=(worker,),
                name=f"benchmark-asyncio-{self.system.name}-{worker}",
            )
            self.threads.append(thread)
            thread.start()
        for ready in self.ready:
            if not ready.wait(5.0):
                raise TimeoutError("benchmark asyncio loop did not start")

    def stop(self, timeout: float) -> None:
        deadline = monotonic() + timeout
        for loop in self.loops:
            if loop is not None:
                loop.call_soon_threadsafe(loop.stop)
        for thread in self.threads:
            thread.join(max(0.0, deadline - monotonic()))
        if any(thread.is_alive() for thread in self.threads):
            raise TimeoutError("benchmark asyncio loop did not stop")

    def get_loop(self, worker: int) -> asyncio.AbstractEventLoop:
        loop = self.loops[worker]
        if loop is None:
            raise RuntimeError("benchmark asyncio loop is not running")
        return loop

    def create_task(self, coro, worker: int) -> None:
        loop = self.get_loop(worker)

        def create() -> None:
            with self.metrics_lock:
                self.scheduled_tasks[worker] += 1
            task = loop.create_task(coro)

            def completed(done: asyncio.Task) -> None:
                if not done.cancelled() and (error := done.exception()) is not None:
                    print(f"asyncio task failed: {error}", file=sys.stderr)

            task.add_done_callback(completed)

        self.call_soon(worker, create)

    def call_soon(self, worker: int, callback) -> None:
        self.get_loop(worker).call_soon_threadsafe(callback)

    def assign_worker(self) -> int:
        with self.metrics_lock:
            worker = self.next_worker
            self.next_worker = (worker + 1) % self.worker_count
            self.assigned_connections[worker] += 1
            return worker

    def worker_stats(self) -> tuple[tuple[int, ...], tuple[int, ...]]:
        with self.metrics_lock:
            return tuple(self.assigned_connections), tuple(self.scheduled_tasks)

    def event_loop(self, worker: int) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.loops[worker] = loop
        self.ready[worker].set()
        try:
            loop.run_forever()
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()


AsyncIO = ExtensionId("benchmark-asyncio", AsyncIOExtension)


class TCPConnection(AbstractBehavior[TCPConnectionCommand]):
    def __init__(
        self,
        context: ActorContext[TCPConnectionCommand],
        sock: socket.socket,
        worker: int,
    ) -> None:
        super().__init__(context)
        self.aio = AsyncIO.get(context.get_system())
        self.socket = sock
        self.worker = worker
        self.handler: ActorRef[Received | ConnectionClosed] | None = None
        self.reading = False
        self.closed = False
        self.close_requested = False
        self.outbound: deque[bytes] = deque()
        self.outbound_messages = 0
        self.outbound_bytes = 0
        self.writer_wake_scheduled = False
        self.writer_ready = asyncio.Event()
        self.lock = Lock()

    def receive(
        self,
        context: ActorContext,
        message: TCPConnectionCommand,
    ) -> AbstractBehavior | None:
        match message:
            case Register(handler=handler) if not self.reading:
                self.handler = handler
                self.reading = True
                self.aio.create_task(self.read(), self.worker)
                self.aio.create_task(self.write_loop(), self.worker)
            case Write() as command:
                self.enqueue_write(command)
            case Close():
                self.request_close()
        return Behaviors.same

    def enqueue_write(self, command: Write) -> None:
        failure = None
        wake = False
        record_bytes = _FRAME_HEADER.size + len(command.data)
        with self.lock:
            if self.closed or self.close_requested:
                failure = "TCP connection is closing"
            elif record_bytes > self.aio.maximum_bytes:
                failure = "TCP record exceeds the maximum batch size"
            elif (
                self.outbound_messages >= _WRITE_MESSAGE_LIMIT
                or self.outbound_bytes + record_bytes > _WRITE_BYTE_LIMIT
            ):
                failure = "TCP write buffer is full"
            else:
                self.outbound.append(command.data)
                self.outbound_messages += 1
                self.outbound_bytes += record_bytes
                if not self.writer_wake_scheduled:
                    self.writer_wake_scheduled = True
                    wake = True
        if failure is not None:
            if command.reply_to is not None:
                command.reply_to.tell(CommandFailed(command, failure))
            return
        if wake:
            self.aio.call_soon(self.worker, self.writer_ready.set)

    def request_close(self) -> None:
        wake = False
        with self.lock:
            if self.closed or self.close_requested:
                return
            self.close_requested = True
            if not self.writer_wake_scheduled:
                self.writer_wake_scheduled = True
                wake = True
        if wake:
            self.aio.call_soon(self.worker, self.writer_ready.set)

    async def read(self) -> None:
        loop = asyncio.get_running_loop()
        if loop is not self.aio.get_loop(self.worker):
            raise RuntimeError("TCP reader is running on the wrong event loop")
        buffer = bytearray()
        try:
            while True:
                data = await loop.sock_recv(self.socket, 64 * 1024)
                if not data:
                    return
                buffer.extend(data)
                payloads = []
                offset = 0
                while len(buffer) - offset >= _FRAME_HEADER.size:
                    (payload_bytes,) = _FRAME_HEADER.unpack_from(buffer, offset)
                    record_bytes = _FRAME_HEADER.size + payload_bytes
                    if record_bytes > self.aio.maximum_bytes:
                        raise ValueError("TCP record exceeds the maximum batch size")
                    if len(buffer) - offset < record_bytes:
                        break
                    start = offset + _FRAME_HEADER.size
                    offset += record_bytes
                    payloads.append(bytes(buffer[start:offset]))
                if offset:
                    del buffer[:offset]
                handler = self.handler
                if handler is not None and payloads:
                    handler.tell(Received(self.context.get_self(), tuple(payloads)))
        except ConnectionError:
            pass
        finally:
            await self.close()

    async def write_loop(self) -> None:
        loop = asyncio.get_running_loop()
        if loop is not self.aio.get_loop(self.worker):
            raise RuntimeError("TCP writer is running on the wrong event loop")
        pending: list[bytes] = []
        pending_bytes = 0
        deadline = 0.0
        try:
            while True:
                if pending:
                    timeout = max(0.0, deadline - loop.time())
                    try:
                        await asyncio.wait_for(self.writer_ready.wait(), timeout)
                    except TimeoutError:
                        pass
                else:
                    await self.writer_ready.wait()
                self.writer_ready.clear()
                while True:
                    with self.lock:
                        if self.closed:
                            return
                        submissions = []
                        while self.outbound and len(submissions) < _OUTBOUND_DRAIN_RECORDS:
                            submissions.append(self.outbound.popleft())
                        if not submissions:
                            self.writer_wake_scheduled = False
                            break
                    for payload in submissions:
                        record_bytes = _FRAME_HEADER.size + len(payload)
                        if pending and (
                            len(pending) >= self.aio.maximum_records
                            or pending_bytes + record_bytes > self.aio.maximum_bytes
                        ):
                            await self.send_batch(loop, pending, pending_bytes)
                            pending = []
                            pending_bytes = 0
                        if not pending:
                            deadline = loop.time() + self.aio.maximum_delay
                        pending.append(payload)
                        pending_bytes += record_bytes
                        if (
                            len(pending) >= self.aio.maximum_records
                            or pending_bytes >= self.aio.maximum_bytes
                            or self.aio.maximum_delay == 0
                        ):
                            await self.send_batch(loop, pending, pending_bytes)
                            pending = []
                            pending_bytes = 0
                with self.lock:
                    close = self.close_requested and not self.outbound
                if pending and (close or deadline <= loop.time()):
                    await self.send_batch(loop, pending, pending_bytes)
                    pending = []
                    pending_bytes = 0
                if close and not pending:
                    await self.close()
                    return
        except Exception:
            await self.close()

    async def send_batch(
        self,
        loop: asyncio.AbstractEventLoop,
        payloads: list[bytes],
        batch_bytes: int,
    ) -> None:
        wire_data = b"".join(_FRAME_HEADER.pack(len(payload)) + payload for payload in payloads)
        await loop.sock_sendall(self.socket, wire_data)
        with self.lock:
            self.outbound_messages -= len(payloads)
            self.outbound_bytes -= batch_bytes

    async def close(self) -> None:
        with self.lock:
            if self.closed:
                return
            self.closed = True
            self.outbound.clear()
            self.outbound_messages = 0
            self.outbound_bytes = 0
        self.writer_ready.set()
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.socket.close()
        handler = self.handler
        if handler is not None:
            handler.tell(ConnectionClosed(self.context.get_self()))
        self.context.get_system().terminate(self.context.get_self())

    @staticmethod
    def create(
        sock: socket.socket,
        worker: int,
    ) -> AbstractBehavior[TCPConnectionCommand]:
        return Behaviors.setup(lambda context: TCPConnection(context, sock, worker))


class TCPListener(AbstractBehavior[TCPListenerMessage]):
    def __init__(self, context: ActorContext[TCPListenerMessage]) -> None:
        super().__init__(context)
        self.aio = AsyncIO.get(context.get_system())
        self.listener: socket.socket | None = None
        self.local: tuple[str, int] | None = None
        self.accept_thread: Thread | None = None
        self.accept_stop = Event()
        self.connection_ids = count(1)

    def receive(
        self,
        context: ActorContext,
        message: TCPListenerMessage,
    ) -> AbstractBehavior | None:
        match message:
            case Bind() as command:
                if self.listener is not None:
                    command.reply_to.tell(CommandFailed(command, "TCP listener is already bound"))
                else:
                    self.bind(command)
            case Unbind() as command:
                listener = self.listener
                local = self.local
                if listener is None or local is None:
                    command.reply_to.tell(CommandFailed(command, "TCP listener is not bound"))
                else:
                    self.unbind(command, listener, local)
        return Behaviors.same

    def bind(self, command: Bind) -> None:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            else:
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((command.host, command.port))
            listener.listen(128)
            listener.settimeout(0.1)
            address = listener.getsockname()
            local = (str(address[0]), int(address[1]))
            self.accept_stop.clear()
            thread = Thread(
                target=self.accept,
                args=(listener, command.accepted_to),
                name=f"benchmark-tcp-accept-{self.context.get_system().name}",
            )
        except OSError as error:
            listener.close()
            command.reply_to.tell(CommandFailed(command, str(error)))
            return
        self.listener = listener
        self.local = local
        self.accept_thread = thread
        thread.start()
        command.reply_to.tell(Bound(local[0], local[1], self.context.get_self()))

    def accept(
        self,
        listener: socket.socket,
        accepted_to: ActorRef[TCPEvent],
    ) -> None:
        while not self.accept_stop.is_set():
            try:
                client, remote = listener.accept()
            except TimeoutError:
                continue
            except OSError as error:
                if not self.accept_stop.is_set():
                    print(f"TCP accept failed: {error}", file=sys.stderr)
                return
            client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            client.setblocking(False)
            local = client.getsockname()
            worker = self.aio.assign_worker()
            try:
                connection = self.context.get_system().spawn(
                    TCPConnection.create(client, worker),
                    f"tcp-connection-{next(self.connection_ids)}",
                )
            except BaseException:
                client.close()
                raise
            accepted_to.tell(
                Connected(
                    connection,
                    str(local[0]),
                    int(local[1]),
                    str(remote[0]),
                    int(remote[1]),
                )
            )

    def unbind(
        self,
        command: Unbind,
        listener: socket.socket,
        local: tuple[str, int],
    ) -> None:
        self.listener = None
        self.local = None
        self.accept_stop.set()
        listener.close()
        thread = self.accept_thread
        self.accept_thread = None
        if thread is not None:
            thread.join(1.0)
        command.reply_to.tell(Unbound(local[0], local[1]))

    @staticmethod
    def create() -> AbstractBehavior[TCPListenerMessage]:
        return Behaviors.setup(TCPListener)


class EchoHandler(AbstractBehavior[TCPEvent]):
    def receive(
        self,
        context: ActorContext,
        message: TCPEvent,
    ) -> AbstractBehavior | None:
        match message:
            case Connected(connection=connection):
                connection.tell(Register(context.get_self()))
            case Received(connection=connection, payloads=payloads):
                for payload in payloads:
                    connection.tell(Write(payload))
        return Behaviors.same

    @staticmethod
    def create() -> AbstractBehavior[TCPEvent]:
        return Behaviors.setup(EchoHandler)


def queue_behavior(messages: Queue) -> AbstractBehavior:
    def receive(context: ActorContext, message: object) -> AbstractBehavior:
        messages.put(message)
        return Behaviors.same

    return Behaviors.receive(receive)


async def close_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
    try:
        await writer.wait_closed()
    except ConnectionError:
        pass


async def request_worker(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    payload: bytes,
    count: int,
    batch_capacity: int,
) -> None:
    record = _FRAME_HEADER.pack(len(payload)) + payload
    remaining = count
    while remaining:
        records = min(batch_capacity, remaining)
        wire_data = record * records
        writer.write(wire_data)
        await writer.drain()
        response = await reader.readexactly(len(wire_data))
        if response != wire_data:
            raise RuntimeError("benchmark server returned an unexpected response")
        remaining -= records


async def benchmark_once(
    host: str,
    port: int,
    requests: int,
    connection_count: int,
    payload_bytes: int,
    maximum_records: int,
    maximum_bytes: int,
) -> float:
    connection_count = min(connection_count, requests)
    connections = await asyncio.gather(
        *(asyncio.open_connection(host, port) for _ in range(connection_count))
    )
    base, extra = divmod(requests, connection_count)
    payload = b"x" * payload_bytes
    batch_capacity = min(
        maximum_records,
        maximum_bytes // (_FRAME_HEADER.size + payload_bytes),
    )
    started = perf_counter()
    try:
        await asyncio.gather(
            *(
                request_worker(
                    reader,
                    writer,
                    payload,
                    base + int(index < extra),
                    batch_capacity,
                )
                for index, (reader, writer) in enumerate(connections)
            )
        )
        return perf_counter() - started
    finally:
        await asyncio.gather(
            *(close_writer(writer) for _, writer in connections),
            return_exceptions=True,
        )


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be nonnegative")
    return parsed


def port_number(value: str) -> int:
    parsed = int(value)
    if not 0 <= parsed <= 65535:
        raise argparse.ArgumentTypeError("port must be between 0 and 65535")
    return parsed


def server_process(
    control: Connection,
    host: str,
    port: int,
    loops: int,
    maximum_records: int,
    maximum_bytes: int,
    maximum_delay_ms: int,
) -> None:
    system = None
    try:
        config = Config(
            {
                "benchmark": {
                    "asyncio": {
                        "loops": loops,
                        "maximum-records": maximum_records,
                        "maximum-bytes": maximum_bytes,
                        "maximum-delay-ms": maximum_delay_ms,
                    }
                }
            }
        )
        system = ActorSystem.create(TCPListener.create(), "TCPListener", config=config)
        bind_results: Queue = Queue()
        handler = system.spawn(EchoHandler.create(), "echo-handler")
        bind_probe = system.spawn(queue_behavior(bind_results), "bind-results")
        system.tell(Bind(host, port, handler, bind_probe))
        bound = bind_results.get(timeout=5.0)
        if isinstance(bound, CommandFailed):
            raise RuntimeError(f"TCP bind failed: {bound.detail}")
        if not isinstance(bound, Bound):
            raise RuntimeError(f"unexpected TCP bind result: {bound!r}")
        control.send(("bound", bound.host, bound.port))
        if control.recv() != "stats":
            raise RuntimeError("unexpected benchmark control command")
        assigned_connections, scheduled_tasks = AsyncIO.get(system).worker_stats()
        control.send(("worker-stats", assigned_connections, scheduled_tasks))
        if control.recv() != "stop":
            raise RuntimeError("unexpected benchmark control command")
        bound.listener.tell(Unbind(bind_probe))
        unbound = bind_results.get(timeout=5.0)
        if not isinstance(unbound, Unbound):
            raise RuntimeError(f"unexpected TCP unbind result: {unbound!r}")
    except BaseException:
        try:
            control.send(("error", traceback.format_exc()))
        except BrokenPipeError, EOFError, OSError:
            pass
    finally:
        if system is not None:
            system.stop()
        control.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark raw TCP roundtrips through an actor")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=port_number, default=9999)
    parser.add_argument("--requests", type=positive_int, default=100_000)
    parser.add_argument("--connections", type=positive_int, default=100)
    parser.add_argument("--payload", type=positive_int, default=64)
    parser.add_argument("--warmup", type=positive_int, default=5_000)
    parser.add_argument("--iterations", type=positive_int, default=3)
    parser.add_argument("--loops", type=positive_int, default=1)
    parser.add_argument("--maximum-records", type=positive_int, default=64)
    parser.add_argument("--maximum-bytes", type=positive_int, default=256 * 1024)
    parser.add_argument("--maximum-delay-ms", type=nonnegative_int, default=1)
    args = parser.parse_args()
    record_bytes = _FRAME_HEADER.size + args.payload
    if record_bytes > args.maximum_bytes:
        parser.error("framed payload exceeds maximum-bytes")
    batch_capacity = min(
        args.maximum_records,
        args.maximum_bytes // record_bytes,
    )
    if batch_capacity > _WRITE_MESSAGE_LIMIT:
        parser.error("maximum in-flight records exceed the TCP write message limit")
    if batch_capacity * record_bytes > _WRITE_BYTE_LIMIT:
        parser.error("maximum in-flight records exceed the TCP write byte limit")

    process_context = multiprocessing.get_context("spawn")
    control, server_control = process_context.Pipe()
    server = process_context.Process(
        target=server_process,
        args=(
            server_control,
            args.host,
            args.port,
            args.loops,
            args.maximum_records,
            args.maximum_bytes,
            args.maximum_delay_ms,
        ),
        name="tcp-benchmark-server",
    )
    server.start()
    server_control.close()
    try:
        if not control.poll(15.0):
            raise TimeoutError("TCP server process did not start")
        startup = control.recv()
        if startup[0] == "error":
            raise RuntimeError(f"TCP server process failed:\n{startup[1]}")
        if len(startup) != 3 or startup[0] != "bound":
            raise RuntimeError(f"unexpected server startup response: {startup!r}")
        host = str(startup[1])
        port = int(startup[2])

        asyncio.run(
            benchmark_once(
                host,
                port,
                args.warmup,
                args.connections,
                args.payload,
                args.maximum_records,
                args.maximum_bytes,
            )
        )

        rates = []
        for iteration in range(1, args.iterations + 1):
            elapsed = asyncio.run(
                benchmark_once(
                    host,
                    port,
                    args.requests,
                    args.connections,
                    args.payload,
                    args.maximum_records,
                    args.maximum_bytes,
                )
            )
            rate = args.requests / elapsed
            rates.append(rate)
            print(
                f"iteration={iteration} processes=2 loops={args.loops} "
                f"maximum_records={args.maximum_records} "
                f"maximum_bytes={args.maximum_bytes} "
                f"maximum_delay_ms={args.maximum_delay_ms} "
                f"requests={args.requests} "
                f"payload={args.payload} elapsed={elapsed:.6f}s "
                f"requests_per_second={rate:,.0f}"
            )

        control.send("stats")
        if not control.poll(5.0):
            raise TimeoutError("TCP server process did not return worker statistics")
        worker_stats = control.recv()
        if len(worker_stats) != 3 or worker_stats[0] != "worker-stats":
            raise RuntimeError(f"unexpected worker statistics: {worker_stats!r}")
        assigned_connections = worker_stats[1]
        scheduled_tasks = worker_stats[2]
        print(
            "worker_connections="
            + ",".join(map(str, assigned_connections))
            + " worker_tasks="
            + ",".join(map(str, scheduled_tasks))
        )
        print(
            f"processes=2 loops={args.loops} "
            f"maximum_records={args.maximum_records} "
            f"maximum_bytes={args.maximum_bytes} "
            f"maximum_delay_ms={args.maximum_delay_ms} "
            f"median_requests_per_second={statistics.median(rates):,.0f}"
        )
    finally:
        if server.is_alive():
            try:
                control.send("stop")
            except BrokenPipeError, EOFError, OSError:
                pass
        server.join(15.0)
        if server.is_alive():
            server.terminate()
            server.join(5.0)
        control.close()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
