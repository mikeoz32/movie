import asyncio
import socket
import threading
import time
from queue import Empty, Queue

import pytest

import movie.io.tcp as tcp_module
from movie.actor import ActorSystem, Behaviors
from movie.config import Config
from movie.io import (
    ASYNCIO_IO,
    TCP,
    Bind,
    Bound,
    Close,
    Closed,
    CommandFailed,
    Connect,
    Connected,
    ConnectionClosed,
    ListenerClosed,
    PeerClosed,
    Read,
    Received,
    Register,
    TcpEndpoint,
    Unbind,
    Unbound,
    Write,
    WriteAccepted,
    WriteCompleted,
)


def probe_behavior(messages: Queue):
    def receive(context, message):
        messages.put(message)
        return Behaviors.same

    return Behaviors.receive(receive)


def create_system(name: str, config: Config | None = None) -> ActorSystem:
    return ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        name,
        config=config,
    )


def connect_pair(system: ActorSystem):
    bind_results: Queue = Queue()
    connect_results: Queue = Queue()
    accepted_events: Queue = Queue()
    outbound_events: Queue = Queue()
    probes = {
        name: system.spawn(probe_behavior(messages), name)
        for name, messages in (
            ("pair-bind-results", bind_results),
            ("pair-connect-results", connect_results),
            ("pair-accepted-events", accepted_events),
            ("pair-outbound-events", outbound_events),
        )
    }
    tcp = TCP.get(system)
    tcp.manager.tell(
        Bind(
            TcpEndpoint("127.0.0.1", 0),
            probes["pair-accepted-events"],
            probes["pair-bind-results"],
        )
    )
    bound = bind_results.get(timeout=2.0)
    assert isinstance(bound, Bound)
    tcp.manager.tell(
        Connect(
            bound.local,
            probes["pair-connect-results"],
        )
    )
    outbound = connect_results.get(timeout=2.0)
    accepted = accepted_events.get(timeout=2.0)
    assert isinstance(outbound, Connected)
    assert isinstance(accepted, Connected)
    outbound.connection.tell(Register(probes["pair-outbound-events"]))
    accepted.connection.tell(Register(probes["pair-accepted-events"]))
    return tcp, bound, outbound, accepted, accepted_events, outbound_events


def receive_bytes(
    events: Queue,
    connection,
    byte_count: int,
    timeout: float = 2.0,
) -> bytes:
    deadline = time.monotonic() + timeout
    received = bytearray()
    while len(received) < byte_count:
        event = events.get(timeout=max(0.0, deadline - time.monotonic()))
        assert isinstance(event, Received)
        assert event.connection is connection
        received.extend(event.data)
    return bytes(received)


def test_tcp_extension_is_an_actor_system_singleton() -> None:
    system = create_system("tcp-extension-singleton")
    try:
        first = TCP.get(system)
        second = TCP.get(system)

        assert first is second
        assert first.manager is second.manager
        assert first.manager.name.startswith("tcp-manager-")
    finally:
        system.stop()


def test_tcp_manager_bind_connect_and_bidirectional_io() -> None:
    system = create_system("tcp-manager-round-trip")
    bind_results: Queue = Queue()
    connect_results: Queue = Queue()
    accepted_events: Queue = Queue()
    outbound_events: Queue = Queue()
    write_results: Queue = Queue()
    close_results: Queue = Queue()
    unbind_results: Queue = Queue()
    probes = {
        name: system.spawn(probe_behavior(messages), name)
        for name, messages in (
            ("bind-results", bind_results),
            ("connect-results", connect_results),
            ("accepted-events", accepted_events),
            ("outbound-events", outbound_events),
            ("write-results", write_results),
            ("close-results", close_results),
            ("unbind-results", unbind_results),
        )
    }
    tcp = TCP.get(system)
    try:
        tcp.manager.tell(
            Bind(
                TcpEndpoint("127.0.0.1", 0),
                probes["accepted-events"],
                probes["bind-results"],
            )
        )
        bound = bind_results.get(timeout=2.0)
        assert isinstance(bound, Bound)
        assert bound.local.host == "127.0.0.1"
        assert bound.local.port > 0

        tcp.manager.tell(
            Connect(
                bound.local,
                probes["connect-results"],
            )
        )
        outbound = connect_results.get(timeout=2.0)
        accepted = accepted_events.get(timeout=2.0)
        assert isinstance(outbound, Connected)
        assert isinstance(accepted, Connected)
        outbound.connection.tell(Register(probes["outbound-events"]))
        accepted.connection.tell(Register(probes["accepted-events"]))

        outbound.connection.tell(Write(b"hello", probes["write-results"]))
        ack = write_results.get(timeout=2.0)
        received = accepted_events.get(timeout=2.0)
        assert ack == WriteAccepted(outbound.connection, 5)
        assert received == Received(accepted.connection, b"hello")

        accepted.connection.tell(Write(b"world", probes["write-results"]))
        ack = write_results.get(timeout=2.0)
        received = outbound_events.get(timeout=2.0)
        assert ack == WriteAccepted(accepted.connection, 5)
        assert received == Received(outbound.connection, b"world")

        outbound.connection.tell(Close(probes["close-results"]))
        assert close_results.get(timeout=2.0) == Closed(outbound.connection)
        assert isinstance(outbound_events.get(timeout=2.0), ConnectionClosed)
        assert isinstance(accepted_events.get(timeout=2.0), ConnectionClosed)

        bound.listener.tell(Unbind(probes["unbind-results"]))
        assert unbind_results.get(timeout=2.0) == Unbound(bound.local)
    finally:
        system.stop()


def test_tcp_connection_does_not_read_before_registration() -> None:
    system = create_system("tcp-registration-gate")
    bind_results: Queue = Queue()
    accepted_events: Queue = Queue()
    connection_events: Queue = Queue()
    bind_probe = system.spawn(probe_behavior(bind_results), "registration-bind-results")
    accept_probe = system.spawn(probe_behavior(accepted_events), "registration-accept-events")
    connection_probe = system.spawn(
        probe_behavior(connection_events),
        "registration-connection-events",
    )
    tcp = TCP.get(system)
    client = None
    bound = None
    try:
        tcp.manager.tell(Bind(TcpEndpoint("127.0.0.1", 0), accept_probe, bind_probe))
        bound = bind_results.get(timeout=2.0)
        assert isinstance(bound, Bound)
        client = socket.create_connection((bound.local.host, bound.local.port))
        accepted = accepted_events.get(timeout=2.0)
        assert isinstance(accepted, Connected)

        client.sendall(b"held-until-register")
        with pytest.raises(Empty):
            accepted_events.get(timeout=0.05)

        accepted.connection.tell(Register(connection_probe))
        assert connection_events.get(timeout=2.0) == Received(
            accepted.connection,
            b"held-until-register",
        )
    finally:
        if client is not None:
            client.close()
        if isinstance(bound, Bound):
            bound.listener.tell(Unbind(bind_probe))
        system.stop()


def test_tcp_connection_keeps_its_first_registration() -> None:
    system = create_system("tcp-first-registration")
    bind_results: Queue = Queue()
    accepted_events: Queue = Queue()
    first_events: Queue = Queue()
    second_events: Queue = Queue()
    bind_probe = system.spawn(probe_behavior(bind_results), "first-registration-bind")
    accept_probe = system.spawn(probe_behavior(accepted_events), "first-registration-accept")
    first_probe = system.spawn(probe_behavior(first_events), "first-registration-first")
    second_probe = system.spawn(probe_behavior(second_events), "first-registration-second")
    tcp = TCP.get(system)
    client = None
    try:
        tcp.manager.tell(Bind(TcpEndpoint("127.0.0.1", 0), accept_probe, bind_probe))
        bound = bind_results.get(timeout=2.0)
        assert isinstance(bound, Bound)
        client = socket.create_connection((bound.local.host, bound.local.port))
        accepted = accepted_events.get(timeout=2.0)
        assert isinstance(accepted, Connected)
        accepted.connection.tell(Register(first_probe))
        accepted.connection.tell(Register(second_probe))

        client.sendall(b"first-handler")
        assert first_events.get(timeout=2.0) == Received(
            accepted.connection,
            b"first-handler",
        )
        with pytest.raises(Empty):
            second_events.get(timeout=0.05)
    finally:
        if client is not None:
            client.close()
        system.stop()


def test_tcp_pull_registration_requires_one_read_per_chunk() -> None:
    system = create_system("tcp-pull-read")
    bind_results: Queue = Queue()
    accepted_events: Queue = Queue()
    read_events: Queue = Queue()
    bind_probe = system.spawn(probe_behavior(bind_results), "pull-read-bind")
    accept_probe = system.spawn(probe_behavior(accepted_events), "pull-read-accept")
    read_probe = system.spawn(probe_behavior(read_events), "pull-read-events")
    tcp = TCP.get(system)
    client = None
    try:
        tcp.manager.tell(Bind(TcpEndpoint("127.0.0.1", 0), accept_probe, bind_probe))
        bound = bind_results.get(timeout=2.0)
        assert isinstance(bound, Bound)
        client = socket.create_connection((bound.local.host, bound.local.port))
        accepted = accepted_events.get(timeout=2.0)
        assert isinstance(accepted, Connected)
        accepted.connection.tell(Register(read_probe, pull_mode=True))

        client.sendall(b"first")
        with pytest.raises(Empty):
            read_events.get(timeout=0.05)
        accepted.connection.tell(Read())
        assert read_events.get(timeout=2.0) == Received(accepted.connection, b"first")

        client.sendall(b"second")
        with pytest.raises(Empty):
            read_events.get(timeout=0.05)
        accepted.connection.tell(Read())
        assert read_events.get(timeout=2.0) == Received(accepted.connection, b"second")
    finally:
        if client is not None:
            client.close()
        system.stop()


def test_tcp_pull_mode_keeps_writes_open_after_peer_eof() -> None:
    system = create_system("tcp-pull-half-close")
    bind_results: Queue = Queue()
    accepted_events: Queue = Queue()
    connection_events: Queue = Queue()
    bind_probe = system.spawn(probe_behavior(bind_results), "half-close-bind")
    accept_probe = system.spawn(probe_behavior(accepted_events), "half-close-accept")
    connection_probe = system.spawn(probe_behavior(connection_events), "half-close-events")
    tcp = TCP.get(system)
    client = None
    try:
        tcp.manager.tell(Bind(TcpEndpoint("127.0.0.1", 0), accept_probe, bind_probe))
        bound = bind_results.get(timeout=2.0)
        assert isinstance(bound, Bound)
        client = socket.create_connection((bound.local.host, bound.local.port))
        client.settimeout(2.0)
        accepted = accepted_events.get(timeout=2.0)
        assert isinstance(accepted, Connected)
        accepted.connection.tell(Register(connection_probe, pull_mode=True))

        client.sendall(b"request")
        accepted.connection.tell(Read())
        assert connection_events.get(timeout=2.0) == Received(
            accepted.connection,
            b"request",
        )
        client.shutdown(socket.SHUT_WR)
        accepted.connection.tell(Read())
        assert connection_events.get(timeout=2.0) == PeerClosed(accepted.connection)

        accepted.connection.tell(Write(b"response", completion_to=connection_probe))
        assert client.recv(8) == b"response"
        assert connection_events.get(timeout=2.0) == WriteCompleted(
            accepted.connection,
            8,
        )
        accepted.connection.tell(Close(connection_probe))
        assert connection_events.get(timeout=2.0) == ConnectionClosed(
            accepted.connection,
            None,
        )
        assert connection_events.get(timeout=2.0) == Closed(accepted.connection)
    finally:
        if client is not None:
            client.close()
        system.stop()


def test_tcp_listeners_are_distributed_across_io_workers() -> None:
    system = create_system(
        "tcp-listener-pool",
        Config({"movie": {"io": {"asyncio": {"event-loop-count": 2}}}}),
    )
    results: Queue = Queue()
    events: Queue = Queue()
    result_probe = system.spawn(probe_behavior(results), "listener-pool-results")
    event_probe = system.spawn(probe_behavior(events), "listener-pool-events")
    tcp = TCP.get(system)
    try:
        tcp.manager.tell(Bind(TcpEndpoint("127.0.0.1", 0), event_probe, result_probe))
        tcp.manager.tell(Bind(TcpEndpoint("127.0.0.1", 0), event_probe, result_probe))
        bound = [results.get(timeout=2.0), results.get(timeout=2.0)]
        assert all(isinstance(result, Bound) for result in bound)

        listener_workers = {
            listener.worker.index
            for listener in tcp._listeners
            if any(listener._ref == result.listener for result in bound)
        }
        assert listener_workers == {0, 1}
    finally:
        system.stop()


def test_tcp_connections_are_distributed_across_io_workers() -> None:
    system = create_system(
        "tcp-worker-placement",
        Config(
            {
                "movie": {
                    "io": {
                        "asyncio": {
                            "event-loop-count": 2,
                        }
                    }
                }
            }
        ),
    )
    tcp, _, outbound, accepted, accepted_events, _ = connect_pair(system)
    try:
        connections = {connection.ref: connection for connection in tcp._connections}

        assert {
            connections[outbound.connection].worker.index,
            connections[accepted.connection].worker.index,
        } == {0, 1}

        outbound.connection.tell(Write(b"worker-handoff"))
        assert accepted_events.get(timeout=2.0) == Received(
            accepted.connection,
            b"worker-handoff",
        )
    finally:
        system.stop()


def test_tcp_listener_survives_accepted_handler_failure_at_io_capacity() -> None:
    system = create_system(
        "tcp-accept-handler-failure",
        Config({"movie": {"io": {"asyncio": {"command-capacity": 1}}}}),
    )
    bind_results: Queue = Queue()
    accepted_events: Queue = Queue()
    bind_probe = system.spawn(probe_behavior(bind_results), "handler-failure-bind")
    tcp = TCP.get(system)

    class RejectFirstConnected:
        def __init__(self) -> None:
            self.calls = 0

        def tell(self, message) -> None:
            self.calls += 1
            if self.calls == 1:
                tcp._io.schedule(lambda: None)
                raise RuntimeError("reject first connection")
            accepted_events.put(message)

    handler = RejectFirstConnected()
    clients = []
    try:
        tcp.manager.tell(Bind(TcpEndpoint("127.0.0.1", 0), handler, bind_probe))
        bound = bind_results.get(timeout=2.0)
        assert isinstance(bound, Bound)

        clients.append(socket.create_connection((bound.local.host, bound.local.port)))
        clients[0].settimeout(2.0)
        assert clients[0].recv(1) == b""

        clients.append(socket.create_connection((bound.local.host, bound.local.port)))
        accepted = accepted_events.get(timeout=2.0)
        assert isinstance(accepted, Connected)
        accepted.connection.tell(Register(bind_probe))
    finally:
        for client in clients:
            client.close()
        system.stop()


def test_tcp_listener_materializes_accepted_connections_concurrently(monkeypatch) -> None:
    system = create_system(
        "tcp-concurrent-accept-setup",
        Config(
            {
                "movie": {
                    "io": {
                        "asyncio": {
                            "command-capacity": 8,
                            "event-loop-count": 2,
                        }
                    }
                }
            }
        ),
    )
    bind_results: Queue = Queue()
    accepted_events: Queue = Queue()
    bind_probe = system.spawn(probe_behavior(bind_results), "concurrent-setup-bind")
    accept_probe = system.spawn(probe_behavior(accepted_events), "concurrent-setup-accept")
    tcp = TCP.get(system)
    original_accept = tcp._accept_connection_on_loop
    entered: Queue = Queue()
    release = threading.Event()

    async def delayed_accept(sock, worker, listener, accepted_setup) -> None:
        entered.put(worker.index)
        while not release.is_set():
            await asyncio.sleep(0.001)
        await original_accept(sock, worker, listener, accepted_setup)

    monkeypatch.setattr(tcp, "_accept_connection_on_loop", delayed_accept)
    clients = []
    try:
        tcp.manager.tell(Bind(TcpEndpoint("127.0.0.1", 0), accept_probe, bind_probe))
        bound = bind_results.get(timeout=2.0)
        assert isinstance(bound, Bound)

        clients.extend(
            socket.create_connection((bound.local.host, bound.local.port)) for _ in range(2)
        )

        assert {entered.get(timeout=1.0), entered.get(timeout=1.0)} == {0, 1}
        release.set()
        assert all(isinstance(accepted_events.get(timeout=2.0), Connected) for _ in clients)
    finally:
        release.set()
        for client in clients:
            client.close()
        system.stop()


def test_tcp_listener_unbind_waits_for_in_flight_accept_setups(monkeypatch) -> None:
    system = create_system(
        "tcp-cancel-accept-setup",
        Config({"movie": {"io": {"asyncio": {"event-loop-count": 2}}}}),
    )
    bind_results: Queue = Queue()
    accepted_events: Queue = Queue()
    bind_probe = system.spawn(probe_behavior(bind_results), "cancel-setup-bind")
    accept_probe = system.spawn(probe_behavior(accepted_events), "cancel-setup-accept")
    tcp = TCP.get(system)
    original_accept = tcp._accept_connection_on_loop
    entered = threading.Event()
    release = threading.Event()

    async def blocked_accept(sock, worker, listener, accepted_setup) -> None:
        entered.set()
        while not release.is_set():
            await asyncio.sleep(0.001)
        await original_accept(sock, worker, listener, accepted_setup)

    monkeypatch.setattr(tcp, "_accept_connection_on_loop", blocked_accept)
    client = None
    try:
        tcp.manager.tell(Bind(TcpEndpoint("127.0.0.1", 0), accept_probe, bind_probe))
        bound = bind_results.get(timeout=2.0)
        assert isinstance(bound, Bound)
        client = socket.create_connection((bound.local.host, bound.local.port))
        client.settimeout(2.0)
        assert entered.wait(1.0)

        bound.listener.tell(Unbind(bind_probe))

        with pytest.raises(Empty):
            bind_results.get(timeout=0.05)
        release.set()
        assert bind_results.get(timeout=2.0) == Unbound(bound.local)
        assert client.recv(1) == b""
        with pytest.raises(Empty):
            accepted_events.get(timeout=0.05)
    finally:
        release.set()
        if client is not None:
            client.close()
        system.stop()


def test_tcp_listener_closes_socket_when_target_task_creation_fails(monkeypatch) -> None:
    system = create_system("tcp-accept-task-creation-failure")
    bind_results: Queue = Queue()
    accepted_events: Queue = Queue()
    bind_probe = system.spawn(probe_behavior(bind_results), "task-failure-bind")
    accept_probe = system.spawn(probe_behavior(accepted_events), "task-failure-accept")
    tcp = TCP.get(system)
    clients = []
    try:
        tcp.manager.tell(Bind(TcpEndpoint("127.0.0.1", 0), accept_probe, bind_probe))
        bound = bind_results.get(timeout=2.0)
        assert isinstance(bound, Bound)
        loop = tcp._io.default_worker._loop
        original_create_task = loop.create_task

        def fail_accepted_task(coroutine, *args, **kwargs):
            if coroutine.cr_code.co_name == "_accept_connection_on_loop":
                raise RuntimeError("injected accepted task creation failure")
            return original_create_task(coroutine, *args, **kwargs)

        monkeypatch.setattr(loop, "create_task", fail_accepted_task)
        clients.append(socket.create_connection((bound.local.host, bound.local.port)))
        clients[0].settimeout(2.0)
        assert clients[0].recv(1) == b""

        monkeypatch.setattr(loop, "create_task", original_create_task)
        clients.append(socket.create_connection((bound.local.host, bound.local.port)))
        assert isinstance(accepted_events.get(timeout=2.0), Connected)
    finally:
        for client in clients:
            client.close()
        system.stop()


def test_tcp_writer_flushes_at_batch_message_limit() -> None:
    system = create_system(
        "tcp-batch-message-limit",
        Config(
            {
                "movie": {
                    "io": {
                        "tcp": {
                            "write-batch-message-limit": 3,
                            "write-batch-byte-limit": 1024,
                            "write-batch-delay-ms": 500,
                        }
                    }
                }
            }
        ),
    )
    _, _, outbound, accepted, accepted_events, _ = connect_pair(system)
    try:
        outbound.connection.tell(Write(b"a"))
        outbound.connection.tell(Write(b"b"))
        with pytest.raises(Empty):
            accepted_events.get(timeout=0.05)

        outbound.connection.tell(Write(b"c"))
        assert receive_bytes(accepted_events, accepted.connection, 3) == b"abc"
    finally:
        system.stop()


def test_tcp_writer_flushes_at_batch_byte_limit() -> None:
    system = create_system(
        "tcp-batch-byte-limit",
        Config(
            {
                "movie": {
                    "io": {
                        "tcp": {
                            "write-batch-message-limit": 64,
                            "write-batch-byte-limit": 3,
                            "write-batch-delay-ms": 500,
                        }
                    }
                }
            }
        ),
    )
    _, _, outbound, accepted, accepted_events, _ = connect_pair(system)
    try:
        outbound.connection.tell(Write(b"a"))
        with pytest.raises(Empty):
            accepted_events.get(timeout=0.05)

        outbound.connection.tell(Write(b"bc"))
        assert receive_bytes(accepted_events, accepted.connection, 3) == b"abc"
    finally:
        system.stop()


def test_tcp_writer_splits_a_write_at_batch_byte_limit(monkeypatch) -> None:
    system = create_system(
        "tcp-batch-byte-split",
        Config(
            {
                "movie": {
                    "io": {
                        "tcp": {
                            "write-batch-message-limit": 64,
                            "write-batch-byte-limit": 3,
                            "write-batch-delay-ms": 0,
                        }
                    }
                }
            }
        ),
    )
    tcp, _, outbound, accepted, accepted_events, outbound_events = connect_pair(system)
    connection = next(
        connection for connection in tcp._connections if connection.ref == outbound.connection
    )
    batch_sizes: Queue = Queue()
    original_sock_sendall = connection._loop.sock_sendall

    async def record_batch(sock, data) -> None:
        batch_sizes.put(len(data))
        await original_sock_sendall(sock, data)

    monkeypatch.setattr(connection._loop, "sock_sendall", record_batch)
    completion_probe = system.spawn(probe_behavior(outbound_events), "byte-split-completion")
    try:
        outbound.connection.tell(Write(b"abcdef", completion_to=completion_probe))
        assert receive_bytes(accepted_events, accepted.connection, 6) == b"abcdef"
        assert [batch_sizes.get(timeout=1.0), batch_sizes.get(timeout=1.0)] == [3, 3]
        assert outbound_events.get(timeout=2.0) == WriteCompleted(outbound.connection, 6)
    finally:
        system.stop()


def test_tcp_write_completion_follows_socket_write(monkeypatch) -> None:
    system = create_system("tcp-write-completion")
    tcp, _, outbound, accepted, accepted_events, outbound_events = connect_pair(system)
    connection = next(
        connection for connection in tcp._connections if connection.ref == outbound.connection
    )
    send_entered = threading.Event()
    send_release = threading.Event()
    original_sock_sendall = connection._loop.sock_sendall
    completion_probe = system.spawn(probe_behavior(outbound_events), "write-completion-events")

    async def delayed_send(sock, data) -> None:
        send_entered.set()
        while not send_release.is_set():
            await asyncio.sleep(0.001)
        await original_sock_sendall(sock, data)

    monkeypatch.setattr(connection._loop, "sock_sendall", delayed_send)
    try:
        outbound.connection.tell(
            Write(
                b"completed",
                reply_to=completion_probe,
                completion_to=completion_probe,
            )
        )
        assert outbound_events.get(timeout=2.0) == WriteAccepted(outbound.connection, 9)
        assert send_entered.wait(1.0)
        with pytest.raises(Empty):
            outbound_events.get(timeout=0.05)

        send_release.set()
        assert receive_bytes(accepted_events, accepted.connection, 9) == b"completed"
        assert outbound_events.get(timeout=2.0) == WriteCompleted(outbound.connection, 9)
    finally:
        send_release.set()
        system.stop()


def test_tcp_writer_flushes_after_batch_delay() -> None:
    system = create_system(
        "tcp-batch-delay",
        Config(
            {
                "movie": {
                    "io": {
                        "tcp": {
                            "write-batch-message-limit": 64,
                            "write-batch-byte-limit": 1024,
                            "write-batch-delay-ms": 20,
                        }
                    }
                }
            }
        ),
    )
    _, _, outbound, accepted, accepted_events, _ = connect_pair(system)
    try:
        outbound.connection.tell(Write(b"delayed"))
        with pytest.raises(Empty):
            accepted_events.get(timeout=0.005)
        assert receive_bytes(accepted_events, accepted.connection, 7) == b"delayed"
    finally:
        system.stop()


def test_tcp_manager_reports_bind_failure_to_explicit_reply_actor() -> None:
    system = create_system("tcp-manager-bind-failure")
    results: Queue = Queue()
    events: Queue = Queue()
    result_probe = system.spawn(probe_behavior(results), "results")
    event_probe = system.spawn(probe_behavior(events), "events")
    tcp = TCP.get(system)
    try:
        tcp.manager.tell(Bind(TcpEndpoint("127.0.0.1", 0), event_probe, result_probe))
        first = results.get(timeout=2.0)
        assert isinstance(first, Bound)

        command = Bind(first.local, event_probe, result_probe)
        tcp.manager.tell(command)
        failure = results.get(timeout=2.0)
        assert isinstance(failure, CommandFailed)
        assert failure.command is command
        assert "bind failed" in failure.detail

        first.listener.tell(Unbind(result_probe))
        assert results.get(timeout=2.0) == Unbound(first.local)
    finally:
        system.stop()


def test_tcp_manager_bounds_in_flight_operations(monkeypatch) -> None:
    system = create_system(
        "tcp-operation-limit",
        Config({"movie": {"io": {"tcp": {"operation-limit": 1}}}}),
    )
    results: Queue = Queue()
    handler_events: Queue = Queue()
    reply_probe = system.spawn(probe_behavior(results), "operation-limit-reply")
    handler_probe = system.spawn(probe_behavior(handler_events), "operation-limit-handler")
    tcp = TCP.get(system)
    resolve_entered = threading.Event()
    resolve_release = threading.Event()
    original_resolve = tcp._resolve

    async def delayed_resolve(endpoint, *, passive):
        resolve_entered.set()
        while not resolve_release.is_set():
            await asyncio.sleep(0.001)
        return await original_resolve(endpoint, passive=passive)

    monkeypatch.setattr(tcp, "_resolve", delayed_resolve)
    first = Bind(TcpEndpoint("127.0.0.1", 0), handler_probe, reply_probe)
    second = Bind(TcpEndpoint("127.0.0.1", 0), handler_probe, reply_probe)
    try:
        tcp.manager.tell(first)
        assert resolve_entered.wait(1.0)
        tcp.manager.tell(second)

        assert results.get(timeout=2.0) == CommandFailed(
            second,
            "TCP operation capacity is full",
        )
        resolve_release.set()
        assert isinstance(results.get(timeout=2.0), Bound)
    finally:
        resolve_release.set()
        system.stop()


def test_terminal_close_bypasses_saturated_asyncio_command_capacity() -> None:
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "tcp-close-admission",
        config=Config(
            {
                "movie": {
                    "io": {
                        "asyncio": {"command-capacity": 1},
                    }
                }
            }
        ),
    )
    close_results: Queue = Queue()
    close_probe = system.spawn(probe_behavior(close_results), "close-admission-results")
    tcp, bound, outbound, _, _, _ = connect_pair(system)
    entered = threading.Event()
    release = threading.Event()

    def block_loop() -> None:
        entered.set()
        release.wait(1.0)

    blocker = ASYNCIO_IO.get(system).schedule(block_loop)
    try:
        assert entered.wait(1.0)
        outbound.connection.tell(Close(close_probe))
        release.set()
        blocker.result(1.0)
        assert close_results.get(timeout=2.0) == Closed(outbound.connection)
    finally:
        release.set()
        bound.listener.tell(Unbind(close_probe))
        system.stop()


def test_tcp_shutdown_retries_saturated_asyncio_control_admission() -> None:
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "tcp-shutdown-admission",
        config=Config(
            {
                "movie": {
                    "io": {
                        "asyncio": {"command-capacity": 1},
                    }
                }
            }
        ),
    )
    tcp = TCP.get(system)
    replies: Queue = Queue()
    reply_probe = system.spawn(probe_behavior(replies), "shutdown-replies")
    tcp.manager.tell(Bind(TcpEndpoint("127.0.0.1", 0), reply_probe, reply_probe))
    assert isinstance(replies.get(timeout=2.0), Bound)
    entered = threading.Event()
    release = threading.Event()

    def block_loop() -> None:
        entered.set()
        release.wait(1.0)

    ASYNCIO_IO.get(system).schedule(block_loop)
    assert entered.wait(1.0)
    errors = []

    def stop_system() -> None:
        try:
            system.stop(2.0)
        except BaseException as error:
            errors.append(error)

    stopper = threading.Thread(target=stop_system)
    stopper.start()
    try:
        time.sleep(0.05)
        assert stopper.is_alive()
        release.set()
        stopper.join(2.0)

        assert not stopper.is_alive()
        assert not errors
        assert not tcp._listeners
        assert not tcp._connections
    finally:
        release.set()
        stopper.join(2.0)
        system.stop()


def test_connection_task_start_failure_rolls_back_actor_and_socket(
    monkeypatch,
) -> None:
    system = create_system("tcp-connection-start-rollback")
    bind_results: Queue = Queue()
    connect_results: Queue = Queue()
    events: Queue = Queue()
    bind_probe = system.spawn(probe_behavior(bind_results), "rollback-bind-results")
    connect_probe = system.spawn(
        probe_behavior(connect_results),
        "rollback-connect-results",
    )
    event_probe = system.spawn(probe_behavior(events), "rollback-events")
    tcp = TCP.get(system)
    tcp.manager.tell(Bind(TcpEndpoint("127.0.0.1", 0), event_probe, bind_probe))
    bound = bind_results.get(timeout=2.0)
    assert isinstance(bound, Bound)
    original_create_task = tcp_module._create_task

    def fail_writer(coroutine):
        if coroutine.cr_code.co_name == "_writer_loop":
            coroutine.close()
            raise RuntimeError("injected writer task failure")
        return original_create_task(coroutine)

    monkeypatch.setattr(tcp_module, "_create_task", fail_writer)
    try:
        tcp.manager.tell(Connect(bound.local, connect_probe))
        failure = connect_results.get(timeout=2.0)
        assert isinstance(failure, CommandFailed)
        assert "injected writer task failure" in failure.detail
        deadline = time.monotonic() + 1.0
        while tcp._connections and time.monotonic() < deadline:
            time.sleep(0.005)
        assert not tcp._connections
    finally:
        bound.listener.tell(Unbind(bind_probe))
        system.stop()


def test_completed_terminal_operations_replay_results() -> None:
    system = create_system("tcp-terminal-result-replay")
    replies: Queue = Queue()
    reply_probe = system.spawn(probe_behavior(replies), "terminal-replies")
    tcp, bound, outbound, _, _, _ = connect_pair(system)
    connection = next(item for item in tcp._connections if item.ref is outbound.connection)
    listener = next(item for item in tcp._listeners if item._ref is bound.listener)
    try:
        outbound.connection.tell(Close(reply_probe))
        assert replies.get(timeout=2.0) == Closed(outbound.connection)

        async def replay_connection_close() -> None:
            connection._request_close_on_loop(reply_probe, None)

        ASYNCIO_IO.get(system).run_coroutine(
            replay_connection_close,
            timeout=1.0,
        )
        assert replies.get(timeout=2.0) == Closed(outbound.connection)

        bound.listener.tell(Unbind(reply_probe))
        assert replies.get(timeout=2.0) == Unbound(bound.local)

        async def replay_listener_close() -> None:
            listener._request_close_on_loop(reply_probe, None)

        ASYNCIO_IO.get(system).run_coroutine(
            replay_listener_close,
            timeout=1.0,
        )
        assert replies.get(timeout=2.0) == Unbound(bound.local)
    finally:
        system.stop()


def test_admitted_close_fences_later_writes_before_loop_cleanup() -> None:
    system = create_system("tcp-close-write-fence")
    close_results: Queue = Queue()
    write_results: Queue = Queue()
    close_probe = system.spawn(probe_behavior(close_results), "fence-close-results")
    write_probe = system.spawn(probe_behavior(write_results), "fence-write-results")
    _, _, outbound, _, _, _ = connect_pair(system)
    entered = threading.Event()
    release = threading.Event()

    def block_loop() -> None:
        entered.set()
        release.wait(1.0)

    blocker = ASYNCIO_IO.get(system).schedule(block_loop)
    try:
        assert entered.wait(1.0)
        close_command = Close(close_probe)
        write_command = Write(b"after-close", write_probe)
        outbound.connection.tell(close_command)
        outbound.connection.tell(write_command)

        failure = write_results.get(timeout=2.0)
        assert isinstance(failure, CommandFailed)
        assert failure.command is write_command

        release.set()
        blocker.result(1.0)
        assert close_results.get(timeout=2.0) == Closed(outbound.connection)
    finally:
        release.set()
        system.stop()


def test_listener_reports_asynchronous_failure_to_handler() -> None:
    system = create_system("tcp-listener-closed-event")
    tcp, bound, _, _, accepted_events, _ = connect_pair(system)
    listener = next(item for item in tcp._listeners if item._ref is bound.listener)
    try:

        async def fail_listener() -> None:
            await listener.close_on_loop("injected listener failure")

        ASYNCIO_IO.get(system).run_coroutine(fail_listener, timeout=1.0)
        event = accepted_events.get(timeout=2.0)
        assert event == ListenerClosed(bound.listener, "injected listener failure")
    finally:
        system.stop()


def test_close_task_creation_failure_uses_settled_fallback(monkeypatch) -> None:
    system = create_system("tcp-close-task-rollback")
    replies: Queue = Queue()
    reply_probe = system.spawn(probe_behavior(replies), "close-task-replies")
    tcp, _, outbound, _, _, _ = connect_pair(system)
    connection = next(item for item in tcp._connections if item.ref is outbound.connection)
    original_create_task = tcp_module._create_task

    def fail_close(coroutine):
        if coroutine.cr_code.co_name == "_finish_close":
            coroutine.close()
            raise RuntimeError("injected close task failure")
        return original_create_task(coroutine)

    monkeypatch.setattr(tcp_module, "_create_task", fail_close)
    try:
        outbound.connection.tell(Close(reply_probe))
        assert replies.get(timeout=2.0) == Closed(outbound.connection)
        assert connection._close_complete
        assert connection._socket.fileno() == -1
    finally:
        system.stop()


def test_listener_close_fallback_keeps_successfully_delivered_connection(monkeypatch) -> None:
    system = create_system("tcp-listener-close-task-rollback")
    bind_results: Queue = Queue()
    accepted_events: Queue = Queue()
    connection_events: Queue = Queue()
    bind_probe = system.spawn(probe_behavior(bind_results), "listener-close-task-bind")
    accept_probe = system.spawn(probe_behavior(accepted_events), "listener-close-task-accept")
    connection_probe = system.spawn(
        probe_behavior(connection_events),
        "listener-close-task-connection",
    )
    tcp = TCP.get(system)
    original_accept = tcp._accept_connection_on_loop
    delivered = threading.Event()
    release = threading.Event()

    async def hold_after_delivery(sock, worker, listener, accepted_setup) -> None:
        await original_accept(sock, worker, listener, accepted_setup)
        delivered.set()
        while not release.is_set():
            await asyncio.sleep(0.001)

    monkeypatch.setattr(tcp, "_accept_connection_on_loop", hold_after_delivery)
    client = None
    try:
        tcp.manager.tell(Bind(TcpEndpoint("127.0.0.1", 0), accept_probe, bind_probe))
        bound = bind_results.get(timeout=2.0)
        assert isinstance(bound, Bound)
        listener = next(item for item in tcp._listeners if item._ref is bound.listener)

        client = socket.create_connection((bound.local.host, bound.local.port))
        accepted = accepted_events.get(timeout=2.0)
        assert isinstance(accepted, Connected)
        assert delivered.wait(1.0)
        accepted.connection.tell(Register(connection_probe))
        assert listener._accept_setups
        assert not any(setup.done() for setup in listener._accept_setups)

        original_create_task = tcp_module._create_task

        def fail_listener_close(coroutine):
            if (
                coroutine.cr_code.co_name == "_finish_close"
                and coroutine.cr_frame.f_locals.get("self") is listener
            ):
                coroutine.close()
                raise RuntimeError("injected listener close task failure")
            return original_create_task(coroutine)

        monkeypatch.setattr(tcp_module, "_create_task", fail_listener_close)
        bound.listener.tell(Unbind(bind_probe))

        assert bind_results.get(timeout=2.0) == Unbound(bound.local)
        client.sendall(b"still-open")
        assert connection_events.get(timeout=2.0) == Received(
            accepted.connection,
            b"still-open",
        )
    finally:
        release.set()
        if client is not None:
            client.close()
        system.stop()


def test_tcp_value_validation() -> None:
    with pytest.raises(ValueError, match="between"):
        TcpEndpoint("127.0.0.1", 65536)
    with pytest.raises(TypeError, match="integer"):
        TcpEndpoint("127.0.0.1", True)
    with pytest.raises(ValueError, match="must not be empty"):
        Write(b"")
    with pytest.raises(TypeError, match="must be bytes"):
        Write("not-bytes")  # type: ignore[arg-type]
