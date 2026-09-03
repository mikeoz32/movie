import socket
from threading import Event, Lock

import pytest

import movie.http.server as http_server
from movie.actor import ActorSystem, Behaviors
from movie.config import Config
from movie.http import HTTP, HttpRequest, HttpResponse
from movie.io import TCP, TcpEndpoint
from movie.streams import Flow, RunnableGraph


def create_system(name: str, config: Config | None = None) -> ActorSystem:
    return ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        name,
        config=config,
    )


def receive_response(
    client: socket.socket,
    buffered: bytes = b"",
) -> tuple[int, dict[str, str], bytes, bytes]:
    while b"\r\n\r\n" not in buffered:
        chunk = client.recv(65536)
        if not chunk:
            raise ConnectionError("HTTP server closed before response headers")
        buffered += chunk
    raw_headers, buffered = buffered.split(b"\r\n\r\n", 1)
    lines = raw_headers.split(b"\r\n")
    version, status, _ = lines[0].decode("latin-1").split(" ", 2)
    assert version == "HTTP/1.1"
    headers = {}
    for line in lines[1:]:
        name, value = line.decode("latin-1").split(":", 1)
        headers[name.lower()] = value.strip()
    content_length = int(headers["content-length"])
    while len(buffered) < content_length:
        chunk = client.recv(65536)
        if not chunk:
            raise ConnectionError("HTTP server closed before response body")
        buffered += chunk
    return int(status), headers, buffered[:content_length], buffered[content_length:]


def echo_response(request: HttpRequest) -> HttpResponse:
    body = request.body or request.target.encode("ascii")
    return HttpResponse(200, (("Content-Type", "application/octet-stream"),), body)


def test_http_server_handles_fragmentation_keep_alive_and_pipelining() -> None:
    system = create_system("http-pipelining")
    binding = (
        HTTP.get(system)
        .bind(
            TcpEndpoint("127.0.0.1", 0),
            Flow.map(echo_response),
        )
        .result(timeout=2.0)
    )
    client = socket.create_connection((binding.local.host, binding.local.port))
    client.settimeout(2.0)
    try:
        client.sendall(b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nhe")
        client.sendall(b"llo")
        status, headers, body, buffered = receive_response(client)
        assert status == 200
        assert headers["content-type"] == "application/octet-stream"
        assert body == b"hello"

        client.sendall(
            b"GET /one HTTP/1.1\r\nHost: localhost\r\n\r\n"
            b"GET /two HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        )
        first = receive_response(client, buffered)
        second = receive_response(client, first[3])
        assert first[0] == 200
        assert first[2] == b"/one"
        assert "connection" not in first[1]
        assert second[0] == 200
        assert second[2] == b"/two"
        assert second[1]["connection"] == "close"
        assert client.recv(1) == b""
    finally:
        client.close()
        system.stop()


def test_http_server_reuses_one_handler_flow_across_connections() -> None:
    system = create_system("http-flow-reuse")
    handler = Flow.map(echo_response, name="shared-http-handler")
    binding = HTTP.get(system).bind(TcpEndpoint("127.0.0.1", 0), handler).result(timeout=2.0)
    clients = [socket.create_connection((binding.local.host, binding.local.port)) for _ in range(2)]
    try:
        for index, client in enumerate(clients):
            client.settimeout(2.0)
            client.sendall(f"GET /{index} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode("ascii"))
        assert receive_response(clients[0])[2] == b"/0"
        assert receive_response(clients[1])[2] == b"/1"
    finally:
        for client in clients:
            client.close()
        system.stop()


def test_http_server_materializes_connections_concurrently(monkeypatch) -> None:
    system = create_system(
        "http-concurrent-materialization",
        Config(
            {
                "movie": {
                    "dispatcher": {
                        "default-dispatcher": {
                            "workers": 4,
                        }
                    }
                }
            }
        ),
    )
    binding = (
        HTTP.get(system)
        .bind(
            TcpEndpoint("127.0.0.1", 0),
            Flow.map(echo_response),
        )
        .result(timeout=2.0)
    )
    original_run = RunnableGraph.run
    first_entered = Event()
    release_first = Event()
    invocation_lock = Lock()
    first = True

    def gated_run(graph, actor_system):
        nonlocal first
        with invocation_lock:
            block = first
            first = False
        if block:
            first_entered.set()
            assert release_first.wait(timeout=2.0)
        return original_run(graph, actor_system)

    monkeypatch.setattr(RunnableGraph, "run", gated_run)
    clients: list[socket.socket] = []
    try:
        first_client = socket.create_connection((binding.local.host, binding.local.port))
        clients.append(first_client)
        assert first_entered.wait(timeout=2.0)

        second_client = socket.create_connection((binding.local.host, binding.local.port))
        clients.append(second_client)
        second_client.settimeout(1.0)
        second_client.sendall(b"GET /second HTTP/1.1\r\nHost: localhost\r\n\r\n")

        assert receive_response(second_client)[2] == b"/second"
    finally:
        release_first.set()
        for client in clients:
            client.close()
        system.stop()


def test_http_server_returns_protocol_error_before_closing() -> None:
    system = create_system("http-protocol-error")
    binding = (
        HTTP.get(system)
        .bind(
            TcpEndpoint("127.0.0.1", 0),
            Flow.map(echo_response),
        )
        .result(timeout=2.0)
    )
    client = socket.create_connection((binding.local.host, binding.local.port))
    client.settimeout(2.0)
    try:
        client.sendall(b"GET / HTTP/1.1\r\n\r\n")
        status, headers, body, _ = receive_response(client)
        assert status == 400
        assert headers["connection"] == "close"
        assert b"Host" in body
        assert client.recv(1) == b""
    finally:
        client.close()
        system.stop()


def test_http_server_orders_protocol_error_after_valid_pipelined_response() -> None:
    system = create_system("http-pipelined-protocol-error")
    binding = (
        HTTP.get(system)
        .bind(
            TcpEndpoint("127.0.0.1", 0),
            Flow.map(echo_response),
        )
        .result(timeout=2.0)
    )
    client = socket.create_connection((binding.local.host, binding.local.port))
    client.settimeout(2.0)
    try:
        client.sendall(
            b"GET /valid HTTP/1.1\r\nHost: localhost\r\n\r\nGET /invalid HTTP/1.1\r\n\r\n"
        )
        first = receive_response(client)
        second = receive_response(client, first[3])
        assert first[0] == 200
        assert first[2] == b"/valid"
        assert second[0] == 400
        assert second[1]["connection"] == "close"
        assert client.recv(1) == b""
    finally:
        client.close()
        system.stop()


def test_http_server_preserves_all_valid_responses_with_response_buffer_one() -> None:
    system = create_system(
        "http-small-response-buffer",
        Config(
            {
                "movie": {
                    "http": {
                        "server": {
                            "max-pipelined-requests": 1,
                            "response-buffer": 1,
                        }
                    }
                }
            }
        ),
    )
    binding = (
        HTTP.get(system)
        .bind(
            TcpEndpoint("127.0.0.1", 0),
            Flow.map(echo_response),
        )
        .result(timeout=2.0)
    )
    client = socket.create_connection((binding.local.host, binding.local.port))
    client.settimeout(2.0)
    try:
        client.sendall(
            b"GET /one HTTP/1.1\r\nHost: localhost\r\n\r\n"
            b"GET /two HTTP/1.1\r\nHost: localhost\r\n\r\n"
            b"GET /invalid HTTP/1.1\r\n\r\n"
        )
        first = receive_response(client)
        second = receive_response(client, first[3])
        third = receive_response(client, second[3])
        assert (first[0], first[2]) == (200, b"/one")
        assert (second[0], second[2]) == (200, b"/two")
        assert third[0] == 400
        assert client.recv(1) == b""
    finally:
        client.close()
        system.stop()


def test_http_server_flushes_response_after_peer_half_close() -> None:
    system = create_system("http-peer-half-close")
    binding = (
        HTTP.get(system)
        .bind(
            TcpEndpoint("127.0.0.1", 0),
            Flow.map(echo_response),
        )
        .result(timeout=2.0)
    )
    client = socket.create_connection((binding.local.host, binding.local.port))
    client.settimeout(2.0)
    try:
        client.sendall(b"GET /half-close HTTP/1.1\r\nHost: localhost\r\n\r\n")
        client.shutdown(socket.SHUT_WR)

        status, _, body, _ = receive_response(client)
        assert status == 200
        assert body == b"/half-close"
        assert client.recv(1) == b""
    finally:
        client.close()
        system.stop()


def test_http_binding_unbind_stops_accepting_connections() -> None:
    system = create_system("http-unbind")
    binding = (
        HTTP.get(system)
        .bind(
            TcpEndpoint("127.0.0.1", 0),
            Flow.map(echo_response),
        )
        .result(timeout=2.0)
    )
    try:
        binding.unbind().result(timeout=2.0)
        with pytest.raises(OSError):
            socket.create_connection((binding.local.host, binding.local.port), timeout=0.2)
    finally:
        system.stop()


def test_http_binding_unbind_keeps_established_connection() -> None:
    system = create_system("http-unbind-established")
    binding = (
        HTTP.get(system)
        .bind(
            TcpEndpoint("127.0.0.1", 0),
            Flow.map(echo_response),
        )
        .result(timeout=2.0)
    )
    client = socket.create_connection((binding.local.host, binding.local.port))
    client.settimeout(2.0)
    try:
        client.sendall(b"GET /before HTTP/1.1\r\nHost: localhost\r\n\r\n")
        assert receive_response(client)[2] == b"/before"

        binding.unbind().result(timeout=2.0)

        client.sendall(b"GET /after HTTP/1.1\r\nHost: localhost\r\n\r\n")
        assert receive_response(client)[2] == b"/after"
    finally:
        client.close()
        system.stop()


def test_http_binding_unbind_drains_accepted_connection(monkeypatch) -> None:
    original_receive = http_server._HttpConnectionSource.receive
    connected_entered = Event()
    release_connected = Event()

    def gated_receive(source, context, message):
        if isinstance(message, http_server.Connected) and not connected_entered.is_set():
            connected_entered.set()
            assert release_connected.wait(timeout=2.0)
        return original_receive(source, context, message)

    monkeypatch.setattr(http_server._HttpConnectionSource, "receive", gated_receive)
    system = create_system("http-unbind-accepted")
    binding = (
        HTTP.get(system)
        .bind(
            TcpEndpoint("127.0.0.1", 0),
            Flow.map(echo_response),
        )
        .result(timeout=2.0)
    )
    client = socket.create_connection((binding.local.host, binding.local.port))
    client.settimeout(2.0)
    try:
        client.sendall(b"GET /accepted HTTP/1.1\r\nHost: localhost\r\n\r\n")
        assert connected_entered.wait(timeout=2.0)

        binding.unbind().result(timeout=2.0)
        release_connected.set()

        assert receive_response(client)[2] == b"/accepted"
    finally:
        release_connected.set()
        client.close()
        system.stop()


def test_http_binding_unbind_is_idempotent() -> None:
    system = create_system("http-unbind-idempotent")
    binding = (
        HTTP.get(system)
        .bind(
            TcpEndpoint("127.0.0.1", 0),
            Flow.map(echo_response),
        )
        .result(timeout=2.0)
    )
    try:
        first = binding.unbind()
        second = binding.unbind()
        assert first is second
        assert first.result(timeout=2.0) is None
        assert binding.unbind().result(timeout=0.0) is None
    finally:
        system.stop()


def test_http_shutdown_from_unbind_callback_does_not_wait_on_callbacks() -> None:
    system = create_system(
        "http-callback-shutdown",
        Config({"movie": {"actor": {"callback-workers": 1}}}),
    )
    binding = (
        HTTP.get(system)
        .bind(
            TcpEndpoint("127.0.0.1", 0),
            Flow.map(echo_response),
        )
        .result(timeout=2.0)
    )
    client = socket.create_connection((binding.local.host, binding.local.port))
    client.settimeout(2.0)
    stopped = Event()
    errors: list[BaseException] = []
    try:
        client.sendall(b"GET /callback HTTP/1.1\r\nHost: localhost\r\n\r\n")
        assert receive_response(client)[2] == b"/callback"

        def stop_system(_future) -> None:
            try:
                system.stop(timeout=2.0)
            except BaseException as error:
                errors.append(error)
            finally:
                stopped.set()

        binding.unbind().add_done_callback(stop_system)

        assert stopped.wait(timeout=3.0)
        assert errors == []
    finally:
        client.close()
        if not stopped.is_set():
            system.stop()


def test_http_binding_unbind_settles_after_actor_system_stop() -> None:
    system = create_system("http-unbind-after-stop")
    tcp = TCP.get(system)
    binding = (
        HTTP.get(system)
        .bind(
            TcpEndpoint("127.0.0.1", 0),
            Flow.map(echo_response),
        )
        .result(timeout=2.0)
    )

    system.stop()

    assert not tcp._listeners
    assert not tcp._connections
    assert binding.unbind().result(timeout=0.0) is None
