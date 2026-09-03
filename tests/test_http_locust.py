from threading import Event

import pytest

from benchmarks.http_server import (
    PLAINTEXT_BODY,
    PLAINTEXT_CONTENT_TYPE,
    handle_request,
    parse_args,
)
from movie.http import HttpRequest


def test_http_load_server_exposes_readiness_and_plaintext_contract() -> None:
    ready = handle_request(HttpRequest("GET", "/ready"))
    plaintext = handle_request(HttpRequest("GET", "/plaintext"))

    assert ready.status == 200
    assert ready.body == b"ready"
    assert plaintext.status == 200
    assert plaintext.header("Content-Type") == PLAINTEXT_CONTENT_TYPE
    assert plaintext.body == PLAINTEXT_BODY


def test_http_load_server_rejects_other_requests() -> None:
    response = handle_request(HttpRequest("POST", "/plaintext"))

    assert response.status == 404
    assert response.body == b"not found"


def test_http_load_server_accepts_internal_shutdown_request() -> None:
    stopping = Event()

    response = handle_request(HttpRequest("POST", "/shutdown"), stopping)

    assert response.status == 200
    assert response.body == b"stopping"
    assert stopping.wait(1.0)


def test_http_load_server_parses_capacity_settings() -> None:
    args = parse_args(
        [
            "--host",
            "127.0.0.1",
            "--port",
            "9000",
            "--connections",
            "200",
            "--workers",
            "8",
            "--io-event-loops",
            "2",
        ]
    )

    assert args.host == "127.0.0.1"
    assert args.port == 9000
    assert args.connections == 200
    assert args.workers == 8
    assert args.io_event_loops == 2


@pytest.mark.parametrize(
    "arguments",
    [
        ["--port", "0"],
        ["--connections", "0"],
        ["--workers", "0"],
        ["--io-event-loops", "0"],
    ],
)
def test_http_load_server_rejects_invalid_capacity_settings(arguments: list[str]) -> None:
    with pytest.raises(SystemExit):
        parse_args(arguments)
