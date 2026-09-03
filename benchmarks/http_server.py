from __future__ import annotations

import argparse
import json
import os
import signal
import sys
from threading import Event, Timer

from benchmarks.http import benchmark_config
from movie.actor import ActorSystem, Behaviors
from movie.http import HTTP, HttpRequest, HttpResponse
from movie.io import TcpEndpoint
from movie.streams import Flow

PLAINTEXT_BODY = b"Hello, World!"
PLAINTEXT_CONTENT_TYPE = "text/plain"
_SHUTDOWN_TIMEOUT = 10.0
_MAX_WORKERS = 256


def handle_request(request: HttpRequest, stopping: Event | None = None) -> HttpResponse:
    if request.method == "GET" and request.target == "/ready":
        return HttpResponse(200, body=b"ready")
    if request.method == "GET" and request.target == "/plaintext":
        return HttpResponse(
            200,
            (("Content-Type", PLAINTEXT_CONTENT_TYPE),),
            PLAINTEXT_BODY,
        )
    if request.method == "POST" and request.target == "/shutdown" and stopping is not None:
        Timer(0.25, stopping.set).start()
        return HttpResponse(200, body=b"stopping")
    return HttpResponse(404, body=b"not found")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    logical_cpus = os.process_cpu_count() or 1
    parser = argparse.ArgumentParser(description="Serve Movie's external HTTP load benchmark")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--connections", type=int, default=100)
    parser.add_argument("--workers", type=int, default=max(1, logical_cpus // 2))
    parser.add_argument("--io-event-loops", type=int, default=2)
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")
    if args.connections <= 0:
        parser.error("connections must be positive")
    if not 1 <= args.workers <= _MAX_WORKERS:
        parser.error(f"workers must be between 1 and {_MAX_WORKERS}")
    if not 1 <= args.io_event_loops <= _MAX_WORKERS:
        parser.error(f"io-event-loops must be between 1 and {_MAX_WORKERS}")
    return args


def serve(args: argparse.Namespace) -> None:
    if sys._is_gil_enabled():
        raise RuntimeError("The Movie HTTP benchmark server requires free-threaded CPython")

    stopping = Event()
    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        signal.signal(shutdown_signal, lambda signum, frame: stopping.set())

    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "http-load-benchmark-server",
        config=benchmark_config(
            args.connections,
            1,
            len(PLAINTEXT_BODY),
            args.workers,
            args.io_event_loops,
        ),
    )
    binding = None
    try:
        binding = (
            HTTP.get(system)
            .bind(
                TcpEndpoint(args.host, args.port),
                Flow.map(
                    lambda request: handle_request(request, stopping),
                    name="load-benchmark-http-handler",
                ),
            )
            .result(timeout=10.0)
        )
        print(
            json.dumps(
                {
                    "event": "ready",
                    "host": binding.local.host,
                    "port": binding.local.port,
                    "python": sys.version,
                    "free_threaded": not sys._is_gil_enabled(),
                    "workers": args.workers,
                    "io_event_loops": args.io_event_loops,
                }
            ),
            flush=True,
        )
        stopping.wait()
    finally:
        try:
            if binding is not None:
                binding.unbind().result(timeout=_SHUTDOWN_TIMEOUT)
        finally:
            system.stop(timeout=_SHUTDOWN_TIMEOUT)


def main() -> None:
    serve(parse_args())


if __name__ == "__main__":
    main()
