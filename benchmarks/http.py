from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing
import os
import platform
import statistics
import sys
import sysconfig
import traceback
from collections import deque
from multiprocessing.connection import Connection
from pathlib import Path
from threading import Lock
from time import perf_counter, perf_counter_ns
from typing import Any, Callable

from benchmarks.runtime import summarize
from movie.actor import ActorSystem, Behaviors
from movie.config import Config
from movie.http import HTTP, HttpRequest, HttpResponse
from movie.io import TcpEndpoint
from movie.streams import Flow

_STARTUP_TIMEOUT = 15.0
_RUN_TIMEOUT = 60.0
_SHUTDOWN_TIMEOUT = 10.0
_PARENT_SHUTDOWN_TIMEOUT = _SHUTDOWN_TIMEOUT * 2 + 5
_MAX_WORKERS = 256
_MAX_CAPACITY_BYTES = 2 * 1024 * 1024 * 1024


def sampled_sequences(requests: int, connections: int) -> frozenset[int]:
    sample_count = min(requests, 1_000)
    if sample_count < connections:
        sampled = set()
        for index in range(sample_count):
            connection = index * connections // sample_count
            assigned = range(connection, requests, connections)
            sampled.add(assigned[index * len(assigned) // sample_count])
        return frozenset(sampled)
    base, remainder = divmod(sample_count, connections)
    sampled = set()
    for connection in range(connections):
        assigned = range(connection, requests, connections)
        quota = min(len(assigned), base + (1 if connection < remainder else 0))
        for index in range(quota):
            sampled.add(assigned[index * len(assigned) // quota])
    if len(sampled) != sample_count:
        raise AssertionError(f"Expected {sample_count} latency samples, selected {len(sampled)}")
    return frozenset(sampled)


def benchmark_config(
    connections: int,
    pipeline_depth: int,
    payload_bytes: int,
    workers: int,
    io_event_loops: int,
) -> Config:
    body_bytes = 8 + payload_bytes
    response_buffer = min(256, pipeline_depth)
    response_bytes = body_bytes + 256
    return Config(
        {
            "movie": {
                "actor": {
                    "log-level": "WARNING",
                    "startup-timeout": 10,
                    "shutdown-timeout": 10,
                    "max-actors": max(100_000, connections * 5 + 100),
                },
                "dispatcher": {
                    "default-dispatcher": {
                        "type": "movie.dispatch.worker_pool.WorkerPoolDispatcherImpl",
                        "workers": workers,
                        "shutdown-timeout": 10,
                    }
                },
                "mailbox": {
                    "default": {
                        "type": "movie.mailbox.default.DefaultMailbox",
                        "capacity": max(100_000, connections * pipeline_depth * 8),
                        "throughput": 100,
                    }
                },
                "io": {
                    "asyncio": {
                        "event-loop-count": io_event_loops,
                        "command-capacity": max(1024, connections * 4),
                    },
                    "tcp": {
                        "backlog": max(128, connections),
                        "write-message-limit": max(1024, pipeline_depth * 2),
                        "write-byte-limit": max(
                            4 * 1024 * 1024,
                            response_bytes * response_buffer * 2,
                        ),
                    },
                },
                "http": {
                    "server": {
                        "max-pipelined-requests": max(32, pipeline_depth * 2),
                        "response-buffer": response_buffer,
                        "max-body-bytes": body_bytes,
                        "max-buffer-bytes": 32 * 1024 + body_bytes + 64 * 1024,
                    }
                },
            }
        }
    )


class _RequestCounter:
    def __init__(self, expected_requests: int, expected_payload: bytes) -> None:
        self._expected_requests = expected_requests
        self._expected_payload = expected_payload
        self._lock = Lock()
        self._count = 0
        self._seen = bytearray(expected_requests)

    @property
    def count(self) -> int:
        with self._lock:
            return self._count

    def handle(self, request: HttpRequest) -> HttpResponse:
        if request.method == "GET" and request.target == "/ready":
            return HttpResponse(200, body=b"ready")
        if request.method != "POST" or request.target != "/benchmark":
            return HttpResponse(404, body=b"not found")
        if len(request.body) < 8 or request.body[8:] != self._expected_payload:
            return HttpResponse(400, body=b"invalid benchmark payload")
        sequence = int.from_bytes(request.body[:8], "big")
        with self._lock:
            if sequence >= self._expected_requests or self._seen[sequence]:
                return HttpResponse(400, body=b"invalid benchmark sequence")
            self._seen[sequence] = 1
            self._count += 1
        return HttpResponse(200, (("Content-Type", "application/octet-stream"),), request.body)


def _server_process(
    control: Connection,
    requests: int,
    connections: int,
    pipeline_depth: int,
    payload_bytes: int,
    workers: int,
    io_event_loops: int,
) -> None:
    system = None
    try:
        payload = b"x" * payload_bytes
        counter = _RequestCounter(requests, payload)
        system = ActorSystem.create(
            Behaviors.receive(lambda context, message: Behaviors.same),
            "http-benchmark-server",
            config=benchmark_config(
                connections,
                pipeline_depth,
                payload_bytes,
                workers,
                io_event_loops,
            ),
        )
        binding = (
            HTTP.get(system)
            .bind(
                TcpEndpoint("127.0.0.1", 0),
                Flow.map(counter.handle, name="benchmark-http-handler"),
            )
            .result(timeout=_STARTUP_TIMEOUT)
        )
        control.send(("ready", binding.local.host, binding.local.port))
        if not control.poll(_STARTUP_TIMEOUT):
            raise TimeoutError("HTTP benchmark parent did not start the workload")
        command = control.recv()
        if command == ("cancel",):
            binding.unbind().result(timeout=_SHUTDOWN_TIMEOUT)
            system.stop(_SHUTDOWN_TIMEOUT)
            system = None
            control.send(("cancelled",))
            return
        if command != ("run", requests):
            raise RuntimeError(f"Unexpected HTTP benchmark command: {command!r}")
        control.send(("running",))
        if not control.poll(_RUN_TIMEOUT):
            raise TimeoutError("HTTP benchmark parent did not request shutdown")
        command = control.recv()
        if command == ("cancel",):
            binding.unbind().result(timeout=_SHUTDOWN_TIMEOUT)
            system.stop(_SHUTDOWN_TIMEOUT)
            system = None
            control.send(("cancelled",))
            return
        if command != ("stop", requests):
            raise RuntimeError(f"Unexpected HTTP benchmark command: {command!r}")
        if counter.count != requests:
            raise AssertionError(f"Expected {requests} requests, handled {counter.count}")
        binding.unbind().result(timeout=_SHUTDOWN_TIMEOUT)
        system.stop(_SHUTDOWN_TIMEOUT)
        system = None
        control.send(("done", counter.count))
    except BaseException as error:
        if system is not None:
            try:
                system.stop(_SHUTDOWN_TIMEOUT)
                system = None
            except BaseException as cleanup_error:
                error.add_note(f"HTTP benchmark server cleanup failed: {cleanup_error!r}")
        try:
            control.send(("error", "".join(traceback.format_exception(error))))
        except BaseException:
            pass
    finally:
        if system is not None:
            try:
                system.stop(_SHUTDOWN_TIMEOUT)
            except BaseException:
                pass
        control.close()


async def _read_response(
    reader: asyncio.StreamReader,
) -> tuple[dict[bytes, list[bytes]], bytes]:
    status_line = await reader.readuntil(b"\r\n")
    if status_line != b"HTTP/1.1 200 OK\r\n":
        raise AssertionError(f"Unexpected HTTP status: {status_line!r}")
    headers: dict[bytes, list[bytes]] = {}
    while True:
        line = await reader.readuntil(b"\r\n")
        if line == b"\r\n":
            break
        name, separator, value = line[:-2].partition(b":")
        if not separator:
            raise AssertionError(f"Malformed HTTP response header: {line!r}")
        headers.setdefault(name.lower(), []).append(value.strip())
    content_lengths = headers.get(b"content-length", [])
    if len(content_lengths) != 1:
        raise AssertionError("HTTP response must have exactly one Content-Length header")
    content_length = int(content_lengths[0])
    return headers, await reader.readexactly(content_length)


def _validate_response(
    headers: dict[bytes, list[bytes]],
    body: bytes,
    expected_sequence: int,
    expected_payload: bytes,
) -> None:
    expected_length = 8 + len(expected_payload)
    if len(body) != expected_length:
        raise AssertionError(
            f"Expected response body length {expected_length}, received {len(body)}"
        )
    if headers.get(b"content-type") != [b"application/octet-stream"]:
        raise AssertionError("HTTP response Content-Type did not match the benchmark contract")
    if b"connection" in headers:
        raise AssertionError("HTTP benchmark response unexpectedly changed connection persistence")
    sequence = int.from_bytes(body[:8], "big")
    if sequence != expected_sequence:
        raise AssertionError(f"Expected response sequence {expected_sequence}, received {sequence}")
    if body[8:] != expected_payload:
        raise AssertionError("HTTP response payload did not match the request")


async def _run_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    sequences: range,
    pipeline_depth: int,
    payload: bytes,
    sampled: frozenset[int],
    sent_at: dict[int, int],
    latencies: list[float],
) -> None:
    body_size = 8 + len(payload)
    headers = (
        b"POST /benchmark HTTP/1.1\r\n"
        b"Host: localhost\r\n" + f"Content-Length: {body_size}\r\n\r\n".encode("ascii")
    )
    pending: deque[int] = deque()
    iterator = iter(sequences)

    def submit(sequence: int) -> None:
        if sequence in sampled:
            sent_at[sequence] = perf_counter_ns()
        writer.write(headers + sequence.to_bytes(8, "big") + payload)
        pending.append(sequence)

    for _ in range(min(pipeline_depth, len(sequences))):
        submit(next(iterator))
    await writer.drain()
    while pending:
        expected_sequence = pending.popleft()
        response_headers, body = await _read_response(reader)
        _validate_response(response_headers, body, expected_sequence, payload)
        started = sent_at.pop(expected_sequence, None)
        if started is not None:
            latencies.append((perf_counter_ns() - started) / 1_000)
        try:
            sequence = next(iterator)
        except StopIteration:
            continue
        submit(sequence)
        await writer.drain()


async def _run_clients(
    host: str,
    port: int,
    requests: int,
    connections: int,
    pipeline_depth: int,
    payload_bytes: int,
    begin_workload: Callable[[], None],
) -> dict[str, Any]:
    setup_started = perf_counter()
    tasks = [asyncio.create_task(asyncio.open_connection(host, port)) for _ in range(connections)]
    clients = []
    try:
        async with asyncio.timeout(_STARTUP_TIMEOUT):
            clients = list(await asyncio.gather(*tasks))
            for _, writer in clients:
                writer.write(b"GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n")
            await asyncio.gather(*(writer.drain() for _, writer in clients))
            preflight = await asyncio.gather(*(_read_response(reader) for reader, _ in clients))
            if any(body != b"ready" for _, body in preflight):
                raise AssertionError("HTTP benchmark preflight response was invalid")
    except BaseException:
        for task in tasks:
            task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        if not clients:
            clients.extend(result for result in results if isinstance(result, tuple))
        await _close_clients(clients)
        raise
    client_setup_ms = (perf_counter() - setup_started) * 1000
    try:
        payload = b"x" * payload_bytes
        sampled = sampled_sequences(requests, connections)
        sent_at: dict[int, int] = {}
        latencies: list[float] = []
        begin_workload()
        started = perf_counter()
        async with asyncio.timeout(_RUN_TIMEOUT):
            await asyncio.gather(
                *(
                    _run_connection(
                        reader,
                        writer,
                        range(index, requests, connections),
                        pipeline_depth,
                        payload,
                        sampled,
                        sent_at,
                        latencies,
                    )
                    for index, (reader, writer) in enumerate(clients)
                )
            )
        elapsed = perf_counter() - started
    finally:
        await _close_clients(clients)
    expected_samples = len(sampled)
    if sent_at or len(latencies) != expected_samples:
        raise AssertionError(
            f"Expected {expected_samples} latency samples, completed {len(latencies)}"
        )
    return {
        "elapsed": elapsed,
        "rate": requests / elapsed,
        "latency_us": latencies,
        "client_setup_ms": client_setup_ms,
    }


async def _close_clients(
    clients: list[tuple[asyncio.StreamReader, asyncio.StreamWriter]],
) -> None:
    for _, writer in clients:
        writer.close()
    try:
        async with asyncio.timeout(_SHUTDOWN_TIMEOUT):
            await asyncio.gather(
                *(writer.wait_closed() for _, writer in clients),
                return_exceptions=True,
            )
    except TimeoutError:
        for _, writer in clients:
            writer.transport.abort()


def run_once(
    requests: int,
    connections: int,
    pipeline_depth: int,
    payload_bytes: int,
    workers: int,
    io_event_loops: int,
) -> dict[str, Any]:
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=True)
    process = context.Process(
        target=_server_process,
        args=(
            child,
            requests,
            connections,
            pipeline_depth,
            payload_bytes,
            workers,
            io_event_loops,
        ),
        name="movie-http-benchmark-server",
    )
    setup_started = perf_counter()
    started = False
    child_open = True
    try:
        process.start()
        started = True
        child.close()
        child_open = False
        if not parent.poll(_STARTUP_TIMEOUT):
            raise TimeoutError("HTTP benchmark server did not become ready")
        ready = parent.recv()
        if not isinstance(ready, tuple) or not ready:
            raise RuntimeError(f"Invalid HTTP benchmark readiness packet: {ready!r}")
        if ready[0] == "error":
            raise RuntimeError(ready[1])
        if len(ready) != 3 or ready[0] != "ready":
            raise RuntimeError(f"Invalid HTTP benchmark readiness packet: {ready!r}")
        _, host, port = ready
        server_setup_ms = (perf_counter() - setup_started) * 1000

        def begin_workload() -> None:
            parent.send(("run", requests))
            if not parent.poll(_STARTUP_TIMEOUT):
                raise TimeoutError("HTTP benchmark server did not acknowledge workload start")
            response = parent.recv()
            if response != ("running",):
                if isinstance(response, tuple) and response and response[0] == "error":
                    raise RuntimeError(response[1])
                raise RuntimeError(f"Invalid HTTP benchmark run packet: {response!r}")

        sample = asyncio.run(
            _run_clients(
                host,
                port,
                requests,
                connections,
                pipeline_depth,
                payload_bytes,
                begin_workload,
            )
        )
        sample["setup_ms"] = server_setup_ms + sample.pop("client_setup_ms")
        parent.send(("stop", requests))
        if not parent.poll(_PARENT_SHUTDOWN_TIMEOUT):
            raise TimeoutError("HTTP benchmark server did not stop")
        completed = parent.recv()
        if completed != ("done", requests):
            if isinstance(completed, tuple) and completed and completed[0] == "error":
                raise RuntimeError(completed[1])
            raise RuntimeError(f"Invalid HTTP benchmark completion packet: {completed!r}")
        process.join(_PARENT_SHUTDOWN_TIMEOUT)
        if process.exitcode != 0:
            raise RuntimeError(f"HTTP benchmark server exited with code {process.exitcode}")
        return sample
    finally:
        if child_open:
            child.close()
        if started:
            if process.is_alive():
                try:
                    parent.send(("cancel",))
                except BrokenPipeError, EOFError, OSError:
                    pass
                process.join(_PARENT_SHUTDOWN_TIMEOUT)
            if process.is_alive():
                process.terminate()
                process.join(_SHUTDOWN_TIMEOUT)
            if process.is_alive():
                process.kill()
                process.join(_SHUTDOWN_TIMEOUT)
            process.close()
        parent.close()


def measure(args: argparse.Namespace) -> dict[str, Any]:
    for _ in range(args.warmups):
        run_once(
            args.requests,
            args.connections,
            args.pipeline_depth,
            args.payload,
            args.workers,
            args.io_event_loops,
        )
    samples = [
        run_once(
            args.requests,
            args.connections,
            args.pipeline_depth,
            args.payload,
            args.workers,
            args.io_event_loops,
        )
        for _ in range(args.iterations)
    ]
    result = summarize(samples)
    setup = [sample["setup_ms"] for sample in samples]
    result["setup_ms"] = {
        "min": min(setup),
        "median": statistics.median(setup),
        "max": max(setup),
    }
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark Movie's Flow-based HTTP/1.1 server over loopback TCP"
    )
    parser.add_argument("--requests", type=int, default=100_000)
    parser.add_argument("--connections", type=int, default=100)
    parser.add_argument("--pipeline-depth", type=int, default=32)
    parser.add_argument("--payload", type=int, default=64)
    parser.add_argument("--workers", type=int, default=os.process_cpu_count() or 1)
    parser.add_argument("--io-event-loops", type=int, default=1)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.requests <= 0 or args.connections <= 0 or args.pipeline_depth <= 0:
        parser.error("requests, connections, and pipeline-depth must be positive")
    if args.connections > args.requests:
        parser.error("connections cannot exceed requests")
    if args.connections > 10_000:
        parser.error("connections cannot exceed 10000")
    if args.payload < 0 or args.warmups < 0 or args.iterations <= 0:
        parser.error("payload and warmups must be nonnegative; iterations must be positive")
    if not 1 <= args.workers <= _MAX_WORKERS:
        parser.error(f"workers must be between 1 and {_MAX_WORKERS}")
    if not 1 <= args.io_event_loops <= _MAX_WORKERS:
        parser.error(f"io-event-loops must be between 1 and {_MAX_WORKERS}")
    if args.pipeline_depth > 256:
        parser.error("pipeline-depth cannot exceed the current stream stage capacity of 256")
    estimated_bytes = args.requests + args.connections * args.pipeline_depth * (args.payload + 512)
    if estimated_bytes > _MAX_CAPACITY_BYTES:
        parser.error("benchmark settings require more than 2 GiB of estimated queue capacity")
    return args


def main() -> None:
    args = parse_args()
    result = measure(args)
    output = {
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "implementation": platform.python_implementation(),
            "free_threaded": not sys._is_gil_enabled(),
            "soabi": sysconfig.get_config_var("SOABI"),
            "cpu": platform.processor(),
            "logical_cpus": os.process_cpu_count(),
        },
        "parameters": {
            "requests": args.requests,
            "connections": args.connections,
            "pipeline_depth": args.pipeline_depth,
            "payload_bytes": args.payload,
            "workers": args.workers,
            "io_event_loops": args.io_event_loops,
            "warmups": args.warmups,
            "iterations": args.iterations,
            "latency_sample_count": min(args.requests, 1_000),
        },
        "benchmarks": {"two_process_http1": result},
    }
    rendered = json.dumps(output, indent=2)
    print(rendered)
    if args.output is not None:
        args.output.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
