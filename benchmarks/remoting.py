from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import platform
import socket
import statistics
import struct
import sys
import sysconfig
import traceback
from collections.abc import Callable
from dataclasses import dataclass, field
from multiprocessing.connection import Connection
from pathlib import Path
from threading import Event
from time import get_clock_info, monotonic, perf_counter_ns
from typing import Any

from benchmarks.runtime import summarize
from movie.actor import AbstractBehavior, ActorContext, ActorSystem, Behaviors
from movie.config import Config
from movie.remoting import (
    Endpoint,
    RemotingConfig,
    SerializerDescriptor,
    SerializerRegistry,
    SerializerRegistryBuilder,
    TransportLimits,
)

MODES = (
    "local_actor",
    "same_process_tcp",
    "same_process_asyncio",
    "two_process_tcp",
    "two_process_asyncio",
)
BENCHMARK_MANIFEST = "movie-benchmark/message-v1"
BENCHMARK_SERIALIZER_ID = 1
LOOPBACK_HOST = "127.0.0.1"

MAX_CAPACITY_BYTES = 2 << 30
MAX_WORKERS = 1_024
MAX_LATENCY_SAMPLES = 1_000
USER_RECORD_OVERHEAD_BYTES = 256
CONTROL_MESSAGE_RESERVE = 16
CONTROL_BYTE_RESERVE = 64 << 10
MINIMUM_RECORD_CAPACITY = 4 << 10

STARTUP_TIMEOUT_SECONDS = 30
COMPLETION_TIMEOUT_SECONDS = 60
SHUTDOWN_TIMEOUT_SECONDS = 15
PIPE_ERROR_LIMIT = 16 << 10


@dataclass(frozen=True, slots=True)
class BenchmarkMessage:
    sequence: int
    sent_at_ns: int
    payload: bytes


class BenchmarkMessageV1Serializer:
    def serialize(
        self,
        value: object,
        manifest: str,
        protocol_minor: int,
    ) -> bytes:
        if (
            not isinstance(value, BenchmarkMessage)
            or manifest != BENCHMARK_MANIFEST
            or protocol_minor != 0
        ):
            raise ValueError("unsupported benchmark message contract")
        return struct.pack(">QQ", value.sequence, value.sent_at_ns) + value.payload

    def deserialize(
        self,
        payload: bytes,
        manifest: str,
        protocol_minor: int,
    ) -> object:
        if (
            manifest != BENCHMARK_MANIFEST
            or protocol_minor != 0
            or len(payload) < 16
        ):
            raise ValueError("invalid benchmark message payload")
        sequence, sent_at_ns = struct.unpack(">QQ", payload[:16])
        return BenchmarkMessage(sequence, sent_at_ns, payload[16:])


@dataclass(slots=True)
class _ReceiverState:
    completed: Event = field(default_factory=Event)
    latencies_us: list[float] = field(default_factory=list)
    received: int = 0
    completed_at_ns: int = 0
    error: BaseException | None = None


def serializer_registry() -> SerializerRegistry:
    descriptor = SerializerDescriptor(
        BENCHMARK_SERIALIZER_ID,
        "movie-benchmark",
        1,
        0,
        frozenset({BENCHMARK_MANIFEST}),
        frozenset({BENCHMARK_MANIFEST}),
    )
    return (
        SerializerRegistryBuilder()
        .register(descriptor, BenchmarkMessageV1Serializer())
        .bind(BenchmarkMessage, BENCHMARK_SERIALIZER_ID, BENCHMARK_MANIFEST)
        .build()
    )


def reserve_endpoint() -> Endpoint:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind((LOOPBACK_HOST, 0))
        return Endpoint(LOOPBACK_HOST, sock.getsockname()[1])
    finally:
        sock.close()


def capacity_settings(messages: int, payload_bytes: int) -> dict[str, int]:
    user_record_bytes = payload_bytes + USER_RECORD_OVERHEAD_BYTES
    transport_queue_bytes = user_record_bytes * messages + CONTROL_BYTE_RESERVE
    return {
        "mailbox_messages": messages,
        "transport_queue_messages": messages + CONTROL_MESSAGE_RESERVE,
        "transport_queue_bytes": transport_queue_bytes,
        "maximum_record_bytes": max(MINIMUM_RECORD_CAPACITY, user_record_bytes),
    }


def remoting_limits(messages: int, payload_bytes: int) -> TransportLimits:
    capacities = capacity_settings(messages, payload_bytes)
    return TransportLimits(
        maximum_record_bytes=capacities["maximum_record_bytes"],
        outbound_message_limit=capacities["transport_queue_messages"],
        outbound_byte_limit=capacities["transport_queue_bytes"],
        inbound_message_limit=capacities["transport_queue_messages"],
        inbound_byte_limit=capacities["transport_queue_bytes"],
    )


def actor_system_config(messages: int, workers: int) -> Config:
    dispatcher_capacity = max(64, workers * 4)
    return Config(
        {
            "movie": {
                "actor": {
                    "callback-workers": 1,
                    "log-level": "WARNING",
                    "max-actors": 4,
                    "startup-timeout": STARTUP_TIMEOUT_SECONDS,
                    "shutdown-timeout": SHUTDOWN_TIMEOUT_SECONDS,
                },
                "dispatcher": {
                    "default-dispatcher": {
                        "type": "movie.dispatch.worker_pool.WorkerPoolDispatcherImpl",
                        "workers": workers,
                        "queue-capacity": dispatcher_capacity,
                        "system-queue-capacity": dispatcher_capacity,
                        "shutdown-timeout": SHUTDOWN_TIMEOUT_SECONDS,
                    }
                },
                "mailbox": {
                    "default": {
                        "type": "movie.mailbox.default.DefaultMailbox",
                        "capacity": messages,
                        "throughput": 100,
                    }
                },
            }
        }
    )


def sample_every(messages: int) -> int:
    return max(1, (messages + MAX_LATENCY_SAMPLES - 1) // MAX_LATENCY_SAMPLES)


def sample_count(messages: int, interval: int) -> int:
    return (messages - 1) // interval + 1


def receiver_behavior(
    messages: int,
    payload: bytes,
    interval: int,
    state: _ReceiverState,
) -> AbstractBehavior[BenchmarkMessage]:
    class Receiver(AbstractBehavior[BenchmarkMessage]):
        def receive(
            self, context: ActorContext, message: BenchmarkMessage
        ) -> AbstractBehavior[BenchmarkMessage]:
            if state.error is not None:
                return self
            try:
                if not isinstance(message, BenchmarkMessage):
                    raise AssertionError(
                        f"Expected BenchmarkMessage, got {type(message).__name__}"
                    )
                if message.sequence != state.received:
                    raise AssertionError(
                        f"Expected sequence {state.received}, got {message.sequence}"
                    )
                if message.payload != payload:
                    raise AssertionError(
                        f"Payload differed at sequence {message.sequence}"
                    )
                expected_sample = message.sequence % interval == 0
                if bool(message.sent_at_ns) != expected_sample:
                    raise AssertionError(
                        f"Latency sample marker differed at sequence {message.sequence}"
                    )

                received_at_ns = perf_counter_ns()
                if message.sent_at_ns:
                    if message.sent_at_ns > received_at_ns:
                        raise AssertionError(
                            "perf_counter_ns moved backwards across message receipt"
                        )
                    state.latencies_us.append(
                        (received_at_ns - message.sent_at_ns) / 1_000
                    )
                state.received += 1
                if state.received == messages:
                    state.completed_at_ns = received_at_ns
                    state.completed.set()
            except BaseException as error:
                state.error = error
                state.completed.set()
            return self

    return Behaviors.setup(Receiver)


def idle_behavior(
    context: ActorContext, message: object
) -> AbstractBehavior[object]:
    return Behaviors.same


def send_messages(
    tell: Callable[[BenchmarkMessage], None],
    messages: int,
    payload: bytes,
    interval: int,
) -> None:
    for sequence in range(messages):
        sent_at_ns = perf_counter_ns() if sequence % interval == 0 else 0
        tell(BenchmarkMessage(sequence, sent_at_ns, payload))


def result_from_state(
    state: _ReceiverState,
    messages: int,
    interval: int,
    started_at_ns: int,
) -> dict[str, Any]:
    remaining = COMPLETION_TIMEOUT_SECONDS - (
        (perf_counter_ns() - started_at_ns) / 1_000_000_000
    )
    if not state.completed.is_set() and (
        remaining <= 0 or not state.completed.wait(remaining)
    ):
        raise TimeoutError(
            f"Benchmark received {state.received}/{messages} messages before the deadline"
        )
    if state.error is not None:
        raise state.error
    if state.received != messages:
        raise AssertionError(f"Expected {messages} messages, got {state.received}")
    expected_samples = sample_count(messages, interval)
    if len(state.latencies_us) != expected_samples:
        raise AssertionError(
            f"Expected {expected_samples} latency samples, got {len(state.latencies_us)}"
        )
    if state.completed_at_ns < started_at_ns:
        raise AssertionError("Receiver completion preceded benchmark start")
    elapsed = (state.completed_at_ns - started_at_ns) / 1_000_000_000
    if elapsed > COMPLETION_TIMEOUT_SECONDS:
        raise TimeoutError(
            f"Benchmark received {messages} messages after the completion deadline"
        )
    return {
        "elapsed": elapsed,
        "rate": messages / elapsed,
        "latency_us": state.latencies_us,
    }


def stop_actor_systems(*systems: ActorSystem | None) -> None:
    errors: list[BaseException] = []
    for system in systems:
        if system is None:
            continue
        try:
            system.stop(timeout=SHUTDOWN_TIMEOUT_SECONDS)
        except BaseException as error:
            errors.append(error)
    if not errors:
        return
    active_error = sys.exception()
    if active_error is not None:
        for error in errors:
            active_error.add_note(f"Actor-system shutdown failed: {error!r}")
        return
    raise errors[0]


def local_actor_once(messages: int, payload_bytes: int, workers: int) -> dict[str, Any]:
    state = _ReceiverState()
    payload = b"x" * payload_bytes
    interval = sample_every(messages)
    system = ActorSystem.create(
        receiver_behavior(messages, payload, interval, state),
        "benchmark-local-actor",
        config=actor_system_config(messages, workers),
    )
    try:
        started_at_ns = perf_counter_ns()
        send_messages(system.tell, messages, payload, interval)
        submitted_at_ns = perf_counter_ns()
        result = result_from_state(state, messages, interval, started_at_ns)
        result["submission_elapsed"] = (
            submitted_at_ns - started_at_ns
        ) / 1_000_000_000
        result["submission_rate"] = messages / result["submission_elapsed"]
        return result
    finally:
        stop_actor_systems(system)


def remoting_config(
    local_endpoint: Endpoint,
    peer_system_name: str,
    peer_endpoint: Endpoint,
    messages: int,
    payload_bytes: int,
    transport_backend: str = "tcp",
) -> RemotingConfig:
    return RemotingConfig(
        local_endpoint,
        {peer_system_name: peer_endpoint},
        serializer_registry(),
        transport_backend=transport_backend,
        limits=remoting_limits(messages, payload_bytes),
        association_timeout=STARTUP_TIMEOUT_SECONDS,
    )


def actor_locator(system_name: str, endpoint: Endpoint) -> str:
    return f"movie://{system_name}@{endpoint.host}:{endpoint.port}/{system_name}"


def same_process_once(
    messages: int,
    payload_bytes: int,
    workers: int,
    transport_backend: str,
) -> dict[str, Any]:
    sender_name = "benchmark-same-process-sender"
    receiver_name = "benchmark-same-process-receiver"
    state = _ReceiverState()
    payload = b"x" * payload_bytes
    interval = sample_every(messages)
    sender_endpoint = reserve_endpoint()
    setup_started_at_ns = perf_counter_ns()
    receiver = ActorSystem.create(
        receiver_behavior(messages, payload, interval, state),
        receiver_name,
        config=actor_system_config(messages, workers),
        remoting=remoting_config(
            Endpoint(LOOPBACK_HOST, 0),
            sender_name,
            sender_endpoint,
            messages,
            payload_bytes,
            transport_backend,
        ),
    )
    sender = None
    try:
        receiver_endpoint = receiver.remoting.endpoint
        sender = ActorSystem.create(
            Behaviors.receive(idle_behavior),
            sender_name,
            config=actor_system_config(messages, workers),
            remoting=remoting_config(
                sender_endpoint,
                receiver_name,
                receiver_endpoint,
                messages,
                payload_bytes,
                transport_backend,
            ),
        )
        sender.remoting.associate(receiver_name, timeout=STARTUP_TIMEOUT_SECONDS)
        remote = sender.remoting.resolve(
            actor_locator(receiver_name, receiver_endpoint),
            timeout=STARTUP_TIMEOUT_SECONDS,
        )
        setup_elapsed_ms = (
            perf_counter_ns() - setup_started_at_ns
        ) / 1_000_000

        started_at_ns = perf_counter_ns()
        send_messages(remote.tell, messages, payload, interval)
        submitted_at_ns = perf_counter_ns()
        result = result_from_state(state, messages, interval, started_at_ns)
        result["submission_elapsed"] = (
            submitted_at_ns - started_at_ns
        ) / 1_000_000_000
        result["submission_rate"] = messages / result["submission_elapsed"]
        result["setup_ms"] = setup_elapsed_ms
        return result
    finally:
        stop_actor_systems(sender, receiver)


def same_process_tcp_once(
    messages: int, payload_bytes: int, workers: int
) -> dict[str, Any]:
    return same_process_once(messages, payload_bytes, workers, "tcp")


def same_process_asyncio_once(
    messages: int, payload_bytes: int, workers: int
) -> dict[str, Any]:
    return same_process_once(messages, payload_bytes, workers, "asyncio")


def bounded_error_text(error: BaseException) -> str:
    rendered = "".join(traceback.format_exception(error))
    return rendered[-PIPE_ERROR_LIMIT:]


def send_child_packet(connection: Connection, packet: tuple[Any, ...]) -> None:
    try:
        connection.send(packet)
    except (BrokenPipeError, EOFError, OSError):
        pass


def two_process_receiver_main(
    connection: Connection,
    sender_host: str,
    sender_port: int,
    messages: int,
    payload_bytes: int,
    workers: int,
    transport_backend: str,
) -> None:
    receiver_name = "benchmark-two-process-receiver"
    sender_name = "benchmark-two-process-sender"
    receiver = None
    try:
        state = _ReceiverState()
        payload = b"x" * payload_bytes
        interval = sample_every(messages)
        receiver = ActorSystem.create(
            receiver_behavior(messages, payload, interval, state),
            receiver_name,
            config=actor_system_config(messages, workers),
            remoting=remoting_config(
                Endpoint(LOOPBACK_HOST, 0),
                sender_name,
                Endpoint(sender_host, sender_port),
                messages,
                payload_bytes,
                transport_backend,
            ),
        )
        endpoint = receiver.remoting.endpoint
        connection.send(("ready", endpoint.host, endpoint.port, perf_counter_ns()))

        if not connection.poll(STARTUP_TIMEOUT_SECONDS):
            raise TimeoutError("Parent did not start the benchmark before the deadline")
        command = connection.recv()
        if command != ("run",):
            raise RuntimeError(f"Unexpected parent command: {command!r}")

        completion_started_at_ns = perf_counter_ns()
        completion_deadline = monotonic() + COMPLETION_TIMEOUT_SECONDS
        while not state.completed.is_set():
            remaining = completion_deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"Receiver processed {state.received}/{messages} messages before the deadline"
                )
            if connection.poll(min(0.05, remaining)):
                command = connection.recv()
                if command == ("stop",):
                    raise RuntimeError("Parent cancelled the benchmark")
                raise RuntimeError(f"Unexpected parent command: {command!r}")

        if state.error is not None:
            raise state.error
        if state.received != messages:
            raise AssertionError(f"Expected {messages} messages, got {state.received}")
        if (
            state.completed_at_ns - completion_started_at_ns
            > COMPLETION_TIMEOUT_SECONDS * 1_000_000_000
        ):
            raise TimeoutError(
                f"Receiver processed {messages} messages after the completion deadline"
            )
        expected_samples = sample_count(messages, interval)
        if len(state.latencies_us) != expected_samples:
            raise AssertionError(
                f"Expected {expected_samples} latency samples, got {len(state.latencies_us)}"
            )

        receiver.stop(timeout=SHUTDOWN_TIMEOUT_SECONDS)
        receiver = None
        connection.send(
            (
                "complete",
                state.completed_at_ns,
                tuple(state.latencies_us),
                state.received,
            )
        )
    except BaseException as error:
        if receiver is not None:
            try:
                receiver.stop(timeout=SHUTDOWN_TIMEOUT_SECONDS)
            except BaseException as shutdown_error:
                error.add_note(f"Receiver actor-system shutdown failed: {shutdown_error!r}")
        send_child_packet(connection, ("error", bounded_error_text(error)))
    finally:
        connection.close()


def receive_child_packet(
    connection: Connection, timeout: float, phase: str
) -> tuple[Any, ...]:
    if not connection.poll(timeout):
        raise TimeoutError(f"Child actor system did not report {phase} before the deadline")
    try:
        packet = connection.recv()
    except EOFError as error:
        raise RuntimeError(
            f"Child actor system exited before reporting {phase}"
        ) from error
    if not isinstance(packet, tuple) or not packet or not isinstance(packet[0], str):
        raise RuntimeError(f"Invalid child actor-system packet during {phase}: {packet!r}")
    if packet[0] == "error":
        detail = packet[1] if len(packet) == 2 and isinstance(packet[1], str) else repr(packet)
        raise RuntimeError(f"Child actor system failed during {phase}:\n{detail}")
    return packet


def verify_ready_packet(
    packet: tuple[Any, ...], earliest_ns: int, received_ns: int
) -> Endpoint:
    if (
        len(packet) != 4
        or packet[0] != "ready"
        or not isinstance(packet[1], str)
        or type(packet[2]) is not int
        or type(packet[3]) is not int
    ):
        raise RuntimeError(f"Invalid child readiness packet: {packet!r}")
    child_clock_ns = packet[3]
    if not earliest_ns <= child_clock_ns <= received_ns:
        raise RuntimeError(
            "Child perf_counter_ns timestamp is not bracketed by parent timestamps"
        )
    return Endpoint(packet[1], packet[2])


def result_from_child_packet(
    packet: tuple[Any, ...],
    messages: int,
    interval: int,
    started_at_ns: int,
) -> dict[str, Any]:
    if len(packet) != 4 or packet[0] != "complete":
        raise RuntimeError(f"Invalid child completion packet: {packet!r}")
    completed_at_ns, latency_values, received = packet[1:]
    if type(completed_at_ns) is not int or type(received) is not int:
        raise RuntimeError(f"Invalid child completion counters: {packet!r}")
    if not isinstance(latency_values, tuple) or any(
        not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0
        for value in latency_values
    ):
        raise RuntimeError("Child reported invalid latency samples")
    if received != messages:
        raise AssertionError(f"Expected {messages} messages, child reported {received}")
    expected_samples = sample_count(messages, interval)
    if len(latency_values) != expected_samples:
        raise AssertionError(
            f"Expected {expected_samples} latency samples, child reported {len(latency_values)}"
        )
    received_at_ns = perf_counter_ns()
    if not started_at_ns <= completed_at_ns <= received_at_ns:
        raise AssertionError("Child completion timestamp is outside the parent time window")
    elapsed = (completed_at_ns - started_at_ns) / 1_000_000_000
    return {
        "elapsed": elapsed,
        "rate": messages / elapsed,
        "latency_us": list(latency_values),
    }


def cleanup_two_process(
    sender: ActorSystem | None,
    process: multiprocessing.Process | None,
    process_started: bool,
    parent_connection: Connection | None,
    child_connection: Connection | None,
    result_received: bool,
) -> None:
    errors: list[BaseException] = []
    if (
        process is not None
        and process_started
        and process.is_alive()
        and parent_connection is not None
        and not result_received
    ):
        try:
            parent_connection.send(("stop",))
        except (BrokenPipeError, EOFError, OSError):
            pass
    try:
        stop_actor_systems(sender)
    except BaseException as error:
        errors.append(error)
    for connection in (parent_connection, child_connection):
        if connection is not None:
            try:
                connection.close()
            except OSError as error:
                errors.append(error)

    if process is not None:
        forced_exit = False
        if process_started:
            process.join(SHUTDOWN_TIMEOUT_SECONDS)
            if process.is_alive():
                forced_exit = True
                process.terminate()
                process.join(SHUTDOWN_TIMEOUT_SECONDS)
            if process.is_alive():
                process.kill()
                process.join(SHUTDOWN_TIMEOUT_SECONDS)
            if process.is_alive():
                errors.append(TimeoutError("Child process could not be stopped"))
            elif not forced_exit and process.exitcode != 0:
                errors.append(
                    RuntimeError(f"Child process exited with code {process.exitcode}")
                )
        try:
            process.close()
        except ValueError as error:
            errors.append(error)

    if not errors:
        return
    active_error = sys.exception()
    if active_error is not None:
        for error in errors:
            active_error.add_note(f"Two-process cleanup failed: {error!r}")
        return
    raise errors[0]


def verify_cross_process_clock() -> str:
    clock = get_clock_info("perf_counter")
    if sys.implementation.name != "cpython" or sys.version_info < (3, 10):
        raise RuntimeError(
            "Cross-process latency requires CPython with a system-wide perf_counter_ns clock"
        )
    if not clock.monotonic or clock.adjustable:
        raise RuntimeError("perf_counter_ns is not a stable monotonic clock")
    return clock.implementation


def two_process_once(
    messages: int,
    payload_bytes: int,
    workers: int,
    transport_backend: str,
) -> dict[str, Any]:
    verify_cross_process_clock()
    sender_name = "benchmark-two-process-sender"
    receiver_name = "benchmark-two-process-receiver"
    sender_endpoint = reserve_endpoint()
    interval = sample_every(messages)
    payload = b"x" * payload_bytes
    context = multiprocessing.get_context("spawn")
    setup_started_at_ns = perf_counter_ns()
    parent_connection = None
    child_connection = None
    process = None
    process_started = False
    result_received = False
    sender = None
    try:
        parent_connection, child_connection = context.Pipe(duplex=True)
        process = context.Process(
            target=two_process_receiver_main,
            args=(
                child_connection,
                sender_endpoint.host,
                sender_endpoint.port,
                messages,
                payload_bytes,
                workers,
                transport_backend,
            ),
            name="movie-benchmark-receiver-process",
        )
        earliest_clock_ns = perf_counter_ns()
        process.start()
        process_started = True
        child_connection.close()
        child_connection = None

        ready_packet = receive_child_packet(
            parent_connection, STARTUP_TIMEOUT_SECONDS, "readiness"
        )
        ready_received_ns = perf_counter_ns()
        receiver_endpoint = verify_ready_packet(
            ready_packet, earliest_clock_ns, ready_received_ns
        )
        sender = ActorSystem.create(
            Behaviors.receive(idle_behavior),
            sender_name,
            config=actor_system_config(messages, workers),
            remoting=remoting_config(
                sender_endpoint,
                receiver_name,
                receiver_endpoint,
                messages,
                payload_bytes,
                transport_backend,
            ),
        )
        sender.remoting.associate(receiver_name, timeout=STARTUP_TIMEOUT_SECONDS)
        remote = sender.remoting.resolve(
            actor_locator(receiver_name, receiver_endpoint),
            timeout=STARTUP_TIMEOUT_SECONDS,
        )
        parent_connection.send(("run",))
        setup_elapsed_ms = (
            perf_counter_ns() - setup_started_at_ns
        ) / 1_000_000

        started_at_ns = perf_counter_ns()
        send_messages(remote.tell, messages, payload, interval)
        submitted_at_ns = perf_counter_ns()
        completion_packet = receive_child_packet(
            parent_connection,
            COMPLETION_TIMEOUT_SECONDS + SHUTDOWN_TIMEOUT_SECONDS,
            "completion",
        )
        result_received = True
        result = result_from_child_packet(
            completion_packet, messages, interval, started_at_ns
        )
        result["submission_elapsed"] = (
            submitted_at_ns - started_at_ns
        ) / 1_000_000_000
        result["submission_rate"] = messages / result["submission_elapsed"]
        result["setup_ms"] = setup_elapsed_ms
        return result
    finally:
        cleanup_two_process(
            sender,
            process,
            process_started,
            parent_connection,
            child_connection,
            result_received,
        )


def two_process_tcp_once(
    messages: int, payload_bytes: int, workers: int
) -> dict[str, Any]:
    return two_process_once(messages, payload_bytes, workers, "tcp")


def two_process_asyncio_once(
    messages: int, payload_bytes: int, workers: int
) -> dict[str, Any]:
    return two_process_once(messages, payload_bytes, workers, "asyncio")


def run_once(
    mode: str, messages: int, payload_bytes: int, workers: int
) -> dict[str, Any]:
    if mode == "local_actor":
        return local_actor_once(messages, payload_bytes, workers)
    if mode == "same_process_tcp":
        return same_process_tcp_once(messages, payload_bytes, workers)
    if mode == "same_process_asyncio":
        return same_process_asyncio_once(messages, payload_bytes, workers)
    if mode == "two_process_tcp":
        return two_process_tcp_once(messages, payload_bytes, workers)
    if mode == "two_process_asyncio":
        return two_process_asyncio_once(messages, payload_bytes, workers)
    raise ValueError(f"Unsupported benchmark mode: {mode}")


def summarize_mode(samples: list[dict[str, Any]]) -> dict[str, Any]:
    summary = summarize(samples)
    submission_rates = [sample["submission_rate"] for sample in samples]
    summary["submission_rate_per_second"] = {
        "min": min(submission_rates),
        "median": statistics.median(submission_rates),
        "max": max(submission_rates),
    }
    setup_values = [sample["setup_ms"] for sample in samples if "setup_ms" in sample]
    if setup_values:
        summary["setup_ms"] = {
            "min": min(setup_values),
            "median": statistics.median(setup_values),
            "max": max(setup_values),
        }
    return summary


def measure_mode(
    mode: str,
    messages: int,
    payload_bytes: int,
    workers: int,
    warmups: int,
    iterations: int,
) -> dict[str, Any]:
    for _ in range(warmups):
        run_once(mode, messages, payload_bytes, workers)
    return summarize_mode(
        [
            run_once(mode, messages, payload_bytes, workers)
            for _ in range(iterations)
        ]
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare local actor messaging and loopback TCP remoting"
    )
    parser.add_argument("--modes", default=",".join(MODES))
    parser.add_argument("--messages", type=int, default=100_000)
    parser.add_argument("--payload", type=int, default=64)
    parser.add_argument("--workers", type=int, default=os.process_cpu_count() or 1)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    modes = tuple(item.strip() for item in args.modes.split(",") if item.strip())
    unknown_modes = [mode for mode in modes if mode not in MODES]
    if not modes:
        parser.error("modes must contain at least one benchmark mode")
    if unknown_modes:
        parser.error(
            f"unknown modes: {', '.join(unknown_modes)}; choose from {', '.join(MODES)}"
        )
    if len(set(modes)) != len(modes):
        parser.error("modes must not contain duplicates")
    if args.messages <= 0 or args.iterations <= 0:
        parser.error("messages and iterations must be positive")
    if args.payload < 0 or args.warmups < 0:
        parser.error("payload and warmups cannot be negative")
    if not 1 <= args.workers <= MAX_WORKERS:
        parser.error(f"workers must be between 1 and {MAX_WORKERS}")

    capacities = capacity_settings(args.messages, args.payload)
    if capacities["transport_queue_bytes"] > MAX_CAPACITY_BYTES:
        parser.error(
            "messages and payload require more than 2 GiB of benchmark queue capacity"
        )
    args.modes = modes
    return args


def main() -> None:
    args = parse_args()
    benchmarks = {
        mode: measure_mode(
            mode,
            args.messages,
            args.payload,
            args.workers,
            args.warmups,
            args.iterations,
        )
        for mode in args.modes
    }
    local_rate = (
        benchmarks["local_actor"]["rate_per_second"]["median"]
        if "local_actor" in benchmarks
        else None
    )
    for summary in benchmarks.values():
        summary["relative_to_local"] = (
            summary["rate_per_second"]["median"] / local_rate
            if local_rate is not None
            else None
        )

    clock = get_clock_info("perf_counter")
    results = {
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "implementation": platform.python_implementation(),
            "free_threaded": not sys._is_gil_enabled(),
            "soabi": sysconfig.get_config_var("SOABI"),
            "cpu": platform.processor(),
            "logical_cpus": os.process_cpu_count(),
            "latency_clock": {
                "name": "time.perf_counter_ns",
                "implementation": clock.implementation,
                "monotonic": clock.monotonic,
                "system_wide_across_processes": sys.implementation.name == "cpython"
                and sys.version_info >= (3, 10),
                "two_process_verification": (
                    "child readiness timestamp bracketed by parent timestamps"
                    if any(mode.startswith("two_process_") for mode in benchmarks)
                    else None
                ),
            },
        },
        "parameters": {
            "modes": list(args.modes),
            "messages": args.messages,
            "payload_bytes": args.payload,
            "workers_per_actor_system": args.workers,
            "warmups": args.warmups,
            "iterations": args.iterations,
            "latency_sample_interval": sample_every(args.messages),
            "capacities": capacity_settings(args.messages, args.payload),
        },
        "benchmarks": benchmarks,
    }
    rendered = json.dumps(results, indent=2)
    print(rendered)
    if args.output is not None:
        args.output.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
