from __future__ import annotations

import argparse
import json
import math
import os
import platform
import statistics
import sys
import sysconfig
from pathlib import Path
from threading import Barrier, Event, Lock, Thread
from time import perf_counter, perf_counter_ns
from typing import Any, Callable

from movie.actor import AbstractBehavior, ActorContext, ActorSystem, Behaviors
from movie.config import Config
from movie.dispatch.worker_pool import WorkerPoolDispatcherImpl
from movie.streams import Sink, Source


def benchmark_config(messages: int, workers: int | None) -> Config:
    dispatcher: dict[str, Any] = {
        "type": "movie.dispatch.worker_pool.WorkerPoolDispatcherImpl",
        "shutdown-timeout": 10,
    }
    if workers is not None:
        dispatcher["workers"] = workers

    return Config(
        {
            "movie": {
                "actor": {"log-level": "WARNING", "shutdown-timeout": 10},
                "dispatcher": {"default-dispatcher": dispatcher},
                "mailbox": {
                    "default": {
                        "type": "movie.mailbox.default.DefaultMailbox",
                        "capacity": max(100_000, messages),
                        "throughput": 100,
                    }
                },
            }
        }
    )


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, round((len(ordered) - 1) * fraction))
    return ordered[index]


def summarize(samples: list[dict[str, Any]]) -> dict[str, Any]:
    rates = [sample["rate"] for sample in samples]
    latencies = [
        latency
        for sample in samples
        for latency in sample.get("latency_us", [])
    ]
    result: dict[str, Any] = {
        "iterations": len(samples),
        "rate_per_second": {
            "min": min(rates),
            "median": statistics.median(rates),
            "max": max(rates),
        },
        "elapsed_seconds": [sample["elapsed"] for sample in samples],
    }
    if latencies:
        result["sampled_latency_us"] = {
            "p50": percentile(latencies, 0.50),
            "p95": percentile(latencies, 0.95),
            "p99": percentile(latencies, 0.99),
        }
    return result


def single_actor_once(messages: int, workers: int | None) -> dict[str, Any]:
    completed = Event()
    latencies: list[float] = []
    sample_every = max(1, messages // 1_000)

    class Counter(AbstractBehavior[tuple[int, int | None]]):
        def __init__(self, context: ActorContext) -> None:
            super().__init__(context)
            self.count = 0

        def receive(self, context: ActorContext, message: tuple[int, int | None]):
            sequence, sent_at = message
            if sequence != self.count:
                raise AssertionError(
                    f"Expected sequence {self.count}, received {sequence}"
                )
            self.count += 1
            if sent_at is not None:
                latencies.append((perf_counter_ns() - sent_at) / 1_000)
            if self.count == messages:
                completed.set()
            return self

    system = ActorSystem.create(
        Behaviors.setup(Counter),
        "single-actor-benchmark",
        config=benchmark_config(messages, workers),
    )
    try:
        started = perf_counter()
        for sequence in range(messages):
            sent_at = perf_counter_ns() if sequence % sample_every == 0 else None
            system.tell((sequence, sent_at))
        if not completed.wait(30):
            raise TimeoutError("Single-actor benchmark did not complete")
        elapsed = perf_counter() - started
        return {
            "elapsed": elapsed,
            "rate": messages / elapsed,
            "latency_us": latencies,
        }
    finally:
        system.stop()


def concurrent_producers_once(
    messages: int, producers: int, workers: int | None
) -> dict[str, Any]:
    completed = Event()
    barrier = Barrier(producers + 1)
    base, remainder = divmod(messages, producers)
    producer_counts = [base + (producer < remainder) for producer in range(producers)]
    total = sum(producer_counts)

    class OrderedCounter(AbstractBehavior[tuple[int, int]]):
        def __init__(self, context: ActorContext) -> None:
            super().__init__(context)
            self.count = 0
            self.last_seen = [-1] * producers

        def receive(self, context: ActorContext, message: tuple[int, int]):
            producer, sequence = message
            expected = self.last_seen[producer] + 1
            if sequence != expected:
                raise AssertionError(
                    f"Producer {producer}: expected {expected}, received {sequence}"
                )
            self.last_seen[producer] = sequence
            self.count += 1
            if self.count == total:
                completed.set()
            return self

    system = ActorSystem.create(
        Behaviors.setup(OrderedCounter),
        "concurrent-producers-benchmark",
        config=benchmark_config(total, workers),
    )

    def produce(producer: int) -> None:
        barrier.wait()
        for sequence in range(producer_counts[producer]):
            system.tell((producer, sequence))

    try:
        threads = [Thread(target=produce, args=(producer,)) for producer in range(producers)]
        for thread in threads:
            thread.start()
        started = perf_counter()
        barrier.wait()
        for thread in threads:
            thread.join()
        if not completed.wait(30):
            raise TimeoutError("Concurrent-producer benchmark did not complete")
        elapsed = perf_counter() - started
        return {"elapsed": elapsed, "rate": total / elapsed}
    finally:
        system.stop()


def dispatcher_submission_once(
    messages: int, producers: int, workers: int | None
) -> dict[str, Any]:
    worker_count = workers or (os.process_cpu_count() or 1)
    dispatcher = WorkerPoolDispatcherImpl(
        Config(
            {
                "workers": worker_count,
                "queue-capacity": messages,
                "shutdown-timeout": 10,
            }
        )
    )
    release = Event()
    worker_entries = [Event() for _ in range(worker_count)]
    producer_barrier = Barrier(producers + 1)
    base, remainder = divmod(messages, producers)
    producer_counts = [base + (index < remainder) for index in range(producers)]
    completed = Event()
    completion_lock = Lock()
    remaining = messages

    def block_worker(entered: Event) -> None:
        entered.set()
        release.wait(30.0)

    def complete_task() -> None:
        nonlocal remaining
        with completion_lock:
            remaining -= 1
            if remaining == 0:
                completed.set()

    def produce(count: int) -> None:
        producer_barrier.wait()
        for _ in range(count):
            dispatcher.dispatch(complete_task)

    dispatcher.start()
    try:
        for entered in worker_entries:
            dispatcher.dispatch(lambda entered=entered: block_worker(entered))
            if not entered.wait(10.0):
                raise TimeoutError("Dispatcher worker did not enter benchmark gate")

        producer_threads = [
            Thread(target=produce, args=(count,)) for count in producer_counts
        ]
        for producer in producer_threads:
            producer.start()
        started = perf_counter()
        producer_barrier.wait()
        for producer in producer_threads:
            producer.join()
        elapsed = perf_counter() - started
        release.set()
        if not completed.wait(30.0):
            raise TimeoutError("Submitted dispatcher tasks did not complete")
        return {"elapsed": elapsed, "rate": messages / elapsed}
    finally:
        release.set()
        dispatcher.stop()


def stream_once(elements: int, workers: int | None) -> dict[str, Any]:
    delivered = 0

    def count(element: int) -> None:
        nonlocal delivered
        if element != delivered:
            raise AssertionError(f"Expected stream element {delivered}, received {element}")
        delivered += 1

    sink, result = Sink.for_each_materialized(count)
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "stream-benchmark",
        config=benchmark_config(elements, workers),
    )
    try:
        started = perf_counter()
        Source.from_iterable(range(elements)).to(sink).run(system)
        result.result(timeout=30)
        elapsed = perf_counter() - started
        if delivered != elements:
            raise AssertionError(
                f"Expected {elements} stream elements, received {delivered}"
            )
        return {"elapsed": elapsed, "rate": elements / elapsed}
    finally:
        system.stop()


def shutdown_once(actors: int, workers: int | None) -> dict[str, Any]:
    started_actors = Event()
    start_lock = Lock()
    start_count = 0

    class Idle(AbstractBehavior[None]):
        def receive(self, context: ActorContext, message: None):
            return self

        def on_signal(
            self, context: ActorContext, message: ActorSystem.SystemMessage
        ) -> None:
            nonlocal start_count
            if isinstance(message, ActorSystem.PreStart):
                with start_lock:
                    start_count += 1
                    if start_count == actors:
                        started_actors.set()

    class Root(AbstractBehavior[None]):
        def __init__(self, context: ActorContext) -> None:
            super().__init__(context)
            for index in range(actors):
                context.spawn(Behaviors.setup(Idle), f"idle-{index}")

        def receive(self, context: ActorContext, message: None):
            return self

    system = ActorSystem.create(
        Behaviors.setup(Root),
        "shutdown-benchmark",
        config=benchmark_config(max(actors, 1), workers),
    )
    try:
        if not started_actors.wait(30):
            raise TimeoutError("Shutdown benchmark did not finish starting actors")
        started = perf_counter()
        system.stop(timeout=30)
        elapsed = perf_counter() - started
        if system.actor_count != 0:
            raise AssertionError(f"Actor registry retained {system.actor_count} actors")
        return {"elapsed": elapsed, "rate": actors / elapsed}
    finally:
        system.stop()


def measure(
    operation: Callable[[], dict[str, Any]], warmups: int, iterations: int
) -> dict[str, Any]:
    for _ in range(warmups):
        operation()
    return summarize([operation() for _ in range(iterations)])


def compare_baseline(
    results: dict[str, Any], baseline_path: Path, max_regression: float
) -> None:
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    if baseline.get("environment") != results["environment"]:
        raise RuntimeError("Baseline environment does not match the current environment")
    comparable_parameters = (
        "messages",
        "actors",
        "producers",
        "workers",
        "warmups",
        "iterations",
    )
    if any(
        baseline.get("parameters", {}).get(parameter)
        != results["parameters"].get(parameter)
        for parameter in comparable_parameters
    ):
        raise RuntimeError("Baseline parameters do not match the current parameters")
    failures = []
    for name, current in results["benchmarks"].items():
        if name not in baseline.get("benchmarks", {}):
            failures.append(f"{name}: missing from baseline")
            continue
        old_rate = baseline["benchmarks"][name]["rate_per_second"]["median"]
        new_rate = current["rate_per_second"]["median"]
        if old_rate <= 0:
            failures.append(f"{name}: baseline rate must be positive")
            continue
        regression = 1 - (new_rate / old_rate)
        if regression > max_regression:
            failures.append(f"{name}: throughput regressed by {regression:.1%}")
    if failures:
        raise RuntimeError("; ".join(failures))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark the Movie actor runtime")
    parser.add_argument("--messages", type=int, default=100_000)
    parser.add_argument("--actors", type=int, default=1_000)
    parser.add_argument("--producers", type=int, default=4)
    parser.add_argument("--workers", type=int)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--max-regression", type=float, default=0.10)
    args = parser.parse_args()
    if min(args.messages, args.actors, args.producers, args.iterations) <= 0:
        parser.error("messages, actors, producers, and iterations must be positive")
    if args.warmups < 0 or not math.isfinite(args.max_regression) or args.max_regression < 0:
        parser.error("warmups and max-regression cannot be negative")
    if args.workers is not None and args.workers <= 0:
        parser.error("workers must be positive")
    return args


def main() -> None:
    args = parse_args()
    workers = args.workers or os.process_cpu_count() or 1
    benchmarks = {
        "single_actor": measure(
            lambda: single_actor_once(args.messages, workers),
            args.warmups,
            args.iterations,
        ),
        "concurrent_producers": measure(
            lambda: concurrent_producers_once(
                args.messages, args.producers, workers
            ),
            args.warmups,
            args.iterations,
        ),
        "dispatcher_submission": measure(
            lambda: dispatcher_submission_once(args.messages, args.producers, workers),
            args.warmups,
            args.iterations,
        ),
        "stream": measure(
            lambda: stream_once(args.messages, workers),
            args.warmups,
            args.iterations,
        ),
        "shutdown": measure(
            lambda: shutdown_once(args.actors, workers),
            args.warmups,
            args.iterations,
        ),
    }
    results = {
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
            "messages": args.messages,
            "actors": args.actors,
            "producers": args.producers,
            "workers": workers,
            "warmups": args.warmups,
            "iterations": args.iterations,
        },
        "benchmarks": benchmarks,
    }
    rendered = json.dumps(results, indent=2)
    print(rendered)
    if args.baseline is not None:
        compare_baseline(results, args.baseline, args.max_regression)
    if args.output is not None:
        args.output.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
