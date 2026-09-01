from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import sys
import sysconfig
from functools import partial
from pathlib import Path
from threading import Event, Thread
from time import perf_counter, perf_counter_ns
from typing import Any

from benchmarks.runtime import benchmark_config
from movie.actor import AbstractBehavior, ActorContext, ActorRef, ActorSystem, Behaviors

_GATE = object()


def cpu_work(value: int, rounds: int) -> int:
    limit = 1_000_000 + 3
    current = value % limit
    following = (current + 1) % limit
    for _ in range(rounds):
        current, following = following, current + following
        if following >= limit:
            following -= limit
    return current ^ following


def calibrate_cpu_work(rounds: int) -> float:
    calls = max(100, min(10_000, 1_000_000 // max(1, rounds)))
    samples = []
    for _ in range(5):
        started = perf_counter_ns()
        checksum = 0
        for sequence in range(calls):
            checksum ^= cpu_work(sequence, rounds)
        elapsed = perf_counter_ns() - started
        if checksum == -1:
            raise AssertionError("Unreachable checksum")
        samples.append(elapsed / calls)
    return statistics.median(samples)


def scaling_once(
    total_messages: int, actors: int, rounds: int, workers: int | None = None
) -> dict[str, Any]:
    ready = [Event() for _ in range(actors)]
    completed = [Event() for _ in range(actors)]
    refs: list[ActorRef] = []
    base, remainder = divmod(total_messages, actors)
    counts = [base + (index < remainder) for index in range(actors)]

    class WorkActor(AbstractBehavior[object]):
        def __init__(self, context: ActorContext, index: int) -> None:
            super().__init__(context)
            self._index = index
            self._expected_sequence = 0
            self._checksum = 0

        def receive(self, context: ActorContext, message: object):
            if message is _GATE:
                ready[self._index].set()
                if counts[self._index] == 0:
                    completed[self._index].set()
                return self

            sequence = int(message)
            if sequence != self._expected_sequence:
                raise AssertionError(
                    f"Actor {self._index}: expected {self._expected_sequence}, "
                    f"received {sequence}"
                )
            self._checksum ^= cpu_work(sequence, rounds)
            self._expected_sequence += 1
            if self._expected_sequence == counts[self._index]:
                completed[self._index].set()
            return self

    class Root(AbstractBehavior[None]):
        def __init__(self, context: ActorContext) -> None:
            super().__init__(context)
            for index in range(actors):
                refs.append(
                    context.spawn(
                        Behaviors.setup(
                            lambda child_context, index=index: WorkActor(
                                child_context, index
                            )
                        ),
                        f"worker-{index}",
                    )
                )

        def receive(self, context: ActorContext, message: None):
            return self

    worker_count = actors if workers is None else workers
    system = ActorSystem.create(
        Behaviors.setup(Root),
        f"scaling-{actors}-{rounds}",
        config=benchmark_config(total_messages + actors, worker_count),
    )
    try:
        if len(refs) != actors:
            raise AssertionError(f"Expected {actors} actor references, got {len(refs)}")
        for ref in refs:
            system.wait_for_actor_start(ref)
            ref.tell(_GATE)
        for event in ready:
            if not event.wait(10.0):
                raise TimeoutError("Not all actors reached the benchmark gate")

        started = perf_counter()
        for ref, count in zip(refs, counts, strict=True):
            for sequence in range(count):
                ref.tell(sequence)

        for event in completed:
            if not event.wait(30.0):
                raise TimeoutError("Scaling benchmark workload did not complete")
        elapsed = perf_counter() - started
        return {
            "runtime": "actors",
            "actors": actors,
            "workers": worker_count,
            "cpu_rounds": rounds,
            "elapsed": elapsed,
            "rate": total_messages / elapsed,
        }
    finally:
        system.stop()


def raw_threads_once(total_messages: int, threads: int, rounds: int) -> dict[str, Any]:
    release = Event()
    ready = [Event() for _ in range(threads)]
    completed = [Event() for _ in range(threads)]
    base, remainder = divmod(total_messages, threads)
    counts = [base + (index < remainder) for index in range(threads)]
    checksums = [0] * threads

    def run(index: int) -> None:
        ready[index].set()
        if not release.wait(30.0):
            return
        checksum = 0
        for sequence in range(counts[index]):
            checksum ^= cpu_work(sequence, rounds)
        checksums[index] = checksum
        completed[index].set()

    workers = [Thread(target=run, args=(index,)) for index in range(threads)]
    for worker in workers:
        worker.start()
    for event in ready:
        if not event.wait(10.0):
            raise TimeoutError("Not all raw threads reached the benchmark gate")

    started = perf_counter()
    release.set()
    for event in completed:
        if not event.wait(30.0):
            raise TimeoutError("Raw thread workload did not complete")
    elapsed = perf_counter() - started
    for worker in workers:
        worker.join()
    if len(checksums) != threads:
        raise AssertionError("Raw thread result count mismatch")
    return {
        "runtime": "raw_threads",
        "actors": threads,
        "workers": threads,
        "cpu_rounds": rounds,
        "elapsed": elapsed,
        "rate": total_messages / elapsed,
    }


def measure(
    total_messages: int,
    actors: int,
    rounds: int,
    warmups: int,
    iterations: int,
    *,
    raw_threads: bool = False,
    workers: int | None = None,
) -> dict[str, Any]:
    if raw_threads:
        operation = raw_threads_once
    else:
        operation = partial(scaling_once, workers=workers)
    for _ in range(warmups):
        operation(total_messages, actors, rounds)
    samples = [
        operation(total_messages, actors, rounds) for _ in range(iterations)
    ]
    rates = [sample["rate"] for sample in samples]
    elapsed = [sample["elapsed"] for sample in samples]
    return {
        "runtime": samples[0]["runtime"],
        "actors": actors,
        "workers": actors if raw_threads else (workers or actors),
        "cpu_rounds": rounds,
        "iterations": iterations,
        "rate_per_second": {
            "min": min(rates),
            "median": statistics.median(rates),
            "max": max(rates),
        },
        "elapsed_seconds": elapsed,
    }


def parse_int_list(value: str) -> list[int]:
    try:
        values = [int(item) for item in value.split(",")]
    except ValueError as error:
        raise argparse.ArgumentTypeError("Expected comma-separated integers") from error
    if not values or any(item < 0 for item in values):
        raise argparse.ArgumentTypeError("Values must be non-negative")
    return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure strong scaling across independent Movie actors"
    )
    parser.add_argument("--messages", type=int, default=100_000)
    parser.add_argument("--actors", type=parse_int_list, default=[1, 2, 4, 8, 16])
    parser.add_argument("--workers", type=parse_int_list)
    parser.add_argument("--cpu-rounds", type=parse_int_list, default=[0, 100])
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--skip-raw", action="store_true")
    args = parser.parse_args()
    if args.messages <= 0 or args.warmups < 0 or args.iterations <= 0:
        parser.error("messages and iterations must be positive; warmups cannot be negative")
    if any(actors <= 0 for actors in args.actors):
        parser.error("actor counts must be positive")
    if args.workers is not None and any(workers <= 0 for workers in args.workers):
        parser.error("worker counts must be positive")
    if args.messages < max(args.actors):
        parser.error("messages must be at least the largest actor count")
    return args


def main() -> None:
    args = parse_args()
    results = []
    for rounds in args.cpu_rounds:
        for actors in args.actors:
            worker_counts = args.workers or [actors]
            for workers in worker_counts:
                results.append(
                    measure(
                        args.messages,
                        actors,
                        rounds,
                        args.warmups,
                        args.iterations,
                        workers=workers,
                    )
                )
            if not args.skip_raw:
                results.append(
                    measure(
                        args.messages,
                        actors,
                        rounds,
                        args.warmups,
                        args.iterations,
                        raw_threads=True,
                    )
                )

    for result in results:
        if result["runtime"] == "actors" and args.workers is not None:
            group = [
                candidate
                for candidate in results
                if candidate["runtime"] == "actors"
                and candidate["cpu_rounds"] == result["cpu_rounds"]
                and candidate["actors"] == result["actors"]
            ]
            baseline_result = min(group, key=lambda candidate: candidate["workers"])
            scale = result["workers"] / baseline_result["workers"]
        else:
            group = [
                candidate
                for candidate in results
                if candidate["runtime"] == result["runtime"]
                and candidate["cpu_rounds"] == result["cpu_rounds"]
            ]
            baseline_result = min(group, key=lambda candidate: candidate["actors"])
            scale = result["actors"] / baseline_result["actors"]
        baseline = baseline_result["rate_per_second"]["median"]
        speedup = result["rate_per_second"]["median"] / baseline
        result["speedup"] = speedup
        result["parallel_efficiency"] = speedup / scale

    report = {
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
            "workers": args.workers,
            "cpu_rounds": args.cpu_rounds,
            "warmups": args.warmups,
            "iterations": args.iterations,
        },
        "cpu_work_ns_per_message": {
            str(rounds): calibrate_cpu_work(rounds) for rounds in args.cpu_rounds
        },
        "results": results,
    }
    rendered = json.dumps(report, indent=2)
    print(rendered)
    if args.output is not None:
        args.output.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
