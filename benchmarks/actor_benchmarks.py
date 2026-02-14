from __future__ import annotations

import argparse
import os
import time
from dataclasses import dataclass
from threading import Event

from movie.actor import AbstractBehavior, ActorContext, ActorRef, ActorSystem, Behaviors


@dataclass(frozen=True)
class ThroughputResult:
    messages: int
    elapsed_s: float

    @property
    def msg_per_sec(self) -> float:
        return self.messages / self.elapsed_s if self.elapsed_s > 0 else float("inf")


@dataclass(frozen=True)
class ParallelismResult:
    actors: int
    work_iterations: int
    sequential_s: float
    parallel_s: float

    @property
    def speedup(self) -> float:
        return self.sequential_s / self.parallel_s if self.parallel_s > 0 else float("inf")


class _ThroughputCounter(AbstractBehavior[int]):
    def __init__(self, context: ActorContext[int], target: int, done: Event) -> None:
        super().__init__(context)
        self._target = target
        self._done = done
        self._count = 0

    def receive(self, context: ActorContext[int], message: int):
        self._count += 1
        if self._count >= self._target:
            self._done.set()
        return None


class _ParallelWorker(AbstractBehavior[int]):
    def __init__(self, context: ActorContext[int], reply_to: ActorRef[int]) -> None:
        super().__init__(context)
        self._reply_to = reply_to

    def receive(self, context: ActorContext[int], message: int):
        acc = 0
        for i in range(message):
            acc += (i * 31) ^ (i >> 3)
        _ = acc
        self._reply_to.tell(1)
        return None


class _ParallelCoordinator(AbstractBehavior[int]):
    def __init__(
        self,
        context: ActorContext[int],
        workers: int,
        iterations: int,
        done: Event,
    ) -> None:
        super().__init__(context)
        self._done = done
        self._workers: list[ActorRef[int]] = []
        self._expected = workers
        self._completed = 0

        for idx in range(workers):
            worker = self.context.spawn(
                Behaviors.setup(lambda ctx, r=context.get_self(): _ParallelWorker(ctx, r)),
                f"bench-worker-{idx}",
            )
            self._workers.append(worker)

        for worker in self._workers:
            worker.tell(iterations)

    def receive(self, context: ActorContext[int], message: int):
        self._completed += message
        if self._completed >= self._expected:
            self._done.set()
        return None


class _NoopRoot(AbstractBehavior[None]):
    def receive(self, context: ActorContext[None], message: None):
        return None


def benchmark_throughput(messages: int, timeout_s: float = 10.0) -> ThroughputResult:
    done = Event()
    system = ActorSystem.create(Behaviors.setup(_NoopRoot), "bench-throughput")
    try:
        target = system.spawn(
            Behaviors.setup(lambda ctx: _ThroughputCounter(ctx, messages, done)),
            "throughput-counter",
        )

        start = time.perf_counter()
        for i in range(messages):
            target.tell(i)

        if not done.wait(timeout_s):
            raise TimeoutError(f"throughput benchmark timed out after {timeout_s}s")
        elapsed = time.perf_counter() - start
        return ThroughputResult(messages=messages, elapsed_s=elapsed)
    finally:
        system.stop()


def _cpu_work(iterations: int) -> int:
    acc = 0
    for i in range(iterations):
        acc += (i * 31) ^ (i >> 3)
    return acc


def benchmark_parallelism(
    actors: int,
    work_iterations: int,
    timeout_s: float = 20.0,
) -> ParallelismResult:
    # Sequential baseline.
    seq_start = time.perf_counter()
    for _ in range(actors):
        _cpu_work(work_iterations)
    sequential_s = time.perf_counter() - seq_start

    # Parallel actor run.
    done = Event()
    system = ActorSystem.create(Behaviors.setup(_NoopRoot), "bench-parallelism")
    try:
        par_start = time.perf_counter()
        system.spawn(
            Behaviors.setup(
                lambda ctx: _ParallelCoordinator(ctx, actors, work_iterations, done)
            ),
            "parallel-coordinator",
        )

        if not done.wait(timeout_s):
            raise TimeoutError(f"parallelism benchmark timed out after {timeout_s}s")
        parallel_s = time.perf_counter() - par_start
    finally:
        system.stop()

    return ParallelismResult(
        actors=actors,
        work_iterations=work_iterations,
        sequential_s=sequential_s,
        parallel_s=parallel_s,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Actor throughput/parallelism benchmarks")
    parser.add_argument("--messages", type=int, default=200_000)
    parser.add_argument("--actors", type=int, default=max(2, os.cpu_count() or 2))
    parser.add_argument("--work-iterations", type=int, default=400_000)
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args()

    throughput = benchmark_throughput(args.messages, timeout_s=args.timeout)
    parallel = benchmark_parallelism(
        args.actors,
        args.work_iterations,
        timeout_s=args.timeout,
    )

    print("=== Throughput ===")
    print(f"messages:   {throughput.messages}")
    print(f"elapsed_s:  {throughput.elapsed_s:.4f}")
    print(f"msg/sec:    {throughput.msg_per_sec:,.0f}")

    print("\n=== Parallelism ===")
    print(f"actors:             {parallel.actors}")
    print(f"work_iterations:    {parallel.work_iterations}")
    print(f"sequential_s:       {parallel.sequential_s:.4f}")
    print(f"parallel_s:         {parallel.parallel_s:.4f}")
    print(f"speedup:            {parallel.speedup:.2f}x")


if __name__ == "__main__":
    main()
