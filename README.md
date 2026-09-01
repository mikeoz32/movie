# Movie

Movie is a typed actor runtime for in-process concurrency on free-threaded CPython 3.14t. It provides hierarchical actors, supervision, bounded mailboxes, configurable dispatchers, lifecycle signals, and a small backpressured streams DSL.

## Production Scope

Movie is an in-process library. Actor state, mailboxes, stream state, and messages are volatile and are lost when the process exits. The runtime does not provide remote actors, durable delivery, persistence, clustering, authentication, or an HTTP health endpoint.

The runtime guarantees:

- At-most-once, FIFO user-message processing per sender while an actor is running.
- One behavior invocation at a time per actor, including on free-threaded CPython.
- Priority processing for lifecycle and supervision messages.
- Bounded user and stash queues with explicit overload errors.
- A bounded dispatcher activation queue with at most one activation per scheduled mailbox.
- A dedicated lifecycle queue that remains available during user-mailbox overload.
- Bounded actor-system and dispatcher shutdown.
- Actor and stream-stage removal from the registry after termination.
- Host-safe logging that does not mutate the process root logger.

`ActorSystem.stop()` initiates termination rather than draining every queued user message. Obtain an application-level acknowledgement before stopping when graceful message completion is required.

## Install

```console
uv add movie-actor-runtime
```

For development:

```console
uv sync --python 3.14t --group test
uv run --python 3.14t pytest
```

## Actor Example

```python
from threading import Event

from movie.actor import ActorContext, ActorSystem, Behaviors

received = Event()


def receive(context: ActorContext[str], message: str):
    context.log.info("Received work")
    received.set()
    return Behaviors.same


system = ActorSystem.create(Behaviors.receive(receive), "worker-system")
system.tell("work")
received.wait(timeout=1)
system.stop(timeout=10)
```

Returning `Behaviors.stopped` from a behavior runs `PostStop`, terminates children, notifies the parent, and unregisters the actor.

## Streams Example

```python
from movie.actor import ActorSystem, Behaviors
from movie.streams import Flow, Sink, Source

system = ActorSystem.create(
    Behaviors.receive(lambda context, message: Behaviors.same),
    "stream-system",
)
sink, result = Sink.collect()

Source.from_iterable(range(5)).via(Flow.map(lambda value: value * 2)).to(sink).run(system)

assert result.result(timeout=1) == [0, 2, 4, 6, 8]
system.stop()
```

Streams track outstanding demand, enforce stage buffer limits, cancel upstream on sink failures, and terminate all stage actors after completion or failure. `Sink.collect()` intentionally retains all output elements in memory.

## Configuration

Movie optionally reads `movie.toml` from the current working directory by default. Set `MOVIE_CONFIG` to an explicit path in deployed processes; a missing explicit path fails startup. Configuration is trusted because dispatcher and mailbox `type` entries load Python classes.

See [`movie.toml.example`](movie.toml.example) for all production-relevant settings.

Key capacity behavior:

- A full mailbox raises `movie.mailbox.default.MailboxCapacityExceeded` at `tell()`.
- A full actor stash raises `RuntimeError`.
- `throughput` normally yields a hot mailbox to other dispatcher work. During dispatcher shutdown, the current worker keeps draining accepted messages if handoff is rejected.
- Streams require the default mailbox capacity to be at least 32 and fail materialization otherwise.

Lifecycle messages use dedicated mailbox and priority dispatcher queues. `max-actors` and `system-queue-capacity` both default to 100,000, ensuring one reserved lifecycle activation per actor. Ordinary activation capacity is configured independently with `queue-capacity`.

Size capacities from measured peak traffic and alert before they are exhausted.

## Benchmarks

Run the complete benchmark suite on fixed, otherwise idle hardware:

```console
uv run python -m benchmarks.runtime \
  --messages 100000 \
  --actors 1000 \
  --warmups 1 \
  --iterations 5 \
  --output benchmark.json
```

The suite measures single-actor throughput and sampled queue latency, concurrent-producer throughput and ordering, stream throughput, and actor-tree shutdown. Every workload verifies exact delivery or registry cleanup.

Use `python -m benchmarks.scaling` to measure independent-actor strong scaling against raw free-threaded Python threads. Pass `--actors 16 --workers 1,2,4,8,16 --skip-raw` for a fixed-topology worker sweep.

Compare a run against a stored baseline and fail on a throughput regression over 10%:

```console
uv run python -m benchmarks.runtime --baseline benchmark.json --max-regression 0.10
```

Do not compare results across different Python builds, worker counts, power modes, or hardware.

## Operations

- Keep actor behavior non-blocking. Put blocking workloads on a separately configured dispatcher when dispatcher selection is added to the public spawn API.
- Use `system.actor_count` as a basic leak/readiness signal. It returns to zero after a complete system shutdown.
- Actor logs use a directly owned `movie.actor.<system>` logger and include `actor_id` and `actor_path`. The internal log queue holds at most 100,000 records.
- Catch `TimeoutError` from `stop()` and terminate the host process according to its shutdown policy; Python cannot safely kill a behavior blocked inside user code.
- Treat actor references as in-process capabilities. Anyone holding a reference can send messages to it.
- Pin `uv.lock`, build wheels with `uv build`, and run the CI gates before release.

## Quality Gates

```console
uv run --python 3.14t ruff check .
uv run --python 3.14t pytest --cov=movie --cov-report=term-missing
uv build --python 3.14t
```

The GitHub Actions workflow is temporarily disabled in `.github/workflows/ci.yml.disabled`.
