# Movie

Movie is a typed actor runtime for local and remote actor systems on free-threaded CPython 3.14t. It provides hierarchical actors, supervision, bounded mailboxes, configurable dispatchers, lifecycle signals, optional SQLite durable state and projections, direct TCP remoting, optional volatile cluster membership, and a small backpressured streams DSL.

## Production Scope

Actor state, mailboxes, stream state, cluster membership, and messages are volatile and are lost when the process exits unless an application explicitly uses `DurableStateBehavior` for selected state. Durable State does not persist mailboxes or turn message delivery into a durable acknowledgement. Remoting provides direct, allowlisted actor-system associations; the optional cluster layer adds coordinated membership but not durable delivery, discovery, authentication, coordinator failover, or an HTTP health endpoint.

The separate contracts are documented in [`docs/persistence-v1.md`](docs/persistence-v1.md), [`docs/projections-v1.md`](docs/projections-v1.md), [`docs/remoting-v1.md`](docs/remoting-v1.md), and [`docs/cluster-v1.md`](docs/cluster-v1.md). Remoting specifies direct TCP associations behind a transport abstraction, at-most-once delivery, explicit serializers, and a trusted-network boundary. See [`CONTEXT.md`](CONTEXT.md) for canonical terminology and the ADRs under [`docs/adr`](docs/adr).

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
- At-most-once remote delivery attempts with a FIFO boundary per recipient and association.

`ActorSystem.stop()` initiates termination rather than draining every queued user message. Obtain an application-level acknowledgement before stopping when graceful message completion is required.

## Install

```console
uv add movie-actor-runtime
```

SQLite Durable State uses an optional asynchronous driver:

```console
uv add "movie-actor-runtime[persistence-sqlite]"
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

## Remoting Example

Remoting is disabled unless an immutable `RemotingConfig` is supplied. Its production TCP Transport Backend is `AsyncioTcpTransport`, running on the Actor System-owned asyncio I/O pool. Every payload serializer and exact message-type binding must be registered explicitly; Movie never adds `pickle` or another implicit serializer.

```python
from dataclasses import dataclass

from movie.actor import ActorSystem, Behaviors
from movie.remoting import (
    Endpoint,
    RemotingConfig,
    SerializerDescriptor,
    SerializerRegistryBuilder,
)


@dataclass(frozen=True)
class Work:
    value: str


class WorkSerializer:
    def serialize(self, value: object, manifest: str, protocol_minor: int) -> bytes:
        if not isinstance(value, Work) or manifest != "work/v1":
            raise ValueError("unsupported payload")
        return value.value.encode("utf-8")

    def deserialize(self, payload: bytes, manifest: str, protocol_minor: int) -> object:
        if manifest != "work/v1":
            raise ValueError("unsupported manifest")
        return Work(payload.decode("utf-8"))


descriptor = SerializerDescriptor(
    1,
    "work-contracts",
    1,
    0,
    frozenset({"work/v1"}),
    frozenset({"work/v1"}),
)
serializers = (
    SerializerRegistryBuilder()
    .register(descriptor, WorkSerializer())
    .bind(Work, 1, "work/v1")
    .build()
)
config = RemotingConfig(
    local=Endpoint("127.0.0.1", 7101),
    peers={"worker-system": Endpoint("127.0.0.1", 7102)},
    serializers=serializers,
)
gateway = ActorSystem.create(
    Behaviors.receive(lambda context, message: Behaviors.same),
    "gateway-system",
    remoting=config,
)

# worker-system must reciprocally allowlist gateway-system at 127.0.0.1:7101.
gateway.remoting.associate("worker-system")
worker = gateway.remoting.resolve(
    "movie://worker-system@127.0.0.1:7102/worker-system"
)
worker.tell(Work("render"))
gateway.stop()
```

`tell()` serializes synchronously and only confirms bounded local association admission. It does not confirm network receipt, mailbox admission, or actor processing. A disconnected `tell()` never reconnects or buffers; call `associate()` explicitly. TCP remoting is unencrypted and unauthenticated, so expose it only on a trusted network or through an authenticated encrypted tunnel.

Use `system.remoting.is_healthy` as the listener readiness signal and inspect `system.remoting.failure` for the first terminal listener error. `system.remoting.metrics` retains cumulative delivery-attempt counters across reconnects, while `system.remoting.associations` exposes current queue gauges and negotiated limits. A failed production listener closes its associations and makes subsequent remoting operations reject deterministically; it does not stop local actors or restart the Actor System.

Subscribe through `system.remoting.health_events.subscribe()` for a bounded stream of association activation, association closure, and listener failure events. Polling never blocks remoting. If a slow subscriber's `dropped_count` increases, resynchronize from the runtime properties above. `RemotingConfig.health_event_capacity` and `health_event_max_subscriptions` bound retained events and live subscriptions.

In canonical `HELLO_ACCEPT`, `outbound_*` is the effective initiator-to-responder direction and `inbound_*` is responder-to-initiator. A responder therefore uses the wire `inbound_*` fields as its own send limits.

## Cluster Membership

Cluster membership is enabled with an immutable `ClusterConfig` in addition to `RemotingConfig`. Cluster v1 uses one statically configured Membership Coordinator. The coordinator and each participant must reciprocally allowlist one another in remoting; participants do not need direct associations with each other.

Remoting and cluster membership are Actor System extensions exposed by the stable `REMOTING` and `CLUSTER` extension IDs. Configuration passed to `ActorSystem.create()` preconfigures those extensions; normal application access remains `system.remoting` and `system.cluster`. The cluster extension depends on remoting, so reverse extension shutdown attempts cluster leave before remoting and its I/O dependency stop.

```python
from movie.actor import ActorSystem, Behaviors
from movie.cluster import ClusterConfig, SeedContact
from movie.remoting import Endpoint, RemotingConfig, SerializerRegistryBuilder

seed_endpoint = Endpoint("127.0.0.1", 7201)
member_endpoint = Endpoint("127.0.0.1", 7202)
cluster = ClusterConfig(
    "render-cluster",
    SeedContact("render-seed", seed_endpoint),
)

seed = ActorSystem.create(
    Behaviors.receive(lambda context, message: Behaviors.same),
    "render-seed",
    remoting=RemotingConfig(
        seed_endpoint,
        {"render-worker": member_endpoint},
        SerializerRegistryBuilder().build(),
    ),
    cluster=cluster,
)
worker = ActorSystem.create(
    Behaviors.receive(lambda context, message: Behaviors.same),
    "render-worker",
    remoting=RemotingConfig(
        member_endpoint,
        {"render-seed": seed_endpoint},
        SerializerRegistryBuilder().build(),
    ),
    cluster=cluster,
)

assert worker.cluster.is_joined
assert {member.identity.system_name for member in seed.cluster.members} == {
    "render-seed",
    "render-worker",
}

worker.stop()  # Attempts bounded graceful leave before remoting shutdown.
seed.stop()
```

`system.cluster.membership` is an immutable local snapshot. Its revision changes whenever that local view changes. `system.cluster.events` is a bounded nonblocking stream of member status and reachability transitions. Heartbeats can mark an `UP` member `UNREACHABLE`, but failure detection never proves failure and never removes membership automatically.

After an operational decision that an exact unreachable incarnation must not return, call `seed.cluster.down(member.identity)` on the Membership Coordinator. Downing transitions only that identity to `LEFT`, allowing a replacement incarnation with the same actor-system name to join. Do not down solely because of a short association interruption or without a deployment-specific partition policy.

Cluster control records use a built-in serializer composed with the application registry. The seed, `heartbeat_interval`, `unreachable_timeout`, `member_limit`, `retired_identity_limit`, and `serializer_id` must match on every participant and are verified during join; the serializer ID must also be collision-free in each application registry. Cluster v1 has no dynamic endpoint admission, gossip, coordinator election or failover, automatic downing, split-brain resolver, role-based placement, singleton, sharding, or durable membership.

## Durable State

`DurableStateBehavior` stores one latest revisioned state or tombstone for an application-defined `PersistenceId`. Configure an explicit SQLite path to start the `DURABLE_STATE` extension before the root behavior; the extension obtains a stable worker from `ASYNCIO_IO`, and no potentially blocking SQLite operation runs on an actor dispatcher or asyncio event-loop thread.

```python
import json
from dataclasses import dataclass

from movie.actor import ActorSystem, Behaviors
from movie.config import Config
from movie.persistence import (
    DurableStateBehavior,
    EncodedState,
    OperationId,
    PersistenceId,
)

@dataclass(frozen=True)
class CounterState:
    value: int = 0

class CounterCodec:
    def encode(self, state):
        return EncodedState("counter/v1", json.dumps(state.value).encode("ascii"))

    def decode(self, manifest, payload):
        if manifest != "counter/v1":
            raise ValueError("unsupported state manifest")
        return CounterState(json.loads(payload))

class Counter(DurableStateBehavior):
    def empty_state(self):
        return CounterState()

    def handle_command(self, state, command, context):
        return self.persist(
            CounterState(state.value + command.amount),
            command.operation_id,
        )

config = Config({
    "movie": {"persistence": {"sqlite": {"path": "data/movie.sqlite3"}}}
})
system = ActorSystem.create(Behaviors.receive(lambda context, message: Behaviors.same), "app", config=config)
counter = system.spawn(
    Behaviors.setup(
        lambda context: Counter(context, PersistenceId("counter", "one"), CounterCodec())
    ),
    "counter-one",
)
```

Recovery runs asynchronously and suspends only user-message processing, leaving commands in the existing bounded mailbox. `persist`, `delete`, `none`, `stop`, `then_run`, and `then_stop` effects preserve commit-before-state semantics. Every mutation requires an `OperationId`; a successful nonduplicate mutation advances an optimistic Revision. See [`docs/persistence-v1.md`](docs/persistence-v1.md) for timeout, duplicate, schema, shutdown, and retry semantics.

## Durable State Projections

Every nonduplicate Durable State mutation also appends an immutable change in the same SQLite transaction. A Projection consumes bounded batches from this Change Feed and resumes from its stored `ProjectionId` offset:

```python
from movie.projection import PROJECTIONS, ProjectionId

async def update_index(changes):
    for change in changes:
        await search_index.replace(change.persistence_id.entity_id, change.payload)

projection = PROJECTIONS.get(system).run_at_least_once(
    ProjectionId("counter-search", "all"),
    entity_type="counter",
    min_slice=0,
    max_slice=1023,
    handler=update_index,
)
```

At-least-once handlers may receive the same batch again after failure and must be idempotent. `run_exactly_once` instead gives the handler a restricted SQLite transaction so read-model writes and the Projection Offset commit atomically in the persistence database. It does not make external effects exactly-once. See [`docs/projections-v1.md`](docs/projections-v1.md) for source, slice, retry, baseline, compaction, ownership, and shutdown semantics.

### Complete Durable Order Example

[`examples/durable_orders.py`](examples/durable_orders.py) is an executable order-processing application that combines the persistence and Projection APIs:

- a `DurableStateBehavior` owns each order and recovers it after an Actor System restart;
- a payment is retried with the same `OperationId` after its acknowledgement is deliberately ignored, without adding a Revision or Change Feed entry;
- a SQLite exactly-once Projection updates an order summary, append-only history, and transactional notification outbox;
- an at-least-once Projection writes to a separate audit database with `(PersistenceId, OperationId)` as its idempotency key;
- both handlers inject one retryable failure to demonstrate replay without duplicate effects;
- an abandoned order becomes a retained tombstone, and the app performs bounded compaction, demonstrates baseline-required rejection, then uses explicit-baseline registration and retirement.

Run it from a checkout with the SQLite extra installed:

```console
uv run --python 3.14t python -m examples.durable_orders --reset
```

The default databases are written under `.movie-example/durable-orders`. Omit `--reset` to retain prior state and use a newly generated Order Identity, or combine `--order-id` with `--reset` for a repeatable run.

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

## Raw TCP

`TCP.get(system).manager` accepts `Bind` and `Connect`. `Bound` returns the listener actor, while inbound and outbound `Connected` events return a connection actor. Send `Register(handler)` to each connection before it may read; that handler then receives arbitrary `Received` byte chunks and `ConnectionClosed`. TCP remains an unframed byte stream, so `Write` boundaries are not preserved.

Listeners and physical connections are assigned round-robin to the actor system's asyncio I/O workers and remain on one worker for their lifetime. The writer keeps bounded message and byte accounting, briefly microbatches adjacent writes, preserves byte order, and reports optional `WriteAccepted` after local queue admission rather than network delivery.

`Register(handler, pull_mode=True)` enables demand-driven reads; each `Read()` permits one socket read. `WriteCompleted` is separate from `WriteAccepted` and reports that one logical write completed its socket write calls. Pull mode also reports peer write-side EOF as `PeerClosed` without discarding pending outbound writes.

## HTTP/1.1

Movie's low-level HTTP server materializes one request/response stream per TCP connection. A reusable `Flow[HttpRequest, HttpResponse]` is the handler contract:

```python
from movie.actor import ActorSystem, Behaviors
from movie.http import HTTP, HttpResponse
from movie.io import TcpEndpoint
from movie.streams import Flow

system = ActorSystem.create(
    Behaviors.receive(lambda context, message: Behaviors.same),
    "http-system",
)
handler = Flow.map(
    lambda request: HttpResponse(
        200,
        (("Content-Type", "text/plain"),),
        b"hello",
    )
)
binding = HTTP.get(system).bind(TcpEndpoint("127.0.0.1", 8080), handler).result(10)

# Later: idempotently stop accepting new connections.
binding.unbind().result(10)
system.stop()
```

The current server supports HTTP/1.1, strict bounded request entities framed by `Content-Length`, persistent connections, ordered pipelining, `HEAD`, and `Connection: close`. A binding is internally materialized as a backpressured source of incoming connections; each connection independently materializes the supplied request/response flow, so slow setup on one connection does not block later connections. It rejects ambiguous framing and unsupported request transfer encodings. TLS, WebSocket, chunked request bodies, streaming entities, routing directives, and application timeouts are outside this first low-level API.

## Configuration

Movie optionally reads `movie.toml` from the current working directory by default. Set `MOVIE_CONFIG` to an explicit path in deployed processes; a missing explicit path fails startup. Configuration is trusted because dispatcher and mailbox `type` entries load Python classes.

See [`movie.toml.example`](movie.toml.example) for all production-relevant settings.

Remoting topology and cluster membership are configured programmatically through immutable `RemotingConfig` and `ClusterConfig` values rather than `movie.toml`. SQLite Durable State and Projections use the operational settings under `movie.persistence` and `movie.projection`.

Key capacity behavior:

- A full mailbox raises `movie.mailbox.default.MailboxCapacityExceeded` at `tell()`.
- A full actor stash raises `RuntimeError`.
- `throughput` normally yields a hot mailbox to other dispatcher work. During dispatcher shutdown, the current worker keeps draining accepted messages if handoff is rejected.
- Streams require the default mailbox capacity to be at least 32 and fail materialization otherwise.
- Durable State rejects work when its operation or pending-byte capacity is full; it never waits for storage capacity on an actor dispatcher.
- Projections bound active runners and retained batch bytes, and share Durable State operation capacity.

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

Compare local delivery with the production asyncio TCP remoting paths using the same message count, payload, ordering checks, and sampled end-to-end latency:

```console
uv run python -m benchmarks.remoting \
  --modes local_actor,same_process_asyncio,two_process_asyncio \
  --messages 100000 \
  --payload 64 \
  --workers 4 \
  --warmups 1 \
  --iterations 5 \
  --output remoting-benchmark.json
```

`local_actor` runs a sender loop against the root actor of one actor system. `same_process_asyncio` uses two actor systems in one process with real loopback associations over the production backend. `two_process_asyncio` keeps the sender actor system in the parent process and runs the receiver actor system in a spawned child process. The legacy `same_process_tcp` and `two_process_tcp` modes remain available for comparison through an explicitly injected threaded transport. Remote summaries report aggregate setup time separately from throughput, and every summary includes a throughput ratio relative to `local_actor` when that mode is selected. The child reports its bounded latency sample set through a multiprocessing pipe; CPython's system-wide `perf_counter_ns` clock is checked during the readiness handshake. Configurations requiring more than 2 GiB of benchmark queue capacity are rejected.

Benchmark the Flow-based HTTP/1.1 server with persistent connections and a continuously replenished request pipeline:

```console
uv run --python 3.14t python -m benchmarks.http \
  --requests 100000 \
  --connections 100 \
  --pipeline-depth 64 \
  --payload 64 \
  --workers 8 \
  --io-event-loops 2 \
  --warmups 1 \
  --iterations 5 \
  --output http-benchmark.json
```

The HTTP benchmark runs the Movie server in a spawned process and standard-library asyncio clients in the parent. It excludes process, connection, and per-connection stream materialization from throughput timing, validates unique request sequences and exact ordered response bodies, and reports up to 1,000 end-to-end latency samples distributed across connections.

Run an external HTTP load test entirely in Docker, without installing another Python on the host:

```powershell
$env:LOCUST_WORKERS = "6"
$env:LOCUST_OUTPUT = "http-locust-u3200-r100-w6"
docker compose -f compose.benchmark.yaml up --build --abort-on-container-failure
docker compose -f compose.benchmark.yaml down --remove-orphans
```

The benchmark image installs free-threaded CPython 3.14t during the build. Locust 2.46.4 runs from its pinned official image with `FastHttpUser`; the master waits for all configured workers and starts only after Movie's `/ready` health check succeeds. Each simulated user reuses HTTP connections by default and validates the status, content type, and exact `Hello, World!` response from `GET /plaintext`, without following redirects. After a successful master run, a one-shot control service stops Movie; `--abort-on-container-failure` makes any nonzero server, master, worker, or control-service exit fail the Compose command. Locust enforces the worker quorum only before the load starts, so treat a mid-run worker-loss message as an invalid sample even if the master exits successfully.

`LOCUST_WORKERS` defaults to 6 and controls both the Compose worker count and Locust's `--expect-workers`. `LOCUST_USERS`, `LOCUST_SPAWN_RATE`, and `LOCUST_RUN_TIME` default to `3200`, `100`, and `60s`. `MOVIE_WORKERS` and `MOVIE_IO_EVENT_LOOPS` default to `8` and `2`. The benchmark sizes Movie's TCP listen backlog to the requested user count. Set a unique `LOCUST_OUTPUT` for each run; Locust writes its `_stats.csv`, `_stats_history.csv`, `_failures.csv`, and `_exceptions.csv` reports under `results/`.

The default profile ramps to 3,200 persistent users at 100 users per second. It is a saturation profile and may return a nonzero status when the shared Docker VM cannot keep every request within Locust's timeout. Raising the spawn rate to 600 users per second additionally stresses TCP connection admission. For a lower-concurrency reference run, set `LOCUST_USERS=200`, `LOCUST_SPAWN_RATE=200`, `LOCUST_RUN_TIME=30s`, and a distinct `LOCUST_OUTPUT` before starting Compose.

Locust's run time includes user ramp-up, while `--reset-stats` clears aggregate measurements once all users have spawned; the history CSV can still contain ramp-up rows. Requests still in flight at the time limit are stopped and excluded from completed-request statistics. Locust performs sequential requests per user rather than HTTP pipelining. Use `benchmarks.http` for exact pipelining and FIFO verification, and do not compare its native loopback throughput directly with the Compose result. Movie, Locust, and the Docker bridge share the Docker VM's CPU allocation; adding load workers can reduce the CPU available to the server. Use a separate load-generator host for an authoritative server ceiling. On Linux hosts where UID 1000 cannot write `results/`, set `LOCUST_UID` and `LOCUST_GID` to the checkout owner's IDs.

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
- Treat local and remote actor references as capabilities. Anyone holding a reference can attempt to send messages to it.
- Pin `uv.lock`, build wheels with `uv build`, and run the CI gates before release.

## Quality Gates

```console
uv run --python 3.14t ruff check .
uv run --python 3.14t pytest --cov=movie --cov-report=term-missing
uv build --python 3.14t
```

The GitHub Actions workflow is temporarily disabled in `.github/workflows/ci.yml.disabled`.
