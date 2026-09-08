# Durable State Persistence v1

This document defines Movie's first persistence contract. Canonical terms are defined in [`CONTEXT.md`](../CONTEXT.md), and the architectural choices are recorded in [ADR-0004](adr/0004-use-async-durable-state-before-event-sourcing.md) and [ADR-0005](adr/0005-project-durable-state-from-a-transactional-change-feed.md). Projection processing is specified separately in [`projections-v1.md`](projections-v1.md).

## Scope

Persistence v1 provides:

- `DurableStateBehavior` with asynchronous Recovery before user command handling;
- one latest state or tombstone per Persistence Identity;
- optimistic Revision checks and mandatory Operation Identities;
- atomic operation deduplication and state mutation;
- an ordered transactional Change Feed containing every nonduplicate mutation;
- resumable at-least-once and SQLite exactly-once Projections;
- stable application codecs and stored Serializer Manifests;
- one local SQLite connection through the Actor System's `ASYNCIO_IO` extension;
- bounded operation and pending-payload admission;
- restart Recovery and bounded Actor System shutdown.

It does not provide event sourcing, snapshots, durable mailboxes, durable message delivery, command replay, a transactional outbox, PostgreSQL, cross-system ownership, sharding, leases, or fencing.

## Installation And Configuration

Install the optional backend dependency:

```console
uv add "movie-actor-runtime[persistence-sqlite]"
```

Configure an explicit database path:

```python
from movie.config import Config

config = Config({
    "movie": {
        "persistence": {
            "sqlite": {"path": "data/movie.sqlite3"},
        }
    }
})
```

The presence of `movie.persistence.sqlite.path` preconfigures and starts `DURABLE_STATE` before the root user behavior. `DURABLE_STATE.get(system).store` exposes the asynchronous store seam. Starting the extension lazily from an actor callback is rejected because database startup is a blocking Actor System lifecycle operation.

Configuration values are:

| Path | Default | Meaning |
|---|---:|---|
| `movie.persistence.startup-timeout` | `10` | Seconds allowed for connection and schema startup. |
| `movie.persistence.operation-timeout` | `5` | Seconds allowed for a mutation to enter its SQLite transaction. |
| `movie.persistence.recovery-timeout` | `operation-timeout` | Seconds allowed to wait for the connection lane and complete a Recovery read. |
| `movie.persistence.operation-capacity` | `1024` | Accepted executing and queued store operations. |
| `movie.persistence.pending-byte-capacity` | `67108864` | Retained manifest and state bytes across accepted writes. |
| `movie.persistence.max-state-bytes` | `4194304` | Maximum manifest plus payload bytes for one state. |
| `movie.persistence.sqlite.path` | none | Required SQLite file path. |

The path's parent directory must already exist. Persistence configuration contains no credentials and may use the normal trusted Movie configuration precedence.

## Identity And Revision

A `PersistenceId(entity_type, entity_id)` is stable application identity. It is independent of Actor Identity, Actor Path, Actor System name, and Actor System Incarnation. Applications sharing one database must choose non-colliding entity type and entity ID values.

No record has Revision `0`. Every successful nonduplicate `upsert` or `delete` commits expected Revision plus one. A stale expected Revision raises `ConcurrentWriteError`; this is optimistic storage concurrency, not a domain version or message acknowledgement.

SQLite is local durable storage. The application must maintain one logical actor owner for each Persistence Identity. Two writers can race, but only one matching expected Revision commits; persistence v1 does not decide which actor is the rightful owner.

## Operation Identity

Every mutation requires an `OperationId`. State or tombstone mutation, its new Revision, and the operation fingerprint commit in one SQLite transaction.

- Reusing an Operation Identity with the same action, manifest, and payload returns its original Revision with `duplicate=True`.
- Reusing it with different stored content raises `OperationConflictError`.
- Deduplication is checked before Revision concurrency and survives delete, actor restart, and Actor System restart.
- A duplicate observed by `DurableStateBehavior` triggers Recovery before callbacks, so a late retry cannot replace a newer authoritative state with an older candidate.

The fingerprint describes the replacement state, not arbitrary command intent. A state-relative command such as increment can compute different replacement bytes when retried after its first commit and therefore conflict. Retry-safe commands should express an absolute replacement or retain processed command identities in their domain state.

## Store Contract

`DurableStateStore` returns nonblocking Futures from:

- `load(persistence_id)`;
- `upsert(persistence_id, expected_revision, operation_id, manifest, payload)`;
- `delete(persistence_id, expected_revision, operation_id)`;
- `changes(entity_type, min_slice, max_slice, after_offset, limit, max_bytes, scan_limit)`;
- `compact_changes(limit)`.

`load` returns `None`, a live `DurableStateRecord`, or a tombstone record. Delete always writes a tombstone, including deletion at Revision `0`; operation history and Revision are not physically purged. `changes` returns a bounded `DurableStateChangeBatch`; its offset covers every returned change and may also advance across changes outside the requested source filter.

The SQLite backend requires a file-backed UTF-8 database and effective WAL mode, and uses `synchronous=FULL`, foreign keys, a bounded busy timeout, and schema version `1`. The schema owns latest state, Operation Identity history, immutable changes, Projection checkpoints, and Change Feed floor/high-watermark metadata. Startup rejects a newer schema or incompatible v1 table definitions, indexes, or triggers. One physical `aiosqlite` connection is pinned to one stable `AsyncioIOWorker`; complete transactions are serialized by one connection lane.

Persistence has its own count and byte admission limits in addition to the shared `ASYNCIO_IO` command limit. Store calls and Projection storage operations share `operation-capacity`. Full capacity raises `PersistenceCapacityError` synchronously for Store calls and causes a running Projection to retry with backoff. A Recovery or Change Feed read that cannot acquire and finish its connection lane before `recovery-timeout` interrupts the read, waits for physical cleanup, and raises `PersistenceOperationTimeout`. A mutation may time out safely while waiting for its connection lane or an external SQLite writer before its transaction starts; after `BEGIN IMMEDIATE`, Movie waits for a definite commit or rollback outcome rather than reporting a false cancellation.

## Durable State Behavior

Subclasses implement:

```python
def empty_state(self) -> State: ...

def handle_command(
    self,
    state: State,
    command: Command,
    context: ActorContext[Command],
) -> DurableEffect[State]: ...
```

Construct the behavior through `Behaviors.setup`. It starts Recovery immediately and suspends user-message processing until Recovery finishes. Commands remain in the actor's existing bounded mailbox; lifecycle and persistence control records retain priority. A custom mailbox used by a durable behavior must advertise `supports_user_suspension = True` and consult `InternalActorContext.can_process_user_messages()` before invoking user messages; the default mailbox does both. The Actor start Future means the behavior was materialized, not that asynchronous Recovery completed. Override `on_recovery_completed` when application readiness needs that distinction.

The command handler receives an isolated state copy and returns one immutable effect:

- `persist(next_state, operation_id)`;
- `delete(operation_id)`;
- `none()`;
- `stop()`;
- optional ordered `then_run(callback)` and `then_stop()` chaining.

The codec's `encode` method returns `EncodedState(manifest, payload)` without mutating its input. Manifests must be stable application schema identifiers, not Python class names. `decode` must accept retained historical manifests and return an independent value. Pickle, module-qualified class identity, and implicit schema inference are outside the contract.

Movie serializes and decodes the candidate before storage, keeps the current live state unchanged during the write, and publishes the candidate only after a confirmed commit. `then_run` executes only after commit and state publication. Callbacks are volatile and may run again after an uncertain application retry; they are not an exactly-once external side-effect mechanism.

## Failure And Recovery

Recovery of a missing row uses a fresh empty state at Revision `0`. Recovery of a tombstone uses a fresh empty state while retaining the tombstone Revision. Recovery of a live row decodes its stored manifest and payload.

A supervised actor restart discards its in-memory state, pending candidate, and Revision, then starts a new Recovery. Old-generation storage completions are ignored before reaching the replacement behavior. Store, codec, schema, concurrency, and timeout failures never publish candidate state or run post-commit callbacks.

Applications may override `on_recovery_failure` and `on_persist_failure`. Hook failures are attached as diagnostics and do not replace the primary persistence failure. The behavior raises `DurableStateRecoveryError` or `DurableStatePersistError` into normal actor supervision.

## Shutdown

Actor System shutdown fences new persistence operations before stopping actors. Already accepted store work remains owned and is drained within the shared shutdown deadline. SQLite closes before `ASYNCIO_IO`; if shutdown times out, a later `ActorSystem.stop()` resumes waiting for the same accepted work and close attempt.

Shutdown does not drain actor mailboxes. An accepted command is not necessarily persisted, and a committed state is not evidence that its command received an application acknowledgement. Obtain a post-commit application acknowledgement before stopping when command completion matters.
