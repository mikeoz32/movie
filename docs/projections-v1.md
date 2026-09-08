# Durable State Projections v1

This document defines Movie's first Projection contract. Canonical terminology is in [`CONTEXT.md`](../CONTEXT.md), Durable State storage is specified in [`persistence-v1.md`](persistence-v1.md), and the Change Feed decision is recorded in [ADR-0005](adr/0005-project-durable-state-from-a-transactional-change-feed.md).

## Scope

Projection v1 provides:

- one immutable Durable State Change for every nonduplicate state or tombstone commit;
- source filters by entity type and inclusive persistence-slice range;
- globally ordered, bounded asynchronous batches;
- durable Projection Identity, source definition, processing mode, and Projection Offset;
- at-least-once handlers with retry and exponential backoff;
- SQLite exactly-once read-model transactions;
- explicit bounded Change Feed compaction and Projection retirement;
- bounded runner, operation, batch-byte, handler-time, and shutdown behavior.

It does not provide domain events, cross-system Projection ownership, leases, fencing, automatic slice assignment, a distributed Projection daemon, automatic compaction, or exactly-once effects outside the persistence SQLite database.

## Change Source

`DurableStateStore.changes` returns a Future of `DurableStateChangeBatch`. Each change contains its global offset, Persistence Identity, Revision, Operation Identity, persistence slice, state manifest/payload or tombstone, and diagnostic commit time. The offset, not commit time, defines ordering.

State mutation, Operation Identity history, the Durable State Change, and the Change Feed high watermark commit in one SQLite transaction. A duplicate Operation Identity does not append another change. Per-Persistence-Identity changes therefore preserve Revision order; changes from different identities are observed in global SQLite commit order.

The batch offset is the next resumable checkpoint. Each poll examines at most `scan-limit` consecutive global offset positions, so a narrow or empty slice never performs an unbounded residual-filter scan. When fewer matching changes than the count/byte limits exist in that window, the offset advances to the window frontier across nonmatching entity types or slices. This prevents an idle filtered Projection from pinning compaction forever. A byte-truncated or count-truncated batch advances only through its final returned change.

Movie has 1,024 stable persistence slices. `persistence_slice(persistence_id)` hashes:

```text
uint16_be(len(entity_type_utf8)) || entity_type_utf8 || entity_id_utf8
```

with SHA-256, interprets the first four digest bytes as an unsigned big-endian integer, and reduces modulo 1,024. This algorithm is part of schema v1 and must not change without migration.

## Running Projections

`PROJECTIONS.get(system)` starts the Projection extension lazily after `DURABLE_STATE`. Start an at-least-once runner with one async batch handler:

```python
from movie.projection import PROJECTIONS, ProjectionId

async def update_search_index(changes):
    for change in changes:
        await search_index.replace(change.persistence_id.entity_id, change.payload)

handle = PROJECTIONS.get(system).run_at_least_once(
    ProjectionId("order-search", "slice-0"),
    entity_type="order",
    min_slice=0,
    max_slice=127,
    handler=update_search_index,
)
```

The handler runs before its Projection Offset commits. A handler or checkpoint failure retries the same uncheckpointed batch, so external effects must be idempotent. Successful empty scans may advance the offset without invoking the handler.

One logical runner must own each Projection Identity across all Actor Systems sharing a database. Local duplicate starts are rejected, and checkpoint compare-and-set detects competing progress, but v1 supplies no cross-system lease or fencing. Changing entity type, slice range, or processing mode requires a new Projection Identity.

`ProjectionHandle.wait_started()` reports whether durable registration ever succeeded. `is_running` reports current state, `offset` reports the latest locally confirmed checkpoint, `last_error` retains the latest retryable error, and `failure` contains only a terminal error. `request_stop()` is nonblocking; `stop(timeout)` requests stop and waits. Handlers execute on the persistence I/O event loop and must never perform blocking work.

## SQLite Exactly Once

Exactly-once is available only for read-model writes in the same SQLite database:

```python
async def update_order_summary(transaction, changes):
    for change in changes:
        await transaction.execute(
            """
            INSERT INTO order_summary (order_id, payload)
            VALUES (?, ?)
            ON CONFLICT(order_id) DO UPDATE SET payload = excluded.payload
            """,
            (change.persistence_id.entity_id, change.payload),
        )

handle = PROJECTIONS.get(system).run_exactly_once(
    ProjectionId("order-summary", "all"),
    entity_type="order",
    min_slice=0,
    max_slice=1023,
    handler=update_order_summary,
)
```

`ProjectionTransaction.execute`, `fetchone`, and `fetchall` are valid only during the handler callback. The handler cannot control transactions, run pragmas, attach databases, change schemas, or write Movie-owned tables. Read-model tables must already exist. Handler writes and the Projection Offset commit under one `BEGIN IMMEDIATE`; handler failure, timeout, cooperative cancellation, forbidden SQL, or offset conflict rolls back before the connection lane is released.

HTTP calls, files, Kafka, and every other external effect remain at-least-once. Use idempotency or write a transactional outbox table through the exactly-once handler and deliver that outbox separately.

## Retry And Shutdown

Registration, source reads, retryable handler failures, and checkpoint writes retry with bounded exponential backoff. Invalid async-handler shape, invalid baseline, stored source mismatch, checkpoint ownership conflict, and exactly-once transaction-contract violations are terminal. Handler timeout is retryable because its offset did not commit.

Stop cooperatively cancels an active handler. Exactly-once waits for rollback before the handle stops. A handler that suppresses cancellation or blocks the event-loop thread can still exhaust the shutdown deadline; Python cannot safely terminate arbitrary user code.

The Projection extension prepares and stops before Durable State and `ASYNCIO_IO`. Projection runners are tracked background tasks and do not permanently consume shared `ASYNCIO_IO` command capacity.

## Compaction And Baselines

`compact_changes()` deletes one configured row-bounded batch no further than the minimum checkpoint of every registered Projection. Call it repeatedly until it returns `0`; Movie never compacts automatically. A stopped Projection remains registered and protects its resume history.

`retire(projection_id)` permanently removes one stopped Projection checkpoint. Retirement is an administrative declaration that the Projection will not resume from that checkpoint.

Compaction durably advances `compacted_through`. A new Projection Identity registered afterward must supply an application-defined `initial_offset` between `compacted_through` and the current high watermark. Omitting it or choosing an offset below retained history raises `ProjectionBaselineError`; a direct Store source read below the floor raises `ChangeFeedCompactedError`. The baseline means the application accepts all changes through that offset as already represented elsewhere.

## Configuration

| Path | Default | Meaning |
|---|---:|---|
| `movie.projection.runner-capacity` | `32` | Active and starting Projection runners. |
| `movie.projection.batch-size` | `100` | Maximum changes delivered in one handler batch. |
| `movie.projection.batch-byte-capacity` | `16777216` | Maximum retained manifest/payload bytes selected for one batch. Must cover `max-state-bytes`. |
| `movie.projection.pending-byte-capacity` | `67108864` | Shared batch-byte reservations across handlers. Must cover one batch. |
| `movie.projection.poll-interval` | `0.1` | Seconds between empty source polls. |
| `movie.projection.retry-min-backoff` | `0.1` | Initial retry delay in seconds. |
| `movie.projection.retry-max-backoff` | `5` | Maximum retry delay in seconds. |
| `movie.projection.handler-timeout` | `30` | Seconds allowed for one async handler attempt. |
| `movie.projection.scan-limit` | `10000` | Maximum global offset span examined by one filtered source poll. |
| `movie.projection.compaction-batch-size` | `1000` | Maximum Change Feed rows deleted by one compaction call. |

The selected batch respects both count and byte limits. While deciding whether one additional row fits, the SQLite adapter may transiently materialize at most one extra `max-state-bytes` record; that row is not delivered or checkpointed and is read again in the next batch.
