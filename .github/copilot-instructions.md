## Repository Context

Movie is a local-process actor runtime for free-threaded CPython 3.14t.

Read these files first:

- `movie/actor/system.py`: public actor-system and lifecycle API.
- `movie/actor/impl/system.py`: registry, startup, shutdown, logging, and dispatch ownership.
- `movie/actor/impl/context.py`: actor state, supervision, children, and behavior invocation.
- `movie/mailbox/default.py`: bounded user mailbox and reliable lifecycle mailbox.
- `movie/dispatch/worker_pool.py`: sharded worker dispatcher and bounded activation queues.
- `movie/streams.py`: demand tracking, stream failure, cancellation, and graph materialization.

Actors must process one behavior callback at a time on free-threaded CPython. User mailboxes and dispatcher activation queues are bounded. Lifecycle messages remain reliable because shutdown and registry cleanup depend on them.

Use `ActorSystem.create(...)`, `context.spawn(...)`, `ActorRef.tell(...)`, and `system.stop(...)` in examples. Returning `Behaviors.stopped` performs normal actor termination. `ActorSystem.stop()` must not be called from inside an actor callback.

Development commands:

```console
uv sync --group test
uv run ruff check .
uv run pytest
uv run python -m benchmarks.runtime --messages 5000 --actors 100 --warmups 0 --iterations 1
uv build
```

When changing concurrency code, test free-threaded CPython 3.14t. Add deterministic regression tests for scheduling rejection, lifecycle cleanup, startup rollback, and shutdown races. Do not replace bounded queues with implicit unbounded work queues.
