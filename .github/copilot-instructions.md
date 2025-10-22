## Quick context

This repository implements a small, local-process Actor Model framework in Python (see `movie/`).
Key components live under `movie/`: `actor.py`, `mailbox.py`, `scheduler.py`, `system_message.py`, and `types.py`.

AI agents should treat this as a single-process, thread-driven actor runtime (no network, no external services).

## Big picture (what to read first)

- `movie/actor.py`: core actor abstractions (ActorSystem, ActorRef, ActorContext, AbstractBehavior, Behaviors). The system is bootstrapped with `ActorSystem.create(...)` and actors are created via `context.spawn(...)` or `ActorSystem.spawn(...)`.
- `movie/mailbox.py` and `movie/scheduler.py`: how messages get queued and executed. Mailboxes schedule mailbox-callables as `Task` objects on the `Scheduler` worker threads.
- `movie/system_message.py`: system-level messages (Failed, Terminated, PostStop) and their semantics.

Read the tests in `tests/` for concrete usage examples: `tests/actor_system_test.py` shows how Behaviors are constructed and how `ActorSystem.create(...)` / `system.tell(...)` / `system.stop()` are used.

## Important project-specific patterns

- Behaviors are factory-like: classes define a static `create()` that returns `Behaviors.setup(factory)` (see tests and `actor.py`). The actor's constructor receives an `ActorContext`.
- `AbstractBehavior.receive(self, context, message)` returns either a new behavior instance (state transition) or `None` to keep the same behavior. Implementations must be side-effecting via the provided `context` (spawn children, tell other actors, etc.).
- `on_signal(self, context, message)` handles system messages (supervision, termination). System messages are sent via `ActorRef.tell_system(...)` in the implementation.
- `LocalActorContext.start()` resolves `DefferedBehavior` by repeatedly calling the factory until a concrete `AbstractBehavior` is returned. Use `Behaviors.setup(...)` for lazy initialization.
- Mailbox types are selectable via `create_mailbox(..., mailbox_type=MailboxType.DEFAULT)`; the two implementations are `DefaultMailbox` and `SingleMessageDispatchMailbox` in `scheduler.py` — they differ in dispatch strategy and threading semantics.

## How messages are delivered

- `ActorRef.tell(message)` enqueues a user message on the actor's mailbox.
- `tell_system` enqueues system messages (see `LocalActorRef.tell_system`).
- Mailboxes use `Scheduler.schedule(Task(mailbox_callable))` to run processing on a worker thread.

## Running the project and tests (developer workflow)

- Python requirement: pyproject declares `requires-python = ">=3.13"`. Use a matching interpreter.
- Install local package and test tools (PowerShell example):

```pwsh
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install -e .
python -m pip install pytest
python -m pytest -q
```

- Primary test commands: `python -m pytest -q` (tests are under `tests/`). Tests instantiate `ActorSystem` and call `start/stop`, so expect thread activity.

## Files to edit for behavior changes

- Add/modify actor behavior logic in `movie/actor.py` (or define new behaviors in separate modules). Prefer adding new behaviors in `tests/` when experimenting.
- If changing message scheduling, update `movie/scheduler.py` and `movie/mailbox.py` together — their APIs are tightly coupled (`create_mailbox`, `Task`, `Scheduler.schedule`).

## Non-obvious implementation details and gotchas

- The actor lifecycle is single-process: `ActorSystemImpl` holds contexts in-memory (`_actors` dict keyed by UUID). There's no persistence or remote delivery.
- `receive` may raise; child failures bubble via `Failed` system messages sent to the parent (see `LocalActorContext.invoke`). Tests rely on this behavior.
- Mailbox and Scheduler use queue primitives and worker threads; shutting down the system requires `ActorSystem.stop()` which calls `Scheduler.stop()` to join worker threads — avoid removing or short-circuiting that sequence.
- Many classes use locking (`RLock`) for concurrency safety — preserve those locks when refactoring to avoid race conditions.

## Useful grep/code patterns for the agent

- Find how actors are spawned: `grep -n "spawn(" -R movie tests`
- Example: `Tests create behaviors with: class X(AbstractBehavior): @staticmethod def create() -> AbstractBehavior: return Behaviors.setup(lambda ctx: X(ctx))` (see `tests/actor_system_test.py`).

## When to ask for human help

- If a change affects the `Scheduler`/`Mailbox` APIs or thread lifecycles, ask a maintainer — subtle race conditions and shutdown ordering are easy to break.
- When introducing new third-party concurrency primitives (new Queue implementations, async event loops), check compatibility with existing synchronous `Scheduler` tests.

If anything is unclear or you want me to expand a section (examples, more cross-file call traces, or a short ADR for Scheduler design), tell me which part and I'll iterate.
