import json
import time
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass
from queue import Queue
from threading import Event

import pytest

from movie.actor import ActorContext, ActorSystem, Behaviors
from movie.config import Config
from movie.mailbox.mailbox import MailboxCapacityExceeded
from movie.persistence import (
    DURABLE_STATE,
    DurableStateBehavior,
    EncodedState,
    OperationConflictError,
    OperationId,
    PersistenceId,
)


@dataclass(frozen=True, slots=True)
class CounterState:
    value: int = 0


@dataclass(frozen=True, slots=True)
class Add:
    value: int
    operation_id: OperationId


@dataclass(frozen=True, slots=True)
class Set:
    value: int
    operation_id: OperationId


@dataclass(frozen=True, slots=True)
class Delete:
    operation_id: OperationId


@dataclass(frozen=True, slots=True)
class Get:
    token: str | None = None


class CounterCodec:
    def encode(self, state: CounterState) -> EncodedState:
        return EncodedState(
            "counter-state/v1",
            json.dumps({"value": state.value}, separators=(",", ":")).encode("ascii"),
        )

    def decode(self, manifest: str, payload: bytes) -> CounterState:
        if manifest != "counter-state/v1":
            raise ValueError("unsupported counter state")
        return CounterState(json.loads(payload)["value"])


class Counter(DurableStateBehavior[Add | Set | Delete | Get, CounterState]):
    def __init__(
        self,
        context: ActorContext,
        persistence_id: PersistenceId,
        observations: Queue,
        committed: Event,
    ) -> None:
        self._observations = observations
        self._committed = committed
        super().__init__(context, persistence_id, CounterCodec())

    def empty_state(self) -> CounterState:
        return CounterState()

    def handle_command(self, state, command, context):
        if isinstance(command, Add):
            return self.persist(
                CounterState(state.value + command.value),
                command.operation_id,
            ).then_run(lambda current: self._committed.set())
        if isinstance(command, Set):
            return self.persist(
                CounterState(command.value),
                command.operation_id,
            ).then_run(lambda current: self._committed.set())
        if isinstance(command, Delete):
            return self.delete(command.operation_id).then_run(
                lambda current: self._committed.set()
            )
        if command.token is None:
            self._observations.put_nowait((state.value, self.revision))
        else:
            self._observations.put_nowait(command.token)
        return self.none()


def create_system(
    name: str,
    path: str,
    *,
    mailbox_capacity: int | None = None,
) -> ActorSystem:
    movie_config = {
        "persistence": {
            "sqlite": {
                "path": path,
            }
        }
    }
    if mailbox_capacity is not None:
        movie_config["mailbox"] = {"default": {"capacity": mailbox_capacity}}
    return ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        name,
        config=Config(
            {
                "movie": movie_config
            }
        ),
    )


def test_durable_behavior_commits_before_next_command_and_recovers_after_respawn(
    tmp_path,
) -> None:
    system = create_system("durable-behavior", str(tmp_path / "behavior.sqlite3"))
    persistence_id = PersistenceId("counter", "one")
    observations = Queue()
    committed = Event()

    def behavior(context):
        return Counter(context, persistence_id, observations, committed)

    first = system.spawn(Behaviors.setup(behavior), "counter-first")
    first.tell(Add(3, OperationId.random()))
    first.tell(Get())

    assert committed.wait(1.0)
    assert observations.get(timeout=1.0) == (3, 1)

    system.terminate(first)
    system.actor_stop_future(first).result(1.0)

    second = system.spawn(Behaviors.setup(behavior), "counter-second")
    try:
        second.tell(Get())
        assert observations.get(timeout=1.0) == (3, 1)
    finally:
        system.stop()


def test_pending_recovery_preserves_bounded_mailbox_fifo(tmp_path, monkeypatch) -> None:
    system = create_system(
        "durable-pending-recovery",
        str(tmp_path / "pending-recovery.sqlite3"),
        mailbox_capacity=2,
    )
    recovery = Future()
    store = system.extension(DURABLE_STATE).store
    monkeypatch.setattr(store, "load", lambda persistence_id: recovery)
    observations = Queue()
    actor = system.spawn(
        Behaviors.setup(
            lambda context: Counter(
                context,
                PersistenceId("counter", "pending-recovery"),
                observations,
                Event(),
            )
        ),
        "counter",
    )
    try:
        system.actor_start_future(actor).result(1.0)
        actor.tell(Get("first"))
        actor.tell(Get("second"))
        with pytest.raises(MailboxCapacityExceeded):
            actor.tell(Get())
        time.sleep(0.05)
        assert observations.empty()

        recovery.set_result(None)

        assert observations.get(timeout=1.0) == "first"
        assert observations.get(timeout=1.0) == "second"
    finally:
        system.stop()


def test_persist_conflict_restarts_and_recovers_before_queued_command(tmp_path) -> None:
    system = create_system("durable-restart", str(tmp_path / "restart.sqlite3"))
    observations = Queue()
    failures = Queue()
    persistence_id = PersistenceId("counter", "restart")
    first_operation = OperationId.random()

    class RestartingCounter(Counter):
        def on_persist_failure(self, error: Exception) -> None:
            failures.put_nowait(error)

    actor = system.spawn(
        Behaviors.setup(
            lambda context: RestartingCounter(
                context,
                persistence_id,
                observations,
                Event(),
            )
        ),
        "counter",
    )
    try:
        actor.tell(Set(1, first_operation))
        actor.tell(Get())
        assert observations.get(timeout=1.0) == (1, 1)

        actor.tell(Set(2, first_operation))
        actor.tell(Get())

        assert isinstance(failures.get(timeout=1.0), OperationConflictError)
        assert observations.get(timeout=1.0) == (1, 1)
    finally:
        system.stop()


def test_delete_effect_recovers_empty_state_at_tombstone_revision(tmp_path) -> None:
    system = create_system("durable-delete", str(tmp_path / "behavior-delete.sqlite3"))
    observations = Queue()
    persistence_id = PersistenceId("counter", "delete")

    def behavior(context):
        return Counter(context, persistence_id, observations, Event())

    first = system.spawn(Behaviors.setup(behavior), "counter-first")
    first.tell(Set(7, OperationId.random()))
    first.tell(Delete(OperationId.random()))
    first.tell(Get())
    assert observations.get(timeout=1.0) == (0, 2)

    system.terminate(first)
    system.actor_stop_future(first).result(1.0)
    second = system.spawn(Behaviors.setup(behavior), "counter-second")
    try:
        second.tell(Get())
        assert observations.get(timeout=1.0) == (0, 2)
    finally:
        system.stop()


def test_behavior_does_not_retain_mutable_empty_state_alias(tmp_path) -> None:
    shared_empty = {"value": 0}
    observations = Queue()

    class MutableCodec:
        def encode(self, state: dict) -> EncodedState:
            return EncodedState("mutable/v1", json.dumps(state).encode("ascii"))

        def decode(self, manifest: str, payload: bytes) -> dict:
            assert manifest == "mutable/v1"
            return json.loads(payload)

    class MutableBehavior(DurableStateBehavior[Get, dict]):
        def empty_state(self) -> dict:
            return shared_empty

        def handle_command(self, state, command, context):
            observations.put_nowait(state["value"])
            return self.none()

    system = create_system("durable-mutable", str(tmp_path / "mutable.sqlite3"))
    actor = system.spawn(
        Behaviors.setup(
            lambda context: MutableBehavior(
                context,
                PersistenceId("mutable", "one"),
                MutableCodec(),
            )
        ),
        "mutable",
    )
    try:
        actor.tell(Get())
        assert observations.get(timeout=1.0) == 0
        shared_empty["value"] = 99
        actor.tell(Get())
        assert observations.get(timeout=1.0) == 0
    finally:
        system.stop()


def test_persist_failure_can_restart_into_a_non_durable_behavior(tmp_path) -> None:
    system = create_system("durable-fallback", str(tmp_path / "fallback.sqlite3"))
    persistence_id = PersistenceId("counter", "fallback")
    observations = Queue()
    factory_calls = 0
    operation_id = OperationId.random()

    def behavior(context):
        nonlocal factory_calls
        factory_calls += 1
        if factory_calls == 1:
            return Counter(context, persistence_id, Queue(), Event())
        return Behaviors.receive(
            lambda inner_context, message: (
                observations.put_nowait(message),
                Behaviors.same,
            )[1]
        )

    actor = system.spawn(Behaviors.setup(behavior), "counter")
    try:
        actor.tell(Set(1, operation_id))
        actor.tell(Set(2, operation_id))
        actor.tell(Get())

        assert observations.get(timeout=1.0) == Get()
    finally:
        system.stop()


def test_user_signal_override_cannot_intercept_recovery_completion(tmp_path) -> None:
    observations = Queue()

    class SignalAwareCounter(Counter):
        def on_signal(self, context, message) -> None:
            pass

    system = create_system("durable-signal", str(tmp_path / "signal.sqlite3"))
    actor = system.spawn(
        Behaviors.setup(
            lambda context: SignalAwareCounter(
                context,
                PersistenceId("counter", "signal"),
                observations,
                Event(),
            )
        ),
        "counter",
    )
    try:
        actor.tell(Get())
        assert observations.get(timeout=1.0) == (0, 0)
    finally:
        system.stop()


def test_duplicate_old_operation_keeps_newer_authoritative_state(tmp_path) -> None:
    system = create_system("durable-duplicate", str(tmp_path / "duplicate.sqlite3"))
    observations = Queue()
    committed = Event()
    persistence_id = PersistenceId("counter", "duplicate")
    actor = system.spawn(
        Behaviors.setup(
            lambda context: Counter(context, persistence_id, observations, committed)
        ),
        "counter",
    )
    first_operation = OperationId.random()
    try:
        actor.tell(Set(1, first_operation))
        actor.tell(Get())
        actor.tell(Set(2, OperationId.random()))
        actor.tell(Get())
        actor.tell(Set(1, first_operation))
        actor.tell(Get())

        assert observations.get(timeout=1.0) == (1, 1)
        assert observations.get(timeout=1.0) == (2, 2)
        assert observations.get(timeout=1.0) == (2, 2)
    finally:
        system.stop()


def test_stop_effect_does_not_invoke_the_rest_of_an_extracted_batch(tmp_path) -> None:
    observations = Queue()
    failures = Queue()

    class StoppingBehavior(DurableStateBehavior[str, CounterState]):
        def empty_state(self) -> CounterState:
            return CounterState()

        def handle_command(self, state, command, context):
            observations.put_nowait(command)
            return self.stop() if command == "stop" else self.none()

        def actor_failed(self, error: Exception) -> None:
            failures.put_nowait(error)
            super().actor_failed(error)

    class ManualDispatcher:
        def __init__(self) -> None:
            self.tasks = deque()

        def dispatch(self, task) -> None:
            self.tasks.append(task)

        def run_all(self) -> None:
            while self.tasks:
                self.tasks.popleft()()

    system = create_system("durable-stop", str(tmp_path / "stop.sqlite3"))
    actor = system.spawn(
        Behaviors.setup(
            lambda context: StoppingBehavior(
                context,
                PersistenceId("counter", "stop"),
                CounterCodec(),
            )
        ),
        "counter",
    )
    try:
        actor.tell("ready")
        assert observations.get(timeout=1.0) == "ready"
        context = system.get_context(actor)
        deadline = time.monotonic() + 1.0
        while context._mailbox._scheduled:
            if time.monotonic() >= deadline:
                raise TimeoutError("actor mailbox did not become idle")
            time.sleep(0.005)
        dispatcher = ManualDispatcher()
        context._mailbox._dispatcher = dispatcher

        actor.tell("stop")
        actor.tell("must-not-run")
        dispatcher.run_all()

        system.actor_stop_future(actor).result(1.0)
        assert observations.get_nowait() == "stop"
        assert observations.empty()
        assert failures.empty()
    finally:
        system.stop()
