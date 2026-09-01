import time
from threading import Event, RLock, Thread

import pytest

from movie.actor import (
    AbstractBehavior,
    ActorContext,
    ActorSystem,
    Behaviors,
    SupervisorDirective,
)
from movie.actor.impl.system import ActorSystemImpl
from movie.actor.ref import ActorRef
from movie.config import Config
from movie.dispatch.worker_pool import WorkerPoolDispatcherImpl

SystemMessage = ActorSystem.SystemMessage


def test_actor_system_creation():
    class Child(AbstractBehavior[str]):
        def __init__(self, context: ActorContext[str]) -> None:
            super().__init__(context)

        @staticmethod
        def create() -> AbstractBehavior[str]:
            return Behaviors.setup(Child)

        def receive(
            self, context: ActorContext, message: str
        ) -> "AbstractBehavior | None":
            context.log.debug(f"Child message: {message}")

    class TestBehavior(AbstractBehavior[str]):
        def __init__(self, context: ActorContext[str]) -> None:
            super().__init__(context)
            self.child = self.context.spawn(Child.create(), "child-actor")

        @staticmethod
        def create() -> AbstractBehavior[str]:
            return Behaviors.setup(lambda ctx: TestBehavior(ctx))

        def receive(
            self, context: ActorContext, message: str
        ) -> "AbstractBehavior | None":
            context.log.debug(f"Received message: {message}")
            self.child.tell(f"Forwarded: {message}")

    system = ActorSystem.create(TestBehavior.create(), "test-system")
    assert system is not None
    system.tell("Hello, Actor!")
    system.stop()


def test_actor_system_load():
    received = Event()
    counter_lock = RLock()

    class Child(AbstractBehavior[str]):
        receive_count = 0

        def __init__(self, context: ActorContext[str]) -> None:
            super().__init__(context)

        @staticmethod
        def create() -> AbstractBehavior[str]:
            return Behaviors.setup(Child)

        def receive(
            self, context: ActorContext, message: str
        ) -> "AbstractBehavior | None":
            with counter_lock:
                Child.receive_count += 1
                if Child.receive_count == 200:
                    received.set()

    class TestBehavior(AbstractBehavior[str]):
        def __init__(self, context: ActorContext[str]) -> None:
            super().__init__(context)
            self.children = [
                self.context.spawn(Child.create(), f"child-{ref}") for ref in range(100)
            ]

        @staticmethod
        def create() -> AbstractBehavior[str]:
            return Behaviors.setup(lambda ctx: TestBehavior(ctx))

        def receive(
            self, context: ActorContext, message: str
        ) -> "AbstractBehavior | None":
            context.log.info(f"Received message: {message}")
            for child in self.children:
                child.tell(f"{message}")

    start = time.perf_counter()
    system = ActorSystem.create(TestBehavior.create(), "test-system")
    end = time.perf_counter()
    print(f"Actors initialization: {end - start:.6f} секунд")
    assert system is not None
    start = time.perf_counter()
    system.tell("Hello, Actor!")
    system.tell("Hello, Actor!")
    assert received.wait(2.0)
    system.stop()
    end = time.perf_counter()
    print(f"Messages processed in : {end - start:.6f} секунд")
    assert Child.receive_count == 200


def test_actor_failed():
    class Child(AbstractBehavior[str]):
        def __init__(self, context: ActorContext[str]) -> None:
            super().__init__(context)

        @staticmethod
        def create() -> AbstractBehavior[str]:
            return Behaviors.setup(Child)

        def receive(
            self, context: ActorContext[str], message: str
        ) -> "AbstractBehavior[str] | None":
            context.log.info(f"Child received message: {message}")
            raise Exception("Simulated failure in Child actor")

    class TestBehavior(AbstractBehavior[str]):
        def __init__(self, context: ActorContext[str]) -> None:
            super().__init__(context)
            self.child = self.context.spawn(Child.create(), "child-actor")

        @staticmethod
        def create() -> AbstractBehavior[str]:
            return Behaviors.setup(lambda ctx: TestBehavior(ctx))

        def receive(
            self, context: ActorContext, message: str
        ) -> "AbstractBehavior | None":
            context.log.error(f"Received message: {message}")
            self.child.tell(f"Forwarded: {message}")

        def on_signal(self, context: ActorContext, message: SystemMessage) -> None:
            context.log.info(f"Received system message: {message}")

    system = ActorSystem.create(TestBehavior.create(), "test-system")
    assert system is not None
    system.tell("Hello, Actor!")
    time.sleep(
        0.5
    )  # TODO: fix error handling in actor system, this line make test stuck
    system.stop()


def test_functional_behavior():
    def behavior(counter: int = 0) -> AbstractBehavior[int]:
        def receive(context: ActorContext, msg: str) -> "AbstractBehavior[int]":
            context.log.info(f"Message count: {counter}")
            return behavior(counter + 1)

        return Behaviors.receive(receive)

    system = ActorSystem.create(behavior(), "functional-behavior-system")
    assert system is not None
    system.tell("Hello, Functional Actor!")
    system.tell("Hello, Functional Actor!")
    system.tell("Hello, Functional Actor!")
    system.tell("Hello, Functional Actor!")
    system.stop()


def test_actor_lifecycle_signals():
    started = Event()
    stopped = Event()

    class TestBehavior(AbstractBehavior[None]):
        def receive(
            self, context: ActorContext, message: None
        ) -> "AbstractBehavior | None":
            return None

        def on_signal(self, context: ActorContext, message: SystemMessage) -> None:
            match message:
                case ActorSystem.PreStart():
                    started.set()
                case ActorSystem.PostStop():
                    stopped.set()

    system = ActorSystem.create(Behaviors.setup(TestBehavior), "lifecycle-system")
    assert started.wait(1.0)
    system.stop()
    assert stopped.wait(1.0)


def test_actor_stop_stops_children():
    child_stopped = Event()

    class Child(AbstractBehavior[None]):
        def receive(
            self, context: ActorContext, message: None
        ) -> "AbstractBehavior | None":
            return None

        def on_signal(self, context: ActorContext, message: SystemMessage) -> None:
            if isinstance(message, ActorSystem.PostStop):
                child_stopped.set()

    class Parent(AbstractBehavior[None]):
        def __init__(self, context: ActorContext[None]) -> None:
            super().__init__(context)
            self.context.spawn(Behaviors.setup(Child), "child")

        def receive(
            self, context: ActorContext, message: None
        ) -> "AbstractBehavior | None":
            return None

    system = ActorSystem.create(Behaviors.setup(Parent), "stop-children-system")
    system.stop()
    assert child_stopped.wait(1.0)


def test_supervision_restart_child():
    restarted = Event()

    class Child(AbstractBehavior[str]):
        starts = 0

        def receive(
            self, context: ActorContext[str], message: str
        ) -> "AbstractBehavior | None":
            raise Exception("boom")

        def on_signal(self, context: ActorContext, message: SystemMessage) -> None:
            if isinstance(message, ActorSystem.PreStart):
                Child.starts += 1
                if Child.starts >= 2:
                    restarted.set()

    class Parent(AbstractBehavior[str]):
        def __init__(self, context: ActorContext[str]) -> None:
            super().__init__(context)
            self.child = self.context.spawn(Behaviors.setup(Child), "child")

        def receive(
            self, context: ActorContext, message: str
        ) -> "AbstractBehavior | None":
            self.child.tell(message)
            return None

        def supervise(
            self,
            context: ActorContext,
            child: "ActorRef",
            exception: Exception,
        ) -> SupervisorDirective:
            return SupervisorDirective.RESTART

    system = ActorSystem.create(Behaviors.setup(Parent), "supervision-system")
    system.tell("fail")
    assert restarted.wait(1.0)
    system.stop()


def test_stopped_behavior_terminates_and_unregisters_actor():
    stopped = Event()

    class Root(AbstractBehavior[str]):
        def receive(self, context: ActorContext, message: str):
            return Behaviors.stopped

        def on_signal(self, context: ActorContext, message: SystemMessage) -> None:
            if isinstance(message, ActorSystem.PostStop):
                stopped.set()

    system = ActorSystem.create(Behaviors.setup(Root), "stopped-behavior-system")
    assert system.actor_count == 2

    system.tell("stop")

    assert stopped.wait(1.0)
    deadline = time.monotonic() + 1.0
    while system.actor_count != 1 and time.monotonic() < deadline:
        time.sleep(0.001)
    assert system.actor_count == 1
    system.stop()
    assert system.actor_count == 0


def test_stopped_behavior_discards_rest_of_current_mailbox_batch():
    received: list[int] = []
    stopped = Event()
    config = Config(
        {
            "movie": {
                "dispatcher": {
                    "default-dispatcher": {
                        "type": "movie.dispatch.worker_pool.WorkerPoolDispatcherImpl",
                        "workers": 1,
                    }
                }
            }
        }
    )

    class Root(AbstractBehavior[int]):
        def receive(self, context: ActorContext, message: int):
            received.append(message)
            return Behaviors.stopped

        def on_signal(self, context: ActorContext, message: SystemMessage) -> None:
            if isinstance(message, ActorSystem.PostStop):
                stopped.set()

    system = ActorSystem.create(
        Behaviors.setup(Root), "batch-stop-system", config=config
    )
    dispatcher = system._dispatchers.default_dispatcher
    entered = Event()
    release = Event()
    dispatcher.dispatch(lambda: (entered.set(), release.wait(1.0)))
    assert entered.wait(1.0)
    for message in range(100):
        system.tell(message)
    release.set()

    assert stopped.wait(1.0)
    assert received == [0]
    system.stop()


def test_base_exception_preserves_unprocessed_batch_tail_for_restart():
    received: list[int] = []
    completed = Event()
    config = Config(
        {
            "movie": {
                "actor": {"log-level": "CRITICAL"},
                "dispatcher": {
                    "default-dispatcher": {
                        "type": "movie.dispatch.worker_pool.WorkerPoolDispatcherImpl",
                        "workers": 1,
                    }
                },
            }
        }
    )

    class Root(AbstractBehavior[int]):
        def receive(self, context: ActorContext, message: int):
            if message == 0:
                raise KeyboardInterrupt("boom")
            received.append(message)
            if message == 99:
                completed.set()
            return self

    system = ActorSystem.create(
        Behaviors.setup(Root), "base-exception-system", config=config
    )
    dispatcher = system._dispatchers.default_dispatcher
    entered = Event()
    release = Event()
    dispatcher.dispatch(lambda: (entered.set(), release.wait(1.0)))
    assert entered.wait(1.0)
    for message in range(100):
        system.tell(message)
    release.set()

    assert completed.wait(2.0)
    assert received == list(range(1, 100))
    system.stop()


def test_parent_stop_during_child_mailbox_creation_cannot_resurrect_child(
    monkeypatch,
):
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "child-publication-system",
    )
    parent_ref = system._root_ref
    parent = system.get_context(parent_ref)
    original_create = system.mailboxes.create_mailbox
    entered = Event()
    release = Event()
    spawn_errors: list[BaseException] = []

    def blocking_create(dispatcher, actor):
        if actor.ref.name == "late-child":
            entered.set()
            release.wait(2.0)
        return original_create(dispatcher, actor)

    monkeypatch.setattr(system.mailboxes, "create_mailbox", blocking_create)

    def spawn_child() -> None:
        try:
            system.spawn(
                Behaviors.receive(lambda context, message: Behaviors.same),
                "late-child",
                parent=parent,
            )
        except BaseException as error:
            spawn_errors.append(error)

    spawner = Thread(target=spawn_child)
    spawner.start()
    assert entered.wait(1.0)
    system.terminate(parent_ref)
    deadline = time.monotonic() + 1.0
    while parent.state.name != "STOPPING" and time.monotonic() < deadline:
        time.sleep(0.001)
    assert parent.state.name == "STOPPING"
    release.set()
    spawner.join(2.0)

    assert not spawner.is_alive()
    assert spawn_errors == []
    assert parent.wait_stopped(1.0)
    assert parent.state.name == "STOPPED"
    assert system.actor_count == 1
    system.stop()


def test_failed_child_startup_notifies_stopping_parent(monkeypatch):
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "failed-child-startup-system",
    )
    parent_ref = system._root_ref
    parent = system.get_context(parent_ref)
    original_create = system.mailboxes.create_mailbox
    entered = Event()
    release = Event()
    spawn_errors: list[BaseException] = []

    def failing_create(dispatcher, actor):
        if actor.ref.name == "failing-child":
            entered.set()
            release.wait(2.0)
            raise ValueError("mailbox failed")
        return original_create(dispatcher, actor)

    monkeypatch.setattr(system.mailboxes, "create_mailbox", failing_create)

    def spawn_child() -> None:
        try:
            system.spawn(
                Behaviors.receive(lambda context, message: Behaviors.same),
                "failing-child",
                parent=parent,
            )
        except BaseException as error:
            spawn_errors.append(error)

    spawner = Thread(target=spawn_child)
    spawner.start()
    assert entered.wait(1.0)
    system.terminate(parent_ref)
    deadline = time.monotonic() + 1.0
    while parent.state.name != "STOPPING" and time.monotonic() < deadline:
        time.sleep(0.001)
    release.set()
    spawner.join(2.0)

    assert len(spawn_errors) == 1
    assert isinstance(spawn_errors[0], ValueError)
    assert parent.wait_stopped(1.0)
    assert system.actor_count == 1
    system.stop()


def test_cross_system_parent_and_termination_are_rejected():
    first = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "first-owner-system",
    )
    second = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "second-owner-system",
    )
    try:
        parent = first.get_context(first._root_ref)
        with pytest.raises(ValueError, match="does not belong"):
            second.spawn(
                Behaviors.receive(lambda context, message: Behaviors.same),
                "foreign-child",
                parent=parent,
            )
        with pytest.raises(ValueError, match="does not belong"):
            second.terminate(first._root_ref)
    finally:
        first.stop()
        second.stop()


def test_blocked_future_callback_does_not_block_lifecycle_settlement():
    config = Config({"movie": {"actor": {"callback-workers": 1}}})
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "future-settlement-system",
        config=config,
    )
    actor_ref = system.spawn(
        Behaviors.receive(lambda context, message: Behaviors.same), "child"
    )
    started_future = system.actor_start_future(actor_ref)
    stopped_future = system.actor_stop_future(actor_ref)
    entered = Event()
    completed = Event()

    def wait_for_stop(future) -> None:
        entered.set()
        stopped_future.result(timeout=1.0)
        completed.set()

    started_future.add_done_callback(wait_for_stop)
    assert entered.wait(1.0)
    system.terminate(actor_ref)

    assert stopped_future.result(timeout=1.0) is None
    assert completed.wait(1.0)
    system.stop()


def test_blocked_future_callback_keeps_system_stopping_until_released():
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "blocked-callback-shutdown-system",
    )
    entered = Event()
    release = Event()
    system.actor_start_future(system._root_ref).add_done_callback(
        lambda future: (entered.set(), release.wait(2.0))
    )
    assert entered.wait(1.0)

    started = time.monotonic()
    with pytest.raises(TimeoutError, match="callbacks"):
        system.stop(timeout=0.05)
    assert time.monotonic() - started < 0.5
    assert system._state.name == "STOPPING"

    release.set()
    system.stop(timeout=2.0)
    assert system._state.name == "STOPPED"


def test_lifecycle_activation_saturation_respects_shutdown_deadline():
    config = Config(
        {
            "movie": {
                "dispatcher": {
                    "default-dispatcher": {
                        "type": "movie.dispatch.worker_pool.WorkerPoolDispatcherImpl",
                        "workers": 1,
                        "queue-capacity": 1,
                    }
                }
            }
        }
    )
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "saturated-lifecycle-system",
        config=config,
    )
    dispatcher = system._dispatchers.default_dispatcher
    entered = Event()
    release = Event()
    dispatcher.dispatch(lambda: (entered.set(), release.wait(2.0)))
    assert entered.wait(1.0)
    dispatcher.dispatch(lambda: None)

    started = time.monotonic()
    with pytest.raises(TimeoutError):
        system.stop(timeout=0.05)
    assert time.monotonic() - started < 0.5

    release.set()
    system.stop(timeout=2.0)
    assert system.actor_count == 0


def test_messages_preserve_order_immediately_after_startup():
    completed = Event()
    received: list[int] = []

    class Ordered(AbstractBehavior[int]):
        def receive(self, context: ActorContext, message: int):
            received.append(message)
            if len(received) == 5_000:
                completed.set()
            return self

    system = ActorSystem.create(Behaviors.setup(Ordered), "ordered-system")
    for sequence in range(5_000):
        system.tell(sequence)

    assert completed.wait(2.0)
    assert received == list(range(5_000))
    system.stop()


def test_duplicate_sibling_names_fail_startup():
    class Child(AbstractBehavior[None]):
        def receive(self, context: ActorContext, message: None):
            return self

    class Parent(Child):
        def __init__(self, context: ActorContext) -> None:
            super().__init__(context)
            context.spawn(Behaviors.setup(Child), "duplicate")
            context.spawn(Behaviors.setup(Child), "duplicate")

    with pytest.raises(RuntimeError, match="failed during startup"):
        ActorSystem.create(Behaviors.setup(Parent), "duplicate-names-system")


def test_actor_system_cannot_be_started_twice():
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "single-start-system",
    )
    try:
        with pytest.raises(RuntimeError, match="started once"):
            system.start()
        assert system.actor_count == 2
    finally:
        system.stop()


def test_actor_capacity_is_enforced() -> None:
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "actor-capacity-system",
        config=Config({"movie": {"actor": {"max-actors": 2}}}),
    )
    try:
        with pytest.raises(RuntimeError, match="capacity exceeded"):
            system.spawn(
                Behaviors.receive(lambda context, message: Behaviors.same),
                "over-capacity",
            )
    finally:
        system.stop()


def test_spawn_is_rejected_after_shutdown_begins():
    entered = Event()
    release = Event()

    class Blocking(AbstractBehavior[str]):
        def receive(self, context: ActorContext, message: str):
            entered.set()
            release.wait(2.0)
            return self

    system = ActorSystem.create(Behaviors.setup(Blocking), "stopping-system")
    system.tell("block")
    assert entered.wait(1.0)

    stop_error: list[Exception] = []

    def stop_system() -> None:
        try:
            system.stop(timeout=2.0)
        except Exception as error:
            stop_error.append(error)

    stopper = Thread(target=stop_system)
    stopper.start()
    deadline = time.monotonic() + 1.0
    while system._state.name != "STOPPING" and time.monotonic() < deadline:
        time.sleep(0.001)
    assert system._state.name == "STOPPING"
    with pytest.raises(RuntimeError, match="not accepting new actors"):
        system.spawn(Behaviors.receive(lambda context, message: Behaviors.same), "late")
    release.set()
    stopper.join(3.0)

    assert not stopper.is_alive()
    assert stop_error == []
    assert system.actor_count == 0


def test_stop_from_actor_callback_fails_without_deadlock():
    rejected = Event()

    def receive(context: ActorContext, message: str):
        with pytest.raises(RuntimeError, match="cannot be called"):
            context.get_system().stop()
        rejected.set()
        return Behaviors.same

    system = ActorSystem.create(Behaviors.receive(receive), "callback-stop-system")
    system.tell("stop")
    assert rejected.wait(1.0)
    system.stop()


def test_invalid_shutdown_timeout_does_not_change_system_state():
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "invalid-timeout-system",
    )
    try:
        with pytest.raises(ValueError, match="must be positive"):
            system.stop(timeout=0)
        assert system._state.name == "RUNNING"
        assert system.actor_count == 2
    finally:
        system.stop()


def test_shutdown_waits_for_accepted_dispatcher_work():
    config = Config(
        {
            "movie": {
                "dispatcher": {
                    "default-dispatcher": {
                        "type": "movie.dispatch.worker_pool.WorkerPoolDispatcherImpl",
                        "workers": 1,
                        "shutdown-timeout": 2,
                    }
                }
            }
        }
    )
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "saturated-shutdown-system",
        config=config,
    )
    dispatcher = system._dispatchers.default_dispatcher
    entered = Event()
    release = Event()
    dispatcher.dispatch(lambda: (entered.set(), release.wait(1.0)))
    assert entered.wait(1.0)
    errors: list[Exception] = []
    stopper = Thread(target=lambda: _stop_and_capture(system, errors))
    stopper.start()
    time.sleep(0.02)
    assert stopper.is_alive()
    release.set()
    stopper.join(3.0)

    assert errors == []
    assert not stopper.is_alive()
    assert system.actor_count == 0


def test_actor_shutdown_timeout_bounds_dispatcher_shutdown():
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "bounded-dispatcher-shutdown-system",
    )
    dispatcher = WorkerPoolDispatcherImpl(
        Config({"workers": 1, "shutdown-timeout": 10})
    )
    dispatcher.start()
    system._dispatchers.register_dispatcher("blocked", dispatcher)
    entered = Event()
    release = Event()
    dispatcher.dispatch(lambda: (entered.set(), release.wait(2.0)))
    assert entered.wait(1.0)

    started = time.monotonic()
    with pytest.raises(TimeoutError):
        system.stop(timeout=0.05)
    elapsed = time.monotonic() - started

    assert elapsed < 0.5
    release.set()
    system.stop(timeout=2.0)
    assert system.actor_count == 0


def test_single_worker_startup_is_deterministic():
    config = Config(
        {
            "movie": {
                "dispatcher": {
                    "default-dispatcher": {
                        "type": "movie.dispatch.worker_pool.WorkerPoolDispatcherImpl",
                        "workers": 1,
                        "shutdown-timeout": 2,
                    }
                }
            }
        }
    )

    for index in range(25):
        system = ActorSystem.create(
            Behaviors.receive(lambda context, message: Behaviors.same),
            f"single-worker-startup-{index}",
            config=config,
        )
        system.stop()


def test_terminate_survives_dispatcher_activation_rejection(monkeypatch):
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "mailbox-only-termination",
    )
    actor_ref = system._root_ref
    context = system.get_context(actor_ref)
    dispatcher = system._dispatchers.default_dispatcher
    deadline = time.monotonic() + 1.0
    while context._mailbox._scheduled and time.monotonic() < deadline:
        time.sleep(0.001)

    original_dispatch = dispatcher.dispatch

    def reject(task) -> None:
        raise RuntimeError("rejected")

    monkeypatch.setattr(dispatcher, "dispatch", reject)
    try:
        system.terminate(actor_ref)
        assert context.wait_stopped(1.0)
        assert context.state.name == "STOPPED"
    finally:
        monkeypatch.setattr(dispatcher, "dispatch", original_dispatch)
        system.stop()


def _stop_and_capture(system: ActorSystem, errors: list[Exception]) -> None:
    try:
        system.stop(timeout=2.0)
    except Exception as error:
        errors.append(error)


def test_concurrent_shutdown_cannot_resurrect_starting_system():
    entered = Event()
    release = Event()

    class Root(AbstractBehavior[None]):
        def receive(self, context: ActorContext, message: None):
            return self

    def create_root(context: ActorContext):
        entered.set()
        release.wait(2.0)
        return Root(context)

    system = ActorSystemImpl(Behaviors.setup(create_root), "concurrent-start-stop")
    start_errors: list[Exception] = []
    stop_errors: list[Exception] = []
    starter = Thread(target=lambda: _start_and_capture(system, start_errors))
    starter.start()
    assert entered.wait(1.0)
    stopper = Thread(target=lambda: _stop_and_capture(system, stop_errors))
    stopper.start()
    time.sleep(0.02)
    release.set()
    starter.join(3.0)
    stopper.join(3.0)

    assert not starter.is_alive()
    assert not stopper.is_alive()
    assert len(start_errors) == 1
    assert "interrupted by shutdown" in str(start_errors[0])
    assert stop_errors == []
    assert system._state.name == "STOPPED"
    assert system.actor_count == 0


def test_timed_out_startup_rollback_finishes_after_setup_unblocks():
    entered = Event()
    release = Event()

    def create_root(context: ActorContext):
        entered.set()
        release.wait(2.0)
        return Behaviors.receive(lambda child_context, message: Behaviors.same)

    system = ActorSystemImpl(Behaviors.setup(create_root), "rollback-finalizer-system")
    system._startup_timeout = 0.05
    system._shutdown_timeout = 0.05

    with pytest.raises(TimeoutError, match="did not start"):
        system.start()
    assert entered.is_set()
    assert system._rollback_started.is_set()

    release.set()
    assert system._terminated.wait(2.0)
    assert system._state.name == "STOPPED"
    assert system.actor_count == 0


def _start_and_capture(system: ActorSystemImpl, errors: list[Exception]) -> None:
    try:
        system.start()
    except Exception as error:
        errors.append(error)
