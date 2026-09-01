import threading
import time

import pytest

from movie.actor import AbstractBehavior, ActorContext, ActorSystem, Behaviors
from movie.config import Config
from movie.streams import (
    Cancel,
    Flow,
    OnNext,
    Sink,
    Source,
    StageBehavior,
    Subscribe,
)


def wait_until(pred, timeout=3.0, interval=0.01, err="timeout"):
    """Крутить до timeout, поки pred() не поверне True."""
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < timeout:
        if pred():
            return
        time.sleep(interval)
    raise AssertionError(err)


def wait_for_actor_count(system: ActorSystem, expected: int) -> None:
    wait_until(
        lambda: system.actor_count == expected,
        timeout=1.0,
        err=f"expected {expected} actors, got {system.actor_count}",
    )


class SafeCollector:
    def __init__(self, expected=None):
        self.lock = threading.Lock()
        self.items = []
        self.count = 0
        self.expected = expected
        self.done = threading.Event()

    def add(self, x):
        with self.lock:
            # self.items.append(x)
            self.count += 1
            if self.expected is not None and self.count >= self.expected:
                self.done.set()

    def wait_count(self, n, timeout=5.0):
        if self.expected is None:
            # якщо expected не задано — чекаємо по лічильнику
            wait_until(lambda: self.count >= n, timeout=timeout)
        else:
            ok = self.done.wait(timeout)
            if not ok:
                raise AssertionError(f"expected {self.expected}, got {self.count}")


@pytest.fixture
def actor_system():
    class Root(AbstractBehavior[None]):
        def receive(
            self, context: ActorContext, message: None
        ) -> "AbstractBehavior | None":
            return None

    system = ActorSystem.create(Behaviors.setup(Root), "streams-test-system")
    try:
        yield system
    finally:
        system.stop()


def test_linear_source(actor_system: ActorSystem):
    N = 10000
    collector = SafeCollector(expected=N)
    g = (
        Source.from_iterable(range(N))
        .via(Flow.map(lambda x: x * 2))
        .to(Sink.for_each(lambda x: collector.add(x)))
    )

    g.run(actor_system)

    collector.wait_count(N, timeout=1)


def test_collect_materialized(actor_system: ActorSystem):
    sink, result_future = Sink.collect()
    g = Source.from_iterable(range(5)).via(Flow.map(lambda x: x * 3)).to(sink)

    run_result = g.run(actor_system)
    assert run_result.materialized is result_future

    result = result_future.result(timeout=1)
    assert result == [0, 3, 6, 9, 12]
    wait_for_actor_count(actor_system, 2)


def test_for_each_materialized(actor_system: ActorSystem):
    seen: list[int] = []

    sink, result_future = Sink.for_each_materialized(lambda x: seen.append(x))
    g = Source.from_iterable(range(4)).to(sink)

    run_result = g.run(actor_system)
    assert run_result.materialized is result_future

    result = result_future.result(timeout=1)
    assert result is None
    assert seen == [0, 1, 2, 3]


def test_collect_materialized_error(actor_system: ActorSystem):
    def boom(x: int) -> int:
        if x == 2:
            raise ValueError("boom")
        return x

    sink, result_future = Sink.collect()
    g = Source.from_iterable(range(4)).via(Flow.map(boom)).to(sink)
    g.run(actor_system)

    with pytest.raises(ValueError, match="boom"):
        result_future.result(timeout=1)
    wait_for_actor_count(actor_system, 2)


def test_for_each_materialized_error(actor_system: ActorSystem):
    def boom(x: int) -> None:
        if x == 1:
            raise RuntimeError("kaput")

    sink, result_future = Sink.for_each_materialized(boom)
    g = Source.from_iterable(range(3)).to(sink)
    g.run(actor_system)

    with pytest.raises(RuntimeError, match="kaput"):
        result_future.result(timeout=1)
    wait_for_actor_count(actor_system, 2)


def test_source_iterator_error_is_materialized(actor_system: ActorSystem):
    def broken_source():
        yield 1
        raise ValueError("source failed")

    sink, result_future = Sink.collect()
    Source.from_iterable(broken_source()).to(sink).run(actor_system)

    with pytest.raises(ValueError, match="source failed"):
        result_future.result(timeout=1)
    wait_for_actor_count(actor_system, 2)


def test_run_result_cancel_settles_future(actor_system: ActorSystem):
    sink, result_future = Sink.collect()
    run_result = Source.from_iterable(range(1_000_000)).to(sink).run(actor_system)

    run_result.cancel()

    assert result_future.cancelled()
    wait_for_actor_count(actor_system, 2)


def test_ready_waits_for_stage_wiring_acknowledgement(actor_system: ActorSystem):
    entered = threading.Event()
    release = threading.Event()

    class DelayedSource(AbstractBehavior):
        def receive(self, context: ActorContext, message):
            if isinstance(message, Subscribe):
                entered.set()
                release.wait(1.0)
                if message.wiring is not None:
                    message.wiring.acknowledge()
            elif isinstance(message, Cancel):
                return Behaviors.stopped
            return self

    source = Source(lambda: Behaviors.setup(DelayedSource))
    run_result = source.to(Sink.for_each(lambda value: None)).run(actor_system)
    try:
        assert entered.wait(1.0)
        assert not run_result.ready.done()
        release.set()
        assert run_result.ready.result(timeout=1.0) is None
    finally:
        release.set()
        run_result.cancel()
    wait_for_actor_count(actor_system, 2)


def test_cancelling_ready_terminates_all_graph_actors(actor_system: ActorSystem):
    entered = threading.Event()
    release = threading.Event()

    class DelayedSource(AbstractBehavior):
        def receive(self, context: ActorContext, message):
            if isinstance(message, Subscribe):
                entered.set()
                release.wait(1.0)
                if message.wiring is not None:
                    message.wiring.acknowledge()
            elif isinstance(message, Cancel):
                return Behaviors.stopped
            return self

    sink, materialized = Sink.collect()
    run_result = Source(lambda: Behaviors.setup(DelayedSource)).to(sink).run(actor_system)
    assert entered.wait(1.0)

    assert run_result.ready.cancel()
    assert materialized.cancelled()
    release.set()

    wait_for_actor_count(actor_system, 2)


def test_cancelling_materialized_future_terminates_graph(actor_system: ActorSystem):
    class PassiveSource(AbstractBehavior):
        def receive(self, context: ActorContext, message):
            if isinstance(message, Subscribe) and message.wiring is not None:
                message.wiring.acknowledge()
            elif isinstance(message, Cancel):
                return Behaviors.stopped
            return self

    sink, materialized = Sink.collect()
    run_result = Source(lambda: Behaviors.setup(PassiveSource)).to(sink).run(actor_system)
    assert run_result.ready.result(timeout=1.0) is None

    assert materialized.cancel()

    wait_for_actor_count(actor_system, 2)


def test_cancel_callbacks_run_after_stage_termination_is_requested(
    actor_system: ActorSystem,
):
    class PassiveSource(AbstractBehavior):
        def receive(self, context: ActorContext, message):
            if isinstance(message, Subscribe) and message.wiring is not None:
                message.wiring.acknowledge()
            return self

    sink, materialized = Sink.collect()
    run_result = Source(lambda: Behaviors.setup(PassiveSource)).to(sink).run(actor_system)
    assert run_result.ready.result(timeout=1.0) is None
    sink_stopped = actor_system.actor_stop_future(run_result.sink)
    callback_completed = threading.Event()

    def wait_for_sink(future) -> None:
        sink_stopped.result(timeout=1.0)
        callback_completed.set()

    materialized.add_done_callback(wait_for_sink)
    assert materialized.cancel()

    assert callback_completed.wait(1.0)
    wait_for_actor_count(actor_system, 2)


def test_unexpected_stage_termination_fails_and_cleans_graph(
    actor_system: ActorSystem,
):
    class PassiveSource(AbstractBehavior):
        def receive(self, context: ActorContext, message):
            if isinstance(message, Subscribe) and message.wiring is not None:
                message.wiring.acknowledge()
            return self

    sink, materialized = Sink.collect()
    run_result = Source(lambda: Behaviors.setup(PassiveSource)).to(sink).run(actor_system)
    assert run_result.ready.result(timeout=1.0) is None

    actor_system.terminate(run_result.stages[0])

    with pytest.raises(RuntimeError, match="terminated unexpectedly"):
        materialized.result(timeout=1.0)
    wait_for_actor_count(actor_system, 2)


def test_base_exception_in_stream_user_code_is_terminal(actor_system: ActorSystem):
    def interrupt(value: int) -> int:
        raise KeyboardInterrupt("stop map")

    sink, materialized = Sink.collect()
    run_result = Source.from_iterable([1]).via(Flow.map(interrupt)).to(sink).run(
        actor_system
    )
    assert run_result.ready.result(timeout=1.0) is None

    with pytest.raises(RuntimeError, match="transform raised BaseException") as raised:
        materialized.result(timeout=1.0)
    assert isinstance(raised.value.__cause__, KeyboardInterrupt)
    wait_for_actor_count(actor_system, 2)


def test_internal_stage_failure_is_terminal(actor_system: ActorSystem):
    class BrokenStage(StageBehavior):
        def receive(self, context: ActorContext, message):
            if isinstance(message, OnNext):
                raise RuntimeError("stage crashed")
            return super().receive(context, message)

    flow = Flow(lambda: Behaviors.setup(BrokenStage))
    sink, materialized = Sink.collect()
    run_result = Source.from_iterable([1]).via(flow).to(sink).run(actor_system)
    assert run_result.ready.result(timeout=1.0) is None

    with pytest.raises(RuntimeError, match="stage crashed"):
        materialized.result(timeout=1.0)
    wait_for_actor_count(actor_system, 2)


def test_actor_lifecycle_future_cannot_be_cancelled(actor_system: ActorSystem):
    actor_ref = actor_system.spawn(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "non-cancellable-lifecycle",
    )
    actor_system.wait_for_actor_start(actor_ref)
    stop_future = actor_system.actor_stop_future(actor_ref)

    assert not stop_future.cancel()
    assert not stop_future.cancelled()
    assert actor_system.get_context(actor_ref).state.name == "RUNNING"

    actor_system.terminate(actor_ref)
    assert stop_future.result(timeout=1.0) is None


def test_materialized_sink_cannot_be_reused(actor_system: ActorSystem):
    class PassiveSource(AbstractBehavior):
        def receive(self, context: ActorContext, message):
            if isinstance(message, Subscribe) and message.wiring is not None:
                message.wiring.acknowledge()
            return self

    sink, materialized = Sink.collect()
    first = Source(lambda: Behaviors.setup(PassiveSource)).to(sink).run(actor_system)
    assert first.ready.result(timeout=1.0) is None

    with pytest.raises(RuntimeError, match="materialized once"):
        Source.from_iterable([1]).to(sink).run(actor_system)

    first.cancel()
    assert materialized.cancelled()
    wait_for_actor_count(actor_system, 2)


def test_cancellation_during_spawn_precedes_callbacks_and_cleans_published_stages(
    actor_system: ActorSystem, monkeypatch
):
    sink, materialized = Sink.collect()
    graph = Source.from_iterable(range(10)).to(sink)
    original_spawn = actor_system.spawn
    entered = threading.Event()
    release = threading.Event()
    callback_ran = threading.Event()
    run_errors: list[BaseException] = []

    def blocking_spawn(behavior, name: str, *, parent=None):
        if name.startswith("Source-"):
            entered.set()
            release.wait(2.0)
        return original_spawn(behavior, name, parent=parent)

    monkeypatch.setattr(actor_system, "spawn", blocking_spawn)

    def run_graph() -> None:
        try:
            graph.run(actor_system)
        except BaseException as error:
            run_errors.append(error)

    runner = threading.Thread(target=run_graph)
    runner.start()
    assert entered.wait(1.0)
    materialized.add_done_callback(lambda future: callback_ran.set())
    canceller = threading.Thread(target=materialized.cancel)
    canceller.start()
    time.sleep(0.02)
    assert not callback_ran.is_set()

    release.set()
    runner.join(2.0)
    canceller.join(2.0)

    assert len(run_errors) == 1
    assert isinstance(run_errors[0], RuntimeError)
    assert callback_ran.wait(1.0)
    wait_for_actor_count(actor_system, 2)


def test_stage_factory_can_cancel_materialization_without_deadlock(
    actor_system: ActorSystem,
):
    sink, materialized = Sink.collect()

    def cancel_factory():
        materialized.cancel()
        return Behaviors.receive(lambda context, message: Behaviors.same)

    graph = Source(cancel_factory).to(sink)

    with pytest.raises(RuntimeError, match="cancelled"):
        graph.run(actor_system)
    assert materialized.cancelled()
    assert actor_system.actor_count == 2


def test_materialized_callback_can_stop_system_without_worker_deadlock():
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "stream-callback-stop-system",
    )
    sink, materialized = Sink.collect()
    stopped = threading.Event()

    def stop_system(future) -> None:
        system.stop(timeout=2.0)
        stopped.set()

    materialized.add_done_callback(stop_system)
    Source.from_iterable(range(10)).to(sink).run(system)

    assert stopped.wait(2.0)
    assert system._terminated.wait(2.0)
    assert system.actor_count == 0


def test_graph_materialization_rolls_back_spawned_stages(actor_system: ActorSystem):
    sink, result_future = Sink.collect(name="duplicate-stage")
    graph = Source.from_iterable(range(10), name="duplicate-stage").to(sink)

    with pytest.raises(ValueError, match="already in use"):
        graph.run(actor_system)
    with pytest.raises(ValueError, match="already in use"):
        result_future.result(timeout=1)
    wait_for_actor_count(actor_system, 2)


def test_stream_rejects_incompatible_mailbox_capacity():
    config = Config(
        {
            "movie": {
                "mailbox": {
                    "default": {
                        "type": "movie.mailbox.default.DefaultMailbox",
                        "capacity": 31,
                        "throughput": 1,
                    }
                }
            }
        }
    )
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "small-mailbox-system",
        config=config,
    )
    sink, result_future = Sink.collect()
    try:
        with pytest.raises(ValueError, match="capacity >= 32"):
            Source.from_iterable(range(10)).to(sink).run(system)
        with pytest.raises(ValueError, match="capacity >= 32"):
            result_future.result(timeout=1)
        assert system.actor_count == 2
    finally:
        system.stop()


def test_source_setup_failure_rolls_back_graph(actor_system: ActorSystem):
    class BrokenIterable:
        def __iter__(self):
            raise ValueError("iterator setup failed")

    sink, result_future = Sink.collect()
    run_result = Source.from_iterable(BrokenIterable()).to(sink).run(actor_system)

    with pytest.raises(ValueError, match="iterator setup failed"):
        run_result.ready.result(timeout=1)
    with pytest.raises(ValueError, match="iterator setup failed"):
        result_future.result(timeout=1)
    wait_for_actor_count(actor_system, 2)
