import pytest, time, threading

from movie.actor import AbstractBehavior, ActorContext, ActorSystem, Behaviors
from movie.streams import Flow, Sink, Source


def wait_until(pred, timeout=3.0, interval=0.01, err="timeout"):
    """Крутить до timeout, поки pred() не поверне True."""
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < timeout:
        if pred():
            return
        time.sleep(interval)
    raise AssertionError(err)


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


def test_for_each_materialized_error(actor_system: ActorSystem):
    def boom(x: int) -> None:
        if x == 1:
            raise RuntimeError("kaput")

    sink, result_future = Sink.for_each_materialized(boom)
    g = Source.from_iterable(range(3)).to(sink)
    g.run(actor_system)

    with pytest.raises(RuntimeError, match="kaput"):
        result_future.result(timeout=1)
