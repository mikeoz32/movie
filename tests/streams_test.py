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
