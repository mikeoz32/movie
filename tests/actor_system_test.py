from threading import Event, RLock
import time
from movie.actor import (
    ActorSystem,
    AbstractBehavior,
    Behaviors,
    ActorContext,
    SupervisorDirective,
)
from movie.system_message import SystemMessage


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
    messages_processed = Event()
    children_started = Event()
    counter_lock = RLock()

    class Child(AbstractBehavior[str]):
        receive_count = 0
        started_count = 0

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
                if Child.receive_count >= 200:
                    messages_processed.set()

        def on_signal(self, context: ActorContext, message: SystemMessage) -> None:
            if isinstance(message, ActorSystem.PreStart):
                with counter_lock:
                    Child.started_count += 1
                    if Child.started_count >= 100:
                        children_started.set()

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
    system = ActorSystem.create(TestBehavior.create(), "test-load-system")
    end = time.perf_counter()
    print(f"Actors initialization: {end - start:.6f} секунд")
    assert system is not None
    assert children_started.wait(2.0)
    start = time.perf_counter()
    system.tell("Hello, Actor!")
    system.tell("Hello, Actor!")
    assert messages_processed.wait(10.0)
    system.stop()
    end = time.perf_counter()
    print(f"Messages processed in : {end - start:.6f} секунд")
    assert Child.receive_count == 200


def test_actor_failed():
    child_restarted = Event()
    child_stopped = Event()

    class Child(AbstractBehavior[str]):
        starts = 0

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

        def on_signal(self, context: ActorContext, message: SystemMessage) -> None:
            if isinstance(message, ActorSystem.PreStart):
                Child.starts += 1
                if Child.starts >= 2:
                    child_restarted.set()
            if isinstance(message, ActorSystem.PostStop):
                child_stopped.set()

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
    assert child_restarted.wait(1.0)
    system.stop()
    assert child_stopped.wait(1.0)


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


def test_supervision_restart_waits_for_children_stop():
    child_restart_started = Event()
    child_post_stop_count_lock = RLock()

    class GrandChild(AbstractBehavior[None]):
        post_stop_count = 0

        def receive(
            self, context: ActorContext[None], message: None
        ) -> "AbstractBehavior | None":
            return None

        def on_signal(self, context: ActorContext, message: SystemMessage) -> None:
            if isinstance(message, ActorSystem.PostStop):
                with child_post_stop_count_lock:
                    GrandChild.post_stop_count += 1

    class Child(AbstractBehavior[str]):
        starts = 0

        def __init__(self, context: ActorContext[str]) -> None:
            super().__init__(context)
            self.context.spawn(Behaviors.setup(GrandChild), "grand-child")

        def receive(
            self, context: ActorContext[str], message: str
        ) -> "AbstractBehavior | None":
            if message == "fail":
                raise RuntimeError("boom")
            return None

        def on_signal(self, context: ActorContext, message: SystemMessage) -> None:
            if isinstance(message, ActorSystem.PreStart):
                Child.starts += 1
                if Child.starts >= 2:
                    child_restart_started.set()

    class Parent(AbstractBehavior[str]):
        def __init__(self, context: ActorContext[str]) -> None:
            super().__init__(context)
            self.child = self.context.spawn(Behaviors.setup(Child), "child")

        def receive(
            self, context: ActorContext[str], message: str
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

    system = ActorSystem.create(Behaviors.setup(Parent), "restart-children-system")
    system.tell("fail")
    assert child_restart_started.wait(1.0)
    with child_post_stop_count_lock:
        assert GrandChild.post_stop_count >= 1
    system.stop()
