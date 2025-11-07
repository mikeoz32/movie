from threading import RLock
import time
from movie.actor import ActorSystem, AbstractBehavior, Behaviors, ActorContext
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
            Child.receive_count += 1

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
    time.sleep(0.01)
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
    # time.sleep(0.1) # TODO: fix error handling in actor system, this line make test stuck
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
