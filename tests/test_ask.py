from dataclasses import dataclass

import pytest

from movie.actor import AbstractBehavior, ActorContext, ActorRef, ActorSystem, Behaviors, ask


class EmptyRoot(AbstractBehavior[None]):
    def receive(self, context: ActorContext[None], message: None):
        return None


@dataclass(frozen=True)
class Ping:
    payload: str
    reply_to: ActorRef[str]


class EchoBehavior(AbstractBehavior[Ping]):
    def receive(self, context: ActorContext[Ping], message: Ping):
        message.reply_to.tell(f"echo:{message.payload}")
        return None


def test_ask_success_reply():
    system = ActorSystem.create(Behaviors.setup(EmptyRoot), "ask-success")
    try:
        target = system.spawn(Behaviors.setup(EchoBehavior), "echo")

        fut = ask(system, target, lambda reply_to: Ping("hello", reply_to), timeout=1.0)

        assert fut.result(timeout=1.0) == "echo:hello"
    finally:
        system.stop()


def test_ask_timeout():
    class SilentBehavior(AbstractBehavior[Ping]):
        def receive(self, context: ActorContext[Ping], message: Ping):
            return None

    system = ActorSystem.create(Behaviors.setup(EmptyRoot), "ask-timeout")
    try:
        target = system.spawn(Behaviors.setup(SilentBehavior), "silent")

        fut = ask(system, target, lambda reply_to: Ping("hello", reply_to), timeout=0.05)

        with pytest.raises(TimeoutError):
            fut.result(timeout=1.0)
    finally:
        system.stop()


def test_ask_factory_exception_propagates():
    system = ActorSystem.create(Behaviors.setup(EmptyRoot), "ask-factory-error")
    try:
        target = system.spawn(Behaviors.setup(EchoBehavior), "echo")

        fut = ask(system, target, lambda _reply_to: (_ for _ in ()).throw(ValueError("boom")))

        with pytest.raises(ValueError, match="boom"):
            fut.result(timeout=1.0)
    finally:
        system.stop()
