from __future__ import annotations

import enum
from typing import TYPE_CHECKING, Callable, Generic

from movie.actor.context import ActorContext
from movie.actor.message import MessageType
from movie.actor.supervision import SupervisorDirective
if TYPE_CHECKING:
    from movie.actor.system import ActorSystem


class BehaviorTag(enum.Enum):
    DEFERRED = enum.auto()
    SAME = enum.auto()
    STOPPED = enum.auto()
    FAILED = enum.auto()


class AbstractBehavior(Generic[MessageType]):
    """
    Actor behavior, FSM style function with state. Function processes messages and changes state.
    """

    def __init__(
        self, context: ActorContext[MessageType], tag: BehaviorTag = BehaviorTag.SAME
    ) -> None:
        self._context = context
        self._tag = tag

    @property
    def context(self) -> ActorContext[MessageType]:
        return self._context

    @property
    def alive(self) -> bool:
        return self._tag != BehaviorTag.STOPPED and self._tag != BehaviorTag.FAILED

    @property
    def same(self) -> bool:
        return self._tag == BehaviorTag.SAME

    def receive(
        self, context: ActorContext, message: MessageType
    ) -> "AbstractBehavior | None": ...

    def on_signal(
        self, context: ActorContext, message: ActorSystem.SystemMessage
    ) -> None: ...

    def supervise(
        self,
        context: ActorContext,
        child: "ActorRef",
        exception: Exception,
    ) -> SupervisorDirective:
        return SupervisorDirective.RESTART


class DefferedBehavior(AbstractBehavior):
    def __init__(self, factory: Callable[[ActorContext], AbstractBehavior]) -> None:
        self._factory = factory
        self._tag = BehaviorTag.DEFERRED

    def __call__(self, context: ActorContext) -> AbstractBehavior:
        return self._factory(context)


class StoppedBehavior(AbstractBehavior):
    def __init__(self) -> None:
        self._tag = BehaviorTag.STOPPED


class FailedBehavior(AbstractBehavior):
    def __init__(self) -> None:
        self._tag = BehaviorTag.FAILED


class SameBehavior(AbstractBehavior):
    def __init__(self) -> None:
        self._tag = BehaviorTag.SAME


class Behaviors:
    stopped = StoppedBehavior()
    failed = FailedBehavior()
    same = SameBehavior()

    @staticmethod
    def setup(
        factory: Callable[[ActorContext], AbstractBehavior],
    ) -> AbstractBehavior:
        return DefferedBehavior(factory)

    @staticmethod
    def receive(
        receive_fn: Callable[[ActorContext, MessageType], AbstractBehavior | None],
    ) -> AbstractBehavior:
        class _ReceiveBehavior(AbstractBehavior):
            def receive(
                self,
                context: ActorContext,
                message: MessageType,
            ) -> AbstractBehavior | None:
                context.log.debug(f"Received message: {message}")
                return receive_fn(context, message)

        def setup(ctx: ActorContext) -> AbstractBehavior:
            return _ReceiveBehavior(ctx)

        return DefferedBehavior(setup)
