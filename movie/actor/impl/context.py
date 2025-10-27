from threading import RLock
from typing import TYPE_CHECKING, Dict, cast
import uuid
from movie.actor.behaviour import (
    AbstractBehavior,
    Behaviors,
    DefferedBehavior,
    SameBehavior,
)
from movie.actor.context import InternalActorContext
from movie.actor.impl.ref import LocalActorRef
from movie.actor.logger import ActorLogger
from movie.actor.message import MessageType
from movie.actor.ref import ActorRef
from movie.actor.system import ActorSystem
from movie.scheduler import Mailbox


if TYPE_CHECKING:
    from movie.actor.impl.system import ActorSystemImpl

class LocalActorContext(InternalActorContext[MessageType]):
    l = RLock()

    def __init__(
        self,
        behavior: AbstractBehavior[MessageType],
        ref: LocalActorRef[MessageType],
        system: ActorSystemImpl,
    ) -> None:
        self._behavior = behavior
        self._system = system
        self._ref = ref
        self._parent: LocalActorRef | None
        self._mailbox: Mailbox | None = None
        self._children: Dict[uuid.UUID, LocalActorRef] = {}
        self._log = system.actor_logger(self)

    def start(self) -> None:
        while isinstance(self._behavior, DefferedBehavior):
            self._behavior = self._behavior(self)
        self._log.debug("started")

    def stop(self) -> None:
        if self._mailbox is not None:
            self._mailbox.stop()

        self._log.debug("stopped")

    def attach_mailbox(self, mailbox: Mailbox) -> None:
        self._mailbox = mailbox

    def get_self(self) -> ActorRef[MessageType]:
        return self._ref

    def get_system(self) -> ActorSystem:
        return self._system

    @property
    def log(self) -> ActorLogger:
        return self._log

    @property
    def ref(self) -> ActorRef[MessageType]:
        return self._ref

    @property
    def system(self) -> ActorSystem:
        return self._system

    def attach_child(self, child: InternalActorContext) -> None:
        child.set_parent(self._ref)

    def set_parent(self, parent: ActorRef) -> None:
        self._parent = cast(LocalActorRef, parent)

    def invoke(self, message: MessageType) -> None:
        with self.l:
            try:
                new_behavior = self._behavior.receive(self, message) or Behaviors.same
                match new_behavior:
                    case DefferedBehavior() as deferred:
                        self._behavior = deferred(self)
                    case SameBehavior():
                        pass
                    case _:
                        self._behavior = new_behavior
            except Exception as e:
                if self._parent is not None:
                    self._parent.tell_system(ActorSystem.Failed(self._ref, e))
                    self._behavior = Behaviors.failed

    def invoke_system(self, message: ActorSystem.SystemMessage) -> None:
        with self.l:
            self._behavior.on_signal(self, message)

    def spawn(self, behavior: AbstractBehavior, name: str) -> ActorRef:
        return self.get_system().spawn(behavior, name, parent=self)
