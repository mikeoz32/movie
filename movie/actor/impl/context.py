from __future__ import annotations

from enum import Enum, auto
from threading import RLock
from typing import TYPE_CHECKING, Any, Dict, Generic, cast
import uuid

from movie.actor.behaviour import (
    AbstractBehavior,
    Behaviors,
    DefferedBehavior,
    SameBehavior,
)
from movie.actor.context import ActorContext, InternalActorContext
from movie.actor.impl.ref import LocalActorRef
from movie.actor.logger import ActorLogger
from movie.actor.message import MessageType
from movie.actor.ref import ActorRef
from movie.actor.supervision import SupervisorDirective
from movie.actor.system import ActorSystem
from movie.scheduler import Mailbox

if TYPE_CHECKING:
    from movie.actor.impl.system import ActorSystemImpl


class ChildrenMixin:
    def __init__(self) -> None:
        self._children: Dict[uuid.UUID, LocalActorRef] = {}
        self._parent: LocalActorRef | None = None

    def get_children(self) -> Dict[uuid.UUID, LocalActorRef]:
        return self._children

    def childern_count(self) -> int:
        return len(self._children.keys())

    def attach_child(self, child: InternalActorContext) -> None:
        child.set_parent(self._ref)
        self._children[child.get_self().id] = cast(LocalActorRef, child.get_self())

    def set_parent(self, parent: ActorRef) -> None:
        self._parent = cast(LocalActorRef, parent)

    def remove_child(self, child_ref: ActorRef) -> None:
        self._children.pop(child_ref.id, None)


class LocalActorContext(ChildrenMixin, InternalActorContext[MessageType]):
    def __init__(
        self,
        behavior: AbstractBehavior[MessageType],
        ref: LocalActorRef[MessageType],
        system: ActorSystemImpl,
        parent_context: "LocalActorContext | None" = None,
    ) -> None:
        ChildrenMixin.__init__(self)

        self.l = RLock()
        self._behavior = behavior
        self._initial_behavior = behavior
        self._system = system
        self._ref = ref
        self._mailbox: Mailbox | None = None
        self._log = system.actor_logger(self)
        if parent_context is not None:
            parent_context.attach_child(self)
        self._stash: list[MessageType] = []
        self._state = ActorState.NEW
        self._restart_pending = False

    @property
    def state(self) -> "ActorState":
        return self._state

    @state.setter
    def state(self, new_state: "ActorState") -> None:
        self._state = new_state

    def stash(self, message: MessageType) -> None:
        self._stash.append(message)

    def unstash_all(self) -> None:
        for message in self._stash:
            self.tell(message)
        self._stash.clear()

    def materialize_behaviour(self) -> None:
        while isinstance(self._behavior, DefferedBehavior):
            self._behavior = self._behavior(self)

    def start(self) -> None:
        if self._state in (ActorState.STARTING, ActorState.RUNNING, ActorState.STOPPED):
            return
        self.state = ActorState.STARTING
        mailbox = self._system.mailboxes.create_mailbox(
            self._system._dispatchers.default_dispatcher, self
        )
        self.attach_mailbox(mailbox)
        self.tell_system(ActorSystem.PreStart())

    def stop(self) -> None:
        self.send_system(ActorSystem.Stop())

    @property
    def is_stopped(self) -> bool:
        return self._state is ActorState.STOPPED

    def _complete_stop(self) -> None:
        if self._state is ActorState.STOPPED:
            return
        self._restart_pending = False
        self.state = ActorState.STOPPED
        self.on_signal(ActorSystem.PostStop())
        if self._parent is not None:
            self._parent.tell_system(ActorSystem.Terminated(self._ref))

    def _stop_children(self) -> None:
        for child in list(self._children.values()):
            child.tell_system(ActorSystem.Stop())

    def _complete_restart(self) -> None:
        self._restart_pending = False
        self._behavior = self._initial_behavior
        self.state = ActorState.STARTING
        self.materialize_behaviour()
        self.state = ActorState.RUNNING
        self.on_signal(ActorSystem.PreStart())
        self.unstash_all()

    def _restart(self) -> None:
        if self._state is ActorState.STOPPED:
            return
        self._restart_pending = True
        if self._state is not ActorState.STOPPING:
            self.state = ActorState.STOPPING
            self._stop_children()
        if self.childern_count() == 0:
            self._complete_restart()

    def _handle_supervision(
        self,
        actor_ref: ActorRef,
        exception: Exception,
        directive: SupervisorDirective,
    ) -> None:
        match directive:
            case SupervisorDirective.RESTART:
                if isinstance(actor_ref, LocalActorRef):
                    actor_ref.tell_system(ActorSystem.Restart())
            case SupervisorDirective.STOP:
                if isinstance(actor_ref, LocalActorRef):
                    actor_ref.tell_system(ActorSystem.Stop())
            case SupervisorDirective.ESCALATE:
                if self._parent is not None:
                    self._parent.tell_system(ActorSystem.Failed(actor_ref, exception))

    def tell(self, message: MessageType) -> None:
        """
        Send a message to this actor's mailbox.
        """
        if self._mailbox is not None:
            self._mailbox.send(message)

    def tell_system(self, message: ActorSystem.SystemMessage) -> None:
        """
        Send a system message to this actor's mailbox.
        """
        if self._mailbox is not None:
            self._mailbox.sendSystem(message)

    def send(self, message: MessageType) -> None:
        if self._state is ActorState.STOPPED:
            return
        if self._state in (ActorState.NEW, ActorState.STARTING):
            self.stash(message)
            return
        if self._state is ActorState.STOPPING:
            return
        if self._state is ActorState.FAILED:
            self.stash(message)
            return
        self.tell(message)

    def send_system(self, message: ActorSystem.SystemMessage) -> None:
        if self._state is ActorState.STOPPED:
            return
        self.tell_system(message)

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

    def on_signal(self, message: ActorSystem.SystemMessage) -> None:
        """
        Actual system message handling.
        """
        with self.l:
            match message:
                case ActorSystem.Failed(actor_ref, exception):
                    directive = self._behavior.supervise(self, actor_ref, exception)
                    self._handle_supervision(actor_ref, exception, directive)
                    return
            self._behavior.on_signal(self, message)

    def on_message(self, message: MessageType) -> None:
        """
        Invoke the actor's behavior with the message received from mailbox.
        """
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
                    self.state = ActorState.FAILED
                    self._behavior = Behaviors.failed
                else:
                    self.send_system(ActorSystem.Stop())

    def invoke(self, message: MessageType) -> None:
        """
        Invoke the actor's behavior with the message received from mailbox.
        """
        if self._state is ActorState.STOPPED:
            return
        if self._state in (ActorState.NEW, ActorState.STARTING):
            self.stash(message)
            return
        if self._state is ActorState.STOPPING:
            return
        if self._state is ActorState.FAILED:
            self.stash(message)
            return
        self.on_message(message)

    def invoke_system(self, message: ActorSystem.SystemMessage) -> None:
        """
        Invoke the actor's behavior with the system message received from mailbox.
        """
        if self._state is ActorState.STOPPED:
            return
        match message:
            case ActorSystem.PreStart():
                if self._state is ActorState.STARTING:
                    self.materialize_behaviour()
                    self.state = ActorState.RUNNING
                    self.on_signal(message)
                    self.unstash_all()
            case ActorSystem.Stop() | ActorSystem.Terminate():
                self._restart_pending = False
                if self._state is not ActorState.STOPPING:
                    self.state = ActorState.STOPPING
                    self._stop_children()
                if self.childern_count() == 0:
                    self._complete_stop()
            case ActorSystem.Terminated(actor_ref):
                self.remove_child(actor_ref)
                if self._state is ActorState.STOPPING and self.childern_count() == 0:
                    if self._restart_pending:
                        self._complete_restart()
                    else:
                        self._complete_stop()
            case ActorSystem.Restart():
                self._restart()
            case ActorSystem.Failed():
                self.on_signal(message)
            case _:
                self.on_signal(message)

    def spawn(self, behavior: AbstractBehavior, name: str) -> ActorRef:
        """
        Spawn a new child actor with the given behavior and name.
        """
        return self.get_system().spawn(behavior, name, parent=self)


class ActorState(Enum):
    NEW = auto()
    STARTING = auto()
    RUNNING = auto()
    STOPPING = auto()
    STOPPED = auto()
    FAILED = auto()
