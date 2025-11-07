import enum
from logging import info, log
import re
from threading import RLock
from typing import TYPE_CHECKING, Any, Dict, Generic, Protocol, cast
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
from movie.actor.system import ActorSystem
from movie.scheduler import Mailbox


if TYPE_CHECKING:
    from movie.actor.impl.system import ActorSystemImpl


class ChildrenMixin:
    def __init__(self) -> None:
        self._children: Dict[uuid.UUID, LocalActorRef] = {}
        self._parent: LocalActorRef | None

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


class ActorState(Generic[MessageType], Protocol):
    """
    NEW → STARTING → RUNNING
    RUNNING --Stop--> STOPPING → STOPPED → DEAD
    RUNNING --Failure--> FAILED → (Supervisor decision)
        ↳ Restart → RESTARTING → STARTING → RUNNING
        ↳ Stop    → STOPPING → STOPPED
    """

    def enter(self, context: "LocalActorContext") -> None: ...
    def start(self, context: "LocalActorContext") -> "ActorState": ...
    def stop(self, context: "LocalActorContext") -> "ActorState": ...
    def send(self, context: "LocalActorContext", message: MessageType) -> None: ...
    def send_system(
        self, context: "LocalActorContext", message: ActorSystem.SystemMessage
    ) -> None: ...
    def invoke(self, context: "LocalActorContext", message: MessageType) -> None: ...
    def invoke_system(
        self, context: "LocalActorContext", message: ActorSystem.SystemMessage
    ) -> "ActorState": ...


class NewState(ActorState[MessageType]):
    """
    Initial state of an actor. Not started yet.

    When:
    - context just created, behavior is not initialized/started.
    What Is Allowed:
    - Did not accepts messages yet.

    Transitions To:
    - StartingState
    """

    def start(self, context: "LocalActorContext") -> "ActorState":
        return State.STARTING.value

    def stop(self, context: "LocalActorContext") -> "ActorState":
        return State.STOPPED.value

    def send(self, context: "LocalActorContext", message: MessageType) -> None:
        "TODO: stash messages"
        context.stash(message)

    def send_system(
        self, context: "LocalActorContext", message: ActorSystem.SystemMessage
    ) -> None:
        pass

    def invoke(self, context: "LocalActorContext", message: MessageType) -> None:
        # Stash messages until actor starts
        context.stash(message)

    def invoke_system(
        self, context: "LocalActorContext", message: ActorSystem.SystemMessage
    ) -> "ActorState":
        # No system message processing in NEW state
        return self


class StartingState(ActorState):
    """
    Actor is in the process of starting.
    When:
    - after start is called, before PreStart is processed.
    What Is Allowed:
    - Resource allocaltion.
    - Behavior initialization.
    - Incoming messages are stashed.

    Transitions To:
    - RunningState
    """

    def enter(self, context: "LocalActorContext") -> None:
        mailbox = context._system.mailboxes.create_mailbox(
            context._system._dispatchers.default_dispatcher, context
        )
        context.attach_mailbox(mailbox)
        context.tell_system(ActorSystem.PreStart())

    def start(self, context: "LocalActorContext") -> "ActorState":
        """
        TODO: send event to event stream that actor already started
        """
        return self

    def stop(self, context: "LocalActorContext") -> "ActorState":
        return self

    def send(self, context: "LocalActorContext", message: MessageType) -> None:
        "TODO: still stash messages"
        context.stash(message)

    def send_system(
        self, context: "LocalActorContext", message: ActorSystem.SystemMessage
    ) -> None:
        """
        Wait for started system message?
        """
        context.tell_system(message)

    def invoke(self, context: "LocalActorContext", message: MessageType) -> None:
        """
        Stash messages until started.
        """
        context.on_message(message)

    def invoke_system(
        self, context: "LocalActorContext", message: ActorSystem.SystemMessage
    ) -> "ActorState":
        match message:
            case ActorSystem.PreStart():
                context.on_signal(message)
                # context.log.info("Actor starting...")
                return State.RUNNING.value
            case _:
                return self


class RunningState(ActorState):
    """
    When:
    - Actor is running and processing messages.
    What is Allowed:
    - Normal message processing.
    - spawning children
    Transitions To:
    - StoppingState
    - FailedState
    """

    def send(self, context: "LocalActorContext", message: MessageType) -> None:
        "TODO: still stash messages"
        context.tell(message)

    def send_system(
        self, context: "LocalActorContext", message: ActorSystem.SystemMessage
    ) -> None:
        """
        Wait for started system message?
        """
        context.tell_system(message)

    def enter(self, context: "LocalActorContext") -> None:
        """
        Unstash messages?
        """
        context.materialize_behaviour()
        # context.log.info("Actor running.")
        context.unstash_all()

    def invoke(self, context: "LocalActorContext", message: MessageType) -> None:
        """
        Regular message handling.
        """
        try:
            context.on_message(message)
            return self
        except Exception as e:
            if context._parent is not None:
                context._parent.tell_system(ActorSystem.Failed(context._ref, e))
                return State.FAILED.value

    def invoke_system(
        self, context: "LocalActorContext", message: ActorSystem.SystemMessage
    ) -> "ActorState":
        """
        Handle system messages in running state.
        """
        match message:
            case ActorSystem.PreStart():
                # Already started, ignore
                return self
            case ActorSystem.Stop():
                # Initiate graceful shutdown
                return State.STOPPING.value
        context.on_signal(message)
        return self


class StoppingState(ActorState):
    """
    When:
    - Actor is in the process of stopping.
    What is Allowed:
    - Releasing resources.
    - Notifying children to stop.
    - Messages are ignored/sent to dead letters.
    Transitions To:
    - StoppedState
    """

    def enter(self, context: "LocalActorContext") -> None:
        # Send PostStop to behavior first
        context.on_signal(ActorSystem.PostStop())

        # Stop all children
        for child_ref in context.get_children().values():
            child_ref.tell_system(ActorSystem.Stop())

        # If no children, transition immediately to StoppedState
        if context.childern_count() == 0:
            context.send_system(ActorSystem.Terminate())

    def start(self, context: "LocalActorContext") -> "ActorState":
        return self

    def stop(self, context: "LocalActorContext") -> "ActorState":
        return State.STOPPED.value

    def send(self, context: "LocalActorContext", message: MessageType) -> None:
        # Ignore messages during stopping
        pass

    def send_system(
        self, context: "LocalActorContext", message: ActorSystem.SystemMessage
    ) -> None:
        context.tell_system(message)

    def invoke(self, context: "LocalActorContext", message: MessageType) -> None:
        # Ignore messages during stopping
        context.log.warning(f"Ignoring message during stopping: {message}")

    def invoke_system(
        self, context: "LocalActorContext", message: ActorSystem.SystemMessage
    ) -> "ActorState":
        match message:
            case ActorSystem.Terminated(actor_ref):
                # Child terminated, check if all children stopped
                context.remove_child(actor_ref)
                if context.childern_count() == 0:
                    # All children stopped, can transition to stopped
                    return State.STOPPED.value
                return self
            case ActorSystem.Terminate():
                return State.STOPPED.value


class StoppedState(ActorState):
    """
    When:
    - Actor has been stopped.
    What is Allowed:
    - No operations allowed.
    Transitions To:
    - (none)
    """

    def enter(self, context: "LocalActorContext") -> None:
        # Stop mailbox
        # TODO: fix mailbox stopping, now blocks but should process remaining messages and exit
        # if context._mailbox is not None:
        #     context._mailbox.stop()

        # Notify parent
        if context._parent is not None:
            context._parent.tell_system(ActorSystem.Terminated(context._ref))

    def start(self, context: "LocalActorContext") -> "ActorState":
        return self

    def stop(self, context: "LocalActorContext") -> "ActorState":
        return self

    def send(self, context: "LocalActorContext", message: MessageType) -> None:
        pass

    def send_system(
        self, context: "LocalActorContext", message: ActorSystem.SystemMessage
    ) -> None:
        pass

    def invoke(self, context: "LocalActorContext", message: MessageType) -> None:
        pass

    def invoke_system(
        self, context: "LocalActorContext", message: ActorSystem.SystemMessage
    ) -> "ActorState":
        return self


class FailedState(ActorState):
    """
    Stage for actor to wait supervisor decision.

    When:
    - Actor has failed due to an unhandled exception.
    What is Allowed:
    - Notify parent about failure.
    Transitions To:
    - StoppingState
    - RestartingState
    """

    def enter(self, context: "LocalActorContext") -> None:
        context.log.error("Actor failed.")

    def start(self, context: "LocalActorContext") -> "ActorState":
        return self

    def stop(self, context: "LocalActorContext") -> "ActorState":
        return State.STOPPING.value

    def send(self, context: "LocalActorContext", message: MessageType) -> None:
        pass

    def send_system(
        self, context: "LocalActorContext", message: ActorSystem.SystemMessage
    ) -> None:
        context.tell_system(message)

    def invoke(self, context: "LocalActorContext", message: MessageType) -> None:
        pass

    def invoke_system(
        self, context: "LocalActorContext", message: ActorSystem.SystemMessage
    ) -> "ActorState":
        match message:
            case ActorSystem.PreStart():
                # Already started, ignore
                return self
            case ActorSystem.Stop():
                # Initiate graceful shutdown
                return State.STOPPING.value
            case _:
                context.tell_system(message)
                return self


class State(enum.Enum):
    NEW = NewState()
    STARTING = StartingState()
    RUNNING = RunningState()
    STOPPING = StoppingState()
    STOPPED = StoppedState()
    FAILED = FailedState()


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
        self._system = system
        self._ref = ref
        self._mailbox: Mailbox | None = None
        self._log = system.actor_logger(self)
        self._state: ActorState = State.NEW.value
        if parent_context is not None:
            parent_context.attach_child(self)
        self._stash: list[MessageType] = []

    def stash(self, message: MessageType) -> None:
        # self.log.info(f"Stashing message: {message}")
        self._stash.append(message)

    def unstash_all(self) -> None:
        # with self.l:
        # self.log.info(f"Unstashing {len(self._stash)} messages")
        for message in self._stash:
            self.tell(message)
        self._stash.clear()

    def materialize_behaviour(self) -> None:
        while isinstance(self._behavior, DefferedBehavior):
            self._behavior = self._behavior(self)

    def start(self) -> None:
        self.state = self.state.start(self)
        # self._log.info("started")

    def stop(self) -> None:
        # Initiate graceful stop via state machine
        self.send_system(ActorSystem.Stop())

    @property
    def state(self) -> ActorState:
        return self._state

    @state.setter
    def state(self, new_state: ActorState) -> None:
        if self._state is not new_state:
            self._state = new_state
            self._state.enter(self)

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
        self.state.send(self, message)

    def send_system(self, message: ActorSystem.SystemMessage) -> None:
        self.state.send_system(self, message)

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
                    print(
                        f"Actor {self._ref.path} received failure from {actor_ref.path}: {exception}"
                    )
            self._behavior.on_signal(self, message)

    def on_message(self, message: MessageType) -> None:
        """
        Invoke the actor's behavior with the message received from mailbox.
        TODO: Delegate to state machine.
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
                    print(f"parent {self._parent.path}")
                    self._parent.tell_system(ActorSystem.Failed(self._ref, e))
                    self._behavior = Behaviors.failed

    def invoke(self, message: MessageType) -> None:
        """
        Invoke the actor's behavior with the message received from mailbox.
        Delegate to state machine.
        """
        self.state.invoke(self, message)

        # self.on_message(message)

    def invoke_system(self, message: ActorSystem.SystemMessage) -> None:
        """
        Invoke the actor's behavior with the system message received from mailbox.
        Delegate to state machine.
        """
        self.state = self.state.invoke_system(self, message)
        # self.on_signal(message)

    def spawn(self, behavior: AbstractBehavior, name: str) -> ActorRef:
        """
        Spawn a new child actor with the given behavior and name.
        """
        return self.get_system().spawn(behavior, name, parent=self)
