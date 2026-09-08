from __future__ import annotations

import uuid
from concurrent.futures import Future, InvalidStateError
from enum import Enum, auto
from threading import Event, Lock, RLock
from typing import TYPE_CHECKING, Dict, cast

from movie.actor.behaviour import (
    AbstractBehavior,
    Behaviors,
    DefferedBehavior,
    SameBehavior,
    StoppedBehavior,
)
from movie.actor.context import ActorBatchFailed, InternalActorContext
from movie.actor.impl.ref import LocalActorRef
from movie.actor.logger import ActorLogger
from movie.actor.message import MessageType
from movie.actor.ref import ActorRef
from movie.actor.supervision import SupervisorDirective
from movie.actor.system import ActorSystem
from movie.mailbox.mailbox import Mailbox

if TYPE_CHECKING:
    from movie.actor.impl.system import ActorSystemImpl


class ChildrenMixin:
    def __init__(self) -> None:
        self._children: Dict[uuid.UUID, LocalActorRef] = {}
        self._parent: LocalActorRef | None = None
        self._children_lock = RLock()

    def get_children(self) -> Dict[uuid.UUID, LocalActorRef]:
        with self._children_lock:
            return dict(self._children)

    def children_count(self) -> int:
        with self._children_lock:
            return len(self._children)

    def attach_child(self, child: InternalActorContext) -> None:
        child_ref = cast(LocalActorRef, child.get_self())
        with self._children_lock:
            if any(existing.name == child_ref.name for existing in self._children.values()):
                raise ValueError(f"Actor name '{child_ref.name}' is already in use")
            child.set_parent(self._ref)
            self._children[child_ref.id] = child_ref

    def set_parent(self, parent: ActorRef) -> None:
        self._parent = cast(LocalActorRef, parent)

    def remove_child(self, child_ref: ActorRef) -> None:
        with self._children_lock:
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

        self._state_lock = Lock()
        self._behavior = behavior
        self._initial_behavior = behavior
        self._system = system
        self._ref = ref
        self._mailbox: Mailbox | None = None
        self._log = system.actor_logger(self)
        self._stash: list[MessageType] = []
        self._stash_capacity = system.config.get_int(
            "movie.actor.stash-capacity", 10_000
        )
        if self._stash_capacity is None or self._stash_capacity <= 0:
            raise ValueError("Actor stash capacity must be positive")
        self._state = ActorState.NEW
        self._user_messages_suspended = False
        self._restart_pending = False
        self._started = Event()
        self._started_future = ref.started_future
        self._startup_error: Exception | None = None
        self._stopped = Event()
        self._stopped_future = ref.stopped_future
        if parent_context is not None:
            parent_context.attach_child(self)

    def attach_child(self, child: InternalActorContext) -> None:
        with self._state_lock:
            if self._state in (ActorState.STOPPING, ActorState.STOPPED):
                raise RuntimeError("Cannot spawn a child while the parent is stopping")
            super().attach_child(child)

    @property
    def state(self) -> "ActorState":
        with self._state_lock:
            return self._state

    @state.setter
    def state(self, new_state: "ActorState") -> None:
        with self._state_lock:
            self._state = new_state

    def stash(self, message: MessageType) -> None:
        with self._state_lock:
            self._stash_locked(message)

    def _stash_locked(self, message: MessageType) -> None:
        if len(self._stash) >= self._stash_capacity:
            raise RuntimeError("Actor stash capacity exceeded")
        self._stash.append(message)

    def _finish_start(self) -> None:
        while True:
            with self._state_lock:
                if not self._stash:
                    self._state = ActorState.RUNNING
                    self._restart_pending = False
                    break
                pending = self._stash
                self._stash = []
            for message in pending:
                self.tell(message)
        self._started.set()
        self._complete_started_future()

    def _complete_started_future(self) -> None:
        try:
            self._started_future.set_result(None)
        except InvalidStateError:
            pass

    def materialize_behaviour(self) -> None:
        while isinstance(self._behavior, DefferedBehavior):
            self._behavior = self._behavior(self)
        if not all(
            hasattr(self._behavior, method)
            for method in ("receive", "on_signal", "supervise")
        ):
            raise TypeError("Behavior factory did not return an AbstractBehavior")

    def start(self) -> None:
        with self._state_lock:
            if self._state is not ActorState.NEW:
                return
            self._state = ActorState.STARTING
        try:
            mailbox = self._system.mailboxes.create_mailbox(
                self._system._dispatchers.default_dispatcher, self
            )
        except BaseException:
            with self._state_lock:
                self._state = ActorState.FAILED
            raise
        with self._state_lock:
            self._mailbox = mailbox
        self._ref.attach_mailbox(mailbox)
        self.tell_system(ActorSystem.PreStart())

    def abort_start(self, error: BaseException) -> None:
        if not isinstance(error, Exception):
            base_error = error
            error = RuntimeError("Actor startup raised BaseException")
            error.__cause__ = base_error
        with self._state_lock:
            self._state = ActorState.FAILED
            self._startup_error = error
        self._complete_stop()

    def stop(self) -> None:
        self.send_system(ActorSystem.Stop())

    @property
    def is_stopped(self) -> bool:
        return self.state is ActorState.STOPPED

    def _complete_stop(self) -> None:
        with self._state_lock:
            if self._state is ActorState.STOPPED:
                return
            self._state = ActorState.STOPPED
        self._ref.stop_user_messages()
        try:
            self.on_signal(ActorSystem.PostStop())
        except BaseException:
            self._log.exception("Actor PostStop signal failed")
        finally:
            try:
                if self._parent is not None:
                    parent_context = self._system.get_context(self._parent)
                    if parent_context is not None:
                        parent_context.remove_child(self._ref)
                    self._send_system(self._parent, ActorSystem.Terminated(self._ref))
            finally:
                self._ref.close()
                self._system.unregister_actor(self._ref)
                with self._state_lock:
                    self._stash.clear()
                if not self._started.is_set():
                    error = self._startup_error or RuntimeError(
                        "Actor stopped before startup completed"
                    )
                    self._startup_error = error
                    self._started.set()
                    self._complete_startup_failure(error)
                self._stopped.set()
                self._complete_stopped_future()

    def _complete_stopped_future(self) -> None:
        try:
            self._stopped_future.set_result(None)
        except InvalidStateError:
            pass

    def _stop_children(self) -> None:
        for child in self.get_children().values():
            self._send_system(child, ActorSystem.Stop())

    def _send_system(
        self, target: LocalActorRef, message: ActorSystem.SystemMessage
    ) -> None:
        target.tell_system(message)

    def _begin_restart(self) -> None:
        with self._state_lock:
            self._restart_pending = True
            self._state = ActorState.STARTING
        self._stop_children()
        if self.children_count() == 0:
            self._complete_restart()

    def _complete_restart(self) -> None:
        with self._state_lock:
            self._user_messages_suspended = False
        self._behavior = self._initial_behavior
        try:
            self.materialize_behaviour()
            self.on_signal(ActorSystem.PreStart())
            self._finish_start()
        except BaseException as error:
            if not isinstance(error, Exception):
                base_error = error
                error = RuntimeError("Actor restart raised BaseException")
                error.__cause__ = base_error
            self._log.error(
                "Actor restart failed; stopping actor",
                exc_info=(type(error), error, error.__traceback__),
            )
            with self._state_lock:
                self._state = ActorState.FAILED
            self._behavior = Behaviors.failed
            self.send_system(ActorSystem.Stop())

    def _fail(self, error: Exception) -> None:
        try:
            self._log.error(
                "Actor failed",
                exc_info=(type(error), error, error.__traceback__),
            )
        except BaseException as logging_error:
            error.add_note(f"Actor failure logging failed: {logging_error!r}")
        with self._state_lock:
            if self._state in (ActorState.STOPPING, ActorState.STOPPED):
                return
            self._state = ActorState.FAILED
        failure_handler = getattr(self._behavior, "actor_failed", None)
        if failure_handler is not None:
            try:
                failure_handler(error)
            except BaseException as handler_error:
                error.add_note(f"Actor failure callback failed: {handler_error!r}")
        self._behavior = Behaviors.failed
        if not self._started.is_set():
            self._startup_error = error
            self._started.set()
            self._complete_startup_failure(error)
        if self._parent is not None:
            try:
                self._send_system(self._parent, ActorSystem.Failed(self._ref, error))
            except BaseException as notification_error:
                error.add_note(
                    f"Supervisor notification failed: {notification_error!r}"
                )
                self.send_system(ActorSystem.Stop())
        else:
            self.send_system(ActorSystem.Stop())

    def _handle_supervision(
        self,
        actor_ref: ActorRef,
        exception: Exception,
        directive: SupervisorDirective,
    ) -> None:
        match directive:
            case SupervisorDirective.RESTART:
                if isinstance(actor_ref, LocalActorRef):
                    self._send_system(actor_ref, ActorSystem.Restart())
            case SupervisorDirective.STOP:
                if isinstance(actor_ref, LocalActorRef):
                    self._send_system(actor_ref, ActorSystem.Stop())
            case SupervisorDirective.ESCALATE:
                if self._parent is not None:
                    self._send_system(
                        self._parent, ActorSystem.Failed(actor_ref, exception)
                    )

    def tell(self, message: MessageType) -> None:
        """
        Send a message to this actor's mailbox.
        """
        with self._state_lock:
            mailbox = self._mailbox
        if mailbox is not None:
            mailbox.send(message)

    def tell_system(self, message: ActorSystem.SystemMessage) -> None:
        """
        Send a system message to this actor's mailbox.
        """
        with self._state_lock:
            mailbox = self._mailbox
        if mailbox is not None:
            mailbox.sendSystem(message)

    def enqueue_control(self, message: ActorSystem.SystemMessage) -> None:
        self.tell_system(message)

    def suspend_user_messages(self) -> None:
        with self._state_lock:
            mailbox = self._mailbox
            if mailbox is not None and getattr(
                mailbox, "supports_user_suspension", False
            ) is not True:
                raise RuntimeError(
                    "Configured mailbox does not support user-message suspension"
                )
            self._user_messages_suspended = True

    def resume_user_messages(self) -> None:
        with self._state_lock:
            self._user_messages_suspended = False

    def send(self, message: MessageType) -> None:
        with self._state_lock:
            if self._state in (ActorState.STOPPING, ActorState.STOPPED):
                return
            if self._state in (ActorState.NEW, ActorState.FAILED) or (
                self._state is ActorState.STARTING and self._restart_pending
            ):
                self._stash_locked(message)
                return
        self.tell(message)

    def send_system(self, message: ActorSystem.SystemMessage) -> None:
        with self._state_lock:
            if self._state is ActorState.STOPPED:
                return
            mailbox = self._mailbox
        if mailbox is not None:
            mailbox.sendSystem(message)

    def attach_mailbox(self, mailbox: Mailbox) -> None:
        with self._state_lock:
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
        try:
            new_behavior = self._behavior.receive(self, message) or Behaviors.same
            match new_behavior:
                case DefferedBehavior() as deferred:
                    self._behavior = deferred(self)
                case SameBehavior():
                    pass
                case StoppedBehavior():
                    self._invoke_system(ActorSystem.Stop())
                case _:
                    self._behavior = new_behavior
        except Exception as error:
            self._fail(error)

    def invoke(self, message: MessageType) -> None:
        """
        Invoke the actor's behavior with the message received from mailbox.
        """
        self._system._enter_actor_callback()
        try:
            self._invoke(message)
        finally:
            self._system._exit_actor_callback()

    def _invoke(self, message: MessageType) -> None:
        with self._state_lock:
            if self._state in (ActorState.STOPPING, ActorState.STOPPED):
                return
            if self._state in (
                ActorState.NEW,
                ActorState.STARTING,
                ActorState.FAILED,
            ):
                self._stash_locked(message)
                return
            if self._user_messages_suspended:
                raise RuntimeError("Mailbox invoked a suspended user message")
        self.on_message(message)

    def invoke_system(self, message: ActorSystem.SystemMessage) -> None:
        """
        Invoke the actor's behavior with the system message received from mailbox.
        """
        self._system._enter_actor_callback()
        try:
            self._invoke_system(message)
        finally:
            self._system._exit_actor_callback()

    def _invoke_system(self, message: ActorSystem.SystemMessage) -> None:
        state = self._state
        if state is ActorState.STOPPED:
            return
        try:
            match message:
                case ActorSystem.PreStart():
                    if state is ActorState.STARTING:
                        self.materialize_behaviour()
                        self.on_signal(message)
                        self._finish_start()
                case ActorSystem.Stop() | ActorSystem.Terminate():
                    begin_stop = self._state is not ActorState.STOPPING
                    if begin_stop:
                        with self._state_lock:
                            self._state = ActorState.STOPPING
                            self._restart_pending = False
                        self._ref.stop_user_messages()
                        self._stop_children()
                    if self.children_count() == 0:
                        self._complete_stop()
                case ActorSystem.Terminated(actor_ref):
                    self.remove_child(actor_ref)
                    state = self._state
                    if state is ActorState.STOPPING and self.children_count() == 0:
                        self._complete_stop()
                    elif self._restart_pending and self.children_count() == 0:
                        self._complete_restart()
                case ActorSystem.Restart():
                    if state not in (ActorState.STOPPING, ActorState.STOPPED):
                        self._begin_restart()
                case ActorSystem.Failed():
                    self.on_signal(message)
                case ActorSystem.ControlMessage() as control:
                    if control.applies_to(self._behavior):
                        control.deliver(self._behavior, self)
                case _:
                    self.on_signal(message)
        except Exception as error:
            self._fail(error)

    def invoke_batch(self, messages: list, *, system: bool) -> list:
        self._system._enter_actor_callback()
        try:
            invoke = self._invoke_system if system else self._invoke
            for index, message in enumerate(messages):
                if not system and not self.can_process_user_messages():
                    state = self.state
                    if state in (ActorState.STARTING, ActorState.RUNNING, ActorState.FAILED):
                        return messages[index:]
                    return []
                try:
                    invoke(message)
                except BaseException as error:
                    failure = RuntimeError("Actor callback raised BaseException")
                    failure.__cause__ = error
                    try:
                        self._fail(failure)
                    except BaseException as failure_error:
                        error.add_note(
                            f"Actor failure handling failed: {failure_error!r}"
                        )
                    raise ActorBatchFailed(
                        error, messages[index + 1 :], system=system
                    ) from error
                if not system and not self.can_process_user_messages():
                    state = self.state
                    if state in (ActorState.STARTING, ActorState.RUNNING, ActorState.FAILED):
                        return messages[index + 1 :]
                    return []
            return []
        finally:
            self._system._exit_actor_callback()

    def can_process_user_messages(self) -> bool:
        with self._state_lock:
            return (
                self._state is ActorState.RUNNING
                and not self._user_messages_suspended
            )

    def spawn(self, behavior: AbstractBehavior, name: str) -> ActorRef:
        """
        Spawn a new child actor with the given behavior and name.
        """
        return self.get_system().spawn(behavior, name, parent=self)

    def wait_stopped(self, timeout: float | None = None) -> bool:
        return self._stopped.wait(timeout)

    def wait_started(self, timeout: float | None = None) -> bool:
        return self._started.wait(timeout)

    @property
    def started_future(self) -> Future[None]:
        return self._started_future

    @property
    def stopped_future(self) -> Future[None]:
        return self._stopped_future

    def _complete_startup_failure(self, error: Exception) -> None:
        try:
            self._started_future.set_exception(error)
        except InvalidStateError:
            pass

    @property
    def startup_error(self) -> Exception | None:
        return self._startup_error


class ActorState(Enum):
    NEW = auto()
    STARTING = auto()
    RUNNING = auto()
    STOPPING = auto()
    STOPPED = auto()
    FAILED = auto()
