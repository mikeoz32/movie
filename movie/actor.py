import enum
from threading import RLock
from typing import Callable, Dict, Generic, Protocol, TypeVar, cast, Any
import uuid
from movie.scheduler import Scheduler, create_mailbox, Mailbox
from movie.system_message import Failed, SystemMessage

from logging import getLogger


MessageType = TypeVar("MessageType")


class ActorRef(Protocol, Generic[MessageType]):
    def tell(self, message: MessageType) -> None: ...

    @property
    def id(self) -> uuid.UUID: ...


class ActorSystem(ActorRef[MessageType], Protocol):
    _impl: "type[ActorSystem] | None" = None

    def __init__(self, behavior: "AbstractBehavior", name: str) -> None: ...

    def start(self) -> None: ...

    def stop(self) -> None: ...

    @staticmethod
    def create(behavior: "AbstractBehavior[MessageType]", name: str) -> "ActorSystem":
        if ActorSystem._impl is None:
            raise NotImplementedError("No ActorSystem implementation available")

        system = ActorSystem._impl(behavior, name)
        system.start()
        return system

    def spawn(
        self,
        behavior: "AbstractBehavior[Any]",
        name: str,
        *,
        parent: "ActorContext | None" = None,
    ) -> "ActorRef": ...


class InternalACtorSystem(ActorSystem[MessageType], Protocol): ...


class ActorSystemImpl(ActorSystem[MessageType]):
    l = RLock()

    def tell(self, message: MessageType) -> None:
        with ActorSystemImpl.l:
            if self._root_ref is not None:
                self._root_ref.tell(message)

    @property
    def id(self) -> uuid.UUID:
        if self._root_ref is not None:
            return self._root_ref.id
        else:
            raise ValueError("Actor system has not been started yet")

    def __init__(self, root_behavior: "AbstractBehavior", name: str) -> None:
        self._scheduler = Scheduler()
        self._root_behavior = root_behavior
        self._root_ref: ActorRef | None = None
        self._name = name

        self._actors: Dict[uuid.UUID, "LocalActorContext"] = {}

        self._logger = getLogger(f"ActorSystem({name})")

    @property
    def log(self):
        return self._logger

    def start(self) -> None:
        self._scheduler.start()

        self._root_ref = self.spawn(self._root_behavior, self._name)

    def stop(self) -> None:
        self._scheduler.stop()

    def spawn(
        self,
        behavior: "AbstractBehavior",
        name: str,
        *,
        parent: "ActorContext | None" = None,
    ) -> "ActorRef":
        with ActorSystemImpl.l:
            ref = LocalActorRef(self, name)
            context = LocalActorContext(behavior, ref, self)
            if parent is not None:
                parent = cast(LocalActorContext, parent)
                parent.attach_child(cast(LocalActorContext, context))

            mailbox = create_mailbox(self._scheduler, context)
            context.attach_mailbox(mailbox)
            with self.l:
                self._actors[ref.id] = context
            context.start()
            return ref

    def get_context(self, ref: ActorRef) -> "LocalActorContext | None":
        with ActorSystemImpl.l:
            return self._actors.get(ref.id, None)


ActorSystem._impl = ActorSystemImpl


class LocalActorRef(ActorRef[MessageType]):
    def __init__(self, system: ActorSystemImpl, path: str) -> None:
        self._lock = RLock()
        self._system = system
        self._path = path
        self._id = uuid.uuid4()

    def tell(self, message: MessageType) -> None:
        with self._lock:
            context = self._system.get_context(self)
            if context is not None:
                if context._mailbox is not None:
                    context._mailbox.send(message)

    def tell_system(self, message: SystemMessage) -> None:
        with self._lock:
            context = self._system.get_context(self)
            print("TELL SYSTEM", message)
            print("CONTEXT", context)
            if context is not None:
                if context._mailbox is not None:
                    context._mailbox.sendSystem(message)

    @property
    def id(self) -> uuid.UUID:
        return self._id


class ActorContext(Protocol, Generic[MessageType]):
    def get_self(self) -> ActorRef[MessageType]: ...

    def get_system(self) -> ActorSystem: ...

    def spawn(self, behavior: "AbstractBehavior", name: str) -> ActorRef: ...

    # Invokes behavior with message
    def invoke(self, message: MessageType) -> None: ...
    def invoke_system(self, message: SystemMessage) -> None: ...


class InternalActorContext(ActorContext[MessageType], Protocol):
    def attach_child(self, child: "InternalActorContext") -> None: ...
    def set_parent(self, parent: "ActorRef") -> None: ...


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

    def on_signal(self, context: ActorContext, message: SystemMessage) -> None: ...


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

    def start(self) -> None:
        while isinstance(self._behavior, DefferedBehavior):
            self._behavior = self._behavior(self)

    def stop(self) -> None:
        if self._mailbox is not None:
            self._mailbox.stop()

    def attach_mailbox(self, mailbox: Mailbox) -> None:
        self._mailbox = mailbox

    def get_self(self) -> ActorRef[MessageType]:
        return self._ref

    def get_system(self) -> ActorSystem:
        return self._system

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
                new_behavior = self._behavior.receive(self, message)
                if (
                    new_behavior is not None
                    and new_behavior != self._behavior
                    and not new_behavior.same
                ):
                    self._behavior = new_behavior
            except Exception as e:
                if self._parent is not None:
                    self._parent.tell_system(Failed(self._ref, e))
                    self._behavior = Behaviors.failed

    def invoke_system(self, message: SystemMessage) -> None:
        with self.l:
            self._behavior.on_signal(self, message)

    def spawn(self, behavior: AbstractBehavior, name: str) -> ActorRef:
        return self.get_system().spawn(behavior, name, parent=self)
