from logging import Formatter, StreamHandler, getLogger, handlers
import queue
from threading import RLock
import time
from typing import Any, Callable, Dict, Generic, Protocol, Type, TypeVar, cast
import uuid
from movie.actor import ActorSystem
from movie.actor.behaviour import AbstractBehavior, Behaviors
from movie.actor.context import ActorContext
from movie.actor.impl.context import LocalActorContext
from movie.actor.impl.ref import LocalActorRef
from movie.actor.logger import ActorLogger
from movie.actor.message import MessageType
from movie.actor.path import Address, RootActorPath
from movie.actor.ref import ActorRef
from movie.actor.system import InternalActorSystem
from movie.config import Config
from movie.dispatch.manager import DispatcherManager
from movie.mailbox.manager import MailboxManager


class Extension(Protocol): ...


E = TypeVar("E", bound=Extension)

default_config = Config(
    {
        "movie": {
            "actor": {},
        }
    }
)


class ExtensionId(Generic[E]): ...


class ExtensionRegisrty:
    def __init__(self, system: InternalActorSystem) -> None:
        self._system = system
        self._by_type: Dict[Type[Extension], Any] = {}
        self._by_id: Dict[ExtensionId[Any], Any] = {}

    def get(self, ext_type: Type[E]) -> E:
        ext = self._by_type.get(ext_type, None)
        if ext is None:
            raise ValueError(f"Extension of type {ext_type} not found")
        return cast(E, ext)

    def get_or_register(
        self, ext_id: ExtensionId[E], factory: Callable[["ActorSystem"], E]
    ) -> E:
        ext = self._by_id.get(ext_id, None)
        if ext is None:
            ext = factory(self._system)
            self._by_id[ext_id] = ext
            self._by_type[type(ext)] = ext
        return cast(E, ext)


class RootGuardianBehavior(AbstractBehavior[Any]):
    def __init__(self, context: ActorContext[Any]) -> None:
        super().__init__(context)

    @staticmethod
    def create() -> "AbstractBehavior[Any]":
        return Behaviors.setup(lambda ctx: RootGuardianBehavior(ctx))

    def receive(self, context: ActorContext, message: Any) -> AbstractBehavior | None:
        return self


class ActorRegistry:
    def __init__(self, system: "ActorSystemImpl") -> None:
        self._root_guardian: ActorRef | None = None
        self._user_guardian: ActorRef | None = None
        self._system_guardian: ActorRef | None = None
        self._actors: Dict[uuid.UUID, "LocalActorContext"] = {}
        self._system = system

    def create_root_guardian(self) -> ActorRef:
        ref = LocalActorRef(self._system, RootActorPath(Address("movie", "/")))
        context = LocalActorContext(
            RootGuardianBehavior.create(), ref, self._system, None
        )

        self._root_guardian = ref
        self._actors[ref.id] = context
        context.start()
        return ref

    def start(self) -> None:
        self._root_guardian = self.create_root_guardian()

    def spawn(
        self,
        behavior: AbstractBehavior[Any],
        name: str,
        *,
        parent: "ActorContext | None" = None,
    ) -> "ActorRef":
        with ActorSystemImpl.l:
            parent = parent or cast(
                LocalActorContext, self._actors[self._root_guardian.id]
            )
            ref = LocalActorRef(
                self._system,
                parent.get_self().path.child(name),
            )
            context = LocalActorContext(
                behavior, ref, self._system, cast(LocalActorContext, parent)
            )
            self._actors[ref.id] = context
            context.start()
            return ref


class ActorSystemImpl(InternalActorSystem[MessageType]):
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

    def __init__(self, root_behavior: AbstractBehavior, name: str) -> None:
        self._config: Config = Config.from_toml_file("movie.toml").with_fallback(
            default_config
        )
        self._extensions = ExtensionRegisrty(self)
        self._dispatchers = DispatcherManager(self._config)
        self._mailboxes = MailboxManager(self._config)
        self._root_behavior = root_behavior
        self._root_ref: ActorRef | None
        self._name = name
        self._actor_registry = ActorRegistry(self)

        self._actors: Dict[uuid.UUID, "LocalActorContext"] = {}

        self._log_queue = queue.Queue(maxsize=100_000)
        self._log_listener: handlers.QueueListener | None = None

        self.setup_logger()

    @property
    def config(self) -> Config:
        return self._config

    def actor_logger(self, ctx: ActorContext) -> ActorLogger:
        logger = ActorLogger(getLogger("actor"), {})
        logger.set_context(ctx)
        logger.setLevel("DEBUG")
        return logger

    def setup_logger(self) -> None:
        stream = StreamHandler()
        stream.setFormatter(
            Formatter(
                "[%(asctime)s %(levelname)s] %(name)s"
                "(%(actor_id)s) %(actor_path)s -> %(message)s"
            )
        )

        self._log_listener = handlers.QueueListener(self._log_queue, stream)
        self._log_listener.start()

        root = getLogger()
        root.setLevel("DEBUG")
        root.addHandler(handlers.QueueHandler(self._log_queue))

    def start(self) -> None:
        self._actor_registry.start()
        self._root_ref = self.spawn(self._root_behavior, self._name)

    def stop(self) -> None:
        self._actor_registry._root_guardian.tell_system(ActorSystem.Stop())
        while True:
            context = self.get_context(self._actor_registry._root_guardian)
            if context is not None and context.is_stopped:
                break
            time.sleep(0.1)

        self._dispatchers.stop_all()
        if self._log_listener is not None:
            self._log_listener.stop()

    def spawn(
        self,
        behavior: "AbstractBehavior",
        name: str,
        *,
        parent: "ActorContext | None" = None,
    ) -> "ActorRef":
        # with ActorSystemImpl.l:
        #     ref = LocalActorRef(
        #         self,
        #         (
        #             RootActorPath(Address("movie", self._name))
        #             if not parent
        #             else parent.get_self().path.child(name)
        #         ),
        #     )
        #     context = LocalActorContext(
        #         behavior, ref, self, cast(LocalActorContext, parent)
        #     )
        #     self._actors[ref.id] = context
        #     context.start()
        #     return ref
        return self._actor_registry.spawn(behavior, name, parent=parent)

    @property
    def mailboxes(self) -> MailboxManager:
        return self._mailboxes

    def get_context(self, ref: ActorRef) -> "LocalActorContext | None":
        with ActorSystemImpl.l:
            return self._actor_registry._actors.get(ref.id, None)

    def unregister_actor(self, ref: ActorRef) -> None:
        with ActorSystemImpl.l:
            self._actors.pop(ref.id, None)
