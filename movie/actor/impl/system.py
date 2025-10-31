from logging import Formatter, StreamHandler, getLogger, handlers
import queue
from threading import RLock
from typing import Any, Callable, Dict, Generic, Protocol, Type, TypeVar, cast
import uuid
from movie.actor import ActorSystem
from movie.actor.behaviour import AbstractBehavior
from movie.actor.context import ActorContext
from movie.actor.impl.context import LocalActorContext
from movie.actor.impl.ref import LocalActorRef
from movie.actor.logger import ActorLogger
from movie.actor.message import MessageType
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


class ActorRegistry:
    def __init__(self) -> None:
        self._root_guardian: ActorRef | None = None
        self._user_guardian: ActorRef | None = None
        self._system_guardian: ActorRef | None = None

    def start(self, system: "ActorSystemImpl") -> None: ...


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
        self._root_ref = self.spawn(self._root_behavior, self._name)

    def stop(self) -> None:
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
        with ActorSystemImpl.l:
            ref = LocalActorRef(self, name)
            context = LocalActorContext(behavior, ref, self)
            if parent is not None:
                parent = cast(LocalActorContext, parent)
                parent.attach_child(cast(LocalActorContext, context))

            mailbox = self._mailboxes.create_mailbox(
                self._dispatchers.default_dispatcher, context
            )
            context.attach_mailbox(mailbox)
            with self.l:
                self._actors[ref.id] = context
            context.start()
            return ref

    def get_context(self, ref: ActorRef) -> "LocalActorContext | None":
        with ActorSystemImpl.l:
            return self._actors.get(ref.id, None)
