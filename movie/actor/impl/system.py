import os
import queue
import uuid
from concurrent.futures import Future
from enum import Enum, auto
from logging import Formatter, Logger, StreamHandler, handlers
from pathlib import Path
from threading import Event, Lock, RLock, Thread, local
from time import monotonic, sleep
from typing import Any, Callable, Dict, Generic, Protocol, Type, TypeVar, cast

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
from movie.future import CallbackExecutor
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


class DeadlineQueueListener(handlers.QueueListener):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._sentinel_enqueued = False

    def stop(self, timeout: float | None = None) -> bool:
        thread = self._thread
        if thread is None:
            return True
        if not self._sentinel_enqueued:
            self.enqueue_sentinel()
            self._sentinel_enqueued = True
        thread.join(timeout)
        if thread.is_alive():
            return False
        self._thread = None
        return True


class ExtensionId(Generic[E]): ...


class ExtensionRegisrty:
    def __init__(self, system: InternalActorSystem) -> None:
        self._system = system
        self._by_type: Dict[Type[Extension], Any] = {}
        self._by_id: Dict[ExtensionId[Any], Any] = {}
        self._lock = RLock()

    def get(self, ext_type: Type[E]) -> E:
        with self._lock:
            ext = self._by_type.get(ext_type, None)
            if ext is None:
                raise ValueError(f"Extension of type {ext_type} not found")
            return cast(E, ext)

    def get_or_register(
        self, ext_id: ExtensionId[E], factory: Callable[["ActorSystem"], E]
    ) -> E:
        with self._lock:
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
        self._lock = RLock()

    def create_root_guardian(self) -> ActorRef:
        with self._lock:
            if len(self._actors) >= self._system._max_actors:
                raise RuntimeError("Actor system capacity exceeded")
        ref = LocalActorRef(
            self._system, RootActorPath(Address("movie", self._system.name))
        )
        context = LocalActorContext(
            RootGuardianBehavior.create(), ref, self._system, None
        )

        with self._lock:
            self._root_guardian = ref
            self._actors[ref.id] = context
        try:
            context.start()
            self._system.wait_for_actor_start(ref)
        except BaseException as error:
            try:
                context.abort_start(error)
            except BaseException as cleanup_error:
                error.add_note(f"Guardian startup cleanup failed: {cleanup_error!r}")
            with self._lock:
                self._root_guardian = None
            raise
        return ref

    def start(self) -> None:
        self.create_root_guardian()

    def spawn(
        self,
        behavior: AbstractBehavior[Any],
        name: str,
        *,
        parent: "ActorContext | None" = None,
    ) -> "ActorRef":
        with self._lock:
            if len(self._actors) >= self._system._max_actors:
                raise RuntimeError("Actor system capacity exceeded")
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
        try:
            context.start()
        except BaseException as error:
            try:
                context.abort_start(error)
            except BaseException as cleanup_error:
                error.add_note(f"Actor startup cleanup failed: {cleanup_error!r}")
            raise
        return ref

    def clear(self) -> None:
        with self._lock:
            self._actors.clear()
            self._root_guardian = None

    @property
    def root_guardian(self) -> ActorRef | None:
        with self._lock:
            return self._root_guardian

    def get(self, ref: ActorRef) -> "LocalActorContext | None":
        with self._lock:
            return self._actors.get(ref.id)

    def unregister(self, ref: ActorRef) -> None:
        with self._lock:
            self._actors.pop(ref.id, None)

    @property
    def actor_count(self) -> int:
        with self._lock:
            return len(self._actors)


class SystemState(Enum):
    NEW = auto()
    STARTING = auto()
    RUNNING = auto()
    STOPPING = auto()
    STOPPED = auto()


class ActorSystemImpl(InternalActorSystem[MessageType]):
    def tell(self, message: MessageType) -> None:
        if self._root_ref is not None:
            self._root_ref.tell(message)

    @property
    def id(self) -> uuid.UUID:
        if self._root_ref is not None:
            return self._root_ref.id
        else:
            raise ValueError("Actor system has not been started yet")

    def __init__(
        self,
        root_behavior: AbstractBehavior,
        name: str,
        *,
        config: Config | None = None,
    ) -> None:
        configured_path = os.environ.get("MOVIE_CONFIG")
        if config is not None:
            loaded_config = config
        elif configured_path is not None:
            if not Path(configured_path).is_file():
                raise FileNotFoundError(f"MOVIE_CONFIG does not exist: {configured_path}")
            loaded_config = Config.from_toml_file(configured_path)
        else:
            loaded_config = Config.from_toml_file("movie.toml", required=False)
        self._config = loaded_config.with_fallback(default_config)
        startup_timeout = self._config.get_int("movie.actor.startup-timeout", 10)
        shutdown_timeout = self._config.get_int("movie.actor.shutdown-timeout", 10)
        callback_workers = self._config.get_int("movie.actor.callback-workers", 4)
        max_actors = self._config.get_int("movie.actor.max-actors", 100_000)
        if startup_timeout is None or startup_timeout <= 0:
            raise ValueError("Actor startup timeout must be positive")
        if shutdown_timeout is None or shutdown_timeout <= 0:
            raise ValueError("Actor shutdown timeout must be positive")
        if callback_workers is None or callback_workers <= 0:
            raise ValueError("Future callback worker count must be positive")
        if max_actors is None or max_actors < 2:
            raise ValueError("Actor capacity must allow the guardian and root actor")
        dispatcher_config = self._config.get_config(
            "movie.dispatcher.default-dispatcher"
        )
        system_queue_capacity = (
            dispatcher_config.get_int("system-queue-capacity", 100_000)
            if dispatcher_config is not None
            else 100_000
        )
        if (
            system_queue_capacity is not None
            and system_queue_capacity < max_actors
        ):
            raise ValueError(
                "Default dispatcher system queue capacity must cover max actors"
            )
        self._startup_timeout = float(startup_timeout)
        self._shutdown_timeout = float(shutdown_timeout)
        self._max_actors = max_actors
        self._extensions = ExtensionRegisrty(self)
        self._dispatchers = DispatcherManager(self._config)
        self._mailboxes = MailboxManager(self._config)
        self._root_behavior = root_behavior
        self._root_ref: ActorRef | None = None
        self._name = name
        self._actor_registry = ActorRegistry(self)
        self._lifecycle_lock = RLock()
        self._stop_lock = Lock()
        self._state = SystemState.NEW
        self._terminated = Event()
        self._rollback_started = Event()
        self._callback_shutdown_started = Event()
        self._callback_state = local()
        self._callbacks = CallbackExecutor(
            name,
            workers=callback_workers,
        )

        self._log_queue = queue.Queue(maxsize=100_000)
        self._log_listener: DeadlineQueueListener | None = None
        self._queue_handler: handlers.QueueHandler | None = None
        self._stream_handler: StreamHandler | None = None
        self._actor_log: Logger

        self.setup_logger()

    @property
    def config(self) -> Config:
        return self._config

    def actor_logger(self, ctx: ActorContext) -> ActorLogger:
        logger = ActorLogger(self._actor_log, {})
        logger.set_context(ctx)
        return logger

    def setup_logger(self) -> None:
        self._actor_log = Logger(f"movie.actor.{self._name}")
        self._actor_log.setLevel(
            self._config.get_string("movie.actor.log-level", "INFO") or "INFO"
        )
        self._actor_log.propagate = False

        stream = StreamHandler()
        stream.setFormatter(
            Formatter(
                "[%(asctime)s %(levelname)s] %(name)s"
                "(%(actor_id)s) %(actor_path)s -> %(message)s"
            )
        )

        self._stream_handler = stream
        self._queue_handler = handlers.QueueHandler(self._log_queue)
        self._actor_log.addHandler(self._queue_handler)
        self._log_listener = DeadlineQueueListener(self._log_queue, stream)
        self._log_listener.start()

    def start(self) -> None:
        with self._lifecycle_lock:
            if self._state is not SystemState.NEW:
                raise RuntimeError("Actor system can only be started once")
            self._state = SystemState.STARTING

        try:
            self._actor_registry.start()
            self._root_ref = self.spawn(self._root_behavior, self._name)
            context = self.get_context(self._root_ref)
            startup_timeout = self._startup_timeout
            if context is None or not context.wait_started(startup_timeout):
                raise TimeoutError(
                    f"Root actor did not start within {startup_timeout} seconds"
                )
            if context.startup_error is not None:
                raise RuntimeError("Root actor failed during startup") from context.startup_error
        except BaseException as startup_error:
            try:
                self._shutdown(self._configured_shutdown_timeout())
            except BaseException as shutdown_error:
                startup_error.add_note(f"Startup rollback failed: {shutdown_error!r}")
                self._start_rollback_finalizer()
            raise

        with self._lifecycle_lock:
            interrupted = self._state is not SystemState.STARTING
            if not interrupted:
                self._state = SystemState.RUNNING
        if interrupted:
            self._terminated.wait(startup_timeout)
            raise RuntimeError("Actor system startup was interrupted by shutdown")

    def stop(self, timeout: float | None = None) -> None:
        if getattr(self._callback_state, "depth", 0) > 0:
            raise RuntimeError("ActorSystem.stop() cannot be called from an actor callback")
        if timeout is None:
            timeout = self._configured_shutdown_timeout()
        elif timeout <= 0:
            raise ValueError("Actor shutdown timeout must be positive")
        self._shutdown(timeout)

    def _configured_shutdown_timeout(self) -> float:
        return self._shutdown_timeout

    def _start_rollback_finalizer(self) -> None:
        if self._rollback_started.is_set():
            return
        self._rollback_started.set()
        Thread(
            target=self._finish_startup_rollback,
            name=f"movie-startup-rollback-{self._name}",
            daemon=True,
        ).start()

    def _finish_startup_rollback(self) -> None:
        while not self._terminated.is_set():
            try:
                self._shutdown(self._shutdown_timeout)
            except BaseException:
                sleep(0.05)

    def _shutdown(self, timeout: float) -> None:
        deadline = monotonic() + timeout
        if not self._stop_lock.acquire(timeout=timeout):
            raise TimeoutError(f"Actor system did not stop within {timeout} seconds")
        try:
            remaining = deadline - monotonic()
            if remaining <= 0 or not self._lifecycle_lock.acquire(timeout=remaining):
                raise TimeoutError(
                    f"Actor system did not stop within {timeout} seconds"
                )
            try:
                if self._state is SystemState.STOPPED:
                    return
                self._state = SystemState.STOPPING
            finally:
                self._lifecycle_lock.release()
            guardian = self._actor_registry.root_guardian
            context = self.get_context(guardian) if guardian is not None else None

            if guardian is not None and context is not None:
                while not context.is_stopped:
                    try:
                        guardian.tell_system(ActorSystem.Stop())
                        break
                    except RuntimeError as error:
                        if monotonic() >= deadline:
                            raise TimeoutError(
                                f"Actor system did not stop within {timeout} seconds"
                            ) from error
                        sleep(0.001)
                remaining = max(0.0, deadline - monotonic())
                if not context.wait_stopped(remaining):
                    raise TimeoutError(
                        f"Actor system did not stop within {timeout} seconds"
                    )

            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"Actor system did not stop within {timeout} seconds"
                )
            self._dispatchers.stop_all(remaining)
            if monotonic() > deadline:
                raise TimeoutError(
                    f"Actor system did not stop within {timeout} seconds"
                )
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"Actor system did not stop within {timeout} seconds"
                )
            self._callbacks.close(remaining)
            self._teardown_logger(deadline)
            if monotonic() > deadline:
                raise TimeoutError(
                    f"Actor system did not stop within {timeout} seconds"
                )
            self._actor_registry.clear()
            if self._callbacks.owns_current_thread():
                self._start_callback_shutdown_finalizer()
                return
            remaining = deadline - monotonic()
            if remaining <= 0 or not self._lifecycle_lock.acquire(timeout=remaining):
                raise TimeoutError(
                    f"Actor system did not stop within {timeout} seconds"
                )
            try:
                self._state = SystemState.STOPPED
                self._terminated.set()
            finally:
                self._lifecycle_lock.release()
        finally:
            self._stop_lock.release()

    def _start_callback_shutdown_finalizer(self) -> None:
        if self._callback_shutdown_started.is_set():
            return
        self._callback_shutdown_started.set()
        Thread(
            target=self._finish_callback_shutdown,
            name=f"movie-callback-shutdown-{self._name}",
            daemon=True,
        ).start()

    def _finish_callback_shutdown(self) -> None:
        while True:
            try:
                self._callbacks.close(self._shutdown_timeout)
                break
            except TimeoutError:
                sleep(0.01)
        with self._lifecycle_lock:
            self._state = SystemState.STOPPED
            self._terminated.set()

    def _teardown_logger(self, deadline: float | None = None) -> None:
        if self._queue_handler is not None:
            self._actor_log.removeHandler(self._queue_handler)
            self._queue_handler.close()
            self._queue_handler = None
        if self._log_listener is not None:
            while True:
                try:
                    remaining = (
                        None if deadline is None else max(0.0, deadline - monotonic())
                    )
                    if remaining == 0 or not self._log_listener.stop(remaining):
                        raise TimeoutError("Actor logger did not stop within the deadline")
                    break
                except queue.Full:
                    if deadline is not None and monotonic() >= deadline:
                        raise TimeoutError(
                            "Actor logger did not stop within the deadline"
                        )
                    try:
                        self._log_queue.get_nowait()
                        self._log_queue.task_done()
                    except queue.Empty:
                        pass
            self._log_listener = None
        if self._stream_handler is not None:
            self._stream_handler.close()
            self._stream_handler = None

    def spawn(
        self,
        behavior: "AbstractBehavior",
        name: str,
        *,
        parent: "ActorContext | None" = None,
    ) -> "ActorRef":
        with self._lifecycle_lock:
            if self._state not in (SystemState.STARTING, SystemState.RUNNING):
                raise RuntimeError("Actor system is not accepting new actors")
            if parent is not None and (
                not isinstance(parent, LocalActorContext)
                or parent._system is not self
                or self.get_context(parent.ref) is not parent
            ):
                raise ValueError("Actor parent does not belong to this actor system")
            return self._actor_registry.spawn(behavior, name, parent=parent)

    @property
    def mailboxes(self) -> MailboxManager:
        return self._mailboxes

    def get_context(self, ref: ActorRef) -> "LocalActorContext | None":
        return self._actor_registry.get(ref)

    def unregister_actor(self, ref: ActorRef) -> None:
        self._actor_registry.unregister(ref)

    def wait_for_actor_start(self, ref: ActorRef, timeout: float | None = None) -> None:
        context = self.get_context(ref)
        if context is None:
            raise RuntimeError("Actor was unregistered before startup completed")
        configured_timeout = self._config.get_int("movie.actor.startup-timeout", 10)
        if timeout is None:
            if configured_timeout is None or configured_timeout <= 0:
                raise ValueError("Actor startup timeout must be positive")
            timeout = float(configured_timeout)
        if not context.wait_started(timeout):
            raise TimeoutError(f"Actor did not start within {timeout} seconds")
        if context.startup_error is not None:
            raise RuntimeError("Actor failed during startup") from context.startup_error

    def actor_start_future(self, ref: ActorRef) -> Future[None]:
        if not isinstance(ref, LocalActorRef) or not ref.belongs_to(self):
            raise ValueError("Actor reference does not belong to this actor system")
        return ref.started_future

    def actor_stop_future(self, ref: ActorRef) -> Future[None]:
        if not isinstance(ref, LocalActorRef) or not ref.belongs_to(self):
            raise ValueError("Actor reference does not belong to this actor system")
        return ref.stopped_future

    def terminate(self, ref: ActorRef) -> None:
        if not isinstance(ref, LocalActorRef) or not ref.belongs_to(self):
            raise ValueError("Actor reference does not belong to this actor system")
        ref.tell_system(ActorSystem.Stop())

    @property
    def actor_count(self) -> int:
        return self._actor_registry.actor_count

    @property
    def name(self) -> str:
        return self._name

    def _enter_actor_callback(self) -> None:
        self._callback_state.depth = getattr(self._callback_state, "depth", 0) + 1

    def _exit_actor_callback(self) -> None:
        self._callback_state.depth -= 1

    def _submit_callback(self, callback) -> None:
        self._callbacks.submit(callback)

    @staticmethod
    def _submit_completion(callback) -> None:
        callback()
