import math
import os
import queue
import uuid
from concurrent.futures import Future
from dataclasses import replace
from enum import Enum, auto
from logging import Formatter, Logger, StreamHandler, handlers
from pathlib import Path
from threading import Event, Lock, RLock, Thread, local
from time import monotonic, sleep
from typing import Any, Dict, TypeVar, cast

from movie.actor import ActorSystem
from movie.actor.behaviour import AbstractBehavior, Behaviors
from movie.actor.context import ActorContext
from movie.actor.dead_letter import (
    DeadLetter,
    DeadLetterBroker,
    DeadLetterReason,
    RemoteAdmissionResult,
    admission_dead_letter_reason,
)
from movie.actor.extension import Extension, ExtensionId, ExtensionRegistry
from movie.actor.identity import ActorIdentity, ActorSystemIncarnationUid, new_incarnation_uid
from movie.actor.impl.context import LocalActorContext
from movie.actor.impl.ref import LocalActorRef
from movie.actor.logger import ActorLogger
from movie.actor.message import MessageType
from movie.actor.path import (
    ActorPath,
    Address,
    RootActorPath,
    is_remote_actor_path,
    parse_actor_path,
)
from movie.actor.ref import ActorRef
from movie.actor.system import InternalActorSystem
from movie.cluster._protocol import _augment_registry
from movie.cluster.config import ClusterConfig
from movie.cluster.extension import CLUSTER, ClusterExtension
from movie.cluster.runtime import ClusterRuntime
from movie.config import Config
from movie.dispatch.manager import DispatcherManager
from movie.future import CallbackExecutor
from movie.mailbox.manager import MailboxManager
from movie.remoting.config import RemotingConfig
from movie.remoting.extension import REMOTING, RemotingExtension

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
        self._paths: Dict[str, uuid.UUID] = {}
        self._system = system
        self._lock = RLock()

    def create_root_guardian(self) -> ActorRef:
        with self._lock:
            if len(self._actors) >= self._system._max_actors:
                raise RuntimeError("Actor system capacity exceeded")
        ref = self._new_ref(RootActorPath(Address("movie", self._system.name)))
        context = LocalActorContext(
            RootGuardianBehavior.create(), ref, self._system, None
        )

        with self._lock:
            self._root_guardian = ref
            self._actors[ref.id] = context
            if ref.path.is_remote_resolvable:
                self._paths[ref.path.remote_path] = ref.id
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
            ref = self._new_ref(parent.get_self().path.child(name))
            context = LocalActorContext(
                behavior, ref, self._system, cast(LocalActorContext, parent)
            )
            if ref.path.is_remote_resolvable and ref.path.remote_path in self._paths:
                raise ValueError(f"Actor path '{ref.path.remote_path}' is already in use")
            self._actors[ref.id] = context
            if ref.path.is_remote_resolvable:
                self._paths[ref.path.remote_path] = ref.id
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
            self._paths.clear()
            self._root_guardian = None

    @property
    def root_guardian(self) -> ActorRef | None:
        with self._lock:
            return self._root_guardian

    def get(self, ref: ActorRef) -> "LocalActorContext | None":
        if not isinstance(ref, LocalActorRef) or not ref.belongs_to(self._system):
            return None
        with self._lock:
            context = self._actors.get(ref.id)
            if context is None or context.ref is not ref:
                return None
            return context

    def get_by_uid(self, actor_uid: uuid.UUID) -> LocalActorRef | None:
        with self._lock:
            context = self._actors.get(actor_uid)
            return context.ref if context is not None else None

    def get_by_path(self, path: ActorPath | str) -> LocalActorRef | None:
        canonical = self._canonical_path(path)
        if canonical is None:
            return None
        with self._lock:
            actor_uid = self._paths.get(canonical)
            context = self._actors.get(actor_uid) if actor_uid is not None else None
            return context.ref if context is not None else None

    def resolve_identity(self, identity: ActorIdentity) -> LocalActorRef | None:
        if identity.system_incarnation_uid != self._system.incarnation_uid:
            return None
        with self._lock:
            context = self._actors.get(identity.actor_uid)
            if context is None or context.ref.identity != identity:
                return None
            return context.ref

    def unregister(self, ref: ActorRef) -> None:
        if not isinstance(ref, LocalActorRef) or not ref.belongs_to(self._system):
            return
        with self._lock:
            context = self._actors.get(ref.id)
            if context is None or context.ref is not ref:
                return
            self._actors.pop(ref.id)
            if self._paths.get(ref.path.remote_path) == ref.id:
                self._paths.pop(ref.path.remote_path)

    @property
    def actor_count(self) -> int:
        with self._lock:
            return len(self._actors)

    def _new_ref(self, path: ActorPath) -> LocalActorRef:
        while True:
            ref = LocalActorRef(self._system, path)
            if ref.id not in self._actors:
                return ref

    def _canonical_path(self, path: ActorPath | str) -> str | None:
        if isinstance(path, ActorPath):
            if (
                not path.is_remote_resolvable
                or path.address.protocol != "movie"
                or path.address.system != self._system.name
            ):
                return None
            return path.remote_path
        if is_remote_actor_path(path):
            return path
        try:
            parsed = parse_actor_path(path)
        except (TypeError, ValueError):
            return None
        if (
            not parsed.is_remote_resolvable
            or parsed.address.protocol != "movie"
            or parsed.address.system != self._system.name
        ):
            return None
        return parsed.remote_path


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

    @property
    def identity(self) -> ActorIdentity:
        if self._root_ref is None:
            raise ValueError("Actor system has not been started yet")
        return self._root_ref.identity

    @property
    def path(self) -> ActorPath:
        if self._root_ref is None:
            raise ValueError("Actor system has not been started yet")
        return self._root_ref.path

    def __init__(
        self,
        root_behavior: AbstractBehavior,
        name: str,
        *,
        config: Config | None = None,
        remoting: RemotingConfig | None = None,
        cluster: ClusterConfig | None = None,
    ) -> None:
        self._name = name
        self._incarnation_uid = new_incarnation_uid()
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
        dead_letter_capacity = self._config.get_int(
            "movie.actor.dead-letters.capacity", 1_000
        )
        dead_letter_subscriptions = self._config.get_int(
            "movie.actor.dead-letters.max-subscriptions", 1_000
        )
        if startup_timeout is None or startup_timeout <= 0:
            raise ValueError("Actor startup timeout must be positive")
        if shutdown_timeout is None or shutdown_timeout <= 0:
            raise ValueError("Actor shutdown timeout must be positive")
        if callback_workers is None or callback_workers <= 0:
            raise ValueError("Future callback worker count must be positive")
        minimum_actor_capacity = 3 if cluster is not None else 2
        if max_actors is None or max_actors < minimum_actor_capacity:
            raise ValueError("Actor capacity must allow the guardian and root actor")
        if (
            dead_letter_capacity is None
            or dead_letter_capacity <= 0
            or dead_letter_subscriptions is None
            or dead_letter_subscriptions <= 0
        ):
            raise ValueError(
                "Dead-letter capacity and subscription limit must be positive"
            )
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
        self._extensions = ExtensionRegistry(self)
        self._dispatchers = DispatcherManager(self._config)
        self._mailboxes = MailboxManager(self._config)
        self._root_behavior = root_behavior
        self._root_ref: ActorRef | None = None
        self._actor_registry = ActorRegistry(self)
        self._dead_letters: DeadLetterBroker[DeadLetter] = DeadLetterBroker(
            dead_letter_capacity, dead_letter_subscriptions
        )
        if cluster is not None:
            if remoting is None:
                raise ValueError("cluster membership requires remoting configuration")
            remoting = replace(
                remoting,
                serializers=_augment_registry(
                    remoting.serializers,
                    cluster.serializer_id,
                ),
            )
            ClusterRuntime._validate_configuration(self, cluster, remoting)
        self._remoting_config = remoting
        self._cluster_config = cluster
        self._cluster_admission: ClusterExtension | None = None
        self._cluster_admission_lock = Lock()
        self._lifecycle_lock = RLock()
        self._stop_lock = Lock()
        self._state = SystemState.NEW
        self._terminated = Event()
        self._rollback_started = Event()
        self._callback_shutdown_started = Event()
        self._callback_state = local()
        if self._remoting_config is not None:
            self._extensions.configure(REMOTING)
        if self._cluster_config is not None:
            self._extensions.configure(CLUSTER)
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

    def extension(self, extension_id: ExtensionId[E]) -> E:
        return self._extensions.get(extension_id)

    def _create_remoting_extension(self) -> RemotingExtension:
        config = self._remoting_config
        if config is None:
            raise RuntimeError("remoting is not configured for this actor system")
        return RemotingExtension(self, config)

    def _create_cluster_extension(self) -> ClusterExtension:
        config = self._cluster_config
        remoting_config = self._remoting_config
        if config is None or remoting_config is None:
            raise RuntimeError("cluster membership is not configured for this actor system")
        remoting = REMOTING.get(self)
        extension = ClusterExtension(self, config, remoting_config, remoting)
        with self._cluster_admission_lock:
            self._cluster_admission = extension
        return extension

    def _clear_cluster_admission(self, extension: ClusterRuntime) -> None:
        with self._cluster_admission_lock:
            if self._cluster_admission is extension:
                self._cluster_admission = None

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
            self._extensions.activate()
            self._root_ref = self.spawn(self._root_behavior, self._name)
            context = self.get_context(self._root_ref)
            startup_timeout = self._startup_timeout
            if context is None or not context.wait_started(startup_timeout):
                raise TimeoutError(
                    f"Root actor did not start within {startup_timeout} seconds"
                )
            if context.startup_error is not None:
                raise RuntimeError("Root actor failed during startup") from context.startup_error
            if self._remoting_config is not None:
                REMOTING.get(self)
            if self._cluster_config is not None:
                CLUSTER.get(self)
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
        if self._is_in_actor_callback():
            raise RuntimeError("ActorSystem.stop() cannot be called from an actor callback")
        if timeout is None:
            timeout = self._configured_shutdown_timeout()
        elif (
            not isinstance(timeout, (int, float))
            or isinstance(timeout, bool)
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
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
            preparation_error: BaseException | None = None
            remaining = deadline - monotonic()
            if remaining <= 0:
                preparation_error = TimeoutError(
                    f"Actor system did not stop within {timeout} seconds"
                )
            else:
                try:
                    self._extensions.prepare_stop_all(remaining)
                except BaseException as error:
                    preparation_error = error
            guardian = self._actor_registry.root_guardian
            context = self.get_context(guardian) if guardian is not None else None

            actor_shutdown_error: BaseException | None = None
            if guardian is not None and context is not None:
                try:
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
                except BaseException as error:
                    actor_shutdown_error = error
            if actor_shutdown_error is not None:
                if preparation_error is not None:
                    actor_shutdown_error.add_note(
                        f"Extension shutdown preparation also failed: {preparation_error!r}"
                    )
                raise actor_shutdown_error

            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"Actor system did not stop within {timeout} seconds"
                )
            try:
                self._extensions.stop_all(remaining)
            except BaseException as error:
                if preparation_error is not None:
                    error.add_note(
                        f"Extension shutdown preparation also failed: {preparation_error!r}"
                    )
                raise

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
                if preparation_error is not None:
                    raise preparation_error
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
            if preparation_error is not None:
                raise preparation_error
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

    def lookup_actor_by_uid(self, actor_uid: uuid.UUID) -> ActorRef | None:
        return self._actor_registry.get_by_uid(actor_uid)

    def lookup_actor_by_path(self, path: ActorPath | str) -> ActorRef | None:
        return self._actor_registry.get_by_path(path)

    def resolve_actor(
        self, identity: ActorIdentity, path: ActorPath | str
    ) -> ActorRef | None:
        by_identity = self._actor_registry.resolve_identity(identity)
        if by_identity is None:
            return None
        by_path = self._actor_registry.get_by_path(path)
        return by_identity if by_path is by_identity else None

    def admit_remote_message(
        self, identity: ActorIdentity, message: Any, **metadata: Any
    ) -> RemoteAdmissionResult:
        ref = self._actor_registry.resolve_identity(identity)
        with self._cluster_admission_lock:
            cluster_admission = self._cluster_admission
        result = (
            cluster_admission._admit_remote_control(
                identity,
                message,
                metadata.get("association_uid"),
                ref.path.remote_path if ref is not None else None,
            )
            if cluster_admission is not None
            else None
        )
        if result is None:
            if ref is None:
                result = RemoteAdmissionResult.ACTOR_NOT_FOUND
            else:
                result = ref.admit_remote_message(message)
        if result is not RemoteAdmissionResult.ACCEPTED:
            self._publish_dead_letter(
                identity,
                admission_dead_letter_reason(result),
                recipient_path=ref.path.canonical if ref is not None else None,
                **metadata,
            )
        return result

    def resolve_remote_path(
        self, path: ActorPath | str
    ) -> tuple[RemoteAdmissionResult, ActorRef | None]:
        ref = self._actor_registry.get_by_path(path)
        if ref is None:
            return RemoteAdmissionResult.ACTOR_NOT_FOUND, None
        context = self._actor_registry.get(ref)
        if context is None:
            return RemoteAdmissionResult.ACTOR_NOT_FOUND, None
        if context.state.name in ("STOPPING", "STOPPED"):
            return RemoteAdmissionResult.ACTOR_STOPPING, None
        return RemoteAdmissionResult.ACCEPTED, ref

    def _publish_dead_letter(
        self,
        recipient: ActorIdentity,
        reason: DeadLetterReason,
        *,
        message: Any | None = None,
        recipient_path: str | None = None,
        association_uid: uuid.UUID | None = None,
        lane_id: int | None = None,
        lane_sequence: int | None = None,
        serializer_id: int | None = None,
        manifest: str | None = None,
        payload_byte_length: int | None = None,
    ) -> None:
        self._dead_letters.publish(
            DeadLetter(
                recipient=recipient,
                reason=reason,
                message=message,
                recipient_path=recipient_path,
                association_uid=association_uid,
                lane_id=lane_id,
                lane_sequence=lane_sequence,
                serializer_id=serializer_id,
                manifest=manifest,
                payload_byte_length=payload_byte_length,
            )
        )

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
    def incarnation_uid(self) -> ActorSystemIncarnationUid:
        return self._incarnation_uid

    @property
    def dead_letters(self) -> DeadLetterBroker[DeadLetter]:
        return self._dead_letters

    @property
    def remoting(self) -> RemotingExtension | None:
        return self._extensions.find(REMOTING)

    @property
    def cluster(self) -> ClusterExtension | None:
        return self._extensions.find(CLUSTER)

    @property
    def name(self) -> str:
        return self._name

    def _enter_actor_callback(self) -> None:
        self._callback_state.depth = getattr(self._callback_state, "depth", 0) + 1

    def _exit_actor_callback(self) -> None:
        self._callback_state.depth -= 1

    def _is_in_actor_callback(self) -> bool:
        return getattr(self._callback_state, "depth", 0) > 0

    def _submit_callback(self, callback) -> None:
        self._callbacks.submit(callback)

    @staticmethod
    def _submit_completion(callback) -> None:
        callback()
