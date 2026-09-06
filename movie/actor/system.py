# Public API
from __future__ import annotations

import sys
import uuid
from concurrent.futures import Future
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, Union

from movie.actor.behaviour import AbstractBehavior
from movie.actor.context import ActorContext
from movie.actor.dead_letter import DeadLetter, DeadLetterBroker, RemoteAdmissionResult
from movie.actor.extension import Extension, ExtensionId
from movie.actor.identity import ActorIdentity, ActorSystemIncarnationUid
from movie.actor.message import MessageType
from movie.actor.path import ActorPath
from movie.actor.ref import ActorRef, InternalActorRef
from movie.config import Config

if TYPE_CHECKING:
    from movie.cluster.config import ClusterConfig
    from movie.cluster.extension import ClusterExtension
    from movie.remoting.config import RemotingConfig
    from movie.remoting.extension import RemotingExtension


E = TypeVar("E", bound=Extension)


class ClassLoader:
    @staticmethod
    def load_class(class_path: str) -> type:
        components = class_path.split(".")
        module_path = ".".join(components[:-1])
        class_name = components[-1]
        module = __import__(module_path, fromlist=[class_name])
        return getattr(module, class_name)


class ActorSystem(ActorRef[MessageType], Protocol):
    @dataclass(frozen=True)
    class PreStart: ...

    @dataclass(frozen=True)
    class PostStop:
        """
        System message indicating that the actor is stopped.
        Is sent from parent to child actors.
        """

    class Stop: ...

    class Terminate: ...

    class Restart: ...

    @dataclass(frozen=True)
    class Terminated:
        """
        System message indicating that an actor has terminated.
        Is sent to watchers of the terminated actor.
        """

        ref: ActorRef

    @dataclass(frozen=True)
    class Failed:
        """
        System message indicating that an actor has failed.
        Is sent to supervisors of the failed actor.
        """

        ref: ActorRef
        exception: Exception

    SystemMessage = Union[
        PreStart,
        PostStop,
        Terminated,
        Failed,
        Stop,
        Terminate,
        Restart,
    ]

    _impl: "type[ActorSystem] | None" = None

    def __init__(self, behavior: AbstractBehavior[MessageType], name: str) -> None: ...

    def start(self) -> None: ...

    def stop(self, timeout: float | None = None) -> None: ...

    @property
    def config(self) -> Config: ...

    @property
    def actor_count(self) -> int: ...

    @property
    def incarnation_uid(self) -> ActorSystemIncarnationUid: ...

    @property
    def dead_letters(self) -> DeadLetterBroker[DeadLetter]: ...

    @property
    def remoting(self) -> RemotingExtension | None: ...

    @property
    def cluster(self) -> ClusterExtension | None: ...

    def extension(self, extension_id: ExtensionId[E]) -> E: ...

    @staticmethod
    def create(
        behavior: AbstractBehavior[MessageType],
        name: str,
        *,
        config: Config | None = None,
        remoting: RemotingConfig | None = None,
        cluster: ClusterConfig | None = None,
    ) -> "ActorSystem":
        is_gil_enabled = getattr(sys, "_is_gil_enabled", lambda: True)
        if (
            sys.implementation.name != "cpython"
            or sys.version_info[:2] != (3, 14)
            or is_gil_enabled()
        ):
            raise RuntimeError(
                "Movie requires free-threaded CPython 3.14t"
            )
        if ActorSystem._impl is None:
            default = "movie.actor.impl.system.ActorSystemImpl"
            try:
                ActorSystem._impl = ClassLoader.load_class(default)
            except ImportError as e:
                raise NotImplementedError("No ActorSystem implementation available", e)

        if cluster is not None and remoting is None:
            raise ValueError("cluster membership requires remoting configuration")
        if cluster is not None:
            system = ActorSystem._impl(
                behavior,
                name,
                config=config,
                remoting=remoting,
                cluster=cluster,
            )
        elif remoting is None:
            system = ActorSystem._impl(behavior, name, config=config)
        else:
            system = ActorSystem._impl(
                behavior,
                name,
                config=config,
                remoting=remoting,
            )
        system.start()
        return system

    def spawn(
        self,
        behavior: AbstractBehavior[Any],
        name: str,
        *,
        parent: ActorContext | None = None,
    ) -> ActorRef[MessageType]: ...

    def terminate(self, ref: ActorRef[Any]) -> None: ...

    def wait_for_actor_start(
        self, ref: ActorRef[Any], timeout: float | None = None
    ) -> None: ...

    def actor_start_future(self, ref: ActorRef[Any]) -> Future[None]: ...

    def actor_stop_future(self, ref: ActorRef[Any]) -> Future[None]: ...

    def _submit_completion(self, callback) -> None: ...

    def _submit_callback(self, callback) -> None: ...


class ExtendedActorSystem(ActorSystem[MessageType], Protocol):
    """
    Extended api for extensions
    """

    pass


# Internal API
class InternalActorSystem(ExtendedActorSystem[MessageType], Protocol):
    def spawn_system(
        self,
        behavior: AbstractBehavior[Any],
        name: str,
        *,
        parent: ActorContext | None = None,
    ) -> InternalActorRef[MessageType]: ...

    def lookup_actor_by_uid(self, actor_uid: uuid.UUID) -> ActorRef[Any] | None: ...

    def lookup_actor_by_path(self, path: ActorPath | str) -> ActorRef[Any] | None: ...

    def resolve_actor(
        self, identity: ActorIdentity, path: ActorPath | str
    ) -> ActorRef[Any] | None: ...

    def admit_remote_message(
        self, identity: ActorIdentity, message: Any, **metadata: Any
    ) -> RemoteAdmissionResult: ...

    def resolve_remote_path(
        self, path: ActorPath | str
    ) -> tuple[RemoteAdmissionResult, ActorRef[Any] | None]: ...
