# Public API
import sys
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Protocol, Union

from movie.actor.behaviour import AbstractBehavior
from movie.actor.context import ActorContext
from movie.actor.message import MessageType
from movie.actor.ref import ActorRef, InternalActorRef
from movie.config import Config


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

    @staticmethod
    def create(
        behavior: AbstractBehavior[MessageType],
        name: str,
        *,
        config: Config | None = None,
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

        system = ActorSystem._impl(behavior, name, config=config)
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

    ...


# Internal API
class InternalActorSystem(ExtendedActorSystem[MessageType], Protocol):
    def spawn_system(
        self,
        behavior: AbstractBehavior[Any],
        name: str,
        *,
        parent: ActorContext | None = None,
    ) -> InternalActorRef[MessageType]: ...
