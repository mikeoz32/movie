# Public API
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

    SystemMessage = Union[PreStart, PostStop, Terminated, Failed, Stop, Terminate]

    _impl: "type[ActorSystem] | None" = None

    def __init__(self, behavior: AbstractBehavior[MessageType], name: str) -> None: ...

    def start(self) -> None: ...

    def stop(self) -> None: ...

    @property
    def config(self) -> Config: ...

    @staticmethod
    def create(behavior: AbstractBehavior[MessageType], name: str) -> "ActorSystem":
        if ActorSystem._impl is None:
            default = "movie.actor.impl.system.ActorSystemImpl"
            try:
                ActorSystem._impl = ClassLoader.load_class(default)
            except ImportError as e:
                raise NotImplementedError("No ActorSystem implementation available", e)

        system = ActorSystem._impl(behavior, name)
        system.start()
        return system

    def spawn(
        self,
        behavior: AbstractBehavior[Any],
        name: str,
        *,
        parent: ActorContext | None = None,
    ) -> ActorRef[MessageType]: ...


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
