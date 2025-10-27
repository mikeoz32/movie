from __future__ import annotations
from typing import Any, Generic, Protocol, TYPE_CHECKING
from movie.actor.message import MessageType

if TYPE_CHECKING:
    from movie.actor.behaviour import AbstractBehavior
    from movie.actor.logger import ActorLogger
    from movie.actor.ref import ActorRef
    from movie.actor.system import ActorSystem

# Public api


class ActorContext(Protocol, Generic[MessageType]):
    def get_self(self) -> ActorRef[MessageType]: ...

    def get_system(self) -> ActorSystem: ...

    def spawn(self, behavior: AbstractBehavior, name: str) -> ActorRef: ...

    # Invokes behavior with message
    def invoke(self, message: MessageType) -> None: ...
    def invoke_system(self, message: ActorSystem.SystemMessage) -> None: ...

    @property
    def log(self) -> ActorLogger: ...


# Internal api


class InternalActorContext(ActorContext[MessageType], Protocol):
    def attach_child(self, child: "InternalActorContext") -> None: ...
    def set_parent(self, parent: ActorRef[Any]) -> None: ...
