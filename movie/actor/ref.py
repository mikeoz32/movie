from __future__ import annotations
from typing import Generic, Protocol
import uuid

from movie.actor.message import MessageType

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from movie.actor.path import ActorPath
    from movie.actor import ActorSystem


class ActorRef(Protocol, Generic[MessageType]):
    def tell(self, message: MessageType) -> None: ...

    @property
    def id(self) -> uuid.UUID: ...

    @property
    def name(self) -> str: ...

    @property
    def path(self) -> ActorPath: ...


class InternalActorRef(ActorRef[MessageType], Protocol):
    def tell_system(self, message: ActorSystem.SystemMessage) -> None:
        """
        Send a system message to the actor.
        Used for internal actor system operations.
        """
        ...
