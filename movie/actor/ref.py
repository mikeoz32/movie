from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Generic, Protocol

from movie.actor.message import MessageType

if TYPE_CHECKING:
    from movie.actor import ActorSystem
    from movie.actor.path import ActorPath


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
