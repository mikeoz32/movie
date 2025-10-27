from typing import Generic, Protocol
import uuid

from movie.actor.message import MessageType


class ActorRef(Protocol, Generic[MessageType]):
    def tell(self, message: MessageType) -> None: ...

    @property
    def id(self) -> uuid.UUID: ...


class InternalActorRef(ActorRef[MessageType], Protocol):
    def tell_system(self, message: SystemMessage) -> None:
        """
        Send a system message to the actor.
        Used for internal actor system operations.
        """
        ...
