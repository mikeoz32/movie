from __future__ import annotations

from typing import TYPE_CHECKING
import uuid

from movie.actor import ActorRef
from movie.actor.message import MessageType
from movie.actor.path import ActorPath

if TYPE_CHECKING:
    from movie.actor.impl.system import ActorSystemImpl
    from movie.actor.system import ActorSystem


class LocalActorRef(ActorRef[MessageType]):
    def __init__(self, system: ActorSystemImpl, path: ActorPath) -> None:
        self._system = system
        self._path = path
        self._id = uuid.uuid4()

    def tell(self, message: MessageType) -> None:
        context = self._system.get_context(self)
        if context is not None:
            context.send(message)

    def tell_system(self, message: ActorSystem.SystemMessage) -> None:
        context = self._system.get_context(self)
        if context is not None:
            context.send_system(message)

    @property
    def id(self) -> uuid.UUID:
        return self._id

    @property
    def name(self) -> str:
        return self._path.name

    @property
    def path(self) -> ActorPath:
        return self._path
