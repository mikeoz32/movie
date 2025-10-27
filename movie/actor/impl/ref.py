from threading import RLock
from typing import TYPE_CHECKING
import uuid

from movie.actor import ActorRef
from movie.actor.message import MessageType
from movie.actor.system import ActorSystem

if TYPE_CHECKING:
    from movie.actor.impl.system import ActorSystemImpl


class LocalActorRef(ActorRef[MessageType]):
    def __init__(self, system: ActorSystemImpl, path: str) -> None:
        self._lock = RLock()
        self._system = system
        self._path = path
        self._id = uuid.uuid4()

    def tell(self, message: MessageType) -> None:
        with self._lock:
            context = self._system.get_context(self)
            if context is not None:
                if context._mailbox is not None:
                    context._mailbox.send(message)

    def tell_system(self, message: ActorSystem.SystemMessage) -> None:
        with self._lock:
            context = self._system.get_context(self)
            if context is not None:
                if context._mailbox is not None:
                    context._mailbox.sendSystem(message)

    @property
    def id(self) -> uuid.UUID:
        return self._id
