"""Identity-bound remote actor references."""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic
from uuid import UUID

from movie.actor.identity import ActorIdentity
from movie.actor.message import MessageType
from movie.actor.path import ActorPath
from movie.actor.ref import ActorRef

if TYPE_CHECKING:
    from movie.remoting.runtime import RemotingRuntime


class RemoteActorRef(ActorRef[MessageType], Generic[MessageType]):
    """A manager-backed capability for one immutable remote actor identity."""

    __slots__ = ("_identity", "_path", "_runtime", "_system_name")

    def __init__(
        self,
        runtime: RemotingRuntime,
        system_name: str,
        identity: ActorIdentity,
        path: ActorPath,
    ) -> None:
        self._runtime = runtime
        self._system_name = system_name
        self._identity = identity
        self._path = path

    def tell(self, message: MessageType) -> None:
        self._runtime._tell(self, message)

    @property
    def id(self) -> UUID:
        return self._identity.actor_uid

    @property
    def identity(self) -> ActorIdentity:
        return self._identity

    @property
    def name(self) -> str:
        return self._path.name

    @property
    def path(self) -> ActorPath:
        return self._path

    @property
    def system_name(self) -> str:
        return self._system_name


__all__ = ["RemoteActorRef"]
