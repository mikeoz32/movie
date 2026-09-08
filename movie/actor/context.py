from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, Protocol

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

    @property
    def log(self) -> ActorLogger: ...


# Internal api


class ActorBatchFailed(BaseException):
    def __init__(
        self, error: BaseException, remaining: list, *, system: bool
    ) -> None:
        super().__init__(str(error))
        self.error = error
        self.remaining = remaining
        self.system = system


class InternalActorContext(ActorContext[MessageType], Protocol):
    def invoke(self, message: MessageType) -> None: ...
    def invoke_system(self, message: ActorSystem.SystemMessage) -> None: ...
    def invoke_batch(self, messages: list, *, system: bool) -> list: ...
    def can_process_user_messages(self) -> bool: ...
    def attach_child(self, child: "InternalActorContext") -> None: ...
    def set_parent(self, parent: ActorRef[Any]) -> None: ...
    def enqueue_control(self, message: ActorSystem.SystemMessage) -> None: ...
    def suspend_user_messages(self) -> None: ...
    def resume_user_messages(self) -> None: ...
