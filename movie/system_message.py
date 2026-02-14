from __future__ import annotations

from dataclasses import dataclass
from typing import Union

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from movie.actor import ActorRef


@dataclass(frozen=True)
class PostStop:
    """
    System message indicating that the actor is stopped.
    Is sent from parent to child actors.
    """

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


SystemMessage = Union[PostStop, Terminated, Failed]
