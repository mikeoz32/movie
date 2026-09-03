from __future__ import annotations

import uuid
from dataclasses import dataclass

ActorSystemIncarnationUid = uuid.UUID


def new_incarnation_uid() -> ActorSystemIncarnationUid:
    """Create a nonzero 128-bit UID for one actor system incarnation."""
    while (uid := uuid.uuid4()).int == 0:
        pass
    return uid


def new_actor_uid() -> uuid.UUID:
    while (uid := uuid.uuid4()).int == 0:
        pass
    return uid


@dataclass(frozen=True, slots=True)
class ActorIdentity:
    system_incarnation_uid: ActorSystemIncarnationUid
    actor_uid: uuid.UUID

    def __post_init__(self) -> None:
        if (
            not isinstance(self.system_incarnation_uid, uuid.UUID)
            or self.system_incarnation_uid.int == 0
        ):
            raise ValueError("Actor system incarnation UID must be a nonzero UUID")
        if not isinstance(self.actor_uid, uuid.UUID) or self.actor_uid.int == 0:
            raise ValueError("Actor UID must be a nonzero UUID")

    @property
    def incarnation_uid(self) -> ActorSystemIncarnationUid:
        return self.system_incarnation_uid
