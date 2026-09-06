"""Immutable public values for volatile cluster membership."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from uuid import UUID

from movie.remoting.transport import Endpoint

_CLUSTER_NAME = re.compile(r"[A-Za-z0-9._~-]{1,255}\Z")


def _validate_name(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.isascii() or _CLUSTER_NAME.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be 1-255 ASCII URI-unreserved characters")


class MemberStatus(Enum):
    JOINING = "joining"
    UP = "up"
    LEAVING = "leaving"
    LEFT = "left"


class Reachability(Enum):
    REACHABLE = "reachable"
    UNREACHABLE = "unreachable"


@dataclass(frozen=True, slots=True)
class MemberIdentity:
    system_name: str
    incarnation_uid: UUID

    def __post_init__(self) -> None:
        _validate_name(self.system_name, "actor-system name")
        if not isinstance(self.incarnation_uid, UUID) or self.incarnation_uid.int == 0:
            raise ValueError("actor system incarnation UID must be a nonzero UUID")


@dataclass(frozen=True, slots=True)
class ClusterMember:
    identity: MemberIdentity
    endpoint: Endpoint
    status: MemberStatus
    reachability: Reachability

    def __post_init__(self) -> None:
        if type(self.identity) is not MemberIdentity:
            raise ValueError("cluster member identity must be a MemberIdentity")
        if type(self.endpoint) is not Endpoint:
            raise ValueError("cluster member endpoint must be an Endpoint")
        if type(self.status) is not MemberStatus:
            raise ValueError("cluster member status must be a MemberStatus")
        if type(self.reachability) is not Reachability:
            raise ValueError("cluster member reachability must be a Reachability")
        if self.endpoint.port <= 0:
            raise ValueError("cluster member endpoint port must be positive")


@dataclass(frozen=True, slots=True)
class MembershipSnapshot:
    cluster_name: str
    revision: int
    self_identity: MemberIdentity
    members: tuple[ClusterMember, ...]

    def __post_init__(self) -> None:
        _validate_name(self.cluster_name, "cluster name")
        if not isinstance(self.revision, int) or isinstance(self.revision, bool):
            raise ValueError("membership revision must be a nonnegative integer")
        if self.revision < 0:
            raise ValueError("membership revision must be a nonnegative integer")
        if type(self.self_identity) is not MemberIdentity:
            raise ValueError("self identity must be a MemberIdentity")
        if type(self.members) is not tuple:
            raise ValueError("membership members must be a tuple")

        identities: set[MemberIdentity] = set()
        active_names: set[str] = set()
        for member in self.members:
            if type(member) is not ClusterMember:
                raise ValueError("membership members must be ClusterMember values")
            if member.identity in identities:
                raise ValueError("membership member identities must be unique")
            identities.add(member.identity)
            if member.status is not MemberStatus.LEFT:
                if member.identity.system_name in active_names:
                    raise ValueError(
                        "membership cannot contain multiple active incarnations "
                        "with one actor-system name"
                    )
                active_names.add(member.identity.system_name)
        if self.self_identity not in identities:
            raise ValueError("membership snapshot must contain self identity")

        members = tuple(
            sorted(
                self.members,
                key=lambda member: (
                    member.identity.system_name,
                    member.identity.incarnation_uid.bytes,
                ),
            )
        )
        object.__setattr__(self, "members", members)


__all__ = [
    "ClusterMember",
    "MemberIdentity",
    "MemberStatus",
    "MembershipSnapshot",
    "Reachability",
]
