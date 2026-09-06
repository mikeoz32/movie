"""Immutable public configuration for one cluster runtime."""

from __future__ import annotations

import math
from dataclasses import dataclass
from hashlib import sha256

from movie.cluster.model import _validate_name
from movie.remoting.transport import Endpoint

_MAX_U32 = (1 << 32) - 1


@dataclass(frozen=True, slots=True)
class SeedContact:
    system_name: str
    endpoint: Endpoint

    def __post_init__(self) -> None:
        _validate_name(self.system_name, "seed actor-system name")
        if type(self.endpoint) is not Endpoint:
            raise ValueError("seed endpoint must be an Endpoint")
        if self.endpoint.port <= 0:
            raise ValueError("seed endpoint port must be positive")


@dataclass(frozen=True, slots=True)
class ClusterConfig:
    name: str
    seed: SeedContact
    heartbeat_interval: float = 0.25
    unreachable_timeout: float = 1.0
    join_timeout: float = 5.0
    reassociation_timeout: float = 0.25
    member_limit: int = 256
    retired_identity_limit: int = 4_096
    control_queue_capacity: int = 1_024
    event_capacity: int = 1_000
    event_max_subscriptions: int = 1_000
    serializer_id: int = 0x4D4F5601

    def __post_init__(self) -> None:
        _validate_name(self.name, "cluster name")
        if type(self.seed) is not SeedContact:
            raise ValueError("cluster seed must be a SeedContact")

        timings = (
            (self.heartbeat_interval, "heartbeat interval"),
            (self.unreachable_timeout, "unreachable timeout"),
            (self.join_timeout, "join timeout"),
            (self.reassociation_timeout, "reassociation timeout"),
        )
        for value, field_name in timings:
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"{field_name} must be a finite positive number")
        if self.unreachable_timeout <= self.heartbeat_interval:
            raise ValueError("unreachable timeout must be greater than heartbeat interval")

        capacities = (
            (self.member_limit, "member limit"),
            (self.retired_identity_limit, "retired identity limit"),
            (self.control_queue_capacity, "control queue capacity"),
            (self.event_capacity, "event capacity"),
            (self.event_max_subscriptions, "event subscription limit"),
        )
        for value, field_name in capacities:
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{field_name} must be a positive integer")
        if self.member_limit > 256:
            raise ValueError("member limit cannot exceed the v1 protocol limit of 256")
        if (
            not isinstance(self.serializer_id, int)
            or isinstance(self.serializer_id, bool)
            or not 1 <= self.serializer_id <= _MAX_U32
        ):
            raise ValueError(f"serializer ID must be between 1 and {_MAX_U32}")

        object.__setattr__(self, "heartbeat_interval", float(self.heartbeat_interval))
        object.__setattr__(self, "unreachable_timeout", float(self.unreachable_timeout))
        object.__setattr__(self, "join_timeout", float(self.join_timeout))
        object.__setattr__(self, "reassociation_timeout", float(self.reassociation_timeout))


def _compatibility_fingerprint(config: ClusterConfig) -> str:
    fields = (
        "movie-cluster-v1",
        config.name,
        config.seed.system_name,
        config.seed.endpoint.host,
        str(config.seed.endpoint.port),
        config.heartbeat_interval.hex(),
        config.unreachable_timeout.hex(),
        str(config.member_limit),
        str(config.retired_identity_limit),
        str(config.serializer_id),
    )
    return sha256("\0".join(fields).encode("ascii")).hexdigest()


__all__ = ["ClusterConfig", "SeedContact"]
