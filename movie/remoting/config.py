"""Immutable public configuration for one remoting runtime."""

from __future__ import annotations

import ipaddress
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from movie.remoting.errors import ProtocolValidationError
from movie.remoting.serialization import SerializerRegistry
from movie.remoting.transport import Endpoint, Transport, TransportLimits
from movie.remoting.wire import MAX_U16, MINIMUM_GOAWAY_FRAME_BYTES

_REMOTE_NAME = re.compile(r"[A-Za-z0-9._~-]{1,255}\Z")
_HOST = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?\Z")


def _default_limits() -> TransportLimits:
    return TransportLimits(
        maximum_record_bytes=1 << 20,
        outbound_message_limit=1_024,
        outbound_byte_limit=16 << 20,
        inbound_message_limit=1_024,
        inbound_byte_limit=16 << 20,
    )


def _validate_remote_name(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.isascii() or _REMOTE_NAME.fullmatch(value) is None:
        raise ProtocolValidationError(
            f"{field_name} must be 1-255 ASCII URI-unreserved characters"
        )


def _validate_host(host: str) -> None:
    if not isinstance(host, str) or not host or not host.isascii():
        raise ProtocolValidationError("endpoint host must be nonempty canonical ASCII text")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None
    if address is not None and address.is_unspecified:
        raise ProtocolValidationError(
            "endpoint host must be advertised, not an unspecified bind address"
        )
    if ":" in host:
        try:
            canonical = str(ipaddress.IPv6Address(host))
        except ValueError as error:
            raise ProtocolValidationError("endpoint host is not canonical IPv6") from error
        if canonical != host:
            raise ProtocolValidationError("endpoint host is not canonical IPv6")
    elif _HOST.fullmatch(host) is None:
        raise ProtocolValidationError("endpoint host is not a canonical DNS or IPv4 host")


@dataclass(frozen=True, slots=True)
class RemotingConfig:
    """Explicit endpoint, peer allowlist, serializers, and bounded limits."""

    local: Endpoint
    peers: Mapping[str, Endpoint]
    serializers: SerializerRegistry
    transport: Transport | None = None
    limits: TransportLimits = field(default_factory=_default_limits)
    lane_count: int = 4
    association_timeout: float = 5.0
    pending_association_limit: int = 64
    association_history_limit: int = 64
    health_event_capacity: int = 1_000
    health_event_max_subscriptions: int = 1_000

    def __post_init__(self) -> None:
        if not isinstance(self.local, Endpoint):
            raise ProtocolValidationError("local endpoint must be an Endpoint")
        _validate_host(self.local.host)
        if not isinstance(self.serializers, SerializerRegistry):
            raise ProtocolValidationError("serializers must be an immutable SerializerRegistry")
        if self.transport is not None and not isinstance(self.transport, Transport):
            raise ProtocolValidationError("transport must implement the Transport SPI")
        if not isinstance(self.limits, TransportLimits):
            raise ProtocolValidationError("limits must be a TransportLimits value")
        if any(
            value <= 0
            for value in (
                self.limits.maximum_record_bytes,
                self.limits.outbound_message_limit,
                self.limits.outbound_byte_limit,
                self.limits.inbound_message_limit,
                self.limits.inbound_byte_limit,
            )
        ):
            raise ProtocolValidationError("all remoting limits must be positive")
        if self.limits.maximum_record_bytes < MINIMUM_GOAWAY_FRAME_BYTES:
            raise ProtocolValidationError(
                "maximum record bytes cannot encode the minimum GOAWAY frame"
            )
        if (
            not isinstance(self.lane_count, int)
            or isinstance(self.lane_count, bool)
            or not 1 <= self.lane_count <= MAX_U16
        ):
            raise ProtocolValidationError(f"lane count must be between 1 and {MAX_U16}")
        for value, field_name in (
            (self.pending_association_limit, "pending association limit"),
            (self.association_history_limit, "association history limit"),
            (self.health_event_capacity, "health event capacity"),
            (
                self.health_event_max_subscriptions,
                "health event subscription limit",
            ),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ProtocolValidationError(f"{field_name} must be positive")
        if (
            not isinstance(self.association_timeout, (int, float))
            or isinstance(self.association_timeout, bool)
            or not math.isfinite(self.association_timeout)
            or self.association_timeout <= 0
        ):
            raise ProtocolValidationError("association timeout must be a finite positive number")

        try:
            peer_items = tuple(self.peers.items())
        except AttributeError as error:
            raise ProtocolValidationError(
                "peers must be a mapping keyed by actor-system name"
            ) from error
        copied: dict[str, Endpoint] = {}
        endpoints: set[Endpoint] = set()
        for system_name, endpoint in peer_items:
            _validate_remote_name(system_name, "peer actor-system name")
            if system_name in copied:
                raise ProtocolValidationError(
                    f"peer actor-system name {system_name!r} is duplicated"
                )
            if not isinstance(endpoint, Endpoint):
                raise ProtocolValidationError("peer endpoint must be an Endpoint")
            _validate_host(endpoint.host)
            if endpoint.port == 0:
                raise ProtocolValidationError("peer endpoint port must be positive")
            if endpoint in endpoints:
                raise ProtocolValidationError("peer endpoints must be unique")
            if self.local.port and endpoint == self.local:
                raise ProtocolValidationError("local and peer endpoints must be unique")
            copied[system_name] = endpoint
            endpoints.add(endpoint)
        object.__setattr__(self, "peers", MappingProxyType(copied))
        object.__setattr__(self, "association_timeout", float(self.association_timeout))


__all__ = ["RemotingConfig"]
