"""Transport service-provider interface for bounded remoting connections."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Protocol, runtime_checkable
from uuid import UUID

from movie.remoting.errors import RemotingError
from movie.remoting.wire import COMMON_HEADER_SIZE, CONTROL_LANE_ID, StreamKind

_MAX_U16 = (1 << 16) - 1
_MAX_U32 = (1 << 32) - 1
_MAX_U64 = (1 << 64) - 1


class TransportError(RemotingError):
    """Base class for transport failures."""


class TransportClosedError(TransportError):
    """An operation requires a transport connection that is still open."""

    def __init__(self, message: str = "transport connection is closed", *, cause=None) -> None:
        super().__init__(message)
        self.cause = cause


class TransportCapacityError(TransportError):
    """A local bounded queue cannot admit another record."""


class TransportFlowControlError(TransportError):
    """A peer exceeded this connection's inbound record bounds."""


class TransportProtocolError(TransportError, ValueError):
    """Transport configuration or record input violates the transport contract."""


class TransportListenError(TransportError):
    """A transport could not create or maintain a listener."""


class TransportConnectError(TransportError):
    """A transport could not establish an outbound connection."""


def _uint(value: int, maximum: int, field: str, *, minimum: int = 0) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not minimum <= value <= maximum:
        raise TransportProtocolError(f"{field} must be between {minimum} and {maximum}")
    return value


@dataclass(frozen=True, slots=True)
class Endpoint:
    """A configured or bound TCP-style network location."""

    host: str
    port: int

    def __post_init__(self) -> None:
        if not isinstance(self.host, str) or "\0" in self.host:
            raise TransportProtocolError("endpoint host must be a string without NUL")
        _uint(self.port, _MAX_U16, "endpoint port")


@dataclass(frozen=True, slots=True)
class TransportLimits:
    """Complete-record and queue bounds owned by one connection."""

    maximum_record_bytes: int
    outbound_message_limit: int
    outbound_byte_limit: int
    inbound_message_limit: int
    inbound_byte_limit: int

    def __post_init__(self) -> None:
        _uint(
            self.maximum_record_bytes,
            _MAX_U32,
            "maximum record bytes",
            minimum=COMMON_HEADER_SIZE,
        )
        _uint(
            self.outbound_message_limit,
            _MAX_U32,
            "outbound message limit",
        )
        _uint(
            self.outbound_byte_limit,
            _MAX_U64,
            "outbound byte limit",
        )
        _uint(
            self.inbound_message_limit,
            _MAX_U32,
            "inbound message limit",
        )
        _uint(
            self.inbound_byte_limit,
            _MAX_U64,
            "inbound byte limit",
        )


@dataclass(frozen=True, slots=True)
class LogicalChannel:
    """A fair-scheduling key carried over a multiplexed transport stream."""

    kind: StreamKind
    lane_id: int

    def __post_init__(self) -> None:
        if not isinstance(self.kind, StreamKind):
            raise TransportProtocolError("logical channel kind must be a StreamKind value")
        _uint(self.lane_id, _MAX_U16, "logical channel lane ID")
        if self.kind in (StreamKind.CONTROL, StreamKind.MULTIPLEXED):
            if self.lane_id != CONTROL_LANE_ID:
                raise TransportProtocolError(
                    f"{self.kind.name} logical channel lane ID must be 0xffff"
                )
        elif self.kind is StreamKind.DELIVERY_LANE:
            if self.lane_id == CONTROL_LANE_ID:
                raise TransportProtocolError(
                    "DELIVERY_LANE logical channel lane ID cannot be 0xffff"
                )
        else:  # pragma: no cover - protects this SPI if StreamKind grows later.
            raise TransportProtocolError(f"unsupported logical channel kind {self.kind!r}")


@dataclass(frozen=True, slots=True)
class TransportRecord:
    """One complete wire frame and its logical scheduling channel."""

    channel: LogicalChannel
    payload: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.channel, LogicalChannel):
            raise TransportProtocolError("transport record channel must be a LogicalChannel")
        if not isinstance(self.payload, bytes):
            raise TransportProtocolError("transport record payload must be bytes")


class ConnectionState(Enum):
    OPEN = "open"
    READ_FAILED = "read_failed"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class TransportConnectionSnapshot:
    """One lock-consistent view of connection state and bounded queue usage."""

    state: ConnectionState
    cause: TransportError | None
    maximum_record_bytes: int
    pending_outbound_messages: int
    pending_outbound_bytes: int
    pending_inbound_messages: int
    pending_inbound_bytes: int


@runtime_checkable
class TransportConnection(Protocol):
    @property
    def association_uid(self) -> UUID: ...

    def send(self, record: TransportRecord) -> None: ...

    def send_prevalidated(
        self,
        record: TransportRecord,
        message_limit: int,
        byte_limit: int,
    ) -> None: ...

    def send_active(self, record: TransportRecord) -> None: ...

    def send_terminal(self, record: TransportRecord) -> None: ...

    def receive(self, timeout: float | None = None) -> TransportRecord: ...

    def receive_many(
        self,
        max_records: int,
        timeout: float | None = None,
    ) -> list[TransportRecord]: ...

    def activate(self, limits: TransportLimits) -> None: ...

    def set_maximum_record_bytes(self, maximum_record_bytes: int) -> None: ...

    def snapshot(self) -> TransportConnectionSnapshot: ...

    def close(self, timeout: float | None = None) -> None: ...


@runtime_checkable
class TransportListener(Protocol):
    @property
    def endpoint(self) -> Endpoint: ...

    def close(self, timeout: float | None = None) -> None: ...


@runtime_checkable
class Transport(Protocol):
    def listen(
        self,
        endpoint: Endpoint,
        limits: TransportLimits,
        on_connection: Callable[[TransportConnection], None],
    ) -> TransportListener: ...

    def connect(
        self,
        endpoint: Endpoint,
        limits: TransportLimits,
        association_uid: UUID,
        timeout: float | None = None,
    ) -> TransportConnection: ...

    def close(self, timeout: float | None = None) -> None: ...


__all__ = [
    "ConnectionState",
    "Endpoint",
    "LogicalChannel",
    "Transport",
    "TransportCapacityError",
    "TransportClosedError",
    "TransportConnectError",
    "TransportConnection",
    "TransportConnectionSnapshot",
    "TransportError",
    "TransportFlowControlError",
    "TransportLimits",
    "TransportListenError",
    "TransportListener",
    "TransportProtocolError",
    "TransportRecord",
]
