"""Bounded local observability for one remoting runtime."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from queue import Empty
from threading import Lock
from time import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from movie.remoting.association import AssociationSnapshot


@dataclass(frozen=True, slots=True)
class RemotingMetrics:
    accepted_delivery_attempts: int = 0
    rejected_delivery_attempts: int = 0
    dead_letter_count: int = 0
    serialization_rejections: int = 0
    deserialization_rejections: int = 0
    sequence_violations: int = 0
    reconnect_count: int = 0


class RemotingHealthEventKind(Enum):
    ASSOCIATION_ACTIVATED = "association_activated"
    ASSOCIATION_CLOSED = "association_closed"
    LISTENER_FAILED = "listener_failed"


@dataclass(frozen=True, kw_only=True, slots=True)
class RemotingHealthEvent:
    kind: RemotingHealthEventKind
    timestamp: float = field(default_factory=time)
    association: AssociationSnapshot | None = None
    failure: BaseException | None = None


class RemotingHealthEventSubscription:
    def __init__(self, stream: RemotingHealthEvents, cursor: int) -> None:
        self._stream = stream
        self._cursor = cursor
        self._dropped = 0
        self._closed = False

    def poll(self) -> RemotingHealthEvent | None:
        return self._stream._poll(self)

    def get_nowait(self) -> RemotingHealthEvent:
        event = self.poll()
        if event is None:
            raise Empty
        return event

    def drain(self, limit: int | None = None) -> list[RemotingHealthEvent]:
        if limit is not None and limit < 0:
            raise ValueError("Remoting health-event drain limit cannot be negative")
        events = []
        while limit is None or len(events) < limit:
            event = self.poll()
            if event is None:
                break
            events.append(event)
        return events

    def close(self) -> None:
        self._stream._unsubscribe(self)

    @property
    def dropped_count(self) -> int:
        with self._stream._lock:
            if not self._closed:
                self._stream._advance_cursor(self)
            return self._dropped

    @property
    def closed(self) -> bool:
        with self._stream._lock:
            return self._closed

    def __enter__(self) -> RemotingHealthEventSubscription:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class RemotingHealthEvents:
    """Bounded event window that never backpressures remoting operations."""

    def __init__(self, capacity: int, max_subscriptions: int) -> None:
        if capacity <= 0 or max_subscriptions <= 0:
            raise ValueError(
                "Remoting health-event capacity and subscription limit must be positive"
            )
        self._capacity = capacity
        self._max_subscriptions = max_subscriptions
        self._entries: deque[tuple[int, RemotingHealthEvent]] = deque(maxlen=capacity)
        self._next_sequence = 0
        self._subscriptions: set[RemotingHealthEventSubscription] = set()
        self._closed = False
        self._lock = Lock()

    def subscribe(self) -> RemotingHealthEventSubscription:
        with self._lock:
            if self._closed:
                raise RuntimeError("Remoting health-event stream is closed")
            if len(self._subscriptions) >= self._max_subscriptions:
                raise RuntimeError("Remoting health-event subscription limit exceeded")
            subscription = RemotingHealthEventSubscription(
                self,
                self._next_sequence,
            )
            self._subscriptions.add(subscription)
            return subscription

    def _publish(self, event: RemotingHealthEvent) -> bool:
        with self._lock:
            if self._closed:
                return False
            self._entries.append((self._next_sequence, event))
            self._next_sequence += 1
            return True

    def close(self) -> None:
        with self._lock:
            self._closed = True

    def _poll(
        self,
        subscription: RemotingHealthEventSubscription,
    ) -> RemotingHealthEvent | None:
        with self._lock:
            if subscription._closed:
                return None
            self._advance_cursor(subscription)
            if not self._entries:
                return None
            first_sequence = self._entries[0][0]
            index = subscription._cursor - first_sequence
            if index >= len(self._entries):
                return None
            sequence, event = self._entries[index]
            subscription._cursor = sequence + 1
            return event

    def _advance_cursor(self, subscription: RemotingHealthEventSubscription) -> None:
        if not self._entries:
            return
        first_sequence = self._entries[0][0]
        if subscription._cursor < first_sequence:
            subscription._dropped += first_sequence - subscription._cursor
            subscription._cursor = first_sequence

    def _unsubscribe(self, subscription: RemotingHealthEventSubscription) -> None:
        with self._lock:
            if subscription._closed:
                return
            self._advance_cursor(subscription)
            subscription._closed = True
            self._subscriptions.discard(subscription)

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def retained_count(self) -> int:
        with self._lock:
            return len(self._entries)

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed


__all__ = [
    "RemotingHealthEvent",
    "RemotingHealthEventKind",
    "RemotingHealthEventSubscription",
    "RemotingHealthEvents",
    "RemotingMetrics",
]
