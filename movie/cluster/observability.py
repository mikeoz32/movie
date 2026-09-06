"""Bounded local observability for one cluster runtime."""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from queue import Empty
from threading import Lock
from time import time

from movie.cluster.model import ClusterMember


class ClusterEventKind(Enum):
    MEMBER_UP = "member_up"
    MEMBER_LEAVING = "member_leaving"
    MEMBER_LEFT = "member_left"
    MEMBER_REACHABLE = "member_reachable"
    MEMBER_UNREACHABLE = "member_unreachable"


@dataclass(frozen=True, kw_only=True, slots=True)
class ClusterEvent:
    kind: ClusterEventKind
    member: ClusterMember
    revision: int
    timestamp: float = field(default_factory=time)

    def __post_init__(self) -> None:
        if type(self.kind) is not ClusterEventKind:
            raise ValueError("cluster event kind must be a ClusterEventKind")
        if type(self.member) is not ClusterMember:
            raise ValueError("cluster event member must be a ClusterMember")
        if (
            not isinstance(self.revision, int)
            or isinstance(self.revision, bool)
            or self.revision < 0
        ):
            raise ValueError("cluster event revision must be a nonnegative integer")
        if (
            not isinstance(self.timestamp, (int, float))
            or isinstance(self.timestamp, bool)
            or not math.isfinite(self.timestamp)
        ):
            raise ValueError("cluster event timestamp must be finite")
        object.__setattr__(self, "timestamp", float(self.timestamp))


class ClusterEventSubscription:
    def __init__(self, stream: ClusterEvents, cursor: int) -> None:
        self._stream = stream
        self._cursor = cursor
        self._dropped = 0
        self._closed = False

    def poll(self) -> ClusterEvent | None:
        return self._stream._poll(self)

    def get_nowait(self) -> ClusterEvent:
        event = self.poll()
        if event is None:
            raise Empty
        return event

    def drain(self, limit: int | None = None) -> list[ClusterEvent]:
        if limit is not None and (
            not isinstance(limit, int) or isinstance(limit, bool) or limit < 0
        ):
            raise ValueError("Cluster event drain limit must be a nonnegative integer")
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

    def __enter__(self) -> ClusterEventSubscription:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class ClusterEvents:
    """Bounded event window that never backpressures cluster operations."""

    def __init__(self, capacity: int, max_subscriptions: int) -> None:
        if (
            not isinstance(capacity, int)
            or isinstance(capacity, bool)
            or capacity <= 0
            or not isinstance(max_subscriptions, int)
            or isinstance(max_subscriptions, bool)
            or max_subscriptions <= 0
        ):
            raise ValueError(
                "Cluster event capacity and subscription limit must be positive integers"
            )
        self._capacity = capacity
        self._max_subscriptions = max_subscriptions
        self._entries: deque[tuple[int, ClusterEvent]] = deque(maxlen=capacity)
        self._next_sequence = 0
        self._subscriptions: set[ClusterEventSubscription] = set()
        self._closed = False
        self._lock = Lock()

    def subscribe(self) -> ClusterEventSubscription:
        with self._lock:
            if self._closed:
                raise RuntimeError("Cluster event stream is closed")
            if len(self._subscriptions) >= self._max_subscriptions:
                raise RuntimeError("Cluster event subscription limit exceeded")
            subscription = ClusterEventSubscription(self, self._next_sequence)
            self._subscriptions.add(subscription)
            return subscription

    def _publish(self, event: ClusterEvent) -> bool:
        with self._lock:
            if self._closed:
                return False
            self._entries.append((self._next_sequence, event))
            self._next_sequence += 1
            return True

    def close(self) -> None:
        with self._lock:
            self._closed = True

    def _poll(self, subscription: ClusterEventSubscription) -> ClusterEvent | None:
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

    def _advance_cursor(self, subscription: ClusterEventSubscription) -> None:
        if not self._entries:
            return
        first_sequence = self._entries[0][0]
        if subscription._cursor < first_sequence:
            subscription._dropped += first_sequence - subscription._cursor
            subscription._cursor = first_sequence

    def _unsubscribe(self, subscription: ClusterEventSubscription) -> None:
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
    "ClusterEvent",
    "ClusterEventKind",
    "ClusterEventSubscription",
    "ClusterEvents",
]
