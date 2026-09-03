from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from queue import Empty
from threading import Lock
from time import time
from typing import Generic, TypeVar
from uuid import UUID

from movie.actor.identity import ActorIdentity

Event = TypeVar("Event")
Message = TypeVar("Message")


class DeadLetterReason(Enum):
    NO_ASSOCIATION = auto()
    STALE_INCARNATION = auto()
    ACTOR_NOT_FOUND = auto()
    ACTOR_STOPPING = auto()
    MAILBOX_FULL = auto()
    DESERIALIZATION_REJECTED = auto()


class RemoteAdmissionResult(Enum):
    ACCEPTED = auto()
    ACTOR_NOT_FOUND = auto()
    ACTOR_STOPPING = auto()
    MAILBOX_FULL = auto()


_ADMISSION_DEAD_LETTER_REASONS = {
    RemoteAdmissionResult.ACTOR_NOT_FOUND: DeadLetterReason.ACTOR_NOT_FOUND,
    RemoteAdmissionResult.ACTOR_STOPPING: DeadLetterReason.ACTOR_STOPPING,
    RemoteAdmissionResult.MAILBOX_FULL: DeadLetterReason.MAILBOX_FULL,
}


def admission_dead_letter_reason(result: RemoteAdmissionResult) -> DeadLetterReason:
    try:
        return _ADMISSION_DEAD_LETTER_REASONS[result]
    except KeyError as error:
        raise ValueError("Accepted admission has no dead-letter reason") from error


@dataclass(frozen=True, kw_only=True, slots=True)
class DeadLetter(Generic[Message]):
    recipient: ActorIdentity
    reason: DeadLetterReason
    message: Message | None = None
    recipient_path: str | None = None
    timestamp: float = field(default_factory=time)
    association_uid: UUID | None = None
    lane_id: int | None = None
    lane_sequence: int | None = None
    serializer_id: int | None = None
    manifest: str | None = None
    payload_byte_length: int | None = None


class DeadLetterSubscription(Generic[Event]):
    def __init__(self, broker: "DeadLetterBroker[Event]", cursor: int) -> None:
        self._broker = broker
        self._cursor = cursor
        self._dropped = 0
        self._closed = False

    def poll(self) -> Event | None:
        return self._broker._poll(self)

    def get_nowait(self) -> Event:
        dead_letter = self.poll()
        if dead_letter is None:
            raise Empty
        return dead_letter

    def drain(self, limit: int | None = None) -> list[Event]:
        if limit is not None and limit < 0:
            raise ValueError("Dead-letter drain limit cannot be negative")
        dead_letters = []
        while limit is None or len(dead_letters) < limit:
            dead_letter = self.poll()
            if dead_letter is None:
                break
            dead_letters.append(dead_letter)
        return dead_letters

    def close(self) -> None:
        self._broker._unsubscribe(self)

    @property
    def dropped_count(self) -> int:
        with self._broker._lock:
            self._broker._advance_cursor(self)
            return self._dropped

    @property
    def closed(self) -> bool:
        with self._broker._lock:
            return self._closed

    def __enter__(self) -> "DeadLetterSubscription[Event]":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class DeadLetterBroker(Generic[Event]):
    """A bounded replay window with nonblocking publication and polling."""

    def __init__(self, capacity: int = 1_000, max_subscriptions: int = 1_000) -> None:
        if capacity <= 0 or max_subscriptions <= 0:
            raise ValueError(
                "Dead-letter capacity and subscription limit must be positive"
            )
        self._capacity = capacity
        self._max_subscriptions = max_subscriptions
        self._entries: deque[tuple[int, Event]] = deque(maxlen=capacity)
        self._next_sequence = 0
        self._subscriptions: set[DeadLetterSubscription[Event]] = set()
        self._closed = False
        self._lock = Lock()

    def publish(self, dead_letter: Event) -> bool:
        with self._lock:
            if self._closed:
                return False
            self._entries.append((self._next_sequence, dead_letter))
            self._next_sequence += 1
            return True

    def subscribe(self) -> DeadLetterSubscription[Event]:
        with self._lock:
            if self._closed:
                raise RuntimeError("Dead-letter broker is closed")
            if len(self._subscriptions) >= self._max_subscriptions:
                raise RuntimeError("Dead-letter subscription limit exceeded")
            subscription = DeadLetterSubscription(self, self._next_sequence)
            self._subscriptions.add(subscription)
            return subscription

    def close(self) -> None:
        with self._lock:
            self._closed = True

    def _poll(self, subscription: DeadLetterSubscription[Event]) -> Event | None:
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
            sequence, dead_letter = self._entries[index]
            subscription._cursor = sequence + 1
            return dead_letter

    def _advance_cursor(self, subscription: DeadLetterSubscription[Event]) -> None:
        if not self._entries:
            return
        first_sequence = self._entries[0][0]
        if subscription._cursor < first_sequence:
            subscription._dropped += first_sequence - subscription._cursor
            subscription._cursor = first_sequence

    def _unsubscribe(self, subscription: DeadLetterSubscription[Event]) -> None:
        with self._lock:
            if subscription._closed:
                return
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
