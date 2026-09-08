from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

S = TypeVar("S")


@dataclass(frozen=True, slots=True)
class EncodedState:
    manifest: str
    payload: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.manifest, str) or not self.manifest:
            raise ValueError("State manifest must be a nonempty string")
        if "\x00" in self.manifest:
            raise ValueError("State manifest must not contain NUL characters")
        if type(self.payload) is not bytes:
            raise ValueError("State payload must be bytes")


class StateCodec(Protocol, Generic[S]):
    """Stable codec with non-mutating encode and independent decode operations."""

    def encode(self, state: S) -> EncodedState: ...

    def decode(self, manifest: str, payload: bytes) -> S: ...


__all__ = ["EncodedState", "StateCodec"]
