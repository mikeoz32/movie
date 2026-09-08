from __future__ import annotations

from dataclasses import dataclass


def _require_name(value: object, field: str) -> None:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError(f"{field} must be a nonempty string without NUL characters")
    if len(value.encode("utf-8")) > 512:
        raise ValueError(f"{field} must not exceed 512 UTF-8 bytes")


@dataclass(frozen=True, slots=True)
class ProjectionId:
    name: str
    key: str

    def __post_init__(self) -> None:
        _require_name(self.name, "Projection name")
        _require_name(self.key, "Projection key")

    def __str__(self) -> str:
        return f"{self.name}:{self.key}"


__all__ = ["ProjectionId"]
