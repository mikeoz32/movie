from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from uuid import UUID, uuid4

PERSISTENCE_SLICE_COUNT = 1_024


def _require_name(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError(f"{field} must be a nonempty string without NUL characters")
    if len(value.encode("utf-8")) > 512:
        raise ValueError(f"{field} must not exceed 512 UTF-8 bytes")
    return value


@dataclass(frozen=True, slots=True)
class PersistenceId:
    entity_type: str
    entity_id: str

    def __post_init__(self) -> None:
        _require_name(self.entity_type, "Persistence entity type")
        _require_name(self.entity_id, "Persistence entity id")

    def __str__(self) -> str:
        return f"{self.entity_type}:{self.entity_id}"


@dataclass(frozen=True, slots=True)
class OperationId:
    value: UUID

    def __post_init__(self) -> None:
        if type(self.value) is not UUID:
            raise ValueError("Operation identity must be a UUID")

    @classmethod
    def random(cls) -> OperationId:
        return cls(uuid4())

    def __str__(self) -> str:
        return str(self.value)


def persistence_slice(persistence_id: PersistenceId) -> int:
    if type(persistence_id) is not PersistenceId:
        raise ValueError("persistence_slice requires a PersistenceId")
    digest = sha256()
    entity_type = persistence_id.entity_type.encode("utf-8")
    entity_id = persistence_id.entity_id.encode("utf-8")
    digest.update(len(entity_type).to_bytes(2, "big"))
    digest.update(entity_type)
    digest.update(entity_id)
    return int.from_bytes(digest.digest()[:4], "big") % PERSISTENCE_SLICE_COUNT


@dataclass(frozen=True, slots=True)
class DurableStateRecord:
    persistence_id: PersistenceId
    revision: int
    operation_id: OperationId
    manifest: str | None
    payload: bytes | None
    deleted: bool

    def __post_init__(self) -> None:
        if type(self.persistence_id) is not PersistenceId:
            raise ValueError("Durable State record requires a PersistenceId")
        if not isinstance(self.revision, int) or isinstance(self.revision, bool):
            raise ValueError("Durable State revision must be a positive integer")
        if self.revision <= 0:
            raise ValueError("Durable State revision must be a positive integer")
        if type(self.operation_id) is not OperationId:
            raise ValueError("Durable State record requires an OperationId")
        if type(self.deleted) is not bool:
            raise ValueError("Durable State deleted flag must be boolean")
        if self.deleted:
            if self.manifest is not None or self.payload is not None:
                raise ValueError("Tombstone must not contain a manifest or payload")
            return
        if not isinstance(self.manifest, str) or not self.manifest or self.payload is None:
            raise ValueError("Live Durable State requires a manifest and payload")
        if "\x00" in self.manifest or type(self.payload) is not bytes:
            raise ValueError("Live Durable State has an invalid manifest or payload")


@dataclass(frozen=True, slots=True)
class WriteResult:
    revision: int
    duplicate: bool = False

    def __post_init__(self) -> None:
        if (
            not isinstance(self.revision, int)
            or isinstance(self.revision, bool)
            or self.revision <= 0
        ):
            raise ValueError("Write result revision must be a positive integer")
        if type(self.duplicate) is not bool:
            raise ValueError("Write result duplicate flag must be boolean")


@dataclass(frozen=True, slots=True)
class DurableStateChange:
    offset: int
    persistence_id: PersistenceId
    revision: int
    operation_id: OperationId
    slice: int
    manifest: str | None
    payload: bytes | None
    deleted: bool
    committed_at_ns: int

    def __post_init__(self) -> None:
        if not isinstance(self.offset, int) or isinstance(self.offset, bool) or self.offset <= 0:
            raise ValueError("Durable State Change offset must be a positive integer")
        if (
            not isinstance(self.slice, int)
            or isinstance(self.slice, bool)
            or not 0 <= self.slice < PERSISTENCE_SLICE_COUNT
        ):
            raise ValueError("Durable State Change slice is outside the persistence range")
        if (
            not isinstance(self.committed_at_ns, int)
            or isinstance(self.committed_at_ns, bool)
            or self.committed_at_ns <= 0
        ):
            raise ValueError("Durable State Change commit time must be positive")
        DurableStateRecord(
            self.persistence_id,
            self.revision,
            self.operation_id,
            self.manifest,
            self.payload,
            self.deleted,
        )


@dataclass(frozen=True, slots=True)
class DurableStateChangeBatch:
    changes: tuple[DurableStateChange, ...]
    offset: int

    def __post_init__(self) -> None:
        if type(self.changes) is not tuple or not all(
            type(change) is DurableStateChange for change in self.changes
        ):
            raise ValueError("Durable State Change batch requires immutable changes")
        if not isinstance(self.offset, int) or isinstance(self.offset, bool) or self.offset < 0:
            raise ValueError("Projection Offset must be a nonnegative integer")
        if self.changes and self.offset < self.changes[-1].offset:
            raise ValueError("Projection Offset must cover every returned change")


__all__ = [
    "DurableStateRecord",
    "DurableStateChange",
    "DurableStateChangeBatch",
    "OperationId",
    "PERSISTENCE_SLICE_COUNT",
    "PersistenceId",
    "WriteResult",
    "persistence_slice",
]
