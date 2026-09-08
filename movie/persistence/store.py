from __future__ import annotations

from concurrent.futures import Future
from typing import Protocol

from movie.persistence.model import (
    DurableStateChangeBatch,
    DurableStateRecord,
    OperationId,
    PersistenceId,
    WriteResult,
)


class DurableStateStore(Protocol):
    def load(self, persistence_id: PersistenceId) -> Future[DurableStateRecord | None]: ...

    def upsert(
        self,
        persistence_id: PersistenceId,
        *,
        expected_revision: int,
        operation_id: OperationId,
        manifest: str,
        payload: bytes,
    ) -> Future[WriteResult]: ...

    def delete(
        self,
        persistence_id: PersistenceId,
        *,
        expected_revision: int,
        operation_id: OperationId,
    ) -> Future[WriteResult]: ...

    def changes(
        self,
        entity_type: str,
        *,
        min_slice: int,
        max_slice: int,
        after_offset: int,
        limit: int,
        max_bytes: int | None = None,
        scan_limit: int = 10_000,
    ) -> Future[DurableStateChangeBatch]: ...

    def compact_changes(self, limit: int = 1_000) -> Future[int]: ...


__all__ = ["DurableStateStore"]
