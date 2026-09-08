from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from movie.projection.errors import ProjectionTransactionError


class ProjectionTransaction(Protocol):
    async def execute(
        self,
        statement: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> int: ...

    async def fetchone(
        self,
        statement: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> tuple | None: ...

    async def fetchall(
        self,
        statement: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple]: ...


class _SQLiteProjectionTransaction:
    _MOVIE_TABLES = frozenset(
        {
            "movie_durable_change",
            "movie_durable_operation",
            "movie_durable_state",
            "movie_projection_feed",
            "movie_projection_offset",
        }
    )
    _FORBIDDEN = frozenset(
        {
            "ATTACH",
            "BEGIN",
            "COMMIT",
            "DETACH",
            "END",
            "PRAGMA",
            "RELEASE",
            "ROLLBACK",
            "SAVEPOINT",
            "VACUUM",
        }
    )

    def __init__(self, connection) -> None:
        self._connection = connection
        self._active = True
        self._authorization_denied = False

    async def _install(self) -> None:
        await self._connection.set_authorizer(self._authorize)

    async def _close(self) -> None:
        self._active = False
        await self._connection.set_authorizer(None)

    @property
    def authorization_denied(self) -> bool:
        return self._authorization_denied

    def _authorize(self, action, argument, _detail, _database, _trigger) -> int:
        denied_actions = {
            sqlite3.SQLITE_ALTER_TABLE,
            sqlite3.SQLITE_ANALYZE,
            sqlite3.SQLITE_ATTACH,
            sqlite3.SQLITE_CREATE_INDEX,
            sqlite3.SQLITE_CREATE_TABLE,
            sqlite3.SQLITE_CREATE_TEMP_INDEX,
            sqlite3.SQLITE_CREATE_TEMP_TABLE,
            sqlite3.SQLITE_CREATE_TEMP_TRIGGER,
            sqlite3.SQLITE_CREATE_TEMP_VIEW,
            sqlite3.SQLITE_CREATE_TRIGGER,
            sqlite3.SQLITE_CREATE_VIEW,
            sqlite3.SQLITE_CREATE_VTABLE,
            sqlite3.SQLITE_DETACH,
            sqlite3.SQLITE_DROP_INDEX,
            sqlite3.SQLITE_DROP_TABLE,
            sqlite3.SQLITE_DROP_TEMP_INDEX,
            sqlite3.SQLITE_DROP_TEMP_TABLE,
            sqlite3.SQLITE_DROP_TEMP_TRIGGER,
            sqlite3.SQLITE_DROP_TEMP_VIEW,
            sqlite3.SQLITE_DROP_TRIGGER,
            sqlite3.SQLITE_DROP_VIEW,
            sqlite3.SQLITE_DROP_VTABLE,
            sqlite3.SQLITE_PRAGMA,
            sqlite3.SQLITE_REINDEX,
            sqlite3.SQLITE_SAVEPOINT,
            sqlite3.SQLITE_TRANSACTION,
        }
        writes = {sqlite3.SQLITE_DELETE, sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE}
        denied = action in denied_actions or (
            action in writes
            and isinstance(argument, str)
            and (
                argument.casefold() in self._MOVIE_TABLES
                or argument.casefold().startswith("sqlite_")
            )
        )
        if denied:
            self._authorization_denied = True
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    async def execute(
        self,
        statement: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> int:
        self._validate(statement)
        cursor = await self._connection.execute(statement, parameters)
        try:
            return cursor.rowcount
        finally:
            await cursor.close()

    async def fetchone(
        self,
        statement: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> tuple | None:
        self._validate(statement)
        async with self._connection.execute(statement, parameters) as cursor:
            return await cursor.fetchone()

    async def fetchall(
        self,
        statement: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple]:
        self._validate(statement)
        async with self._connection.execute(statement, parameters) as cursor:
            return await cursor.fetchall()

    def _validate(self, statement: str) -> None:
        if not self._active:
            raise RuntimeError("Projection transaction is no longer active")
        if not isinstance(statement, str) or not statement.strip():
            self._authorization_denied = True
            raise ProjectionTransactionError(
                "Projection SQL statement must be nonempty"
            )
        operation = statement.lstrip().split(None, 1)[0].upper()
        if operation in self._FORBIDDEN:
            self._authorization_denied = True
            raise ProjectionTransactionError(
                "Projection handler cannot control its transaction"
            )

__all__ = ["ProjectionTransaction"]
