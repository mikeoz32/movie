from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Awaitable, Callable
from concurrent.futures import Future
from contextlib import asynccontextmanager
from enum import Enum, auto
from hashlib import sha256
from threading import Condition, Lock
from time import monotonic, sleep, time_ns
from typing import TYPE_CHECKING, TypeVar
from uuid import UUID

from movie.future import RuntimeFuture
from movie.io import AsyncioIOCapacityError, AsyncioIOWorker
from movie.persistence.errors import (
    ChangeFeedCompactedError,
    ConcurrentWriteError,
    OperationConflictError,
    PersistenceCapacityError,
    PersistenceOperationTimeout,
    PersistenceSchemaError,
)
from movie.persistence.model import (
    PERSISTENCE_SLICE_COUNT,
    DurableStateChange,
    DurableStateChangeBatch,
    DurableStateRecord,
    OperationId,
    PersistenceId,
    WriteResult,
    persistence_slice,
)

if TYPE_CHECKING:
    from movie.actor.system import ExtendedActorSystem


T = TypeVar("T")

_STATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS movie_durable_state (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    operation_id TEXT NOT NULL,
    manifest TEXT,
    payload BLOB,
    deleted INTEGER NOT NULL CHECK (deleted IN (0, 1)),
    CHECK (
        (deleted = 0 AND manifest IS NOT NULL AND payload IS NOT NULL)
        OR (deleted = 1 AND manifest IS NULL AND payload IS NULL)
    ),
    PRIMARY KEY (entity_type, entity_id)
)
"""

_OPERATION_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS movie_durable_operation (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    fingerprint BLOB NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    PRIMARY KEY (entity_type, entity_id, operation_id)
)
"""

_CHANGE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS movie_durable_change (
    global_offset INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    persistence_slice INTEGER NOT NULL CHECK (
        persistence_slice >= 0 AND persistence_slice < 1024
    ),
    revision INTEGER NOT NULL CHECK (revision > 0),
    operation_id TEXT NOT NULL,
    manifest TEXT,
    payload BLOB,
    deleted INTEGER NOT NULL CHECK (deleted IN (0, 1)),
    committed_at_ns INTEGER NOT NULL CHECK (committed_at_ns > 0),
    CHECK (
        (deleted = 0 AND manifest IS NOT NULL AND payload IS NOT NULL)
        OR (deleted = 1 AND manifest IS NULL AND payload IS NULL)
    ),
    UNIQUE (entity_type, entity_id, revision)
)
"""

_CHANGE_QUERY_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS movie_durable_change_entity_offset
ON movie_durable_change (entity_type, global_offset)
"""

_PROJECTION_OFFSET_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS movie_projection_offset (
    projection_name TEXT NOT NULL,
    projection_key TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    min_slice INTEGER NOT NULL CHECK (min_slice >= 0 AND min_slice < 1024),
    max_slice INTEGER NOT NULL CHECK (max_slice >= min_slice AND max_slice < 1024),
    processing_mode TEXT NOT NULL CHECK (
        processing_mode IN ('at-least-once', 'sqlite-exactly-once')
    ),
    projection_offset INTEGER NOT NULL CHECK (projection_offset >= 0),
    PRIMARY KEY (projection_name, projection_key)
)
"""

_PROJECTION_FEED_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS movie_projection_feed (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    high_watermark INTEGER NOT NULL CHECK (high_watermark >= 0),
    compacted_through INTEGER NOT NULL CHECK (
        compacted_through >= 0 AND compacted_through <= high_watermark
    )
)
"""


class _State(Enum):
    NEW = auto()
    STARTING = auto()
    RUNNING = auto()
    STOPPING = auto()
    STOPPED = auto()


class _ProjectionOffsetMismatch(Exception):
    def __init__(self, actual_offset: int) -> None:
        self.actual_offset = actual_offset


class SQLiteDurableStateStore:
    """One asynchronously accessed SQLite durable-state connection."""

    def __init__(
        self,
        system: ExtendedActorSystem,
        worker: AsyncioIOWorker,
        path: str,
        *,
        operation_capacity: int,
        pending_byte_capacity: int,
        max_state_bytes: int,
        operation_timeout: float,
        recovery_timeout: float,
    ) -> None:
        normalized_path = path.strip().lower()
        if normalized_path == ":memory:" or (
            normalized_path.startswith("file:")
            and (
                normalized_path.startswith("file::memory:")
                or "mode=memory" in normalized_path
            )
        ):
            raise ValueError("Durable State requires a file-backed SQLite database")
        self._system = system
        self._worker = worker
        self._path = path
        self._operation_capacity = operation_capacity
        self._pending_byte_capacity = pending_byte_capacity
        self._max_state_bytes = max_state_bytes
        self._operation_timeout = operation_timeout
        self._recovery_timeout = recovery_timeout
        self._condition = Condition(Lock())
        self._stop_lock = Lock()
        self._state = _State.NEW
        self._connection = None
        self._operation_lock: asyncio.Lock | None = None
        self._startup: Future | None = None
        self._startup_error: BaseException | None = None
        self._startup_settled = False
        self._close: Future | None = None
        self._pending: dict[Future, int] = {}
        self._internal_operations = 0
        self._pending_bytes = 0
        self._executing_operations = 0

    @property
    def pending_operations(self) -> int:
        with self._condition:
            return len(self._pending) + self._internal_operations

    @property
    def pending_bytes(self) -> int:
        with self._condition:
            return self._pending_bytes

    @property
    def executing_operations(self) -> int:
        with self._condition:
            return self._executing_operations

    def start(self, timeout: float) -> None:
        if self._worker.owns_current_thread():
            raise RuntimeError("SQLite store cannot start on its Asyncio I/O worker")
        deadline = monotonic() + timeout
        with self._condition:
            if self._state is not _State.NEW:
                raise RuntimeError("SQLite durable-state store can only start once")
            self._state = _State.STARTING
            try:
                startup = self._worker.submit_coroutine(lambda: self._open(deadline))
            except BaseException:
                self._state = _State.STOPPED
                self._condition.notify_all()
                raise
            self._startup = startup
        startup.add_done_callback(self._complete_startup)
        try:
            startup.result(timeout)
        except TimeoutError as error:
            raise TimeoutError(
                "SQLite durable-state store did not start in time"
            ) from error
        with self._condition:
            while not self._startup_settled:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError("SQLite durable-state store did not start in time")
                self._condition.wait(remaining)
            if self._state is _State.RUNNING:
                return
            error = self._startup_error or RuntimeError(
                "SQLite durable-state store startup was interrupted"
            )
        raise error

    def _complete_startup(self, startup: Future) -> None:
        try:
            connection, operation_lock = startup.result()
        except BaseException as error:
            with self._condition:
                self._startup_error = error
                self._startup_settled = True
                if self._state is _State.STARTING:
                    self._state = _State.STOPPED
                self._condition.notify_all()
            return
        with self._condition:
            self._connection = connection
            self._operation_lock = operation_lock
            self._startup_settled = True
            if self._state is _State.STARTING:
                self._state = _State.RUNNING
            self._condition.notify_all()

    async def _open(self, deadline: float):
        try:
            import aiosqlite
        except ImportError as error:
            raise RuntimeError(
                "SQLite persistence requires movie-actor-runtime[persistence-sqlite]"
            ) from error

        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError("SQLite durable-state store did not start in time")
        connection = await aiosqlite.connect(
            self._path,
            isolation_level=None,
            timeout=remaining,
        )
        try:
            await self._set_busy_timeout(connection, deadline)
            async with connection.execute("PRAGMA journal_mode=WAL") as cursor:
                journal_mode = await cursor.fetchone()
            if journal_mode is None or journal_mode[0].lower() != "wal":
                raise PersistenceSchemaError(
                    "SQLite persistence requires effective WAL journal mode"
                )
            async with connection.execute("PRAGMA encoding") as cursor:
                encoding = await cursor.fetchone()
            if encoding is None or encoding[0].upper() != "UTF-8":
                raise PersistenceSchemaError(
                    "SQLite persistence schema requires UTF-8 database encoding"
                )
            await connection.execute("PRAGMA synchronous=FULL")
            await connection.execute("PRAGMA foreign_keys=ON")
            await self._set_busy_timeout(connection, deadline)
            await connection.execute("BEGIN EXCLUSIVE")
            try:
                async with connection.execute("PRAGMA user_version") as cursor:
                    version_row = await cursor.fetchone()
                version = version_row[0]
                if version > 1:
                    raise PersistenceSchemaError(
                        f"SQLite persistence has newer schema version {version}"
                    )
                if version == 0:
                    await connection.execute(_STATE_TABLE_SQL)
                    await connection.execute(_OPERATION_TABLE_SQL)
                    await connection.execute(_CHANGE_TABLE_SQL)
                    await connection.execute(_CHANGE_QUERY_INDEX_SQL)
                    await connection.execute(_PROJECTION_OFFSET_TABLE_SQL)
                    await connection.execute(_PROJECTION_FEED_TABLE_SQL)
                    await connection.execute(
                        "INSERT INTO movie_projection_feed VALUES (1, 0, 0)"
                    )
                    await connection.execute("PRAGMA user_version=1")
                await self._validate_schema(connection)
                await connection.commit()
            except BaseException as error:
                try:
                    await connection.rollback()
                except BaseException as rollback_error:
                    error.add_note(f"SQLite schema rollback also failed: {rollback_error!r}")
                raise
        except BaseException as error:
            await connection.close()
            if isinstance(error, sqlite3.OperationalError) and (
                "locked" in str(error).lower() or "busy" in str(error).lower()
            ):
                raise TimeoutError(
                    "SQLite durable-state store did not start in time"
                ) from error
            raise
        return connection, asyncio.Lock()

    @staticmethod
    async def _set_busy_timeout(connection, deadline: float) -> None:
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError("SQLite durable-state store did not start in time")
        await connection.execute(
            f"PRAGMA busy_timeout={max(1, int(remaining * 1_000))}"
        )

    @staticmethod
    async def _validate_schema(connection) -> None:
        expected = {
            "movie_durable_state": (
                _STATE_TABLE_SQL,
                ("entity_type", "TEXT", 1, 1),
                ("entity_id", "TEXT", 1, 2),
                ("revision", "INTEGER", 1, 0),
                ("operation_id", "TEXT", 1, 0),
                ("manifest", "TEXT", 0, 0),
                ("payload", "BLOB", 0, 0),
                ("deleted", "INTEGER", 1, 0),
            ),
            "movie_durable_operation": (
                _OPERATION_TABLE_SQL,
                ("entity_type", "TEXT", 1, 1),
                ("entity_id", "TEXT", 1, 2),
                ("operation_id", "TEXT", 1, 3),
                ("fingerprint", "BLOB", 1, 0),
                ("revision", "INTEGER", 1, 0),
            ),
        }
        for table, (expected_sql, *expected_columns) in expected.items():
            async with connection.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
                (table,),
            ) as cursor:
                definition = await cursor.fetchone()
            actual_sql = "" if definition is None else definition[0]
            normalized_actual = "".join(actual_sql.split()).casefold()
            normalized_expected = "".join(expected_sql.split()).casefold()
            normalized_without_guard = normalized_expected.replace("ifnotexists", "")
            if normalized_actual not in (
                normalized_expected,
                normalized_without_guard,
            ):
                raise PersistenceSchemaError(
                    f"SQLite persistence table {table!r} has an incompatible schema"
                )
            async with connection.execute(f"PRAGMA table_info({table})") as cursor:
                rows = await cursor.fetchall()
            columns = tuple((row[1], row[2].upper(), row[3], row[5]) for row in rows)
            if columns != tuple(expected_columns):
                raise PersistenceSchemaError(
                    f"SQLite persistence table {table!r} has an incompatible schema"
                )
            async with connection.execute(f"PRAGMA index_list({table})") as cursor:
                indexes = await cursor.fetchall()
            if any(row[2] and row[3] != "pk" for row in indexes):
                raise PersistenceSchemaError(
                    f"SQLite persistence table {table!r} has incompatible indexes"
                )
            primary_key = next((row[1] for row in indexes if row[3] == "pk"), None)
            if primary_key is None:
                raise PersistenceSchemaError(
                    f"SQLite persistence table {table!r} has an incompatible schema"
                )
            async with connection.execute(
                f"PRAGMA index_xinfo({primary_key})"
            ) as cursor:
                index_columns = await cursor.fetchall()
            expected_keys = tuple(
                (name, "BINARY")
                for name, _type, _not_null, position in expected_columns
                if position
            )
            keys = tuple(
                (row[2], row[4])
                for row in index_columns
                if row[5] and row[1] >= 0
            )
            if keys != expected_keys:
                raise PersistenceSchemaError(
                    f"SQLite persistence table {table!r} has an incompatible schema"
                )
        async with connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            ("movie_durable_change",),
        ) as cursor:
            change_definition = await cursor.fetchone()
        actual_change_sql = "" if change_definition is None else change_definition[0]
        normalized_change_sql = "".join(actual_change_sql.split()).casefold()
        normalized_expected_change_sql = "".join(_CHANGE_TABLE_SQL.split()).casefold()
        if normalized_change_sql not in (
            normalized_expected_change_sql,
            normalized_expected_change_sql.replace("ifnotexists", ""),
        ):
            raise PersistenceSchemaError(
                "SQLite persistence table 'movie_durable_change' has an incompatible schema"
            )
        async with connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
            ("movie_durable_change_entity_offset",),
        ) as cursor:
            change_index_definition = await cursor.fetchone()
        actual_change_index_sql = (
            "" if change_index_definition is None else change_index_definition[0]
        )
        normalized_change_index_sql = "".join(
            actual_change_index_sql.split()
        ).casefold()
        normalized_expected_change_index_sql = "".join(
            _CHANGE_QUERY_INDEX_SQL.split()
        ).casefold()
        if normalized_change_index_sql not in (
            normalized_expected_change_index_sql,
            normalized_expected_change_index_sql.replace("ifnotexists", ""),
        ):
            raise PersistenceSchemaError(
                "SQLite persistence Change Feed query index is incompatible"
            )
        async with connection.execute(
            "PRAGMA index_xinfo(movie_durable_change_entity_offset)"
        ) as cursor:
            change_index_columns = await cursor.fetchall()
        change_index_keys = tuple(
            (row[2], row[4])
            for row in change_index_columns
            if row[5] and row[1] >= 0
        )
        if change_index_keys != (
            ("entity_type", "BINARY"),
            ("global_offset", "BINARY"),
        ):
            raise PersistenceSchemaError(
                "SQLite persistence Change Feed query index is incompatible"
            )
        for table in ("movie_durable_change", "movie_projection_offset"):
            async with connection.execute(f"PRAGMA index_list({table})") as cursor:
                indexes = await cursor.fetchall()
            if any(row[2] and row[3] == "c" for row in indexes):
                raise PersistenceSchemaError(
                    f"SQLite persistence table {table!r} has incompatible indexes"
                )
        async with connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            ("movie_projection_offset",),
        ) as cursor:
            offset_definition = await cursor.fetchone()
        actual_offset_sql = "" if offset_definition is None else offset_definition[0]
        normalized_offset_sql = "".join(actual_offset_sql.split()).casefold()
        normalized_expected_offset_sql = "".join(
            _PROJECTION_OFFSET_TABLE_SQL.split()
        ).casefold()
        if normalized_offset_sql not in (
            normalized_expected_offset_sql,
            normalized_expected_offset_sql.replace("ifnotexists", ""),
        ):
            raise PersistenceSchemaError(
                "SQLite persistence table 'movie_projection_offset' has an "
                "incompatible schema"
            )
        async with connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            ("movie_projection_feed",),
        ) as cursor:
            feed_definition = await cursor.fetchone()
        actual_feed_sql = "" if feed_definition is None else feed_definition[0]
        normalized_feed_sql = "".join(actual_feed_sql.split()).casefold()
        normalized_expected_feed_sql = "".join(
            _PROJECTION_FEED_TABLE_SQL.split()
        ).casefold()
        if normalized_feed_sql not in (
            normalized_expected_feed_sql,
            normalized_expected_feed_sql.replace("ifnotexists", ""),
        ):
            raise PersistenceSchemaError(
                "SQLite persistence table 'movie_projection_feed' has an incompatible schema"
            )
        async with connection.execute(
            "SELECT high_watermark, compacted_through FROM movie_projection_feed "
            "WHERE singleton = 1"
        ) as cursor:
            feed_metadata = await cursor.fetchone()
        if feed_metadata is None:
            raise PersistenceSchemaError("SQLite persistence Change Feed metadata is missing")
        high_watermark, compacted_through = feed_metadata
        async with connection.execute(
            "SELECT seq FROM sqlite_sequence WHERE name = 'movie_durable_change'"
        ) as cursor:
            sequence = await cursor.fetchone()
        sequence_value = 0 if sequence is None else sequence[0]
        async with connection.execute(
            "SELECT max(global_offset) FROM movie_durable_change"
        ) as cursor:
            retained_high_watermark = (await cursor.fetchone())[0]
        expected_retained_high_watermark = (
            None if compacted_through == high_watermark else high_watermark
        )
        if (
            sequence_value != high_watermark
            or retained_high_watermark != expected_retained_high_watermark
        ):
            raise PersistenceSchemaError(
                "SQLite persistence Change Feed sequence is incompatible"
            )
        async with connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'trigger'
              AND tbl_name IN (
                  'movie_durable_state',
                  'movie_durable_operation',
                  'movie_durable_change',
                  'movie_projection_offset',
                  'movie_projection_feed'
              )
            LIMIT 1
            """
        ) as cursor:
            trigger = await cursor.fetchone()
        if trigger is not None:
            raise PersistenceSchemaError(
                "SQLite persistence tables must not have triggers"
            )

    def load(self, persistence_id: PersistenceId) -> RuntimeFuture[DurableStateRecord | None]:
        if type(persistence_id) is not PersistenceId:
            raise ValueError("load requires a PersistenceId")
        deadline = monotonic() + self._recovery_timeout
        return self._submit(
            lambda: self._load(persistence_id, deadline),
            retained_bytes=0,
        )

    async def _load(
        self,
        persistence_id: PersistenceId,
        deadline: float,
    ) -> DurableStateRecord | None:
        connection, operation_lock = self._resources()
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise PersistenceOperationTimeout(
                f"Durable State load timed out for {persistence_id}"
            )
        try:
            await asyncio.wait_for(operation_lock.acquire(), remaining)
        except TimeoutError as error:
            raise PersistenceOperationTimeout(
                f"Durable State load timed out for {persistence_id}"
            ) from error
        try:
            self._begin_execution()
            load = asyncio.create_task(self._read_record(connection, persistence_id))
            try:
                row = await asyncio.wait_for(
                    asyncio.shield(load),
                    max(0.0, deadline - monotonic()),
                )
            except TimeoutError as error:
                await connection.interrupt()
                try:
                    await load
                except BaseException:
                    pass
                raise PersistenceOperationTimeout(
                    f"Durable State load timed out for {persistence_id}"
                ) from error
        finally:
            self._finish_execution()
            operation_lock.release()
        if row is None:
            return None
        revision, operation_id, manifest, payload, deleted, retained_bytes = row
        if retained_bytes > self._max_state_bytes:
            raise PersistenceCapacityError(
                f"Recovered state exceeds max-state-bytes for {persistence_id}"
            )
        return DurableStateRecord(
            persistence_id,
            revision,
            OperationId(UUID(operation_id)),
            manifest,
            bytes(payload) if payload is not None else None,
            bool(deleted),
        )

    async def _read_record(self, connection, persistence_id: PersistenceId):
        async with connection.execute(
            """
            SELECT
                revision,
                operation_id,
                CASE
                    WHEN coalesce(length(CAST(manifest AS BLOB)), 0)
                       + coalesce(length(payload), 0) <= ?
                    THEN manifest
                END,
                CASE
                    WHEN coalesce(length(CAST(manifest AS BLOB)), 0)
                       + coalesce(length(payload), 0) <= ?
                    THEN payload
                END,
                deleted,
                coalesce(length(CAST(manifest AS BLOB)), 0)
                    + coalesce(length(payload), 0)
            FROM movie_durable_state
            WHERE entity_type = ? AND entity_id = ?
            """,
            (
                self._max_state_bytes,
                self._max_state_bytes,
                persistence_id.entity_type,
                persistence_id.entity_id,
            ),
        ) as cursor:
            return await cursor.fetchone()

    def upsert(
        self,
        persistence_id: PersistenceId,
        *,
        expected_revision: int,
        operation_id: OperationId,
        manifest: str,
        payload: bytes,
    ) -> RuntimeFuture[WriteResult]:
        if type(persistence_id) is not PersistenceId:
            raise ValueError("upsert requires a PersistenceId")
        if not isinstance(expected_revision, int) or isinstance(expected_revision, bool):
            raise ValueError("Expected revision must be a nonnegative integer")
        if expected_revision < 0:
            raise ValueError("Expected revision must be a nonnegative integer")
        if type(operation_id) is not OperationId:
            raise ValueError("upsert requires an OperationId")
        if not isinstance(manifest, str) or not manifest:
            raise ValueError("State manifest must be a nonempty string")
        if "\x00" in manifest:
            raise ValueError("State manifest must not contain NUL characters")
        if type(payload) is not bytes:
            raise ValueError("State payload must be bytes")
        retained_bytes = len(manifest.encode("utf-8")) + len(payload)
        if retained_bytes > self._max_state_bytes:
            raise PersistenceCapacityError("State record exceeds max-state-bytes")
        deadline = monotonic() + self._operation_timeout
        fingerprint = self._fingerprint(False, manifest, payload)
        return self._submit(
            lambda: self._upsert(
                persistence_id,
                expected_revision,
                operation_id,
                manifest,
                payload,
                fingerprint,
                deadline,
            ),
            retained_bytes=retained_bytes,
        )

    async def _upsert(
        self,
        persistence_id: PersistenceId,
        expected_revision: int,
        operation_id: OperationId,
        manifest: str,
        payload: bytes,
        fingerprint: bytes,
        deadline: float,
    ) -> WriteResult:
        return await self._mutate(
            persistence_id,
            expected_revision,
            operation_id,
            manifest,
            payload,
            fingerprint,
            deleted=False,
            deadline=deadline,
        )

    def delete(
        self,
        persistence_id: PersistenceId,
        *,
        expected_revision: int,
        operation_id: OperationId,
    ) -> RuntimeFuture[WriteResult]:
        if type(persistence_id) is not PersistenceId:
            raise ValueError("delete requires a PersistenceId")
        if (
            not isinstance(expected_revision, int)
            or isinstance(expected_revision, bool)
            or expected_revision < 0
        ):
            raise ValueError("Expected revision must be a nonnegative integer")
        if type(operation_id) is not OperationId:
            raise ValueError("delete requires an OperationId")
        deadline = monotonic() + self._operation_timeout
        fingerprint = self._fingerprint(True, None, None)
        return self._submit(
            lambda: self._mutate(
                persistence_id,
                expected_revision,
                operation_id,
                None,
                None,
                fingerprint,
                deleted=True,
                deadline=deadline,
            ),
            retained_bytes=0,
        )

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
    ) -> RuntimeFuture[DurableStateChangeBatch]:
        if (
            not isinstance(entity_type, str)
            or not entity_type
            or "\x00" in entity_type
            or len(entity_type.encode("utf-8")) > 512
        ):
            raise ValueError("Projection entity type is invalid")
        for value, field in (
            (min_slice, "minimum slice"),
            (max_slice, "maximum slice"),
            (after_offset, "Projection Offset"),
            (limit, "change batch limit"),
            (scan_limit, "change scan limit"),
        ):
            if not isinstance(value, int) or isinstance(value, bool):
                raise ValueError(f"{field} must be an integer")
        if not 0 <= min_slice <= max_slice < PERSISTENCE_SLICE_COUNT:
            raise ValueError("Projection slice range is invalid")
        if after_offset < 0:
            raise ValueError("Projection Offset must be nonnegative")
        if not 1 <= limit <= 1_024:
            raise ValueError("Change batch limit must be between 1 and 1024")
        if not 1 <= scan_limit <= 1_000_000:
            raise ValueError("Change scan limit must be between 1 and 1000000")
        byte_limit = self._max_state_bytes if max_bytes is None else max_bytes
        if (
            not isinstance(byte_limit, int)
            or isinstance(byte_limit, bool)
            or byte_limit < self._max_state_bytes
        ):
            raise ValueError("Change batch byte limit must cover one maximum state")
        deadline = monotonic() + self._recovery_timeout
        return self._submit(
            lambda: self._changes(
                entity_type,
                min_slice,
                max_slice,
                after_offset,
                limit,
                byte_limit,
                scan_limit,
                deadline,
            ),
            retained_bytes=byte_limit,
        )

    async def _changes(
        self,
        entity_type: str,
        min_slice: int,
        max_slice: int,
        after_offset: int,
        limit: int,
        max_bytes: int,
        scan_limit: int,
        deadline: float,
    ) -> DurableStateChangeBatch:
        connection, operation_lock = self._resources()
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise PersistenceOperationTimeout("Durable State Change query timed out")
        try:
            await asyncio.wait_for(operation_lock.acquire(), remaining)
        except TimeoutError as error:
            raise PersistenceOperationTimeout(
                "Durable State Change query timed out"
            ) from error
        try:
            self._begin_execution()
            read = asyncio.create_task(
                self._read_changes(
                    connection,
                    entity_type,
                    min_slice,
                    max_slice,
                    after_offset,
                    limit,
                    max_bytes,
                    scan_limit,
                )
            )
            try:
                rows, scan_through, exhausted = await asyncio.wait_for(
                    asyncio.shield(read),
                    max(0.0, deadline - monotonic()),
                )
            except TimeoutError:
                await connection.interrupt()
                try:
                    await read
                except BaseException:
                    pass
                raise
        except TimeoutError as error:
            raise PersistenceOperationTimeout(
                "Durable State Change query timed out"
            ) from error
        finally:
            self._finish_execution()
            operation_lock.release()
        changes = tuple(self._change_from_row(row) for row in rows)
        offset = (
            max(after_offset, scan_through)
            if exhausted
            else changes[-1].offset
        )
        return DurableStateChangeBatch(changes, offset)

    async def _read_changes(
        self,
        connection,
        entity_type: str,
        min_slice: int,
        max_slice: int,
        after_offset: int,
        limit: int,
        max_bytes: int,
        scan_limit: int,
    ):
        await connection.execute("BEGIN")
        try:
            async with connection.execute(
                "SELECT high_watermark, compacted_through "
                "FROM movie_projection_feed WHERE singleton = 1"
            ) as cursor:
                high_watermark, compacted_through = await cursor.fetchone()
            if after_offset < compacted_through:
                raise ChangeFeedCompactedError(after_offset, compacted_through)
            scan_through = min(high_watermark, after_offset + scan_limit)
            async with connection.execute(
                """
                SELECT
                    global_offset,
                    entity_type,
                    entity_id,
                    revision,
                    operation_id,
                    persistence_slice,
                    CASE
                        WHEN coalesce(length(CAST(manifest AS BLOB)), 0)
                           + coalesce(length(payload), 0) <= ?
                        THEN manifest
                    END,
                    CASE
                        WHEN coalesce(length(CAST(manifest AS BLOB)), 0)
                           + coalesce(length(payload), 0) <= ?
                        THEN payload
                    END,
                    deleted,
                    committed_at_ns,
                    coalesce(length(CAST(manifest AS BLOB)), 0)
                        + coalesce(length(payload), 0)
                FROM movie_durable_change
                WHERE entity_type = ?
                  AND persistence_slice BETWEEN ? AND ?
                  AND global_offset > ?
                  AND global_offset <= ?
                ORDER BY global_offset
                LIMIT ?
                """,
                (
                    self._max_state_bytes,
                    self._max_state_bytes,
                    entity_type,
                    min_slice,
                    max_slice,
                    after_offset,
                    scan_through,
                    limit,
                ),
            ) as cursor:
                rows = []
                retained_bytes = 0
                exhausted = False
                while len(rows) < limit:
                    row = await cursor.fetchone()
                    if row is None:
                        exhausted = True
                        break
                    state_bytes = row[-1]
                    if state_bytes > self._max_state_bytes:
                        raise PersistenceCapacityError(
                            "Durable State Change exceeds max-state-bytes"
                        )
                    if rows and retained_bytes + state_bytes > max_bytes:
                        break
                    rows.append(row[:-1])
                    retained_bytes += state_bytes
            await connection.commit()
            return rows, scan_through, exhausted
        except BaseException:
            await connection.rollback()
            raise

    @staticmethod
    def _change_from_row(row) -> DurableStateChange:
        (
            offset,
            entity_type,
            entity_id,
            revision,
            operation_id,
            persistence_slice,
            manifest,
            payload,
            deleted,
            committed_at_ns,
        ) = row
        return DurableStateChange(
            offset,
            PersistenceId(entity_type, entity_id),
            revision,
            OperationId(UUID(operation_id)),
            persistence_slice,
            manifest,
            bytes(payload) if payload is not None else None,
            bool(deleted),
            committed_at_ns,
        )

    async def _mutate(
        self,
        persistence_id: PersistenceId,
        expected_revision: int,
        operation_id: OperationId,
        manifest: str | None,
        payload: bytes | None,
        fingerprint: bytes,
        *,
        deleted: bool,
        deadline: float,
    ) -> WriteResult:
        connection, operation_lock = self._resources()
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise PersistenceOperationTimeout(
                f"Durable State mutation timed out before commit for {persistence_id}"
            )
        try:
            await asyncio.wait_for(operation_lock.acquire(), remaining)
        except TimeoutError as error:
            raise PersistenceOperationTimeout(
                f"Durable State mutation timed out before commit for {persistence_id}"
            ) from error
        try:
            self._begin_execution()
            try:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise PersistenceOperationTimeout(
                        f"Durable State mutation timed out before commit for {persistence_id}"
                    )
                await connection.execute(
                    f"PRAGMA busy_timeout={max(1, int(remaining * 1_000))}"
                )
                if monotonic() >= deadline:
                    raise PersistenceOperationTimeout(
                        f"Durable State mutation timed out before commit for {persistence_id}"
                    )
                await connection.execute("BEGIN IMMEDIATE")
                if monotonic() >= deadline:
                    raise PersistenceOperationTimeout(
                        f"Durable State mutation timed out before commit for {persistence_id}"
                    )
                async with connection.execute(
                    """
                    SELECT fingerprint, revision
                    FROM movie_durable_operation
                    WHERE entity_type = ? AND entity_id = ? AND operation_id = ?
                    """,
                    (
                        persistence_id.entity_type,
                        persistence_id.entity_id,
                        str(operation_id),
                    ),
                ) as cursor:
                    duplicate = await cursor.fetchone()
                if duplicate is not None:
                    if bytes(duplicate[0]) != fingerprint:
                        raise OperationConflictError(
                            "Operation identity was reused with different content"
                        )
                    await connection.commit()
                    return WriteResult(duplicate[1], duplicate=True)

                async with connection.execute(
                    """
                    SELECT revision FROM movie_durable_state
                    WHERE entity_type = ? AND entity_id = ?
                    """,
                    (persistence_id.entity_type, persistence_id.entity_id),
                ) as cursor:
                    current = await cursor.fetchone()
                actual_revision = current[0] if current is not None else 0
                if actual_revision != expected_revision:
                    raise ConcurrentWriteError(expected_revision, actual_revision)
                revision = actual_revision + 1
                await connection.execute(
                    """
                    INSERT INTO movie_durable_state (
                        entity_type, entity_id, revision, operation_id,
                        manifest, payload, deleted
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(entity_type, entity_id) DO UPDATE SET
                        revision = excluded.revision,
                        operation_id = excluded.operation_id,
                        manifest = excluded.manifest,
                        payload = excluded.payload,
                        deleted = excluded.deleted
                    """,
                    (
                        persistence_id.entity_type,
                        persistence_id.entity_id,
                        revision,
                        str(operation_id),
                        manifest,
                        payload,
                        int(deleted),
                    ),
                )
                await connection.execute(
                    """
                    INSERT INTO movie_durable_operation (
                        entity_type, entity_id, operation_id, fingerprint, revision
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        persistence_id.entity_type,
                        persistence_id.entity_id,
                        str(operation_id),
                        fingerprint,
                        revision,
                    ),
                )
                await connection.execute(
                    """
                    INSERT INTO movie_durable_change (
                        entity_type,
                        entity_id,
                        persistence_slice,
                        revision,
                        operation_id,
                        manifest,
                        payload,
                        deleted,
                        committed_at_ns
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        persistence_id.entity_type,
                        persistence_id.entity_id,
                        persistence_slice(persistence_id),
                        revision,
                        str(operation_id),
                        manifest,
                        payload,
                        int(deleted),
                        time_ns(),
                    ),
                )
                await connection.execute(
                    """
                    UPDATE movie_projection_feed
                    SET high_watermark = (
                        SELECT max(global_offset) FROM movie_durable_change
                    )
                    WHERE singleton = 1
                    """
                )
                await connection.commit()
                return WriteResult(revision)
            except BaseException as error:
                try:
                    await connection.rollback()
                except BaseException as rollback_error:
                    error.add_note(f"SQLite rollback also failed: {rollback_error!r}")
                if isinstance(error, sqlite3.OperationalError) and (
                    "locked" in str(error).lower() or "busy" in str(error).lower()
                ):
                    raise PersistenceOperationTimeout(
                        f"Durable State mutation timed out before commit for {persistence_id}"
                    ) from error
                raise
        finally:
            self._finish_execution()
            operation_lock.release()

    async def _projection_register(
        self,
        projection_name: str,
        projection_key: str,
        entity_type: str,
        min_slice: int,
        max_slice: int,
        processing_mode: str,
        initial_offset: int | None,
    ) -> tuple[int, str]:
        async with self._projection_operation(), self._write_transaction() as connection:
            async with connection.execute(
                """
                SELECT
                    entity_type,
                    min_slice,
                    max_slice,
                    processing_mode,
                    projection_offset
                FROM movie_projection_offset
                WHERE projection_name = ? AND projection_key = ?
                """,
                (projection_name, projection_key),
            ) as cursor:
                existing = await cursor.fetchone()
            if existing is None:
                async with connection.execute(
                    """
                    SELECT compacted_through, high_watermark
                    FROM movie_projection_feed
                    WHERE singleton = 1
                    """
                ) as cursor:
                    compacted_through, high_watermark = await cursor.fetchone()
                if initial_offset is None:
                    if compacted_through > 0:
                        return 0, "baseline-required"
                    offset = 0
                elif not compacted_through <= initial_offset <= high_watermark:
                    return 0, "invalid-baseline"
                else:
                    offset = initial_offset
                await connection.execute(
                    """
                    INSERT INTO movie_projection_offset (
                        projection_name,
                        projection_key,
                        entity_type,
                        min_slice,
                        max_slice,
                        processing_mode,
                        projection_offset
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        projection_name,
                        projection_key,
                        entity_type,
                        min_slice,
                        max_slice,
                        processing_mode,
                        offset,
                    ),
                )
                return offset, "ok"
            (
                stored_entity_type,
                stored_min_slice,
                stored_max_slice,
                stored_processing_mode,
                offset,
            ) = existing
            source_matches = (
                stored_entity_type == entity_type
                and stored_min_slice == min_slice
                and stored_max_slice == max_slice
                and stored_processing_mode == processing_mode
            )
            return offset, "ok" if source_matches else "source-conflict"

    async def _projection_save_offset(
        self,
        projection_name: str,
        projection_key: str,
        expected_offset: int,
        new_offset: int,
    ) -> tuple[bool, int]:
        async with self._projection_operation(), self._write_transaction() as connection:
            cursor = await connection.execute(
                """
                UPDATE movie_projection_offset
                SET projection_offset = ?
                WHERE projection_name = ?
                  AND projection_key = ?
                  AND projection_offset = ?
                """,
                (
                    new_offset,
                    projection_name,
                    projection_key,
                    expected_offset,
                ),
            )
            if cursor.rowcount == 1:
                return True, new_offset
            async with connection.execute(
                """
                SELECT projection_offset
                FROM movie_projection_offset
                WHERE projection_name = ? AND projection_key = ?
                """,
                (projection_name, projection_key),
            ) as read:
                current = await read.fetchone()
            return False, -1 if current is None else current[0]

    async def _projection_commit(
        self,
        projection_name: str,
        projection_key: str,
        expected_offset: int,
        new_offset: int,
        apply,
    ) -> tuple[bool, int]:
        try:
            async with (
                self._projection_operation(),
                self._write_transaction() as connection,
            ):
                async with connection.execute(
                    """
                    SELECT projection_offset
                    FROM movie_projection_offset
                    WHERE projection_name = ? AND projection_key = ?
                    """,
                    (projection_name, projection_key),
                ) as cursor:
                    current = await cursor.fetchone()
                actual_offset = -1 if current is None else current[0]
                if actual_offset != expected_offset:
                    raise _ProjectionOffsetMismatch(actual_offset)
                await apply(connection)
                cursor = await connection.execute(
                    """
                    UPDATE movie_projection_offset
                    SET projection_offset = ?
                    WHERE projection_name = ?
                      AND projection_key = ?
                      AND projection_offset = ?
                    """,
                    (
                        new_offset,
                        projection_name,
                        projection_key,
                        expected_offset,
                    ),
                )
                if cursor.rowcount != 1:
                    raise _ProjectionOffsetMismatch(expected_offset)
        except _ProjectionOffsetMismatch as error:
            return False, error.actual_offset
        return True, new_offset

    async def _projection_changes(
        self,
        entity_type: str,
        min_slice: int,
        max_slice: int,
        after_offset: int,
        limit: int,
        max_bytes: int,
        scan_limit: int,
    ) -> DurableStateChangeBatch:
        async with self._projection_operation():
            return await self._changes(
                entity_type,
                min_slice,
                max_slice,
                after_offset,
                limit,
                max_bytes,
                scan_limit,
                monotonic() + self._recovery_timeout,
            )

    def compact_changes(self, limit: int = 1_000) -> RuntimeFuture[int]:
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 100_000
        ):
            raise ValueError("Change Feed compaction limit must be between 1 and 100000")
        return self._submit(lambda: self._compact_changes(limit), retained_bytes=0)

    async def _compact_changes(self, limit: int) -> int:
        async with self._write_transaction() as connection:
            async with connection.execute(
                "SELECT min(projection_offset) FROM movie_projection_offset"
            ) as cursor:
                row = await cursor.fetchone()
            minimum_offset = None if row is None else row[0]
            if minimum_offset is None or minimum_offset <= 0:
                return 0
            async with connection.execute(
                """
                SELECT global_offset
                FROM movie_durable_change
                WHERE global_offset <= ?
                ORDER BY global_offset
                LIMIT ?
                """,
                (minimum_offset, limit),
            ) as cursor:
                offsets = await cursor.fetchall()
            if not offsets:
                return 0
            compacted_through = offsets[-1][0]
            cursor = await connection.execute(
                "DELETE FROM movie_durable_change WHERE global_offset <= ?",
                (compacted_through,),
            )
            if cursor.rowcount:
                await connection.execute(
                    """
                    UPDATE movie_projection_feed
                    SET compacted_through = ?
                    WHERE singleton = 1
                    """,
                    (compacted_through,),
                )
            return cursor.rowcount

    def _retire_projection(
        self,
        projection_name: str,
        projection_key: str,
    ) -> RuntimeFuture[bool]:
        return self._submit(
            lambda: self._delete_projection_offset(projection_name, projection_key),
            retained_bytes=0,
        )

    async def _delete_projection_offset(
        self,
        projection_name: str,
        projection_key: str,
    ) -> bool:
        async with self._write_transaction() as connection:
            cursor = await connection.execute(
                """
                DELETE FROM movie_projection_offset
                WHERE projection_name = ? AND projection_key = ?
                """,
                (projection_name, projection_key),
            )
            return cursor.rowcount == 1

    @asynccontextmanager
    async def _projection_operation(self):
        with self._condition:
            if self._state not in (_State.RUNNING, _State.STOPPING):
                raise RuntimeError("SQLite durable-state store is not running")
            if (
                len(self._pending) + self._internal_operations
                >= self._operation_capacity
            ):
                raise PersistenceCapacityError("Persistence operation capacity is full")
            self._internal_operations += 1
        try:
            yield
        finally:
            with self._condition:
                self._internal_operations -= 1
                self._condition.notify_all()

    @asynccontextmanager
    async def _write_transaction(self):
        connection, operation_lock = self._resources()
        deadline = monotonic() + self._operation_timeout
        try:
            await asyncio.wait_for(operation_lock.acquire(), self._operation_timeout)
        except TimeoutError as error:
            raise PersistenceOperationTimeout(
                "Projection offset transaction timed out before commit"
            ) from error
        try:
            self._begin_execution()
            try:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise PersistenceOperationTimeout(
                        "Projection offset transaction timed out before commit"
                    )
                await connection.execute(
                    f"PRAGMA busy_timeout={max(1, int(remaining * 1_000))}"
                )
                await connection.execute("BEGIN IMMEDIATE")
                if monotonic() >= deadline:
                    raise PersistenceOperationTimeout(
                        "Projection offset transaction timed out before commit"
                    )
                yield connection
                await connection.commit()
            except BaseException as error:
                try:
                    await connection.rollback()
                except BaseException as rollback_error:
                    error.add_note(
                        f"SQLite projection rollback also failed: {rollback_error!r}"
                    )
                if isinstance(error, sqlite3.OperationalError) and (
                    "locked" in str(error).lower() or "busy" in str(error).lower()
                ):
                    raise PersistenceOperationTimeout(
                        "Projection offset transaction timed out before commit"
                    ) from error
                raise
        finally:
            self._finish_execution()
            operation_lock.release()

    def _begin_execution(self) -> None:
        with self._condition:
            self._executing_operations += 1

    def _finish_execution(self) -> None:
        with self._condition:
            self._executing_operations -= 1
            self._condition.notify_all()

    @staticmethod
    def _fingerprint(
        deleted: bool,
        manifest: str | None,
        payload: bytes | None,
    ) -> bytes:
        digest = sha256()
        if deleted:
            digest.update(b"delete")
        else:
            assert manifest is not None and payload is not None
            encoded_manifest = manifest.encode("utf-8")
            digest.update(b"upsert")
            digest.update(len(encoded_manifest).to_bytes(4, "big"))
            digest.update(encoded_manifest)
            digest.update(payload)
        return digest.digest()

    def _resources(self):
        connection = self._connection
        operation_lock = self._operation_lock
        if connection is None or operation_lock is None:
            raise RuntimeError("SQLite durable-state store is not running")
        return connection, operation_lock

    def _submit(
        self,
        factory: Callable[[], Awaitable[T]],
        *,
        retained_bytes: int,
    ) -> RuntimeFuture[T]:
        with self._condition:
            if self._state is not _State.RUNNING:
                raise RuntimeError("SQLite durable-state store is not accepting operations")
            if (
                len(self._pending) + self._internal_operations
                >= self._operation_capacity
            ):
                raise PersistenceCapacityError("Persistence operation capacity is full")
            if self._pending_bytes + retained_bytes > self._pending_byte_capacity:
                raise PersistenceCapacityError("Persistence pending byte capacity is full")
            result = RuntimeFuture[T](self._system._submit_callback, cancellable=False)
            try:
                source = self._worker.submit_coroutine(factory)
            except AsyncioIOCapacityError as error:
                raise PersistenceCapacityError(
                    "Shared Asyncio I/O command capacity is full"
                ) from error
            self._pending[source] = retained_bytes
            self._pending_bytes += retained_bytes

        def complete(source: Future[T]) -> None:
            try:
                value = source.result()
                error = None
            except BaseException as source_error:
                value = None
                error = source_error
            with self._condition:
                retained = self._pending.pop(source, 0)
                self._pending_bytes -= retained
                self._condition.notify_all()
            if error is None:
                result.set_result(value)
            elif isinstance(error, Exception):
                result.set_exception(error)
            else:
                wrapped = RuntimeError("SQLite persistence operation raised BaseException")
                wrapped.__cause__ = error
                result.set_exception(wrapped)

        source.add_done_callback(complete)
        return result

    def prepare_stop(self) -> None:
        with self._condition:
            if self._state in (_State.STARTING, _State.RUNNING):
                self._state = _State.STOPPING
                self._condition.notify_all()

    def stop(self, timeout: float) -> None:
        if self._worker.owns_current_thread():
            raise RuntimeError("SQLite store cannot stop on its Asyncio I/O worker")
        deadline = monotonic() + timeout
        if not self._stop_lock.acquire(timeout=timeout):
            raise TimeoutError("SQLite durable-state store did not stop in time")
        try:
            self._stop_before(deadline)
        finally:
            self._stop_lock.release()

    def _stop_before(self, deadline: float) -> None:
        with self._condition:
            if self._state is _State.STOPPED:
                return
            if self._state is _State.NEW:
                self._state = _State.STOPPED
                self._condition.notify_all()
                return
            self._state = _State.STOPPING
            while self._startup is not None and not self._startup_settled:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError("SQLite durable-state store did not stop in time")
                self._condition.wait(remaining)
            while self._pending or self._internal_operations:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError("SQLite durable-state store did not stop in time")
                self._condition.wait(remaining)
            connection = self._connection
        if connection is not None:
            close = self._submit_close(connection, deadline)
            close.result(max(0.0, deadline - monotonic()))
        with self._condition:
            self._connection = None
            self._operation_lock = None
            self._state = _State.STOPPED
            self._condition.notify_all()

    def _submit_close(self, connection, deadline: float) -> Future:
        while True:
            with self._condition:
                if self._close is not None:
                    return self._close
            if monotonic() >= deadline:
                raise TimeoutError("SQLite durable-state store did not stop in time")
            try:
                close = self._worker.submit_coroutine(connection.close)
            except AsyncioIOCapacityError:
                sleep(min(0.001, max(0.0, deadline - monotonic())))
                continue
            with self._condition:
                if self._close is None:
                    self._close = close
                    return close
            return self._close


__all__ = ["SQLiteDurableStateStore"]
