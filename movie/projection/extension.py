from __future__ import annotations

import asyncio
import inspect
import math
import sqlite3
from collections.abc import Awaitable, Callable, Coroutine
from concurrent.futures import Future
from enum import Enum, auto
from threading import Event, Lock
from time import monotonic
from typing import Any

from movie.actor.extension import ExtensionId
from movie.future import RuntimeFuture
from movie.io import AsyncioIOCapacityError
from movie.persistence import (
    DURABLE_STATE,
    PERSISTENCE_SLICE_COUNT,
    ChangeFeedCompactedError,
    DurableStateChange,
)
from movie.projection.errors import (
    ProjectionAlreadyRunningError,
    ProjectionBaselineError,
    ProjectionCapacityError,
    ProjectionHandlerTimeout,
    ProjectionOffsetConflictError,
    ProjectionSourceConflictError,
    ProjectionTransactionError,
)
from movie.projection.model import ProjectionId
from movie.projection.transaction import (
    ProjectionTransaction,
    _SQLiteProjectionTransaction,
)

AtLeastOnceHandler = Callable[[tuple[DurableStateChange, ...]], Awaitable[None]]
ExactlyOnceHandler = Callable[
    [ProjectionTransaction, tuple[DurableStateChange, ...]],
    Awaitable[None],
]


class _State(Enum):
    NEW = auto()
    RUNNING = auto()
    STOPPING = auto()
    STOPPED = auto()


class _InvalidHandler(TypeError):
    pass


class _StopRequested(Exception):
    pass


class ProjectionHandle:
    def __init__(self, extension, projection_id: ProjectionId, worker) -> None:
        self._projection_id = projection_id
        self._extension = extension
        self._worker = worker
        self._lock = Lock()
        self._stop_requested = Event()
        self._started = Event()
        self._stopped = Event()
        self._source: Future | None = None
        self._task: asyncio.Task[None] | None = None
        self._offset = 0
        self._failure: BaseException | None = None
        self._last_error: BaseException | None = None
        self._startup_succeeded = False
        self._running = False

    @property
    def projection_id(self) -> ProjectionId:
        return self._projection_id

    @property
    def offset(self) -> int:
        with self._lock:
            return self._offset

    @property
    def failure(self) -> BaseException | None:
        with self._lock:
            return self._failure

    @property
    def last_error(self) -> BaseException | None:
        with self._lock:
            return self._last_error

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._running

    def wait_started(self, timeout: float | None = None) -> bool:
        if not self._started.wait(timeout):
            return False
        with self._lock:
            return self._startup_succeeded

    def wait_stopped(self, timeout: float | None = None) -> bool:
        return self._stopped.wait(timeout)

    def stop(self, timeout: float | None = None) -> None:
        if self._worker.owns_current_thread():
            raise RuntimeError("Projection cannot synchronously stop its I/O worker")
        self.request_stop()
        if not self._stopped.wait(timeout):
            raise TimeoutError(f"Projection {self.projection_id} did not stop in time")

    def request_stop(self) -> None:
        self._stop_requested.set()

    def _bind(self, source: Future) -> None:
        with self._lock:
            self._source = source
        source.add_done_callback(self._complete)

    def _complete(self, source: Future) -> None:
        error = None
        try:
            source.result()
        except BaseException as source_error:
            error = source_error
        with self._lock:
            self._failure = error
            self._running = False
        self._extension._handle_stopped(self)
        self._stopped.set()
        self._started.set()

    def _mark_started(self, offset: int) -> None:
        with self._lock:
            self._offset = offset
            self._startup_succeeded = True
            self._running = True
        self._started.set()

    def _advance(self, offset: int) -> None:
        with self._lock:
            self._offset = offset

    def _record_failure(self, error: BaseException) -> None:
        with self._lock:
            self._last_error = error


class ProjectionExtension:
    def __init__(self, system) -> None:
        self._system = system
        self._lock = Lock()
        self._state = _State.NEW
        self._handles: dict[ProjectionId, ProjectionHandle] = {}
        self._retiring: set[ProjectionId] = set()
        self._store = None
        self._runner_capacity = 0
        self._batch_size = 0
        self._poll_interval = 0.0
        self._retry_min_backoff = 0.0
        self._retry_max_backoff = 0.0
        self._handler_timeout = 0.0
        self._scan_limit = 0
        self._batch_byte_capacity = 0
        self._pending_byte_capacity = 0
        self._batch_slots: asyncio.Semaphore | None = None
        self._compaction_batch_size = 0

    def start(self) -> None:
        with self._lock:
            if self._state is not _State.NEW:
                raise RuntimeError("Projection extension can only start once")
            self._runner_capacity = self._positive_int("runner-capacity", 32)
            self._batch_size = self._positive_int("batch-size", 100, maximum=1_024)
            self._batch_byte_capacity = self._positive_int(
                "batch-byte-capacity", 16 * 1_024 * 1_024
            )
            self._pending_byte_capacity = self._positive_int(
                "pending-byte-capacity", 64 * 1_024 * 1_024
            )
            self._compaction_batch_size = self._positive_int(
                "compaction-batch-size", 1_000, maximum=100_000
            )
            self._poll_interval = self._positive_number("poll-interval", 0.1)
            self._retry_min_backoff = self._positive_number(
                "retry-min-backoff", 0.1
            )
            self._retry_max_backoff = self._positive_number(
                "retry-max-backoff", 5.0
            )
            self._handler_timeout = self._positive_number("handler-timeout", 30.0)
            self._scan_limit = self._positive_int(
                "scan-limit", 10_000, maximum=1_000_000
            )
            if self._retry_max_backoff < self._retry_min_backoff:
                raise ValueError(
                    "movie.projection.retry-max-backoff must not be below retry-min-backoff"
                )
            self._store = DURABLE_STATE.get(self._system).store
            if self._batch_byte_capacity < self._store._max_state_bytes:
                raise ValueError(
                    "movie.projection.batch-byte-capacity must cover max-state-bytes"
                )
            if self._pending_byte_capacity < self._batch_byte_capacity:
                raise ValueError(
                    "movie.projection.pending-byte-capacity must cover one batch"
                )
            self._batch_slots = asyncio.Semaphore(
                self._pending_byte_capacity // self._batch_byte_capacity
            )
            self._state = _State.RUNNING

    def run_at_least_once(
        self,
        projection_id: ProjectionId,
        *,
        entity_type: str,
        min_slice: int,
        max_slice: int,
        handler: AtLeastOnceHandler,
        batch_size: int | None = None,
        initial_offset: int | None = None,
    ) -> ProjectionHandle:
        return self._start_runner(
            projection_id,
            entity_type,
            min_slice,
            max_slice,
            handler,
            batch_size,
            initial_offset,
            self._run_at_least_once,
        )

    def run_exactly_once(
        self,
        projection_id: ProjectionId,
        *,
        entity_type: str,
        min_slice: int,
        max_slice: int,
        handler: ExactlyOnceHandler,
        batch_size: int | None = None,
        initial_offset: int | None = None,
    ) -> ProjectionHandle:
        return self._start_runner(
            projection_id,
            entity_type,
            min_slice,
            max_slice,
            handler,
            batch_size,
            initial_offset,
            self._run_exactly_once,
        )

    def _start_runner(
        self,
        projection_id: ProjectionId,
        entity_type: str,
        min_slice: int,
        max_slice: int,
        handler,
        batch_size: int | None,
        initial_offset: int | None,
        runner,
    ) -> ProjectionHandle:
        if type(projection_id) is not ProjectionId:
            raise ValueError("Projection requires a ProjectionId")
        if not callable(handler):
            raise TypeError("Projection handler must be callable")
        self._validate_initial_offset(initial_offset)
        self._validate_source(entity_type, min_slice, max_slice)
        size = self._batch_size if batch_size is None else batch_size
        if not isinstance(size, int) or isinstance(size, bool) or not 1 <= size <= 1_024:
            raise ValueError("Projection batch size must be between 1 and 1024")
        with self._lock:
            if self._state is not _State.RUNNING:
                raise RuntimeError("Projection extension is not accepting runners")
            if projection_id in self._handles or projection_id in self._retiring:
                raise ProjectionAlreadyRunningError(
                    f"Projection {projection_id} is already running"
                )
            if len(self._handles) >= self._runner_capacity:
                raise ProjectionCapacityError("Projection runner capacity is full")
            store = self._store
            assert store is not None
            handle = ProjectionHandle(self, projection_id, store._worker)
            self._handles[projection_id] = handle
            try:
                source = self._launch_runner(
                    handle,
                    lambda: runner(
                        handle,
                        entity_type,
                        min_slice,
                        max_slice,
                        handler,
                        size,
                        initial_offset,
                    ),
                )
            except BaseException as error:
                self._handles.pop(projection_id, None)
                if isinstance(error, AsyncioIOCapacityError):
                    raise ProjectionCapacityError(
                        "Shared Asyncio I/O command capacity is full"
                    ) from error
                raise
        handle._bind(source)
        return handle

    def compact_changes(self) -> Future[int]:
        with self._lock:
            if self._state is not _State.RUNNING:
                raise RuntimeError("Projection extension is not accepting operations")
            store = self._store
        assert store is not None
        return store.compact_changes(self._compaction_batch_size)

    def retire(self, projection_id: ProjectionId) -> Future[bool]:
        if type(projection_id) is not ProjectionId:
            raise ValueError("Projection retirement requires a ProjectionId")
        with self._lock:
            if self._state is not _State.RUNNING:
                raise RuntimeError("Projection extension is not accepting operations")
            if projection_id in self._handles or projection_id in self._retiring:
                raise ProjectionAlreadyRunningError(
                    f"Projection {projection_id} must stop before retirement"
                )
            store = self._store
            self._retiring.add(projection_id)
        assert store is not None
        try:
            source = store._retire_projection(projection_id.name, projection_id.key)
        except BaseException:
            with self._lock:
                self._retiring.discard(projection_id)
            raise
        result = RuntimeFuture[bool](self._system._submit_callback, cancellable=False)

        def complete(source: Future[bool]) -> None:
            try:
                retired = source.result()
                error = None
            except BaseException as source_error:
                retired = False
                error = source_error
            with self._lock:
                self._retiring.discard(projection_id)
            if error is None:
                result.set_result(retired)
            elif isinstance(error, Exception):
                result.set_exception(error)
            else:
                wrapped = RuntimeError("Projection retirement raised BaseException")
                wrapped.__cause__ = error
                result.set_exception(wrapped)

        source.add_internal_done_callback(complete)
        return result

    def _launch_runner(
        self,
        handle: ProjectionHandle,
        factory: Callable[[], Coroutine[Any, Any, None]],
    ) -> Future[None]:
        completed: Future[None] = Future()

        def launch() -> None:
            if handle._stop_requested.is_set():
                completed.set_result(None)
                return
            try:
                task = asyncio.create_task(factory())
            except BaseException as error:
                completed.set_exception(error)
                return
            with handle._lock:
                handle._task = task

            def complete(task: asyncio.Task[None]) -> None:
                if task.cancelled():
                    completed.cancel()
                    return
                error = task.exception()
                if error is None:
                    completed.set_result(None)
                else:
                    completed.set_exception(error)

            task.add_done_callback(complete)

        handle._worker.schedule(launch)
        return completed

    async def _run_at_least_once(
        self,
        handle: ProjectionHandle,
        entity_type: str,
        min_slice: int,
        max_slice: int,
        handler: AtLeastOnceHandler,
        batch_size: int,
        initial_offset: int | None,
    ) -> None:
        store = self._store
        assert store is not None
        registered = await self._register(
            handle,
            entity_type,
            min_slice,
            max_slice,
            "at-least-once",
            initial_offset,
        )
        if registered is None:
            return
        offset = registered
        handle._mark_started(offset)
        backoff = self._retry_min_backoff
        while not handle._stop_requested.is_set():
            if not await self._reserve_batch(handle):
                return
            delay = None
            try:
                batch = await store._projection_changes(
                    entity_type,
                    min_slice,
                    max_slice,
                    offset,
                    batch_size,
                    self._batch_byte_capacity,
                    self._scan_limit,
                )
                if batch.changes:
                    result = handler(batch.changes)
                    if not inspect.isawaitable(result):
                        raise _InvalidHandler(
                            "Projection handler must return an awaitable"
                        )
                    await self._await_handler(handle, result)
                if batch.offset != offset:
                    saved, actual_offset = await store._projection_save_offset(
                        handle.projection_id.name,
                        handle.projection_id.key,
                        offset,
                        batch.offset,
                    )
                    if not saved:
                        raise ProjectionOffsetConflictError(
                            f"Projection {handle.projection_id} expected offset "
                            f"{offset}, found {actual_offset}"
                        )
                    offset = batch.offset
                    handle._advance(offset)
                backoff = self._retry_min_backoff
                if not batch.changes:
                    delay = self._poll_interval
            except _StopRequested:
                return
            except ChangeFeedCompactedError as error:
                raise ProjectionBaselineError(
                    f"Projection {handle.projection_id} fell behind compacted history"
                ) from error
            except (_InvalidHandler, ProjectionOffsetConflictError):
                raise
            except Exception as error:
                handle._record_failure(error)
                delay = backoff
                backoff = min(backoff * 2, self._retry_max_backoff)
            finally:
                self._release_batch()
            if delay is not None:
                await self._delay(handle, delay)

    async def _run_exactly_once(
        self,
        handle: ProjectionHandle,
        entity_type: str,
        min_slice: int,
        max_slice: int,
        handler: ExactlyOnceHandler,
        batch_size: int,
        initial_offset: int | None,
    ) -> None:
        store = self._store
        assert store is not None
        registered = await self._register(
            handle,
            entity_type,
            min_slice,
            max_slice,
            "sqlite-exactly-once",
            initial_offset,
        )
        if registered is None:
            return
        offset = registered
        handle._mark_started(offset)
        backoff = self._retry_min_backoff
        while not handle._stop_requested.is_set():
            if not await self._reserve_batch(handle):
                return
            delay = None
            try:
                batch = await store._projection_changes(
                    entity_type,
                    min_slice,
                    max_slice,
                    offset,
                    batch_size,
                    self._batch_byte_capacity,
                    self._scan_limit,
                )

                async def apply(connection) -> None:
                    if not batch.changes:
                        return
                    transaction = _SQLiteProjectionTransaction(connection)
                    await transaction._install()
                    try:
                        try:
                            result = handler(transaction, batch.changes)
                            if not inspect.isawaitable(result):
                                raise _InvalidHandler(
                                    "Projection handler must return an awaitable"
                                )
                            await self._await_handler(handle, result)
                            if transaction.authorization_denied:
                                raise ProjectionTransactionError(
                                    "Projection handler attempted a forbidden "
                                    "SQLite operation"
                                )
                        except sqlite3.DatabaseError as error:
                            if transaction.authorization_denied:
                                raise ProjectionTransactionError(
                                    "Projection handler attempted a forbidden SQLite operation"
                                ) from error
                            raise
                    finally:
                        await transaction._close()

                if batch.offset != offset:
                    saved, actual_offset = await store._projection_commit(
                        handle.projection_id.name,
                        handle.projection_id.key,
                        offset,
                        batch.offset,
                        apply,
                    )
                    if not saved:
                        raise ProjectionOffsetConflictError(
                            f"Projection {handle.projection_id} expected offset "
                            f"{offset}, found {actual_offset}"
                        )
                    offset = batch.offset
                    handle._advance(offset)
                backoff = self._retry_min_backoff
                if not batch.changes:
                    delay = self._poll_interval
            except _StopRequested:
                return
            except ChangeFeedCompactedError as error:
                raise ProjectionBaselineError(
                    f"Projection {handle.projection_id} fell behind compacted history"
                ) from error
            except (
                _InvalidHandler,
                ProjectionOffsetConflictError,
                ProjectionTransactionError,
            ):
                raise
            except Exception as error:
                handle._record_failure(error)
                delay = backoff
                backoff = min(backoff * 2, self._retry_max_backoff)
            finally:
                self._release_batch()
            if delay is not None:
                await self._delay(handle, delay)

    async def _reserve_batch(self, handle: ProjectionHandle) -> bool:
        slots = self._batch_slots
        assert slots is not None
        while not handle._stop_requested.is_set():
            try:
                await asyncio.wait_for(slots.acquire(), 0.05)
                return True
            except TimeoutError:
                pass
        return False

    async def _register(
        self,
        handle: ProjectionHandle,
        entity_type: str,
        min_slice: int,
        max_slice: int,
        processing_mode: str,
        initial_offset: int | None,
    ) -> int | None:
        store = self._store
        assert store is not None
        backoff = self._retry_min_backoff
        while not handle._stop_requested.is_set():
            try:
                offset, registration = await store._projection_register(
                    handle.projection_id.name,
                    handle.projection_id.key,
                    entity_type,
                    min_slice,
                    max_slice,
                    processing_mode,
                    initial_offset,
                )
            except Exception as error:
                handle._record_failure(error)
                await self._delay(handle, backoff)
                backoff = min(backoff * 2, self._retry_max_backoff)
                continue
            if registration in ("baseline-required", "invalid-baseline"):
                raise ProjectionBaselineError(
                    f"Projection {handle.projection_id} requires a valid initial baseline"
                )
            if registration != "ok":
                raise ProjectionSourceConflictError(
                    f"Projection {handle.projection_id} has a different stored source"
                )
            return offset
        return None

    def _release_batch(self) -> None:
        slots = self._batch_slots
        assert slots is not None
        slots.release()

    async def _await_handler(self, handle: ProjectionHandle, awaitable) -> None:
        task = asyncio.ensure_future(awaitable)
        deadline = monotonic() + self._handler_timeout
        while True:
            remaining = deadline - monotonic()
            if handle._stop_requested.is_set() or remaining <= 0:
                task.cancel()
                cleanup_error = None
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except BaseException as error:
                    cleanup_error = error
                if handle._stop_requested.is_set():
                    if cleanup_error is not None:
                        handle._record_failure(cleanup_error)
                    raise _StopRequested from cleanup_error
                timeout_error = ProjectionHandlerTimeout(
                    f"Projection {handle.projection_id} handler timed out"
                )
                raise timeout_error from cleanup_error
            done, _pending = await asyncio.wait(
                (task,),
                timeout=min(remaining, 0.05),
            )
            if done:
                task.result()
                return

    @staticmethod
    async def _delay(handle: ProjectionHandle, seconds: float) -> None:
        deadline = monotonic() + seconds
        while not handle._stop_requested.is_set():
            remaining = deadline - monotonic()
            if remaining <= 0:
                return
            await asyncio.sleep(min(remaining, 0.05))

    def prepare_stop(self, timeout: float) -> None:
        with self._lock:
            if self._state in (_State.NEW, _State.RUNNING):
                self._state = _State.STOPPING
            handles = tuple(self._handles.values())
        for handle in handles:
            handle._stop_requested.set()

    def stop(self, timeout: float) -> None:
        deadline = monotonic() + timeout
        self.prepare_stop(timeout)
        with self._lock:
            handles = tuple(self._handles.values())
        for handle in handles:
            if not handle.wait_stopped(max(0.0, deadline - monotonic())):
                raise TimeoutError("Projection runners did not stop in time")
        with self._lock:
            self._state = _State.STOPPED

    def _handle_stopped(self, handle: ProjectionHandle) -> None:
        with self._lock:
            if self._handles.get(handle.projection_id) is handle:
                self._handles.pop(handle.projection_id, None)

    def _positive_int(self, name: str, default: int, *, maximum: int | None = None) -> int:
        path = f"movie.projection.{name}"
        value = self._system.config.get_int(path, default)
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value <= 0
            or (maximum is not None and value > maximum)
        ):
            raise ValueError(f"{path} must be a positive integer")
        return value

    def _positive_number(self, name: str, default: float) -> float:
        path = f"movie.projection.{name}"
        value = self._system.config.get(path, default)
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or value <= 0
        ):
            raise ValueError(f"{path} must be a positive number")
        return float(value)

    @staticmethod
    def _validate_source(entity_type: str, min_slice: int, max_slice: int) -> None:
        if (
            not isinstance(entity_type, str)
            or not entity_type
            or "\x00" in entity_type
            or len(entity_type.encode("utf-8")) > 512
        ):
            raise ValueError("Projection entity type is invalid")
        if (
            not isinstance(min_slice, int)
            or isinstance(min_slice, bool)
            or not isinstance(max_slice, int)
            or isinstance(max_slice, bool)
            or not 0 <= min_slice <= max_slice < PERSISTENCE_SLICE_COUNT
        ):
            raise ValueError("Projection slice range is invalid")

    @staticmethod
    def _validate_initial_offset(initial_offset: int | None) -> None:
        if initial_offset is not None and (
            not isinstance(initial_offset, int)
            or isinstance(initial_offset, bool)
            or initial_offset < 0
        ):
            raise ValueError("Projection initial offset must be nonnegative")


PROJECTIONS: ExtensionId[ProjectionExtension] = ExtensionId(
    "projections",
    ProjectionExtension,
)


__all__ = [
    "PROJECTIONS",
    "AtLeastOnceHandler",
    "ExactlyOnceHandler",
    "ProjectionExtension",
    "ProjectionHandle",
]
