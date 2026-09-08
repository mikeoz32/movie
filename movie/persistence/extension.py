from __future__ import annotations

import math
from threading import Lock

from movie.actor.extension import ExtensionId
from movie.io import ASYNCIO_IO
from movie.persistence.sqlite import SQLiteDurableStateStore


class DurableStateExtension:
    """Actor System extension that owns the configured durable-state store."""

    def __init__(self, system) -> None:
        self._system = system
        self._store: SQLiteDurableStateStore | None = None
        self._start_lock = Lock()

    @property
    def store(self) -> SQLiteDurableStateStore:
        store = self._store
        if store is None:
            raise RuntimeError("Durable State extension is not running")
        return store

    def start(self) -> None:
        with self._start_lock:
            if self._store is not None:
                raise RuntimeError("Durable State extension can only start once")
            if self._system._is_in_actor_callback():
                raise RuntimeError(
                    "Durable State extension cannot start from an actor callback"
                )
            path = self._system.config.get("movie.persistence.sqlite.path")
            if not isinstance(path, str) or not path.strip():
                raise ValueError("movie.persistence.sqlite.path must be configured")
            normalized_path = path.strip().lower()
            if normalized_path == ":memory:" or (
                normalized_path.startswith("file:")
                and (
                    normalized_path.startswith("file::memory:")
                    or "mode=memory" in normalized_path
                )
            ):
                raise ValueError("Durable State requires a file-backed SQLite database")
            operation_capacity = self._positive_setting(
                "movie.persistence.operation-capacity",
                1_024,
            )
            pending_byte_capacity = self._positive_setting(
                "movie.persistence.pending-byte-capacity",
                64 * 1_024 * 1_024,
            )
            max_state_bytes = self._positive_setting(
                "movie.persistence.max-state-bytes",
                4 * 1_024 * 1_024,
            )
            if max_state_bytes > pending_byte_capacity:
                raise ValueError("max-state-bytes must not exceed pending-byte-capacity")
            startup_timeout = self._positive_number_setting(
                "movie.persistence.startup-timeout",
                10.0,
            )
            operation_timeout = self._positive_number_setting(
                "movie.persistence.operation-timeout",
                5.0,
            )
            recovery_timeout = self._positive_number_setting(
                "movie.persistence.recovery-timeout",
                operation_timeout,
            )
            io = ASYNCIO_IO.get(self._system)
            store = SQLiteDurableStateStore(
                self._system,
                io.select_worker(),
                path,
                operation_capacity=operation_capacity,
                pending_byte_capacity=pending_byte_capacity,
                max_state_bytes=max_state_bytes,
                operation_timeout=operation_timeout,
                recovery_timeout=recovery_timeout,
            )
            self._store = store
            store.start(startup_timeout)

    def prepare_stop(self, timeout: float) -> None:
        store = self._store
        if store is not None:
            store.prepare_stop()

    def stop(self, timeout: float) -> None:
        store = self._store
        if store is not None:
            store.stop(timeout)

    def _positive_setting(self, path: str, default: int) -> int:
        value = self._system.config.get_int(path, default)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError(f"{path} must be a positive integer")
        return value

    def _positive_number_setting(self, path: str, default: float) -> float:
        value = self._system.config.get(path, default)
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or value <= 0
        ):
            raise ValueError(f"{path} must be a positive number")
        return float(value)


DURABLE_STATE: ExtensionId[DurableStateExtension] = ExtensionId(
    "durable-state",
    DurableStateExtension,
)


__all__ = ["DURABLE_STATE", "DurableStateExtension"]
