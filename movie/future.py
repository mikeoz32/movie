from __future__ import annotations

import logging
from collections import deque
from concurrent.futures import Future
from concurrent.futures._base import (
    CANCELLED,
    CANCELLED_AND_NOTIFIED,
    FINISHED,
    PENDING,
    RUNNING,
)
from queue import Queue
from threading import Lock, Thread, current_thread
from time import monotonic
from typing import Callable, Generic, TypeVar

T = TypeVar("T")


class CallbackExecutor:
    def __init__(self, name: str, workers: int = 4) -> None:
        if workers <= 0:
            raise ValueError("Future callback worker count must be positive")
        self._name = name
        self._worker_count = workers
        self._queue = Queue()
        self._lock = Lock()
        self._threads: list[Thread] = []
        self._closed = False

    def submit(self, callback: Callable[[], None]) -> None:
        with self._lock:
            if self._closed:
                run_inline = True
            else:
                run_inline = False
                if not self._threads:
                    self._threads = [
                        Thread(
                            target=self._run,
                            name=f"movie-callbacks-{self._name}-{index}",
                            daemon=True,
                        )
                        for index in range(self._worker_count)
                    ]
                    for thread in self._threads:
                        thread.start()
                self._queue.put(callback)
        if run_inline:
            self._execute(callback)

    def close(self, timeout: float) -> None:
        deadline = monotonic() + timeout
        with self._lock:
            if not self._closed:
                self._closed = True
                for _ in self._threads:
                    self._queue.put(None)
            threads = list(self._threads)
        caller = current_thread()
        for thread in threads:
            if thread is caller:
                continue
            remaining = deadline - monotonic()
            if remaining <= 0:
                break
            thread.join(remaining)
        if any(thread is not caller and thread.is_alive() for thread in threads):
            raise TimeoutError("Future callbacks did not stop within the deadline")

    def owns_current_thread(self) -> bool:
        caller = current_thread()
        with self._lock:
            return any(thread is caller for thread in self._threads)

    def _run(self) -> None:
        while True:
            callback = self._queue.get()
            if callback is None:
                return
            self._execute(callback)

    @staticmethod
    def _execute(callback: Callable[[], None]) -> None:
        try:
            callback()
        except BaseException:
            logging.getLogger("movie.future").exception("Future callback failed")


class RuntimeFuture(Future[T], Generic[T]):
    """Future that never runs registered callbacks on its settlement thread."""

    def __init__(
        self,
        submit_callback: Callable[[Callable[[], None]], None] | None = None,
        *,
        cancellable: bool = True,
    ) -> None:
        super().__init__()
        self._submit_callback = submit_callback or CallbackExecutor._execute
        self._cancel_hook: Callable[[], None] | None = None
        self._internal_callbacks: list[Callable[[RuntimeFuture[T]], None]] = []
        self._callbacks_started = False
        self._callback_draining = False
        self._pending_callbacks = deque()
        self._cancellable = cancellable
        self._bound = submit_callback is not None

    def bind_callback_submit(
        self,
        submit_callback: Callable[[Callable[[], None]], None],
        *,
        cancel_hook: Callable[[], None] | None = None,
    ) -> None:
        with self._condition:
            if self._bound or self._state != PENDING:
                raise RuntimeError("Future is already bound or completed")
            self._submit_callback = submit_callback
            self._cancel_hook = cancel_hook
            self._bound = True

    def set_cancel_hook(self, hook: Callable[[], None]) -> None:
        run_hook = False
        with self._condition:
            if self._cancel_hook is not None:
                raise RuntimeError("Future cancellation hook is already installed")
            self._cancel_hook = hook
            run_hook = self._state in (CANCELLED, CANCELLED_AND_NOTIFIED)
        if run_hook:
            hook()

    def cancel(self) -> bool:
        with self._condition:
            if not self._cancellable:
                return False
            if self._state in (RUNNING, FINISHED):
                return False
            if self._state in (CANCELLED, CANCELLED_AND_NOTIFIED):
                return True
            self._state = CANCELLED
            self._condition.notify_all()
            hook = self._cancel_hook

        if hook is not None:
            try:
                hook()
            except BaseException:
                logging.getLogger("movie.future").exception(
                    "Future cancellation hook failed"
                )
        self._invoke_callbacks()
        return True

    def add_done_callback(self, fn) -> None:
        start_runner = False
        with self._condition:
            if self._state not in (
                CANCELLED,
                CANCELLED_AND_NOTIFIED,
                FINISHED,
            ) or not self._callbacks_started:
                self._done_callbacks.append(fn)
                return
            self._pending_callbacks.append(fn)
            if not self._callback_draining:
                self._callback_draining = True
                start_runner = True
        if start_runner:
            self._submit_callback(self._drain_user_callbacks)

    def add_internal_done_callback(self, fn) -> None:
        with self._condition:
            if self._state not in (
                CANCELLED,
                CANCELLED_AND_NOTIFIED,
                FINISHED,
            ) or not self._callbacks_started:
                self._internal_callbacks.append(fn)
                return
        self._run_internal_callback(fn)

    def _invoke_callbacks(self) -> None:
        start_runner = False
        with self._condition:
            if self._callbacks_started:
                return
            self._callbacks_started = True
            internal_callbacks = tuple(self._internal_callbacks)
            self._pending_callbacks.extend(self._done_callbacks)
            if self._pending_callbacks and not self._callback_draining:
                self._callback_draining = True
                start_runner = True
        for callback in internal_callbacks:
            self._run_internal_callback(callback)
        if start_runner:
            self._submit_callback(self._drain_user_callbacks)

    def _drain_user_callbacks(self) -> None:
        while True:
            with self._condition:
                if not self._pending_callbacks:
                    self._callback_draining = False
                    return
                callback = self._pending_callbacks.popleft()
            try:
                callback(self)
            except BaseException:
                logging.getLogger("movie.future").exception(
                    "Future callback failed"
                )

    def _run_internal_callback(self, callback) -> None:
        try:
            callback(self)
        except BaseException:
            logging.getLogger("movie.future").exception(
                "Internal Future callback failed"
            )
