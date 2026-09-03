from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from concurrent.futures import Future
from enum import Enum, auto
from threading import Condition, Event, Lock, Thread, get_ident
from time import monotonic
from typing import Any, TypeVar

from movie.actor.extension import ExtensionId
from movie.actor.system import ExtendedActorSystem


class AsyncioIOStateError(RuntimeError):
    pass


class AsyncioIOCapacityError(RuntimeError):
    pass


class _State(Enum):
    NEW = auto()
    STARTING = auto()
    RUNNING = auto()
    STOPPING = auto()
    STOPPED = auto()


T = TypeVar("T")


class AsyncioIOWorker:
    """One stable event-loop affinity within an actor system's I/O pool."""

    def __init__(self, extension: AsyncioIOExtension, index: int) -> None:
        self._extension = extension
        self.index = index
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: Thread | None = None
        self._thread_id: int | None = None
        self._startup_error: BaseException | None = None
        self._pending_commands = 0
        self._ready = Event()
        self._stopped = Event()

    @property
    def thread(self) -> Thread | None:
        with self._extension._condition:
            return self._thread

    @property
    def pending_commands(self) -> int:
        with self._extension._condition:
            return self._pending_commands

    def wait_stopped(self, timeout: float | None = None) -> bool:
        return self._stopped.wait(timeout)

    def owns_current_thread(self) -> bool:
        with self._extension._condition:
            return self._thread_id == get_ident()

    def run_coroutine(
        self,
        factory: Callable[[], Coroutine[Any, Any, T]],
        timeout: float | None = None,
        *,
        cancel_on_timeout: bool = True,
    ) -> T:
        return self._extension._run_coroutine(
            self,
            factory,
            timeout,
            cancel_on_timeout=cancel_on_timeout,
        )

    def submit_coroutine(
        self,
        factory: Callable[[], Coroutine[Any, Any, T]],
    ) -> Future[T]:
        return self._extension._submit_coroutine(self, factory)

    def schedule(self, callback: Callable[..., Any], *args: Any) -> Future[None]:
        return self._extension._schedule(self, callback, *args)

    def _schedule_control(self, callback: Callable[..., Any], *args: Any) -> Future[None]:
        return self._extension._schedule_control(self, callback, *args)


class AsyncioIOExtension:
    """Actor-system-owned pool of asyncio event-loop workers."""

    def __init__(self, system: ExtendedActorSystem) -> None:
        self._name = system.name
        startup_timeout = system.config.get_int("movie.io.asyncio.startup-timeout", 10)
        command_capacity = system.config.get_int(
            "movie.io.asyncio.command-capacity",
            1024,
        )
        worker_count = system.config.get_int("movie.io.asyncio.event-loop-count", 1)
        for value, field in (
            (startup_timeout, "Asyncio I/O startup timeout"),
            (command_capacity, "Asyncio I/O command capacity"),
            (worker_count, "Asyncio I/O event loop count"),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{field} must be positive")
        self._startup_timeout = float(startup_timeout)
        self._command_capacity = command_capacity
        self._condition = Condition(Lock())
        self._state = _State.NEW
        self._workers = tuple(AsyncioIOWorker(self, index) for index in range(worker_count))
        self._next_worker = 0
        self._stopped = Event()
        self._pending_commands = 0
        self._stop_requested = False

    @property
    def workers(self) -> tuple[AsyncioIOWorker, ...]:
        return self._workers

    @property
    def default_worker(self) -> AsyncioIOWorker:
        return self._workers[0]

    @property
    def thread(self) -> Thread | None:
        return self.default_worker.thread

    @property
    def pending_commands(self) -> int:
        with self._condition:
            return self._pending_commands

    def wait_stopped(self, timeout: float | None = None) -> bool:
        return self._stopped.wait(timeout)

    def start(self) -> None:
        with self._condition:
            if self._state is not _State.NEW:
                raise AsyncioIOStateError("Asyncio I/O extension can only start once")
            self._state = _State.STARTING

        for worker in self._workers:
            with self._condition:
                if self._state is not _State.STARTING:
                    worker._ready.set()
                    worker._stopped.set()
                    continue
                suffix = "" if worker.index == 0 else f"-{worker.index}"
                thread = Thread(
                    target=self._run_worker,
                    args=(worker,),
                    name=f"movie-asyncio-io-{self._name}{suffix}",
                    daemon=True,
                )
                worker._thread = thread
                try:
                    thread.start()
                except BaseException as error:
                    worker._thread = None
                    worker._startup_error = error
                    worker._ready.set()
                    worker._stopped.set()
                    self._state = _State.STOPPING
                    self._condition.notify_all()
                    break

        for worker in self._workers:
            if worker._thread is None:
                worker._ready.set()
                worker._stopped.set()

        deadline = monotonic() + self._startup_timeout
        for worker in self._workers:
            if not worker._ready.wait(max(0.0, deadline - monotonic())):
                try:
                    self.stop(self._startup_timeout)
                except BaseException:
                    pass
                raise TimeoutError("Asyncio I/O workers did not start before the deadline")

        with self._condition:
            startup_error = next(
                (
                    worker._startup_error
                    for worker in self._workers
                    if worker._startup_error is not None
                ),
                None,
            )
            running = self._state is _State.STARTING and startup_error is None
            if running:
                self._state = _State.RUNNING
                self._condition.notify_all()

        if not running:
            try:
                self.stop(self._startup_timeout)
            except BaseException:
                pass
            if startup_error is not None:
                raise AsyncioIOStateError("Asyncio I/O worker failed to start") from startup_error
            raise AsyncioIOStateError("Asyncio I/O workers stopped during startup")

    def stop(self, timeout: float) -> None:
        if timeout < 0:
            raise ValueError("Asyncio I/O shutdown timeout must be nonnegative")
        if self.owns_current_thread():
            raise AsyncioIOStateError("Asyncio I/O pool cannot synchronously stop itself")
        deadline = monotonic() + timeout
        with self._condition:
            if self._state is _State.STOPPED:
                return
            if self._state is _State.NEW:
                self._state = _State.STOPPED
                for worker in self._workers:
                    worker._ready.set()
                    worker._stopped.set()
                self._stopped.set()
                return
            self._state = _State.STOPPING
            while self._pending_commands:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError("Asyncio I/O commands did not settle before the deadline")
                self._condition.wait(remaining)
            loops = tuple(worker._loop for worker in self._workers if worker._loop is not None)
            threads = tuple(
                worker._thread for worker in self._workers if worker._thread is not None
            )
            request_stop = not self._stop_requested
            self._stop_requested = True

        if request_stop:
            for loop in loops:
                try:
                    loop.call_soon_threadsafe(loop.stop)
                except RuntimeError:
                    pass
        for thread in threads:
            thread.join(max(0.0, deadline - monotonic()))
            if thread.is_alive():
                raise TimeoutError("Asyncio I/O loop did not stop before the deadline")
        with self._condition:
            if all(worker._stopped.is_set() for worker in self._workers):
                self._state = _State.STOPPED
                self._stopped.set()
        if not self._stopped.wait(max(0.0, deadline - monotonic())):
            raise TimeoutError("Asyncio I/O loop did not stop before the deadline")

    def select_worker(self) -> AsyncioIOWorker:
        with self._condition:
            if self._state is not _State.RUNNING:
                raise AsyncioIOStateError("Asyncio I/O extension is not running")
            worker = self._workers[self._next_worker]
            self._next_worker = (self._next_worker + 1) % len(self._workers)
            return worker

    def owns_current_thread(self) -> bool:
        owner = get_ident()
        with self._condition:
            return any(worker._thread_id == owner for worker in self._workers)

    def run_coroutine(
        self,
        factory: Callable[[], Coroutine[Any, Any, T]],
        timeout: float | None = None,
        *,
        cancel_on_timeout: bool = True,
    ) -> T:
        return self.default_worker.run_coroutine(
            factory,
            timeout,
            cancel_on_timeout=cancel_on_timeout,
        )

    def schedule(self, callback: Callable[..., Any], *args: Any) -> Future[None]:
        return self.default_worker.schedule(callback, *args)

    def _run_coroutine(
        self,
        worker: AsyncioIOWorker,
        factory: Callable[[], Coroutine[Any, Any, T]],
        timeout: float | None,
        *,
        cancel_on_timeout: bool,
    ) -> T:
        if self.owns_current_thread():
            raise AsyncioIOStateError("Cannot block an Asyncio I/O worker on itself")
        future = self._submit_coroutine(worker, factory)
        try:
            return future.result(timeout)
        except TimeoutError:
            if cancel_on_timeout:
                future.cancel()
            raise

    def _submit_coroutine(
        self,
        worker: AsyncioIOWorker,
        factory: Callable[[], Coroutine[Any, Any, T]],
    ) -> Future[T]:
        loop = self._admit_command(worker)
        try:
            coroutine = factory()
        except BaseException:
            self._release_command(worker)
            raise
        completed: Future[T] = Future()

        def run() -> None:
            if completed.cancelled():
                coroutine.close()
                self._release_command(worker)
                return
            try:
                task = loop.create_task(coroutine)
            except BaseException as error:
                coroutine.close()
                try:
                    if completed.set_running_or_notify_cancel():
                        completed.set_exception(error)
                finally:
                    self._release_command(worker)
                return

            def cancel_task(result: Future[T]) -> None:
                if result.cancelled() and not task.done():
                    try:
                        loop.call_soon_threadsafe(task.cancel)
                    except RuntimeError:
                        pass

            def complete(task: asyncio.Task[T]) -> None:
                try:
                    if task.cancelled():
                        completed.cancel()
                    elif completed.set_running_or_notify_cancel():
                        error = task.exception()
                        if error is None:
                            completed.set_result(task.result())
                        else:
                            completed.set_exception(error)
                finally:
                    self._release_command(worker)

            completed.add_done_callback(cancel_task)
            task.add_done_callback(complete)

        try:
            loop.call_soon_threadsafe(run)
        except BaseException:
            coroutine.close()
            self._release_command(worker)
            raise
        return completed

    def _schedule(
        self,
        worker: AsyncioIOWorker,
        callback: Callable[..., Any],
        *args: Any,
    ) -> Future[None]:
        loop = self._admit_command(worker)
        completed: Future[None] = Future()

        def invoke() -> None:
            if not completed.set_running_or_notify_cancel():
                self._release_command(worker)
                return
            try:
                callback(*args)
            except BaseException as error:
                completed.set_exception(error)
            else:
                completed.set_result(None)
            finally:
                self._release_command(worker)

        try:
            loop.call_soon_threadsafe(invoke)
        except BaseException:
            self._release_command(worker)
            raise
        return completed

    def _schedule_control(
        self,
        worker: AsyncioIOWorker,
        callback: Callable[..., Any],
        *args: Any,
    ) -> Future[None]:
        with self._condition:
            if self._state is not _State.RUNNING or worker._loop is None:
                raise AsyncioIOStateError("Asyncio I/O extension is not running")
            loop = worker._loop
        completed: Future[None] = Future()

        def invoke() -> None:
            if not completed.set_running_or_notify_cancel():
                return
            try:
                callback(*args)
            except BaseException as error:
                completed.set_exception(error)
            else:
                completed.set_result(None)

        try:
            loop.call_soon_threadsafe(invoke)
        except BaseException:
            completed.cancel()
            raise
        return completed

    def _admit_command(
        self,
        worker: AsyncioIOWorker,
    ) -> asyncio.AbstractEventLoop:
        with self._condition:
            if self._state is not _State.RUNNING or worker._loop is None:
                raise AsyncioIOStateError("Asyncio I/O extension is not running")
            if self._pending_commands >= self._command_capacity:
                raise AsyncioIOCapacityError("Asyncio I/O command capacity is full")
            self._pending_commands += 1
            worker._pending_commands += 1
            return worker._loop

    def _release_command(self, worker: AsyncioIOWorker) -> None:
        with self._condition:
            self._pending_commands -= 1
            worker._pending_commands -= 1
            self._condition.notify_all()

    def _run_worker(self, worker: AsyncioIOWorker) -> None:
        loop: asyncio.AbstractEventLoop | None = None
        stop_others: tuple[asyncio.AbstractEventLoop, ...] = ()
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            with self._condition:
                worker._loop = loop
                worker._thread_id = get_ident()
                run = self._state is _State.STARTING
                self._condition.notify_all()
            worker._ready.set()
            if run:
                loop.run_forever()
        except BaseException as error:
            with self._condition:
                worker._startup_error = error
            worker._ready.set()
        finally:
            try:
                if loop is not None:
                    pending = asyncio.all_tasks(loop)
                    for task in pending:
                        task.cancel()
                    if pending:
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                    loop.run_until_complete(loop.shutdown_asyncgens())
                    loop.run_until_complete(loop.shutdown_default_executor())
            finally:
                try:
                    if loop is not None:
                        loop.close()
                finally:
                    worker._stopped.set()
                    with self._condition:
                        worker._loop = None
                        worker._thread_id = None
                        if self._state is _State.RUNNING:
                            self._state = _State.STOPPING
                            stop_others = tuple(
                                other._loop
                                for other in self._workers
                                if other is not worker and other._loop is not None
                            )
                        if all(item._stopped.is_set() for item in self._workers):
                            self._state = _State.STOPPED
                            self._stopped.set()
                        self._condition.notify_all()
                    for other_loop in stop_others:
                        try:
                            other_loop.call_soon_threadsafe(other_loop.stop)
                        except RuntimeError:
                            pass


ASYNCIO_IO: ExtensionId[AsyncioIOExtension] = ExtensionId(
    "asyncio-io",
    AsyncioIOExtension,
)


__all__ = [
    "ASYNCIO_IO",
    "AsyncioIOCapacityError",
    "AsyncioIOExtension",
    "AsyncioIOStateError",
    "AsyncioIOWorker",
]
