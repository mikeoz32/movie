import logging
import os
from collections import deque
from enum import Enum, auto
from queue import SimpleQueue
from threading import (
    Event,
    Lock,
    Thread,
    current_thread,
    get_ident,
    local,
)
from time import monotonic

from movie.config import Config
from movie.dispatch.dispatcher import InternalDispatcher, Task

_REAPER_QUEUE = SimpleQueue()
_REAPER_LOCK = Lock()
_REAPER_STARTED = False


def _reap_abandoned_tasks() -> None:
    while True:
        queues = _REAPER_QUEUE.get()
        for tasks in queues:
            tasks.clear()


def _submit_abandoned_tasks(queues: list[deque]) -> None:
    global _REAPER_STARTED
    with _REAPER_LOCK:
        if not _REAPER_STARTED:
            Thread(
                target=_reap_abandoned_tasks,
                name="movie-dispatcher-reaper",
                daemon=True,
            ).start()
            _REAPER_STARTED = True
    _REAPER_QUEUE.put(queues)


class DispatcherCapacityExceeded(RuntimeError):
    pass


class DispatcherStopped(RuntimeError):
    pass


class WorkQueue:
    def __init__(self, capacity: int, system_capacity: int) -> None:
        self.lock = Lock()
        self.ready = Event()
        self.tasks = deque()
        self.system_tasks = deque()
        self.capacity = capacity
        self.system_capacity = system_capacity

    def take_system(self, *, owner: bool = True) -> Task | None:
        with self.lock:
            if self.system_tasks:
                task = self.system_tasks.popleft()
            else:
                if owner and not self.tasks:
                    self.ready.clear()
                return None
            if owner and not self.system_tasks and not self.tasks:
                self.ready.clear()
            return task

    def take_ordinary(self, *, owner: bool = True) -> Task | None:
        with self.lock:
            if self.tasks:
                task = self.tasks.popleft()
            else:
                if owner and not self.system_tasks:
                    self.ready.clear()
                return None
            if owner and not self.system_tasks and not self.tasks:
                self.ready.clear()
            return task

    def detach(self) -> tuple[deque, deque]:
        with self.lock:
            tasks = self.tasks
            self.tasks = deque()
            system_tasks = self.system_tasks
            self.system_tasks = deque()
            self.ready.clear()
            return tasks, system_tasks


class Worker:
    def __init__(self, dispatcher: "WorkerPoolDispatcherImpl", index: int) -> None:
        self._dispatcher = dispatcher
        self._index = index
        self._busy = Event()
        self._thread = Thread(
            target=self.run,
            name=f"movie-dispatcher-{index}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def join(self, timeout: float | None = None) -> None:
        self._thread.join(timeout)

    @property
    def is_alive(self) -> bool:
        return self._thread.is_alive()

    @property
    def is_busy(self) -> bool:
        return self._busy.is_set()

    @property
    def thread(self) -> Thread:
        return self._thread

    def run(self) -> None:
        while True:
            try:
                task = self._dispatcher.next_task(self._index)
            except DispatcherStopped:
                return
            self._busy.set()
            try:
                task()
            except BaseException:
                logging.getLogger("movie.dispatcher").exception("Dispatcher task failed")
            finally:
                self._busy.clear()


class DispatcherState(Enum):
    NEW = auto()
    STARTING = auto()
    RUNNING = auto()
    STOPPING = auto()
    STOPPED = auto()


class WorkerPoolDispatcherImpl(InternalDispatcher):
    cooperative_shutdown = True

    def __init__(self, config: Config | None = None) -> None:
        config = config or Config({})
        default_workers = os.process_cpu_count() or 1
        workers = config.get_int("workers", default_workers)
        shutdown_timeout = config.get_int("shutdown-timeout", 10)
        queue_capacity = config.get_int("queue-capacity", 100_000)
        system_queue_capacity = config.get_int("system-queue-capacity", 100_000)
        if (
            workers is None
            or workers <= 0
            or shutdown_timeout is None
            or shutdown_timeout <= 0
            or queue_capacity is None
            or queue_capacity <= 0
            or system_queue_capacity is None
            or system_queue_capacity <= 0
        ):
            raise ValueError(
                "Worker count, queue capacities, and shutdown timeout must be positive"
            )

        self._worker_count = workers
        base_capacity, extra_capacity = divmod(queue_capacity, workers)
        base_system_capacity, extra_system_capacity = divmod(
            system_queue_capacity, workers
        )
        self._queues = [
            WorkQueue(
                base_capacity + (index < extra_capacity),
                base_system_capacity + (index < extra_system_capacity),
            )
            for index in range(workers)
        ]
        self._workers = [Worker(self, index) for index in range(workers)]
        self._placement = local()
        self._idle_lock = Lock()
        self._idle_workers: set[int] = set()
        self._idle_available = Event()
        self._system_lock = Lock()
        self._system_count = 0
        self._system_available = Event()
        self._lifecycle_lock = Lock()
        self._state = DispatcherState.NEW
        self._abort = Event()
        self._shutdown_timeout = float(shutdown_timeout)

    def start(self) -> None:
        with self._lifecycle_lock:
            if self._state is not DispatcherState.NEW:
                raise RuntimeError("Dispatcher has already been started")
            self._set_state(DispatcherState.STARTING)
            started_workers = []
            try:
                for worker in self._workers:
                    worker.start()
                    started_workers.append(worker)
            except BaseException:
                self._abort.set()
                self._set_state(DispatcherState.STOPPING)
                self._wake_all()
                for worker in started_workers:
                    worker.join(self._shutdown_timeout)
                self._set_state(DispatcherState.STOPPED)
                raise
            self._set_state(DispatcherState.RUNNING)

    def stop(self, timeout: float | None = None) -> None:
        if any(worker.thread is current_thread() for worker in self._workers):
            raise RuntimeError("Dispatcher cannot stop from one of its own workers")
        if timeout is not None and timeout <= 0:
            raise ValueError("Dispatcher shutdown timeout must be positive")

        shutdown_timeout = self._shutdown_timeout if timeout is None else timeout
        deadline = monotonic() + shutdown_timeout
        if not self._lifecycle_lock.acquire(timeout=shutdown_timeout):
            raise TimeoutError(
                f"Dispatcher did not stop within {shutdown_timeout:g} seconds"
            )
        try:
            if self._state is DispatcherState.STOPPED:
                return
            if self._state is DispatcherState.NEW:
                self._set_state(DispatcherState.STOPPED, deadline)
                return
            if self._state is DispatcherState.RUNNING:
                self._set_state(DispatcherState.STOPPING, deadline)
                self._wake_all()

            for worker in self._workers:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    break
                worker.join(remaining)

            if any(worker.is_alive for worker in self._workers):
                self._abort.set()
                self._wake_all()
                raise TimeoutError(
                    f"Dispatcher did not stop within {shutdown_timeout:g} seconds"
                )
            self._detach_abandoned_tasks()
            self._set_state(DispatcherState.STOPPED, deadline)
        finally:
            self._lifecycle_lock.release()

    def dispatch(self, task: Task) -> None:
        self._publish(task, system=False)

    def dispatch_system(self, task: Task) -> None:
        self._publish(task, system=True)

    def _publish(self, task: Task, *, system: bool) -> None:
        start = self._choose_worker()
        for offset in range(self._worker_count):
            worker_index = (start + offset) % self._worker_count
            queue = self._queues[worker_index]
            with queue.lock:
                if self._state is not DispatcherState.RUNNING:
                    raise RuntimeError("Dispatcher is not accepting tasks")
                if not system and len(queue.tasks) >= queue.capacity:
                    continue
                if system and len(queue.system_tasks) >= queue.system_capacity:
                    continue
                was_empty = not queue.tasks and not queue.system_tasks
                target = queue.system_tasks if system else queue.tasks
                target.append(task)
                if system:
                    with self._system_lock:
                        self._system_count += 1
                        self._system_available.set()
                if was_empty:
                    queue.ready.set()
                return
        queue_type = "system" if system else "ordinary"
        raise DispatcherCapacityExceeded(
            f"Dispatcher {queue_type} activation queue is full"
        )

    def _choose_worker(self) -> int:
        start = getattr(self._placement, "next", None)
        if start is None:
            start = ((get_ident() >> 4) * 0x9E3779B1) % self._worker_count
        alternate = (start + 1) % self._worker_count
        if self._workers[start].is_busy and not self._workers[alternate].is_busy:
            start = alternate
        elif self._workers[start].is_busy and self._idle_available.is_set():
            with self._idle_lock:
                if self._idle_workers:
                    start = self._idle_workers.pop()
                    if not self._idle_workers:
                        self._idle_available.clear()
        self._placement.next = (start + 1) % self._worker_count
        return start

    def next_task(self, worker_index: int) -> Task:
        own_queue = self._queues[worker_index]
        while True:
            if self._abort.is_set():
                raise DispatcherStopped
            queued = self._take_available(worker_index)
            if queued is not None:
                return queued
            if self._state in (DispatcherState.STOPPING, DispatcherState.STOPPED):
                raise DispatcherStopped
            self._mark_idle(worker_index)
            queued = self._take_available(worker_index)
            if queued is not None:
                self._mark_active(worker_index)
                return queued
            if self._state in (DispatcherState.STOPPING, DispatcherState.STOPPED):
                self._mark_active(worker_index)
                raise DispatcherStopped
            own_queue.ready.wait()
            self._mark_active(worker_index)

    def _take_available(self, worker_index: int) -> Task | None:
        if self._system_available.is_set():
            for offset in range(self._worker_count):
                queued = self._queues[
                    (worker_index + offset) % self._worker_count
                ].take_system(owner=offset == 0)
                if queued is not None:
                    with self._system_lock:
                        self._system_count -= 1
                        if self._system_count == 0:
                            self._system_available.clear()
                    return queued

        queued = self._queues[worker_index].take_ordinary()
        if queued is not None:
            return queued
        for offset in range(1, self._worker_count):
            queued = self._queues[
                (worker_index + offset) % self._worker_count
            ].take_ordinary(owner=False)
            if queued is not None:
                return queued
        return None

    def _mark_idle(self, worker_index: int) -> None:
        with self._idle_lock:
            self._idle_workers.add(worker_index)
            self._idle_available.set()

    def _mark_active(self, worker_index: int) -> None:
        with self._idle_lock:
            self._idle_workers.discard(worker_index)
            if not self._idle_workers:
                self._idle_available.clear()

    def _set_state(
        self, state: DispatcherState, deadline: float | None = None
    ) -> None:
        acquired = []
        try:
            for queue in self._queues:
                if deadline is None:
                    queue.lock.acquire()
                else:
                    remaining = deadline - monotonic()
                    if remaining <= 0 or not queue.lock.acquire(timeout=remaining):
                        raise TimeoutError("Dispatcher state transition timed out")
                acquired.append(queue.lock)
            self._state = state
        finally:
            for lock in reversed(acquired):
                lock.release()

    def _wake_all(self) -> None:
        for queue in self._queues:
            queue.ready.set()

    def _detach_abandoned_tasks(self) -> None:
        abandoned = []
        for queue in self._queues:
            tasks, system_tasks = queue.detach()
            if tasks:
                abandoned.append(tasks)
            if system_tasks:
                abandoned.append(system_tasks)
        with self._system_lock:
            self._system_count = 0
            self._system_available.clear()
        if abandoned:
            _submit_abandoned_tasks(abandoned)
