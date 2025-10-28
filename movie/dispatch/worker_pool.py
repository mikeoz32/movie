from concurrent.futures import Future
import os
from queue import Empty, Queue, ShutDown
from threading import Thread
from typing import Protocol, Tuple
from movie.dispatch.dispatcher import InternalDispatcher, Task


class WorkerPoolDispatcher(InternalDispatcher, Protocol):
    def get_task(self) -> Task: ...

    @property
    def queue(self) -> Queue[Task]: ...


class Worker:
    """
    A worker that executes tasks.
    """

    def __init__(self, dispatcher: WorkerPoolDispatcher) -> None:
        self._running = False
        self._thread: Thread = Thread(target=self.run, daemon=True)
        self._queue: Queue[Tuple[Future, Task]] = Queue()
        self._dispatcher = dispatcher

    def submit(self, task: Task) -> Future:
        f = Future()
        self._queue.put((f, task))
        return f

    def load(self) -> int:
        return self._queue.qsize()

    def start(self) -> None:
        self._running = True
        self._thread.start()

    def stop(self) -> None:
        self._queue.join()
        self._queue.shutdown()
        self._running = False
        self._thread.join()

    def run(
        self,
    ) -> None:
        while self._running is True:
            task = None
            queue = None
            try:
                task = self._queue.get(timeout=0.01)
                queue = self._queue
            except Empty:
                try:
                    task = self._dispatcher.get_task()
                    queue = self._dispatcher.queue
                except Empty:
                    continue
            except ShutDown:
                break
            if task is not None:
                (f, t) = task
                try:
                    result = t()
                    f.set_result(result)
                except Exception as e:
                    f.set_exception(e)
                del task
                queue.task_done()


class WorkerPoolDispatcherImpl(WorkerPoolDispatcher):
    def __init__(self) -> None:
        self._cpu_count = os.cpu_count() or 1
        self._workers: list[Worker | None] = [None] * self._cpu_count
        self._queue: Queue[Task] = Queue()

        for i in range(self._cpu_count):
            worker = Worker(self)
            self._workers[i] = worker

    def start(self) -> None:
        for worker in self._workers:
            if worker is not None:
                worker.start()

    def stop(self) -> None:
        for worker in self._workers:
            if worker is not None:
                worker.stop()
        self._queue.join()
        self._queue.shutdown(True)

    def schedule(self, task: Task) -> None:
        try:
            worker = self._min_loaded_worker()
            worker.submit(task)
        except RuntimeError:
            self._queue.put(task)

    def get_task(self) -> Task:
        return self._queue.get(block=False)

    @property
    def queue(self) -> Queue[Task]:
        return self._queue

    def _min_loaded_worker(self) -> Worker:
        min_load = float("inf")
        selected_worker: Worker | None = None

        for worker in self._workers:
            if worker is not None:
                load = worker.load()
                if load < min_load:
                    min_load = load
                    selected_worker = worker

        if selected_worker is None:
            raise RuntimeError("No available workers")

        return selected_worker
