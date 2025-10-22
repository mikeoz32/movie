import enum
import os
from threading import Thread
from queue import Empty, Queue, ShutDown
from typing import Callable, Protocol

from typing import TYPE_CHECKING

# For preventing circular imports
if TYPE_CHECKING:
    from movie.actor import ActorContext


class Mailbox(Protocol):
    def send(self, message) -> None: ...
    def sendSystem(self, message) -> None: ...
    def stop(self) -> None: ...


class DefaultMailbox(Mailbox):
    """
    An actor mailbox. Handles user and system messages and processes them.
    """

    def __init__(self, scheduler: "Scheduler", actor: ActorContext) -> None:
        self._scheduler = scheduler
        self._messages: Queue = Queue()
        self._system_messages: Queue = Queue()
        self._actor = actor
        self._scheduled = False

    def send(self, message) -> None:
        self._messages.put(message)
        if not self._scheduled:
            self._scheduler.schedule(Task(self))
        self._scheduled = True

    def sendSystem(self, message) -> None:
        self._system_messages.put(message)
        if not self._scheduled:
            self._scheduler.schedule(Task(self))
        self._scheduled = True

    def stop(self) -> None:
        self._messages.join()
        self._system_messages.join()
        self._messages.shutdown()
        self._system_messages.shutdown()

    def __call__(self) -> None:
        while True:
            try:
                message = self._messages.get(block=False)
                self._actor.invoke(message)
                self._messages.task_done()
            except Empty:
                break

        while True:
            try:
                system_message = self._system_messages.get(block=False)
                self._actor.invoke_system(system_message)
                self._system_messages.task_done()
            except Empty:
                break
        self._scheduled = False


class SingleMessageDispatchMailbox(DefaultMailbox):
    """
    An actor mailbox that processes one message at a time and possibly in different threads.
    """

    def __call__(self) -> None:
        try:
            message = self._messages.get(block=False)
            self._actor.invoke(message)
            self._messages.task_done()
            if not self._messages.empty():
                self._scheduler.schedule(Task(self))
        except Empty:
            return

        try:
            message = self._system_messages.get(block=False)
            self._actor.invoke_system(message)
            self._messages.task_done()
            if not self._system_messages.empty():
                self._scheduler.schedule(Task(self))
        except Empty:
            return


class MailboxType(enum.Enum):
    DEFAULT = DefaultMailbox
    SINGLE_MESSAGE_DISPATCH = SingleMessageDispatchMailbox


def create_mailbox(
    scheduler: "Scheduler",
    actor: ActorContext,
    mailbox_type: MailboxType = MailboxType.DEFAULT,
) -> Mailbox:
    return mailbox_type.value(scheduler, actor)


class Task:
    def __init__(self, target: Callable) -> None:
        self._target = target

    def __call__(self) -> None:
        self._target()


class Scheduler:
    """
    A threaded task scheduler
    """

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

    def _min_loaded_worker(self) -> "Worker":
        min_load = float("inf")
        selected_worker: "Worker | None" = None

        for worker in self._workers:
            if worker is not None:
                load = worker.load()
                if load < min_load:
                    min_load = load
                    selected_worker = worker

        if selected_worker is None:
            raise RuntimeError("No available workers")

        return selected_worker


class Worker:
    """
    A worker that executes tasks.
    """

    def __init__(self, scheduler: Scheduler) -> None:
        self._running = False
        self._thread: Thread = Thread(target=self.run, daemon=True)
        self._queue: Queue[Task] = Queue()
        self._scheduler = scheduler

    def submit(self, task: Task) -> None:
        self._queue.put(task)

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
                    task = self._scheduler.get_task()
                    queue = self._scheduler._queue
                except Empty:
                    continue
            except ShutDown:
                break
            if task is not None:
                task()
                del task
                queue.task_done()
