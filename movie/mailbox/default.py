from queue import Empty, Queue
from threading import RLock
from movie.actor.context import ActorContext
from movie.dispatch.dispatcher import Dispatcher
from movie.mailbox.mailbox import Mailbox


class DefaultMailbox(Mailbox):
    """
    An actor mailbox. Handles user and system messages and processes them.
    """

    def __init__(self, dispatcher: Dispatcher, actor: ActorContext) -> None:
        self._dispatcher = dispatcher
        self._messages: Queue = Queue()
        self._system_messages: Queue = Queue()
        self._actor = actor
        self._scheduled = False
        self._lock = RLock()

    def _schedule_if_needed(self) -> None:
        should_dispatch = False
        with self._lock:
            if not self._scheduled:
                self._scheduled = True
                should_dispatch = True
        if should_dispatch:
            self._dispatcher.dispatch(self)

    def send(self, message) -> None:
        self._messages.put(message)
        self._schedule_if_needed()

    def sendSystem(self, message) -> None:
        self._system_messages.put(message)
        self._schedule_if_needed()

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
            except Exception:
                self._messages.task_done()
                raise

        while True:
            try:
                system_message = self._system_messages.get(block=False)
                self._actor.invoke_system(system_message)
                self._system_messages.task_done()
            except Empty:
                break
            except Exception:
                self._system_messages.task_done()
                raise
        should_redispatch = False
        with self._lock:
            if self._messages.empty() and self._system_messages.empty():
                self._scheduled = False
            else:
                should_redispatch = True

        if should_redispatch:
            self._dispatcher.dispatch(self)
