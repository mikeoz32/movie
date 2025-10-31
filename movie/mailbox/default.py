from queue import Empty, Queue
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

    def send(self, message) -> None:
        self._messages.put(message)
        if not self._scheduled:
            self._dispatcher.dispatch(self)
        self._scheduled = True

    def sendSystem(self, message) -> None:
        self._system_messages.put(message)
        if not self._scheduled:
            self._dispatcher.dispatch(self)
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
