from typing import MutableMapping

from movie.dispatch.dispatcher import Dispatcher

DEFAULT_DISPATCHER_ID = "movie.dispatcher.default-dispatcher"


class DispatcherManager:
    def __init__(self):
        self._dispatchers: MutableMapping[str, Dispatcher] = {}

    def register_dispatcher(self, name: str, dispatcher: Dispatcher) -> None:
        self._dispatchers[name] = dispatcher

    def unregister_dispatcher(self, name) -> None:
        if name in self._dispatchers:
            del self._dispatchers[name]
