from typing import MutableMapping

from movie.config import Config
from movie.dispatch.dispatcher import Dispatcher, InternalDispatcher

DEFAULT_DISPATCHER_ID = "default-dispatcher"
INTERNAL_DISPATCHER_ID = "internal-dispatcher"

default_config = Config(
    {
        DEFAULT_DISPATCHER_ID: {
            "type": "movie.dispatch.worker_pool.WorkerPoolDispatcherImpl"
        },
        INTERNAL_DISPATCHER_ID: {
            "type": "movie.dispatch.worker_pool.WorkerPoolDispatcherImpl"
        },
    }
)


class DispatcherConfigurator:
    def __init__(self, config: Config) -> None:
        self._config = config

    def create_dispatcher(self) -> InternalDispatcher:
        dispatcher = self._config.get_instance("type", Dispatcher)
        if not dispatcher:
            raise ValueError("Failed to create dispatcher from config")
        return dispatcher()


class DispatcherManager:
    def __init__(self, config: Config):
        self._config = (
            config.get_config("movie.dispatcher") or default_config
        ).with_fallback(default_config)

        self._dispatchers: MutableMapping[str, InternalDispatcher] = {}

    def register_dispatcher(self, name: str, dispatcher: InternalDispatcher) -> None:
        self._dispatchers[name] = dispatcher

    def unregister_dispatcher(self, name) -> None:
        if name in self._dispatchers:
            del self._dispatchers[name]

    def lookup(self, name: str) -> Dispatcher:
        dispatcher = self._dispatchers.get(name, None)
        if dispatcher is None:
            configurator = DispatcherConfigurator(
                self._config.get_config(name) or Config({})
            )
            dispatcher = configurator.create_dispatcher()
            dispatcher.start()
            self.register_dispatcher(name, dispatcher)
        return dispatcher

    def stop_all(self) -> None:
        for dispatcher in self._dispatchers.values():
            dispatcher.stop()
        self._dispatchers.clear()

    @property
    def default_dispatcher(self) -> Dispatcher:
        return self.lookup(DEFAULT_DISPATCHER_ID)

    @property
    def internal_dispatcher(self) -> Dispatcher:
        return self.lookup(INTERNAL_DISPATCHER_ID)
