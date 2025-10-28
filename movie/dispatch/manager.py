from dataclasses import dataclass
from typing import MutableMapping, cast

from movie.config import Config
from movie.dispatch.dispatcher import Dispatcher

DEFAULT_DISPATCHER_ID = "movie.dispatcher.default-dispatcher"


@dataclass(frozen=True)
class DispatcherManagerSettings:
    default_dispatcher: Dispatcher

    @staticmethod
    def from_config(config: Config) -> "DispatcherManagerSettings":
        default_dispatcher = config.get_instance(DEFAULT_DISPATCHER_ID, Dispatcher)
        return DispatcherManagerSettings(
            default_dispatcher=cast(Dispatcher, default_dispatcher)
        )


class DispatcherManager:
    def __init__(self, config: Config):
        self._settings = DispatcherManagerSettings.from_config(config)
        self._dispatchers: MutableMapping[str, Dispatcher] = {}

    def register_dispatcher(self, name: str, dispatcher: Dispatcher) -> None:
        self._dispatchers[name] = dispatcher

    def unregister_dispatcher(self, name) -> None:
        if name in self._dispatchers:
            del self._dispatchers[name]
