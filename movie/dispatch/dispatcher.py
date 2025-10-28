from typing import Callable, Protocol

Task = Callable[..., None]


class Dispatcher(Protocol):
    """
    Dispatcher is responsible for dispatching tasks to be executed in some execution context.
    """

    def dispatch(self, task: Task) -> None: ...


class InternalDispatcher(Dispatcher, Protocol):
    def start(self) -> None: ...
    def stop(self) -> None: ...
