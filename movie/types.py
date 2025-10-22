from typing import Protocol, TypeVar


MessageType = TypeVar("MessageType")

class Scheduler(Protocol):
    def start(self) -> None: ...

    def stop(self) -> None: ...

