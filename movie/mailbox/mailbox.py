from enum import Enum, auto
from typing import Protocol


class MailboxCapacityExceeded(RuntimeError):
    pass


class MailboxAdmissionResult(Enum):
    ACCEPTED = auto()
    STOPPING = auto()
    FULL = auto()


class Mailbox(Protocol):
    def send(self, message) -> None: ...
    def try_send(self, message) -> MailboxAdmissionResult: ...
    def sendSystem(self, message) -> None: ...
    def stop_user_messages(self) -> list: ...
    def close(self) -> None: ...

