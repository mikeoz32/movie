from __future__ import annotations

import uuid
from collections import deque
from threading import Lock
from typing import TYPE_CHECKING, Generic

from movie.actor import ActorRef
from movie.actor.message import MessageType
from movie.actor.path import ActorPath
from movie.future import RuntimeFuture

if TYPE_CHECKING:
    from movie.actor.impl.system import ActorSystemImpl
    from movie.actor.system import ActorSystem
    from movie.mailbox.mailbox import Mailbox


class ActorCell(Generic[MessageType]):
    def __init__(self) -> None:
        self._lock = Lock()
        self._mailbox: Mailbox | None = None
        self._pending_system_messages = deque()
        self._accepting_user_messages = True
        self._closed = False

    def attach(self, mailbox: Mailbox) -> None:
        with self._lock:
            if self._closed or self._mailbox is not None:
                raise RuntimeError("Actor cell cannot attach a mailbox")
            self._mailbox = mailbox
            pending = list(self._pending_system_messages)
            self._pending_system_messages.clear()
        for message in pending:
            mailbox.sendSystem(message)

    def tell(self, message: MessageType) -> None:
        with self._lock:
            if self._closed or not self._accepting_user_messages:
                return
            mailbox = self._mailbox
        if mailbox is None:
            raise RuntimeError("Actor mailbox is not ready")
        mailbox.send(message)

    def tell_system(self, message: ActorSystem.SystemMessage) -> None:
        with self._lock:
            if self._closed:
                return
            mailbox = self._mailbox
        if mailbox is None:
            with self._lock:
                if not self._closed and self._mailbox is None:
                    self._pending_system_messages.append(message)
                    return
                mailbox = self._mailbox
            if mailbox is None:
                return
        mailbox.sendSystem(message)

    def stop_user_messages(self) -> None:
        with self._lock:
            self._accepting_user_messages = False
            mailbox = self._mailbox
        if mailbox is not None:
            mailbox.stop_user_messages()

    def close(self) -> None:
        with self._lock:
            self._accepting_user_messages = False
            self._closed = True
            mailbox = self._mailbox
            self._mailbox = None
            self._pending_system_messages.clear()
        if mailbox is not None:
            mailbox.close()


class LocalActorRef(ActorRef[MessageType]):
    def __init__(self, system: ActorSystemImpl, path: ActorPath) -> None:
        self._system = system
        self._path = path
        self._id = uuid.uuid4()
        self._cell: ActorCell[MessageType] = ActorCell()
        self._started_future: RuntimeFuture[None] = RuntimeFuture(
            system._submit_callback, cancellable=False
        )
        self._stopped_future: RuntimeFuture[None] = RuntimeFuture(
            system._submit_callback, cancellable=False
        )

    def tell(self, message: MessageType) -> None:
        self._cell.tell(message)

    def tell_system(self, message: ActorSystem.SystemMessage) -> None:
        self._cell.tell_system(message)

    def attach_mailbox(self, mailbox: Mailbox) -> None:
        self._cell.attach(mailbox)

    def stop_user_messages(self) -> None:
        self._cell.stop_user_messages()

    def close(self) -> None:
        self._cell.close()

    def belongs_to(self, system: ActorSystemImpl) -> bool:
        return self._system is system

    @property
    def started_future(self) -> RuntimeFuture[None]:
        return self._started_future

    @property
    def stopped_future(self) -> RuntimeFuture[None]:
        return self._stopped_future

    @property
    def id(self) -> uuid.UUID:
        return self._id

    @property
    def name(self) -> str:
        return self._path.name

    @property
    def path(self) -> ActorPath:
        return self._path
