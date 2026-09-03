from __future__ import annotations

import uuid
from collections import deque
from threading import Lock
from typing import TYPE_CHECKING, Generic

from movie.actor import ActorRef
from movie.actor.dead_letter import (
    RemoteAdmissionResult,
    admission_dead_letter_reason,
)
from movie.actor.identity import ActorIdentity, new_actor_uid
from movie.actor.message import MessageType
from movie.actor.path import ActorPath
from movie.future import RuntimeFuture
from movie.mailbox.mailbox import (
    MailboxAdmissionResult,
    MailboxCapacityExceeded,
)

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

    def tell(self, message: MessageType) -> bool:
        with self._lock:
            if self._closed or not self._accepting_user_messages:
                return False
            mailbox = self._mailbox
        if mailbox is None:
            raise RuntimeError("Actor mailbox is not ready")
        mailbox.send(message)
        return True

    def admit(self, message: MessageType) -> MailboxAdmissionResult:
        with self._lock:
            if self._closed or not self._accepting_user_messages:
                return MailboxAdmissionResult.STOPPING
            mailbox = self._mailbox
        if mailbox is None:
            return MailboxAdmissionResult.FULL
        return mailbox.try_send(message)

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

    def stop_user_messages(self) -> list:
        with self._lock:
            self._accepting_user_messages = False
            mailbox = self._mailbox
        if mailbox is not None:
            return mailbox.stop_user_messages()
        return []

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
        self._identity = ActorIdentity(system.incarnation_uid, new_actor_uid())
        self._cell: ActorCell[MessageType] = ActorCell()
        self._started_future: RuntimeFuture[None] = RuntimeFuture(
            system._submit_callback, cancellable=False
        )
        self._stopped_future: RuntimeFuture[None] = RuntimeFuture(
            system._submit_callback, cancellable=False
        )

    def tell(self, message: MessageType) -> None:
        try:
            if not self._cell.tell(message):
                self._dead_letter(message, RemoteAdmissionResult.ACTOR_STOPPING)
        except MailboxCapacityExceeded:
            self._dead_letter(message, RemoteAdmissionResult.MAILBOX_FULL)
            raise

    def admit_remote_message(self, message: MessageType) -> RemoteAdmissionResult:
        result = self._cell.admit(message)
        match result:
            case MailboxAdmissionResult.ACCEPTED:
                return RemoteAdmissionResult.ACCEPTED
            case MailboxAdmissionResult.STOPPING:
                return RemoteAdmissionResult.ACTOR_STOPPING
            case MailboxAdmissionResult.FULL:
                return RemoteAdmissionResult.MAILBOX_FULL


    def tell_system(self, message: ActorSystem.SystemMessage) -> None:
        self._cell.tell_system(message)

    def attach_mailbox(self, mailbox: Mailbox) -> None:
        self._cell.attach(mailbox)

    def stop_user_messages(self) -> None:
        for message in self._cell.stop_user_messages():
            self._dead_letter(message, RemoteAdmissionResult.ACTOR_STOPPING)

    def close(self) -> None:
        self._cell.close()

    def belongs_to(self, system: ActorSystemImpl) -> bool:
        return (
            self._system is system
            and self._identity.system_incarnation_uid == system.incarnation_uid
        )

    def _dead_letter(
        self, message: MessageType, reason: RemoteAdmissionResult
    ) -> None:
        self._system._publish_dead_letter(
            self._identity,
            admission_dead_letter_reason(reason),
            message=message,
            recipient_path=self._path.canonical,
        )

    @property
    def started_future(self) -> RuntimeFuture[None]:
        return self._started_future

    @property
    def stopped_future(self) -> RuntimeFuture[None]:
        return self._stopped_future

    @property
    def id(self) -> uuid.UUID:
        return self._identity.actor_uid

    @property
    def identity(self) -> ActorIdentity:
        return self._identity

    @property
    def name(self) -> str:
        return self._path.name

    @property
    def path(self) -> ActorPath:
        return self._path
