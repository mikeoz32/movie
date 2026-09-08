from collections import deque
from threading import Lock

from movie.actor.context import ActorBatchFailed, InternalActorContext
from movie.config import Config
from movie.dispatch.dispatcher import Dispatcher
from movie.mailbox.mailbox import (
    Mailbox,
    MailboxAdmissionResult,
    MailboxCapacityExceeded,
)


class _Entry:
    __slots__ = ("message",)

    def __init__(self, message) -> None:
        self.message = message


class DefaultMailbox(Mailbox):
    """Serializes one actor's user and lifecycle messages."""

    supports_user_suspension = True

    def __init__(
        self,
        dispatcher: Dispatcher,
        actor: InternalActorContext,
        config: Config | None = None,
    ) -> None:
        config = config or Config({})
        capacity = config.get_int("capacity", 100_000)
        throughput = config.get_int("throughput", 100)
        if capacity is None or capacity <= 0 or throughput is None or throughput <= 0:
            raise ValueError("Mailbox capacity and throughput must be positive")

        self._dispatcher = dispatcher
        self._actor = actor
        self._capacity = capacity
        self._throughput = throughput
        self._messages = deque()
        self._system_messages = deque()
        self._in_flight_user_messages = 0
        self._lock = Lock()
        self._scheduled = False
        self._running = False
        self._inline_run_requested = False
        self._accepting_user_messages = True
        self._closed = False

    def send(self, message) -> None:
        self._enqueue(self._messages, message, user_message=True, report_full=False)

    def try_send(self, message) -> MailboxAdmissionResult:
        try:
            return self._enqueue(
                self._messages, message, user_message=True, report_full=True
            )
        except Exception:
            return MailboxAdmissionResult.FULL

    def sendSystem(self, message) -> None:
        self._enqueue(
            self._system_messages, message, user_message=False, report_full=False
        )

    def _enqueue(
        self, target: deque, message, *, user_message: bool, report_full: bool
    ) -> MailboxAdmissionResult:
        with self._lock:
            if self._closed or (user_message and not self._accepting_user_messages):
                return MailboxAdmissionResult.STOPPING
            if user_message and (
                len(self._messages) + self._in_flight_user_messages >= self._capacity
            ):
                if report_full:
                    return MailboxAdmissionResult.FULL
                raise MailboxCapacityExceeded("User mailbox is full")
            if self._scheduled or (
                user_message and not self._actor.can_process_user_messages()
            ):
                target.append(message)
                return MailboxAdmissionResult.ACCEPTED
            entry = _Entry(message)
            target.append(entry)
            self._scheduled = True
        try:
            self._dispatcher.dispatch(self)
        except BaseException:
            lifecycle_accepted = False
            publish_fallback = False
            with self._lock:
                queued = next(
                    (queued for queued in target if queued is entry), None
                )
                if queued is not None and user_message:
                    target.remove(entry)
                elif queued is not None:
                    lifecycle_accepted = True
                if queued is not None and (
                    self._system_messages or self._messages
                ):
                    self._scheduled = True
                    publish_fallback = True
                else:
                    if queued is not None:
                        self._scheduled = False
            if publish_fallback:
                self._dispatcher.dispatch_system(self)
            if lifecycle_accepted:
                return MailboxAdmissionResult.ACCEPTED
            raise
        return MailboxAdmissionResult.ACCEPTED

    def close(self) -> None:
        with self._lock:
            self._accepting_user_messages = False
            self._closed = True
            self._messages.clear()
            self._system_messages.clear()
            self._in_flight_user_messages = 0

    def stop_user_messages(self) -> list:
        with self._lock:
            self._accepting_user_messages = False
            messages = [
                item.message if isinstance(item, _Entry) else item
                for item in self._messages
            ]
            self._messages.clear()
            return messages

    def __call__(self) -> None:
        with self._lock:
            if self._running:
                self._inline_run_requested = True
                return
            self._running = True
        pending_error: BaseException | None = None
        while True:
            try:
                self._run_batch()
            except ActorBatchFailed as failure:
                self._restore_batch(failure.remaining, system=failure.system)
                self._recover_after_failure()
                if pending_error is None:
                    pending_error = failure.error
            except BaseException as error:
                with self._lock:
                    self._in_flight_user_messages = 0
                self._recover_after_failure()
                if pending_error is None:
                    pending_error = error
            with self._lock:
                run_inline = self._inline_run_requested
                self._inline_run_requested = False
                if not run_inline:
                    self._running = False
            if run_inline:
                continue
            if pending_error is not None:
                raise pending_error
            return

    def _run_batch(self) -> None:
        while True:
            with self._lock:
                if self._closed:
                    self._scheduled = False
                    return
                if not self._system_messages and not self._messages:
                    self._scheduled = False
                    return
                if (
                    not self._system_messages
                    and not self._actor.can_process_user_messages()
                ):
                    self._scheduled = False
                    return
                source = self._system_messages or self._messages
                system_batch = source is self._system_messages
                batch = [
                    item.message if isinstance(item := source.popleft(), _Entry) else item
                    for _ in range(min(self._throughput, len(source)))
                ]
                if not system_batch:
                    self._in_flight_user_messages += len(batch)

            remaining = self._actor.invoke_batch(batch, system=system_batch)
            self._restore_batch(remaining, system=system_batch)

            with self._lock:
                if self._closed:
                    self._scheduled = False
                    return
                if not self._system_messages and not self._messages:
                    self._scheduled = False
                    return
                if (
                    not self._system_messages
                    and not self._actor.can_process_user_messages()
                ):
                    self._scheduled = False
                    return
            try:
                self._dispatcher.dispatch(self)
            except BaseException as error:
                if not isinstance(error, RuntimeError):
                    raise
            else:
                with self._lock:
                    ran_inline = self._inline_run_requested
                    self._inline_run_requested = False
                if not ran_inline:
                    return

    def _restore_batch(self, messages: list, *, system: bool = False) -> None:
        with self._lock:
            if not system:
                self._in_flight_user_messages = 0
            if self._closed or not messages:
                return
            target = self._system_messages if system else self._messages
            target.extendleft(reversed(messages))

    def _recover_after_failure(self) -> None:
        with self._lock:
            if self._closed or (not self._system_messages and not self._messages):
                self._scheduled = False
                return
            if (
                not self._system_messages
                and not self._actor.can_process_user_messages()
            ):
                self._scheduled = False
                return
        try:
            self._dispatcher.dispatch(self)
        except BaseException:
            with self._lock:
                has_lifecycle = bool(self._system_messages)
                if not has_lifecycle:
                    self._scheduled = False
            if has_lifecycle:
                self._dispatcher.dispatch_system(self)
