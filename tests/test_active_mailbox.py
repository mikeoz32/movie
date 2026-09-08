from collections import deque
from queue import Queue
from threading import Barrier, Thread

import pytest

from movie.actor.context import ActorBatchFailed
from movie.config import Config
from movie.mailbox.default import DefaultMailbox, MailboxCapacityExceeded
from movie.mailbox.mailbox import MailboxAdmissionResult


class ManualDispatcher:
    def __init__(self) -> None:
        self.tasks = deque()

    def dispatch(self, task) -> None:
        self.tasks.append(task)

    def run_all(self) -> None:
        while self.tasks:
            self.tasks.popleft()()


class InlineDispatcher:
    def dispatch(self, task) -> None:
        task()


class RejectFirstDispatcher(ManualDispatcher):
    def __init__(self) -> None:
        super().__init__()
        self.calls = 0

    def dispatch(self, task) -> None:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("rejected")
        super().dispatch(task)


class RejectHandoffDispatcher(ManualDispatcher):
    def __init__(self) -> None:
        super().__init__()
        self.calls = 0

    def dispatch(self, task) -> None:
        self.calls += 1
        if self.calls == 2:
            raise RuntimeError("saturated")
        super().dispatch(task)


class RecordingActor:
    def __init__(self) -> None:
        self.messages = []

    def invoke(self, message) -> None:
        self.messages.append(("user", message))

    def invoke_system(self, message) -> None:
        self.messages.append(("system", message))

    def invoke_batch(self, messages, *, system: bool) -> list:
        invoke = self.invoke_system if system else self.invoke
        for message in messages:
            invoke(message)
        return []

    def can_process_user_messages(self) -> bool:
        return True


def test_sequential_dispatch_does_not_strand_messages() -> None:
    actor = RecordingActor()
    dispatcher = ManualDispatcher()
    mailbox = DefaultMailbox(dispatcher, actor)

    mailbox.send("first")
    mailbox.send("second")
    dispatcher.run_all()

    assert actor.messages == [("user", "first"), ("user", "second")]


def test_inline_dispatcher_does_not_deadlock_or_recurse_per_batch() -> None:
    class ReentrantActor(RecordingActor):
        def invoke(self, message) -> None:
            super().invoke(message)
            if message == 0:
                for nested in range(1, 2_001):
                    mailbox.send(nested)

    actor = ReentrantActor()
    mailbox = DefaultMailbox(
        InlineDispatcher(), actor, Config({"capacity": 3_000, "throughput": 1})
    )

    mailbox.send(0)

    assert actor.messages == [("user", message) for message in range(2_001)]
    assert mailbox._scheduled is False


def test_inline_failure_does_not_roll_back_nested_identical_message() -> None:
    token = object()

    class FailingActor(RecordingActor):
        def __init__(self) -> None:
            super().__init__()
            self.first = True

        def invoke_batch(self, messages, *, system: bool) -> list:
            for message in messages:
                self.messages.append(("user", message))
                if self.first:
                    self.first = False
                    mailbox.send(token)
                    raise ActorBatchFailed(
                        KeyboardInterrupt("boom"), [], system=False
                    )
            return []

    actor = FailingActor()
    mailbox = DefaultMailbox(InlineDispatcher(), actor)

    with pytest.raises(KeyboardInterrupt, match="boom"):
        mailbox.send(token)

    assert actor.messages == [("user", token), ("user", token)]
    assert mailbox._scheduled is False


def test_inline_failure_recovery_uses_iterative_trampoline() -> None:
    class RepeatingFailureActor(RecordingActor):
        def __init__(self) -> None:
            super().__init__()
            self.count = 0

        def invoke_batch(self, messages, *, system: bool) -> list:
            self.count += 1
            if self.count < 1_500:
                mailbox.sendSystem(self.count)
            raise ActorBatchFailed(KeyboardInterrupt("boom"), [], system=True)

    actor = RepeatingFailureActor()
    mailbox = DefaultMailbox(InlineDispatcher(), actor)

    with pytest.raises(KeyboardInterrupt, match="boom"):
        mailbox.sendSystem(0)

    assert actor.count == 1_500
    assert mailbox._scheduled is False


def test_raw_batch_failure_releases_in_flight_capacity() -> None:
    class RawFailingActor(RecordingActor):
        def invoke_batch(self, messages, *, system: bool) -> list:
            raise KeyboardInterrupt("boom")

    dispatcher = ManualDispatcher()
    mailbox = DefaultMailbox(
        dispatcher,
        RawFailingActor(),
        Config({"capacity": 1, "throughput": 1}),
    )
    mailbox.send("first")

    with pytest.raises(KeyboardInterrupt, match="boom"):
        dispatcher.run_all()

    assert mailbox.try_send("second") is MailboxAdmissionResult.ACCEPTED


def test_system_messages_are_prioritized_and_batches_are_bounded() -> None:
    actor = RecordingActor()
    dispatcher = ManualDispatcher()
    mailbox = DefaultMailbox(
        dispatcher,
        actor,
        Config({"capacity": 4, "system-capacity": 2, "throughput": 1}),
    )

    mailbox.send("user-1")
    mailbox.send("user-2")
    mailbox.sendSystem("stop")
    dispatcher.run_all()

    assert actor.messages == [
        ("system", "stop"),
        ("user", "user-1"),
        ("user", "user-2"),
    ]


def test_mailbox_rejects_overload() -> None:
    mailbox = DefaultMailbox(
        ManualDispatcher(),
        RecordingActor(),
        Config({"capacity": 1, "system-capacity": 1, "throughput": 1}),
    )

    mailbox.send("accepted")
    with pytest.raises(MailboxCapacityExceeded, match="User mailbox is full"):
        mailbox.send("rejected")


def test_nonblocking_admission_reports_full_and_stopping() -> None:
    mailbox = DefaultMailbox(
        ManualDispatcher(),
        RecordingActor(),
        Config({"capacity": 1, "throughput": 1}),
    )

    assert mailbox.try_send("accepted") is MailboxAdmissionResult.ACCEPTED
    assert mailbox.try_send("full") is MailboxAdmissionResult.FULL
    assert mailbox.stop_user_messages() == ["accepted"]
    assert mailbox.try_send("stopping") is MailboxAdmissionResult.STOPPING


def test_concurrent_admission_never_exceeds_mailbox_capacity() -> None:
    capacity = 7
    attempts = 64
    mailbox = DefaultMailbox(
        ManualDispatcher(),
        RecordingActor(),
        Config({"capacity": capacity, "throughput": 1}),
    )
    barrier = Barrier(attempts)
    results: Queue[MailboxAdmissionResult] = Queue()

    def admit(message: int) -> None:
        barrier.wait()
        results.put(mailbox.try_send(message))

    threads = [Thread(target=admit, args=(message,)) for message in range(attempts)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(2.0)

    assert all(not thread.is_alive() for thread in threads)
    admitted = [results.get_nowait() for _ in range(attempts)]
    assert admitted.count(MailboxAdmissionResult.ACCEPTED) == capacity
    assert admitted.count(MailboxAdmissionResult.FULL) == attempts - capacity
    assert len(mailbox.stop_user_messages()) == capacity


def test_extracted_batch_remains_counted_toward_capacity() -> None:
    class SuspendingActor(RecordingActor):
        def __init__(self) -> None:
            super().__init__()
            self.suspended = False
            self.admissions = []

        def invoke_batch(self, messages, *, system: bool) -> list:
            self.messages.append(("user", messages[0]))
            self.admissions.extend(
                [mailbox.try_send("new-1"), mailbox.try_send("new-2")]
            )
            self.suspended = True
            return messages[1:]

        def can_process_user_messages(self) -> bool:
            return not self.suspended

    actor = SuspendingActor()
    dispatcher = ManualDispatcher()
    mailbox = DefaultMailbox(
        dispatcher,
        actor,
        Config({"capacity": 2, "throughput": 2}),
    )

    mailbox.send("first")
    mailbox.send("second")
    dispatcher.run_all()

    assert actor.admissions == [
        MailboxAdmissionResult.FULL,
        MailboxAdmissionResult.FULL,
    ]
    assert mailbox.stop_user_messages() == ["second"]


def test_dispatcher_rejection_rolls_back_message() -> None:
    actor = RecordingActor()
    dispatcher = RejectFirstDispatcher()
    mailbox = DefaultMailbox(dispatcher, actor)

    with pytest.raises(RuntimeError, match="rejected"):
        mailbox.send("not-accepted")
    mailbox.send("accepted")
    dispatcher.run_all()

    assert actor.messages == [("user", "accepted")]


def test_dispatcher_handoff_rejection_does_not_strand_messages() -> None:
    actor = RecordingActor()
    dispatcher = RejectHandoffDispatcher()
    mailbox = DefaultMailbox(
        dispatcher,
        actor,
        Config({"capacity": 10, "throughput": 1}),
    )

    mailbox.send("first")
    mailbox.send("second")
    dispatcher.run_all()

    assert actor.messages == [("user", "first"), ("user", "second")]
    assert not mailbox._messages
    assert mailbox._scheduled is False


@pytest.mark.parametrize(
    "settings",
    [
        {"capacity": 0, "throughput": 1},
        {"capacity": 1, "throughput": 0},
    ],
)
def test_mailbox_rejects_non_positive_settings(settings) -> None:
    with pytest.raises(ValueError, match="must be positive"):
        DefaultMailbox(ManualDispatcher(), RecordingActor(), Config(settings))
