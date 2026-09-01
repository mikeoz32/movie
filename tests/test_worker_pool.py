from threading import Event, Thread
from time import monotonic, sleep

import pytest

from movie.config import Config
from movie.dispatch.worker_pool import (
    DispatcherCapacityExceeded,
    DispatcherState,
    WorkerPoolDispatcherImpl,
)


def test_dispatcher_rejects_non_positive_settings() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        WorkerPoolDispatcherImpl(Config({"workers": 0}))
    with pytest.raises(ValueError, match="must be positive"):
        WorkerPoolDispatcherImpl(Config({"shutdown-timeout": 0}))


def test_dispatcher_rejects_new_work_while_draining_accepted_work() -> None:
    dispatcher = WorkerPoolDispatcherImpl(
        Config({"workers": 1, "shutdown-timeout": 2})
    )
    entered = Event()
    release = Event()
    completed = Event()
    dispatcher.start()

    def accepted_task() -> None:
        entered.set()
        release.wait(1.0)
        completed.set()

    dispatcher.dispatch(accepted_task)
    assert entered.wait(1.0)
    stopper = Thread(target=dispatcher.stop)
    stopper.start()

    deadline = monotonic() + 1.0
    while dispatcher._state is DispatcherState.RUNNING and monotonic() < deadline:
        sleep(0.001)
    assert dispatcher._state is DispatcherState.STOPPING

    with pytest.raises(RuntimeError, match="not accepting"):
        dispatcher.dispatch(lambda: None)
    release.set()
    stopper.join(2.0)

    assert completed.is_set()
    assert not stopper.is_alive()


def test_dispatcher_activation_capacity_is_bounded() -> None:
    dispatcher = WorkerPoolDispatcherImpl(
        Config({"workers": 1, "queue-capacity": 1, "shutdown-timeout": 2})
    )
    entered = Event()
    release = Event()
    dispatcher.start()
    dispatcher.dispatch(lambda: (entered.set(), release.wait(1.0)))
    assert entered.wait(1.0)
    dispatcher.dispatch(lambda: None)

    with pytest.raises(DispatcherCapacityExceeded, match="queue is full"):
        dispatcher.dispatch(lambda: None)

    release.set()
    dispatcher.stop()


def test_dispatcher_uses_total_capacity_across_shards() -> None:
    workers = 8
    dispatcher = WorkerPoolDispatcherImpl(
        Config({"workers": workers, "queue-capacity": workers, "shutdown-timeout": 2})
    )
    release = Event()
    entered = [Event() for _ in range(workers)]
    dispatcher.start()
    for worker_entered in entered:
        dispatcher.dispatch(
            lambda worker_entered=worker_entered: (
                worker_entered.set(),
                release.wait(1.0),
            )
        )
        assert worker_entered.wait(1.0)

    for _ in range(workers):
        dispatcher.dispatch(lambda: None)
    with pytest.raises(DispatcherCapacityExceeded):
        dispatcher.dispatch(lambda: None)

    release.set()
    dispatcher.stop()


def test_idle_worker_is_woken_to_steal_from_busy_worker() -> None:
    dispatcher = WorkerPoolDispatcherImpl(
        Config({"workers": 3, "queue-capacity": 10, "shutdown-timeout": 2})
    )
    release = Event()
    first_entered = Event()
    second_entered = Event()
    target_ran = Event()
    dispatcher.start()
    dispatcher._placement.next = 0
    dispatcher.dispatch(lambda: (first_entered.set(), release.wait(1.0)))
    assert first_entered.wait(1.0)
    dispatcher._placement.next = 1
    dispatcher.dispatch(lambda: (second_entered.set(), release.wait(1.0)))
    assert second_entered.wait(1.0)
    assert dispatcher._idle_available.wait(1.0)

    dispatcher._placement.next = 0
    dispatcher.dispatch(target_ran.set)

    assert target_ran.wait(1.0)
    release.set()
    dispatcher.stop()


def test_system_activation_has_priority_over_ordinary_work() -> None:
    dispatcher = WorkerPoolDispatcherImpl(
        Config({"workers": 1, "queue-capacity": 2, "shutdown-timeout": 2})
    )
    release = Event()
    entered = Event()
    completed = Event()
    order = []
    dispatcher.start()
    dispatcher.dispatch(lambda: (entered.set(), release.wait(1.0)))
    assert entered.wait(1.0)
    dispatcher.dispatch(lambda: order.append("ordinary"))
    dispatcher.dispatch_system(lambda: (order.append("system"), completed.set()))

    release.set()
    assert completed.wait(1.0)
    dispatcher.stop()

    assert order == ["system", "ordinary"]


def test_idle_worker_steals_foreign_system_work_before_local_ordinary_work() -> None:
    dispatcher = WorkerPoolDispatcherImpl(
        Config({"workers": 2, "queue-capacity": 10, "shutdown-timeout": 2})
    )
    first_release = Event()
    second_release = Event()
    first_entered = Event()
    second_entered = Event()
    system_ran = Event()
    ordinary_ran = Event()
    order = []
    dispatcher.start()
    dispatcher._placement.next = 0
    dispatcher.dispatch(lambda: (first_entered.set(), first_release.wait(1.0)))
    assert first_entered.wait(1.0)
    dispatcher._placement.next = 1
    dispatcher.dispatch(lambda: (second_entered.set(), second_release.wait(1.0)))
    assert second_entered.wait(1.0)
    dispatcher._placement.next = 0
    dispatcher.dispatch(lambda: (order.append("ordinary"), ordinary_ran.set()))
    dispatcher._placement.next = 1
    dispatcher.dispatch_system(lambda: (order.append("system"), system_ran.set()))

    first_release.set()
    assert system_ran.wait(1.0)
    assert order[0] == "system"
    second_release.set()
    dispatcher.stop()
