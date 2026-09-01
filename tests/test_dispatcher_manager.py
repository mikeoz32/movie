from threading import Event, Thread
from time import monotonic, sleep

import pytest

from movie.actor import Behaviors
from movie.actor.system import ActorSystem
from movie.config import Config
from movie.dispatch.manager import DEFAULT_DISPATCHER_ID, DispatcherManager
from movie.dispatch.worker_pool import WorkerPoolDispatcherImpl


def test_dispatcher_manager_init() -> None:
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same), "test-system"
    )
    manager = DispatcherManager(system.config)
    try:
        assert manager.default_dispatcher is not None
        assert manager.internal_dispatcher is not None
        assert manager.lookup(DEFAULT_DISPATCHER_ID) is manager.default_dispatcher
        assert manager.lookup("internal-dispatcher") is manager.internal_dispatcher
        with pytest.raises(ValueError):
            manager.lookup("non-existent-dispatcher")
    finally:
        manager.stop_all()
        system.stop()


def test_concurrent_stop_all_waits_for_the_active_shutdown() -> None:
    manager = DispatcherManager(Config({}))
    dispatcher = WorkerPoolDispatcherImpl(
        Config({"workers": 1, "shutdown-timeout": 2})
    )
    dispatcher.start()
    manager.register_dispatcher("blocked", dispatcher)
    entered = Event()
    release = Event()
    dispatcher.dispatch(lambda: (entered.set(), release.wait(1.0)))
    assert entered.wait(1.0)
    errors: list[BaseException] = []

    def stop() -> None:
        try:
            manager.stop_all(2.0)
        except BaseException as error:
            errors.append(error)

    first = Thread(target=stop)
    first.start()
    deadline = monotonic() + 1.0
    while not manager._stopping and monotonic() < deadline:
        sleep(0.001)
    second = Thread(target=stop)
    second.start()
    sleep(0.02)

    assert first.is_alive()
    assert second.is_alive()
    release.set()
    first.join(2.0)
    second.join(2.0)

    assert errors == []
    assert not first.is_alive()
    assert not second.is_alive()


def test_duplicate_dispatcher_registration_is_rejected() -> None:
    manager = DispatcherManager(Config({}))
    first = WorkerPoolDispatcherImpl(Config({"workers": 1}))
    second = WorkerPoolDispatcherImpl(Config({"workers": 1}))
    first.start()
    second.start()
    manager.register_dispatcher("duplicate", first)
    try:
        with pytest.raises(ValueError, match="already registered"):
            manager.register_dispatcher("duplicate", second)
    finally:
        second.stop()
        manager.stop_all()


def test_manager_timeout_bounds_non_cooperative_dispatcher() -> None:
    class SlowDispatcher:
        def dispatch(self, task) -> None:
            pass

        def dispatch_system(self, task) -> None:
            pass

        def start(self) -> None:
            pass

        def stop(self, timeout=None) -> None:
            sleep(0.2)

    manager = DispatcherManager(Config({}))
    manager.register_dispatcher("slow", SlowDispatcher())

    started = monotonic()
    with pytest.raises(TimeoutError):
        manager.stop_all(0.02)
    assert monotonic() - started < 0.1

    sleep(0.25)
    manager.stop_all(0.2)


def test_manager_timeout_includes_initial_lock_acquisition() -> None:
    manager = DispatcherManager(Config({}))
    entered = Event()
    release = Event()

    def hold_manager_lock() -> None:
        with manager._state_changed:
            entered.set()
            release.wait(1.0)

    holder = Thread(target=hold_manager_lock)
    holder.start()
    assert entered.wait(1.0)

    started = monotonic()
    with pytest.raises(TimeoutError):
        manager.stop_all(0.02)
    assert monotonic() - started < 0.1

    release.set()
    holder.join(1.0)
    manager.stop_all()


def test_lookup_rejects_dispatcher_while_unregistering() -> None:
    class BlockingDispatcher:
        def __init__(self) -> None:
            self.entered = Event()
            self.release = Event()

        def dispatch(self, task) -> None:
            pass

        def dispatch_system(self, task) -> None:
            pass

        def start(self) -> None:
            pass

        def stop(self, timeout=None) -> None:
            self.entered.set()
            self.release.wait(1.0)

    manager = DispatcherManager(Config({}))
    dispatcher = BlockingDispatcher()
    manager.register_dispatcher("blocking", dispatcher)
    unregister = Thread(target=lambda: manager.unregister_dispatcher("blocking"))
    unregister.start()
    assert dispatcher.entered.wait(1.0)

    with pytest.raises(RuntimeError, match="unregistering"):
        manager.lookup("blocking")

    dispatcher.release.set()
    unregister.join(1.0)
    manager.stop_all()
