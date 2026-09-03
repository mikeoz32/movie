import asyncio
import threading
import time
from types import SimpleNamespace

import pytest

from movie.actor import ActorSystem, Behaviors
from movie.config import Config
from movie.io import (
    ASYNCIO_IO,
    AsyncioIOCapacityError,
    AsyncioIOExtension,
    AsyncioIOStateError,
)


def create_system(name: str, config: Config | None = None) -> ActorSystem:
    return ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        name,
        config=config,
    )


def test_asyncio_io_owns_a_running_loop_thread() -> None:
    system = create_system("asyncio-io-loop")
    extension = ASYNCIO_IO.get(system)
    try:
        result = extension.run_coroutine(
            lambda: _loop_identity(),
            timeout=1.0,
        )

        assert result[0]
        assert result[1] == extension.thread.ident
        assert extension.thread.name == "movie-asyncio-io-asyncio-io-loop"
    finally:
        system.stop()
    assert not extension.thread.is_alive()


async def _loop_identity() -> tuple[bool, int]:
    return asyncio.get_running_loop().is_running(), threading.get_ident()


def test_actor_systems_own_independent_asyncio_loops() -> None:
    first = create_system("asyncio-io-first")
    second = create_system("asyncio-io-second")
    first_extension = ASYNCIO_IO.get(first)
    second_extension = ASYNCIO_IO.get(second)
    try:
        first_identity = first_extension.run_coroutine(_loop_identity, timeout=1.0)
        second_identity = second_extension.run_coroutine(_loop_identity, timeout=1.0)

        assert first_identity[1] != second_identity[1]
        first.stop()
        assert not first_extension.thread.is_alive()
        assert second_extension.thread.is_alive()
    finally:
        first.stop()
        second.stop()


def test_asyncio_io_starts_and_stops_configured_worker_pool() -> None:
    system = create_system(
        "asyncio-io-pool",
        Config(
            {
                "movie": {
                    "io": {
                        "asyncio": {
                            "event-loop-count": 3,
                        }
                    }
                }
            }
        ),
    )
    extension = ASYNCIO_IO.get(system)
    try:
        identities = [
            worker.run_coroutine(_loop_identity, timeout=1.0) for worker in extension.workers
        ]

        assert len(extension.workers) == 3
        assert len({identity[1] for identity in identities}) == 3
        assert extension.thread is extension.default_worker.thread
        assert [extension.select_worker().index for _ in range(4)] == [0, 1, 2, 0]
    finally:
        system.stop()

    assert all(not worker.thread.is_alive() for worker in extension.workers)


def test_asyncio_io_command_capacity_is_shared_by_workers() -> None:
    system = create_system(
        "asyncio-io-pool-capacity",
        Config(
            {
                "movie": {
                    "io": {
                        "asyncio": {
                            "event-loop-count": 2,
                            "command-capacity": 1,
                        }
                    }
                }
            }
        ),
    )
    extension = ASYNCIO_IO.get(system)
    entered = threading.Event()
    release = threading.Event()

    def block_loop() -> None:
        entered.set()
        release.wait(1.0)

    try:
        first = extension.workers[0].schedule(block_loop)
        assert entered.wait(1.0)
        with pytest.raises(AsyncioIOCapacityError):
            extension.workers[1].schedule(lambda: None)
        release.set()
        first.result(1.0)
    finally:
        release.set()
        system.stop()


def test_asyncio_io_partial_pool_startup_rolls_back_workers(monkeypatch) -> None:
    extension = AsyncioIOExtension(
        SimpleNamespace(
            name="partial-start",
            config=Config(
                {
                    "movie": {
                        "io": {
                            "asyncio": {
                                "event-loop-count": 2,
                            }
                        }
                    }
                }
            ),
        )
    )
    original_new_event_loop = asyncio.new_event_loop
    calls = 0
    lock = threading.Lock()

    def fail_second_loop():
        nonlocal calls
        with lock:
            calls += 1
            current = calls
        if current == 2:
            raise RuntimeError("injected worker startup failure")
        return original_new_event_loop()

    monkeypatch.setattr(asyncio, "new_event_loop", fail_second_loop)

    with pytest.raises(AsyncioIOStateError, match="failed to start"):
        extension.start()

    assert extension.wait_stopped(1.0)
    assert all(
        worker.thread is None or not worker.thread.is_alive() for worker in extension.workers
    )


def test_asyncio_io_thread_start_failure_stops_pool(monkeypatch) -> None:
    extension = AsyncioIOExtension(SimpleNamespace(name="thread-start-failure", config=Config({})))

    def fail_start(thread) -> None:
        raise RuntimeError("injected thread start failure")

    monkeypatch.setattr(threading.Thread, "start", fail_start)

    with pytest.raises(AsyncioIOStateError, match="failed to start"):
        extension.start()

    assert extension.wait_stopped(0.0)
    assert extension.default_worker.thread is None
    extension.stop(0.0)


def test_asyncio_io_bounds_cross_thread_commands() -> None:
    config = Config(
        {
            "movie": {
                "io": {
                    "asyncio": {
                        "command-capacity": 1,
                    }
                }
            }
        }
    )
    system = create_system("asyncio-io-capacity", config)
    extension = ASYNCIO_IO.get(system)
    entered = threading.Event()
    release = threading.Event()

    def block_loop() -> None:
        entered.set()
        release.wait(1.0)

    try:
        first = extension.schedule(block_loop)
        assert entered.wait(1.0)
        with pytest.raises(AsyncioIOCapacityError):
            extension.schedule(lambda: None)
        release.set()
        first.result(1.0)
        assert extension.pending_commands == 0
    finally:
        release.set()
        system.stop()


def test_asyncio_io_cancelled_coroutine_holds_capacity_until_cleanup_finishes() -> None:
    config = Config(
        {
            "movie": {
                "io": {
                    "asyncio": {
                        "command-capacity": 1,
                    }
                }
            }
        }
    )
    system = create_system("asyncio-io-cancellation-capacity", config)
    extension = ASYNCIO_IO.get(system)
    entered = threading.Event()
    cleanup_started = threading.Event()
    cleanup_release = threading.Event()

    async def cancellable() -> None:
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            while not cleanup_release.is_set():
                await asyncio.sleep(0.001)

    try:
        future = extension.default_worker.submit_coroutine(cancellable)
        assert entered.wait(1.0)
        assert future.cancel()
        assert cleanup_started.wait(1.0)
        assert extension.pending_commands == 1
        with pytest.raises(AsyncioIOCapacityError):
            extension.schedule(lambda: None)

        cleanup_release.set()
        deadline = time.monotonic() + 1.0
        while extension.pending_commands and time.monotonic() < deadline:
            time.sleep(0.001)
        assert extension.pending_commands == 0
        extension.schedule(lambda: None).result(1.0)
    finally:
        cleanup_release.set()
        system.stop()


def test_asyncio_io_cancelled_task_creation_failure_releases_capacity(monkeypatch) -> None:
    system = create_system(
        "asyncio-io-cancelled-task-creation",
        Config({"movie": {"io": {"asyncio": {"command-capacity": 1}}}}),
    )
    extension = ASYNCIO_IO.get(system)
    loop = extension.default_worker._loop
    create_entered = threading.Event()
    create_release = threading.Event()
    original_create_task = loop.create_task

    def fail_create_task(coroutine, *args, **kwargs):
        create_entered.set()
        create_release.wait(1.0)
        raise RuntimeError("injected task creation failure")

    monkeypatch.setattr(loop, "create_task", fail_create_task)
    try:
        future = extension.default_worker.submit_coroutine(lambda: asyncio.sleep(0))
        assert create_entered.wait(1.0)
        assert future.cancel()
        create_release.set()

        deadline = time.monotonic() + 1.0
        while extension.pending_commands and time.monotonic() < deadline:
            time.sleep(0.001)
        assert extension.pending_commands == 0
    finally:
        create_release.set()
        monkeypatch.setattr(loop, "create_task", original_create_task)
        system.stop()


def test_asyncio_io_stop_before_start_is_complete() -> None:
    extension = AsyncioIOExtension(SimpleNamespace(name="never-started", config=Config({})))

    extension.stop(0.0)

    assert extension.thread is None
    with pytest.raises(AsyncioIOStateError, match="start once"):
        extension.start()


def test_asyncio_io_stop_during_loop_creation_is_not_lost(monkeypatch) -> None:
    extension = AsyncioIOExtension(SimpleNamespace(name="starting-stop", config=Config({})))
    entered = threading.Event()
    release = threading.Event()
    original_new_event_loop = asyncio.new_event_loop
    start_errors = []
    stop_errors = []

    def delayed_new_event_loop():
        entered.set()
        release.wait(1.0)
        return original_new_event_loop()

    monkeypatch.setattr(asyncio, "new_event_loop", delayed_new_event_loop)
    starter = threading.Thread(target=lambda: _capture_error(extension.start, start_errors))
    stopper = threading.Thread(
        target=lambda: _capture_error(lambda: extension.stop(1.0), stop_errors)
    )
    starter.start()
    assert entered.wait(1.0)
    stopper.start()
    release.set()
    starter.join(1.0)
    stopper.join(1.0)

    assert not starter.is_alive()
    assert not stopper.is_alive()
    assert not stop_errors
    assert len(start_errors) == 1
    assert isinstance(start_errors[0], AsyncioIOStateError)
    assert not extension.thread.is_alive()


def test_asyncio_io_stop_waits_for_admitted_commands() -> None:
    system = create_system("asyncio-io-command-drain")
    extension = ASYNCIO_IO.get(system)
    entered = threading.Event()
    release = threading.Event()
    stop_errors = []

    def block_loop() -> None:
        entered.set()
        release.wait(1.0)

    command = extension.schedule(block_loop)
    assert entered.wait(1.0)
    stopper = threading.Thread(
        target=lambda: _capture_error(lambda: extension.stop(1.0), stop_errors)
    )
    try:
        stopper.start()
        assert stopper.is_alive()
        release.set()
        command.result(1.0)
        stopper.join(1.0)

        assert not stopper.is_alive()
        assert not stop_errors
        assert extension.pending_commands == 0
    finally:
        release.set()
        stopper.join(1.0)
        system.stop()


def test_cancelled_scheduled_command_is_not_invoked() -> None:
    system = create_system("asyncio-io-cancelled-command")
    extension = ASYNCIO_IO.get(system)
    entered = threading.Event()
    release = threading.Event()
    invoked = threading.Event()

    def block_loop() -> None:
        entered.set()
        release.wait(1.0)

    try:
        first = extension.schedule(block_loop)
        assert entered.wait(1.0)
        cancelled = extension.schedule(invoked.set)
        assert cancelled.cancel()
        release.set()
        first.result(1.0)
        wait_until(lambda: extension.pending_commands == 0)

        assert cancelled.cancelled()
        assert not invoked.is_set()
    finally:
        release.set()
        system.stop()


def test_coroutine_can_continue_cleanup_after_caller_timeout() -> None:
    system = create_system("asyncio-io-timeout-cleanup")
    extension = ASYNCIO_IO.get(system)
    completed = threading.Event()

    async def finish_later() -> None:
        await asyncio.sleep(0.05)
        completed.set()

    try:
        with pytest.raises(TimeoutError):
            extension.run_coroutine(
                finish_later,
                timeout=0.001,
                cancel_on_timeout=False,
            )
        assert completed.wait(1.0)
        wait_until(lambda: extension.pending_commands == 0)
    finally:
        system.stop()


def test_stop_retry_does_not_interrupt_executor_teardown(monkeypatch) -> None:
    system = create_system("asyncio-io-stop-retry")
    extension = ASYNCIO_IO.get(system)

    async def current_loop():
        return asyncio.get_running_loop()

    loop = extension.run_coroutine(current_loop, timeout=1.0)
    entered = threading.Event()
    release = threading.Event()

    async def delayed_executor_shutdown() -> None:
        entered.set()
        while not release.is_set():
            await asyncio.sleep(0.005)

    monkeypatch.setattr(loop, "shutdown_default_executor", delayed_executor_shutdown)
    try:
        with pytest.raises(TimeoutError, match="loop did not stop"):
            extension.stop(0.01)
        assert entered.wait(1.0)

        errors = []
        retry = threading.Thread(target=lambda: _capture_error(lambda: extension.stop(1.0), errors))
        retry.start()
        assert retry.is_alive()
        release.set()
        retry.join(1.0)

        assert not retry.is_alive()
        assert not errors
        assert extension.wait_stopped(0.0)
    finally:
        release.set()
        system.stop()


def _capture_error(operation, errors) -> None:
    try:
        operation()
    except BaseException as error:
        errors.append(error)


def wait_until(predicate, timeout: float = 1.0) -> None:
    deadline = threading.Event()
    for _ in range(int(timeout / 0.005)):
        if predicate():
            return
        deadline.wait(0.005)
    pytest.fail("condition was not met before the deadline")
