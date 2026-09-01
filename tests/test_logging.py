import logging
import os
import threading
import time

import pytest

from movie.actor import ActorSystem, Behaviors
from movie.config import Config


def test_actor_system_does_not_modify_root_logging() -> None:
    root = logging.getLogger()
    original_level = root.level
    original_handlers = tuple(root.handlers)

    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "logging-system",
    )
    system.stop()

    assert root.level == original_level
    assert tuple(root.handlers) == original_handlers


def test_failed_startup_does_not_leak_logging_thread() -> None:
    before = {thread.ident for thread in threading.enumerate()}
    config = Config(
        {
            "movie": {
                "dispatcher": {
                    "default-dispatcher": {"type": "does.not.Exist"}
                }
            }
        }
    )

    with pytest.raises(ModuleNotFoundError):
        ActorSystem.create(
            Behaviors.receive(lambda context, message: Behaviors.same),
            "failed-startup-system",
            config=config,
        )

    time.sleep(0.05)
    leaked = [
        thread
        for thread in threading.enumerate()
        if thread.ident not in before and thread.is_alive()
    ]
    assert leaked == []


def test_explicit_missing_config_fails_closed(monkeypatch) -> None:
    monkeypatch.setenv("MOVIE_CONFIG", os.path.join("missing", "movie.toml"))

    with pytest.raises(FileNotFoundError, match="MOVIE_CONFIG does not exist"):
        ActorSystem.create(
            Behaviors.receive(lambda context, message: Behaviors.same),
            "missing-config-system",
        )


def test_invalid_log_level_does_not_start_listener_thread() -> None:
    before = {thread.ident for thread in threading.enumerate()}

    with pytest.raises(ValueError, match="Unknown level"):
        ActorSystem.create(
            Behaviors.receive(lambda context, message: Behaviors.same),
            "invalid-logging-system",
            config=Config({"movie": {"actor": {"log-level": "NOT-A-LEVEL"}}}),
        )

    time.sleep(0.05)
    leaked = [
        thread
        for thread in threading.enumerate()
        if thread.ident not in before and thread.is_alive()
    ]
    assert leaked == []


def test_invalid_actor_timeouts_are_validated_before_threads_start() -> None:
    before = {thread.ident for thread in threading.enumerate()}
    config = Config(
        {"movie": {"actor": {"startup-timeout": 0, "shutdown-timeout": 0}}}
    )

    with pytest.raises(ValueError, match="startup timeout"):
        ActorSystem.create(
            Behaviors.receive(lambda context, message: Behaviors.same),
            "invalid-timeout-startup-system",
            config=config,
        )

    time.sleep(0.05)
    leaked = [
        thread
        for thread in threading.enumerate()
        if thread.ident not in before and thread.is_alive()
    ]
    assert leaked == []


def test_lifecycle_capacity_mismatch_is_validated_before_threads_start() -> None:
    before = {thread.ident for thread in threading.enumerate()}
    config = Config(
        {
            "movie": {
                "actor": {"max-actors": 10},
                "dispatcher": {
                    "default-dispatcher": {"system-queue-capacity": 9}
                },
            }
        }
    )

    with pytest.raises(ValueError, match="system queue capacity"):
        ActorSystem.create(
            Behaviors.receive(lambda context, message: Behaviors.same),
            "invalid-lifecycle-capacity-system",
            config=config,
        )

    time.sleep(0.05)
    leaked = [
        thread
        for thread in threading.enumerate()
        if thread.ident not in before and thread.is_alive()
    ]
    assert leaked == []


def test_shutdown_timeout_bounds_blocked_log_listener() -> None:
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "blocked-logging-system",
    )
    entered = threading.Event()
    release = threading.Event()

    def blocking_emit(record) -> None:
        entered.set()
        release.wait(2.0)

    system._stream_handler.emit = blocking_emit
    system._actor_log.info(
        "block listener", extra={"actor_id": "test", "actor_path": "test"}
    )
    assert entered.wait(1.0)

    started = time.monotonic()
    with pytest.raises(TimeoutError, match="logger"):
        system.stop(timeout=0.05)
    assert time.monotonic() - started < 0.5

    release.set()
    system.stop(timeout=2.0)
    assert system.actor_count == 0
