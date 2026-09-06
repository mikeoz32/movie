import threading
from dataclasses import dataclass

import pytest

from movie.actor import AbstractBehavior, ActorSystem, Behaviors, ExtensionId
from movie.actor.impl.system import ActorSystemImpl


@dataclass
class RecordingExtension:
    events: list[str]
    name: str

    def start(self) -> None:
        self.events.append(f"start:{self.name}")

    def stop(self, timeout: float) -> None:
        assert timeout > 0
        self.events.append(f"stop:{self.name}")


def create_system(name: str) -> ActorSystem:
    return ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        name,
    )


def test_extension_is_available_during_root_actor_setup() -> None:
    events = []
    extension_id = ExtensionId(
        "setup-extension",
        lambda system: RecordingExtension(events, "setup"),
    )

    def setup(context):
        assert extension_id.get(context.get_system()).name == "setup"
        return Behaviors.receive(lambda inner_context, message: Behaviors.same)

    system = ActorSystem.create(Behaviors.setup(setup), "extension-setup")
    try:
        assert events == ["start:setup"]
    finally:
        system.stop()
    assert events == ["start:setup", "stop:setup"]


def test_concurrent_extension_lookup_creates_and_starts_one_instance() -> None:
    events = []
    factory_calls = 0
    factory_lock = threading.Lock()

    def factory(system):
        nonlocal factory_calls
        with factory_lock:
            factory_calls += 1
        return RecordingExtension(events, "singleton")

    extension_id = ExtensionId("singleton-extension", factory)
    system = create_system("extension-singleton")
    instances = []
    errors = []
    barrier = threading.Barrier(9)

    def lookup() -> None:
        try:
            barrier.wait()
            instances.append(extension_id.get(system))
        except BaseException as error:
            errors.append(error)

    threads = [threading.Thread(target=lookup) for _ in range(8)]
    try:
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(1.0)

        assert not errors
        assert all(not thread.is_alive() for thread in threads)
        assert factory_calls == 1
        assert len({id(instance) for instance in instances}) == 1
        assert events == ["start:singleton"]
    finally:
        system.stop()


def test_managed_extensions_stop_in_reverse_start_order() -> None:
    events = []
    first = ExtensionId("first", lambda system: RecordingExtension(events, "first"))
    second = ExtensionId("second", lambda system: RecordingExtension(events, "second"))
    system = create_system("extension-order")

    first.get(system)
    second.get(system)
    system.stop()

    assert events == [
        "start:first",
        "start:second",
        "stop:second",
        "stop:first",
    ]
    with pytest.raises(RuntimeError, match="not accepting"):
        first.get(system)


def test_extension_shutdown_can_resume_after_timeout() -> None:
    release = threading.Event()

    class BlockingExtension:
        def start(self) -> None:
            pass

        def stop(self, timeout: float) -> None:
            if not release.wait(timeout):
                raise TimeoutError("blocked extension")

    extension_id = ExtensionId("blocking", lambda system: BlockingExtension())
    system = create_system("extension-timeout")
    extension_id.get(system)

    with pytest.raises(TimeoutError, match="blocked extension"):
        system.stop(0.05)
    release.set()
    system.stop(1.0)


def test_extension_dependency_cycle_fails_without_deadlock() -> None:
    system = create_system("extension-cycle")
    ids = {}
    first = ExtensionId("cycle-first", lambda actor_system: ids["second"].get(actor_system))
    second = ExtensionId("cycle-second", lambda actor_system: first.get(actor_system))
    ids["second"] = second
    try:
        with pytest.raises(RuntimeError, match="Recursive initialization"):
            first.get(system)
    finally:
        system.stop()


def test_extension_factory_worker_can_initialize_another_extension() -> None:
    second = ExtensionId("worker-dependency", lambda system: object())
    worker_result = []

    def first_factory(system):
        worker = threading.Thread(
            target=lambda: worker_result.append(second.get(system))
        )
        worker.start()
        worker.join(1.0)
        if worker.is_alive():
            raise TimeoutError("extension dependency worker deadlocked")
        return object()

    first = ExtensionId("worker-parent", first_factory)
    system = create_system("extension-worker-dependency")
    try:
        assert first.get(system) is not None
        assert len(worker_result) == 1
    finally:
        system.stop()


def test_cross_thread_extension_dependency_cycle_fails_without_deadlock() -> None:
    system = create_system("extension-cross-thread-cycle")
    barrier = threading.Barrier(2)
    calls = {"first": 0, "second": 0}
    ids = {}

    def factory(name, other_name):
        def create(actor_system):
            calls[name] += 1
            if calls[name] == 1:
                barrier.wait()
            return ids[other_name].get(actor_system)

        return create

    ids["first"] = ExtensionId("cross-cycle-first", factory("first", "second"))
    ids["second"] = ExtensionId("cross-cycle-second", factory("second", "first"))
    errors = []

    def lookup(name) -> None:
        try:
            ids[name].get(system)
        except BaseException as error:
            errors.append(error)

    threads = [threading.Thread(target=lookup, args=(name,)) for name in ids]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(1.0)

        assert all(not thread.is_alive() for thread in threads)
        assert len(errors) == 2
        assert all(isinstance(error, RuntimeError) for error in errors)
    finally:
        system.stop()


def test_partially_started_extension_is_stopped_after_failure() -> None:
    events = []

    class FailingExtension:
        def start(self) -> None:
            events.append("start")
            raise RuntimeError("injected startup failure")

        def stop(self, timeout: float) -> None:
            events.append("stop")

    extension_id = ExtensionId("failing", lambda system: FailingExtension())
    system = create_system("extension-start-failure")
    try:
        with pytest.raises(RuntimeError, match="injected startup failure"):
            extension_id.get(system)
        assert events == ["start", "stop"]
    finally:
        system.stop()


def test_extension_does_not_restart_after_startup_cleanup_fails() -> None:
    events = []

    class DirtyExtension:
        def start(self) -> None:
            events.append("start")
            raise RuntimeError("injected startup failure")

        def stop(self, timeout: float) -> None:
            events.append("stop")
            if events.count("stop") == 1:
                raise RuntimeError("injected cleanup failure")

    extension_id = ExtensionId("dirty", lambda system: DirtyExtension())
    system = create_system("extension-cleanup-failure")

    with pytest.raises(RuntimeError, match="injected startup failure"):
        extension_id.get(system)
    with pytest.raises(RuntimeError, match="failed during startup"):
        extension_id.get(system)

    system.stop()

    assert events == ["start", "stop", "stop"]

def test_configured_extension_stops_when_actor_startup_fails() -> None:
    events = []
    extension_id = ExtensionId(
        "configured-startup-failure",
        lambda system: RecordingExtension(events, "configured"),
    )

    def fail_setup(context):
        raise RuntimeError("injected actor startup failure")

    system = ActorSystemImpl(
        Behaviors.setup(fail_setup),
        "configured-extension-startup-failure",
    )
    extension = system._extensions.configure(extension_id)

    with pytest.raises(RuntimeError, match="Root actor failed during startup"):
        system.start()

    assert events == ["stop:configured"]
    assert system._extensions.find(extension_id) is extension


def test_unstarted_configured_extension_stops_before_started_dependency() -> None:
    events = []
    first = ExtensionId("configured-first", lambda system: RecordingExtension(events, "first"))
    second = ExtensionId(
        "configured-second",
        lambda system: RecordingExtension(events, "second"),
    )

    def fail_after_starting_dependency(context):
        first.get(context.get_system())
        raise RuntimeError("injected actor startup failure")

    system = ActorSystemImpl(
        Behaviors.setup(fail_after_starting_dependency),
        "configured-extension-order",
    )
    system._extensions.configure(first)
    system._extensions.configure(second)

    with pytest.raises(RuntimeError, match="Root actor failed during startup"):
        system.start()

    assert events == ["start:first", "stop:second", "stop:first"]


def test_starting_extension_can_be_interrupted_by_shutdown() -> None:
    events = []
    start_entered = threading.Event()
    release_start = threading.Event()
    errors = []

    class StartingExtension:
        def start(self) -> None:
            events.append("start")
            start_entered.set()
            assert release_start.wait(1.0)

        def prepare_stop(self, timeout: float) -> None:
            events.append("prepare")
            release_start.set()

        def stop(self, timeout: float) -> None:
            events.append("stop")

    extension_id = ExtensionId("starting-extension", lambda system: StartingExtension())
    system = create_system("extension-start-interruption")

    def lookup() -> None:
        try:
            extension_id.get(system)
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=lookup)
    worker.start()
    try:
        assert start_entered.wait(1.0)
        system.stop(1.0)
    finally:
        release_start.set()
        worker.join(1.0)

    assert not worker.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert events == ["start", "prepare", "stop"]


def test_actor_post_stop_runs_before_extension_shutdown() -> None:
    events = []
    extension_id = ExtensionId(
        "post-stop-extension",
        lambda system: RecordingExtension(events, "post-stop"),
    )

    class PostStopBehavior(AbstractBehavior):
        def receive(self, context, message):
            return Behaviors.same

        def on_signal(self, context, message) -> None:
            if isinstance(message, ActorSystem.PostStop):
                extension_id.get(context.get_system())
                events.append("actor:post-stop")

    system = ActorSystem.create(
        Behaviors.setup(lambda context: PostStopBehavior(context)),
        "extension-post-stop",
    )
    extension_id.get(system)

    system.stop()

    assert events == [
        "start:post-stop",
        "actor:post-stop",
        "stop:post-stop",
    ]


def test_pre_actor_stop_extensions_prepare_in_reverse_start_order() -> None:
    events = []

    @dataclass
    class PreparedExtension(RecordingExtension):
        def prepare_stop(self, timeout: float) -> None:
            assert timeout > 0
            events.append(f"prepare:{self.name}")

    first = ExtensionId("prepared-first", lambda system: PreparedExtension(events, "first"))
    second = ExtensionId("prepared-second", lambda system: PreparedExtension(events, "second"))

    class PostStopBehavior(AbstractBehavior):
        def receive(self, context, message):
            return Behaviors.same

        def on_signal(self, context, message) -> None:
            if isinstance(message, ActorSystem.PostStop):
                assert first.get(context.get_system()).name == "first"
                with pytest.raises(RuntimeError, match="new extensions"):
                    ExtensionId("too-late", lambda system: object()).get(
                        context.get_system()
                    )
                events.append("actor:post-stop")

    system = ActorSystem.create(
        Behaviors.setup(lambda context: PostStopBehavior(context)),
        "extension-prepare",
    )
    first.get(system)
    second.get(system)

    system.stop()

    assert events == [
        "start:first",
        "start:second",
        "prepare:second",
        "prepare:first",
        "actor:post-stop",
        "stop:second",
        "stop:first",
    ]
