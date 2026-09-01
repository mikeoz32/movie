from threading import Event

from movie.actor import ActorSystem, Behaviors
from movie.future import RuntimeFuture


def test_runtime_future_callbacks_preserve_registration_order() -> None:
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "future-callback-order-system",
    )
    future: RuntimeFuture[None] = RuntimeFuture(system._submit_callback)
    first_entered = Event()
    release_first = Event()
    completed = Event()
    order = []

    def first(done) -> None:
        first_entered.set()
        release_first.wait(1.0)
        order.append("first")

    def second(done) -> None:
        order.append("second")
        completed.set()

    future.add_done_callback(first)
    future.add_done_callback(second)
    future.set_result(None)
    assert first_entered.wait(1.0)
    assert not completed.is_set()
    release_first.set()

    assert completed.wait(1.0)
    assert order == ["first", "second"]
    system.stop()


def test_late_callback_joins_existing_serial_callback_drain() -> None:
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "future-late-callback-system",
    )
    future: RuntimeFuture[None] = RuntimeFuture(system._submit_callback)
    first_entered = Event()
    release_first = Event()
    late_completed = Event()

    future.add_done_callback(
        lambda done: (first_entered.set(), release_first.wait(1.0))
    )
    future.set_result(None)
    assert first_entered.wait(1.0)
    future.add_done_callback(lambda done: late_completed.set())
    assert not late_completed.is_set()

    release_first.set()
    assert late_completed.wait(1.0)
    system.stop()


def test_stop_from_callback_stays_stopping_until_callback_drain_exits() -> None:
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "future-callback-shutdown-finalizer-system",
    )
    system._shutdown_timeout = 0.05
    future: RuntimeFuture[None] = RuntimeFuture(system._submit_callback)
    stop_returned = Event()
    second_entered = Event()
    release_second = Event()

    def stop_system(done) -> None:
        system.stop(timeout=2.0)
        stop_returned.set()

    def block_after_stop(done) -> None:
        second_entered.set()
        release_second.wait(1.0)

    future.add_done_callback(stop_system)
    future.add_done_callback(block_after_stop)
    future.set_result(None)

    assert stop_returned.wait(1.0)
    assert second_entered.wait(1.0)
    assert system._state.name == "STOPPING"
    assert not system._terminated.wait(0.1)
    release_second.set()
    assert system._terminated.wait(2.0)
    assert system._state.name == "STOPPED"
