import uuid
from queue import Empty, Queue
from threading import Barrier, Thread

import pytest

from movie.actor import ActorSystem, Behaviors
from movie.actor.dead_letter import (
    DeadLetter,
    DeadLetterBroker,
    DeadLetterReason,
    RemoteAdmissionResult,
)
from movie.actor.identity import ActorIdentity


def _dead_letter(sequence: int) -> DeadLetter[int]:
    return DeadLetter(
        message=sequence,
        recipient=ActorIdentity(uuid.UUID(int=1), uuid.UUID(int=sequence + 1)),
        recipient_path=f"movie://system/actor-{sequence}",
        reason=DeadLetterReason.ACTOR_NOT_FOUND,
    )


def test_broker_retains_a_bounded_window_for_slow_subscriptions() -> None:
    broker: DeadLetterBroker[int] = DeadLetterBroker(capacity=2)
    subscription = broker.subscribe()

    for sequence in range(5):
        assert broker.publish(_dead_letter(sequence))

    assert broker.retained_count == 2
    assert [letter.message for letter in subscription.drain()] == [3, 4]
    assert subscription.dropped_count == 3


def test_subscriptions_only_observe_attempts_published_after_subscription() -> None:
    broker: DeadLetterBroker[int] = DeadLetterBroker(capacity=3)
    broker.publish(_dead_letter(0))
    subscription = broker.subscribe()
    broker.publish(_dead_letter(1))

    assert subscription.get_nowait().message == 1
    with pytest.raises(Empty):
        subscription.get_nowait()


def test_concurrent_publication_is_bounded_and_accounts_for_overwrite() -> None:
    publishers = 8
    per_publisher = 100
    broker: DeadLetterBroker[int] = DeadLetterBroker(capacity=37)
    subscription = broker.subscribe()
    barrier = Barrier(publishers)
    completed: Queue[bool] = Queue()

    def publish_batch(publisher: int) -> None:
        barrier.wait()
        for offset in range(per_publisher):
            broker.publish(_dead_letter(publisher * per_publisher + offset))
        completed.put(True)

    threads = [Thread(target=publish_batch, args=(index,)) for index in range(publishers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(2.0)

    assert all(not thread.is_alive() for thread in threads)
    assert completed.qsize() == publishers
    retained = subscription.drain()
    assert len(retained) == broker.capacity
    assert subscription.dropped_count + len(retained) == publishers * per_publisher


def test_closed_broker_rejects_new_subscriptions_without_blocking_publishers() -> None:
    broker: DeadLetterBroker[int] = DeadLetterBroker(capacity=1)
    subscription = broker.subscribe()
    subscription.close()

    assert subscription.closed
    broker.close()
    assert broker.publish(_dead_letter(0)) is False
    with pytest.raises(RuntimeError, match="closed"):
        broker.subscribe()


def test_remote_rejection_and_local_tell_to_stopped_actor_are_dead_letters() -> None:
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "dead-letter-system",
    )
    subscription = system.dead_letters.subscribe()
    try:
        actor = system.spawn(
            Behaviors.receive(lambda context, message: Behaviors.same), "worker"
        )
        unknown = ActorIdentity(system.incarnation_uid, uuid.uuid4())
        assert (
            system.admit_remote_message(unknown, "remote")
            is RemoteAdmissionResult.ACTOR_NOT_FOUND
        )

        stopped = system.actor_stop_future(actor)
        system.terminate(actor)
        stopped.result(timeout=1.0)
        actor.tell("local")

        letters = subscription.drain()
        assert [(letter.message, letter.reason) for letter in letters] == [
            (None, DeadLetterReason.ACTOR_NOT_FOUND),
            ("local", DeadLetterReason.ACTOR_STOPPING),
        ]
    finally:
        system.stop()


def test_stale_reference_publishes_after_actor_system_stop() -> None:
    system = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "post-stop-dead-letter-system",
    )
    target = system._root_ref
    subscription = system.dead_letters.subscribe()

    system.stop()
    target.tell("after-system-stop")

    letter = subscription.drain()[-1]
    assert letter.message == "after-system-stop"
    assert letter.reason is DeadLetterReason.ACTOR_STOPPING
