import socket
import time
from dataclasses import dataclass
from queue import Empty
from threading import Event, Thread

import pytest

from movie.actor import ActorSystem, Behaviors
from movie.remoting import (
    AssociationState,
    Endpoint,
    NoAssociationError,
    RemotingConfig,
    RemotingHealthEventKind,
    RemotingMetrics,
    SerializerDescriptor,
    SerializerRegistryBuilder,
    UnknownSerializerError,
)


@dataclass(frozen=True)
class ObservedMessage:
    value: str


class ObservedSerializer:
    def serialize(self, value: object, manifest: str, protocol_minor: int) -> bytes:
        if not isinstance(value, ObservedMessage) or manifest != "observed/v1":
            raise ValueError("unsupported message")
        return value.value.encode("utf-8")

    def deserialize(self, payload: bytes, manifest: str, protocol_minor: int) -> object:
        if manifest != "observed/v1":
            raise ValueError("unsupported manifest")
        return ObservedMessage(payload.decode("utf-8"))


def endpoint() -> Endpoint:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return Endpoint("127.0.0.1", sock.getsockname()[1])
    finally:
        sock.close()


def registry():
    descriptor = SerializerDescriptor(
        31,
        "observed",
        1,
        0,
        frozenset({"observed/v1"}),
        frozenset({"observed/v1"}),
    )
    return (
        SerializerRegistryBuilder()
        .register(descriptor, ObservedSerializer())
        .bind(ObservedMessage, 31, "observed/v1")
        .build()
    )


def config(local: Endpoint, peer_name: str, peer: Endpoint, **changes):
    values = {
        "local": local,
        "peers": {peer_name: peer},
        "serializers": registry(),
        "association_timeout": 1.0,
    }
    values.update(changes)
    return RemotingConfig(**values)


def locator(system) -> str:
    endpoint = system.remoting.endpoint
    return f"movie://{system.name}@{endpoint.host}:{endpoint.port}/{system.name}"


def wait_until(predicate, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("condition was not met before the deadline")
        time.sleep(0.005)


def test_runtime_metrics_are_cumulative_across_reconnects_and_local_rejections() -> None:
    sender_endpoint = endpoint()
    receiver_endpoint = endpoint()
    delivered = Event()
    received = []

    def receive(context, message):
        received.append(message)
        delivered.set()
        return Behaviors.same

    sender = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "metrics-sender",
        remoting=config(
            sender_endpoint,
            "metrics-receiver",
            receiver_endpoint,
            association_history_limit=1,
        ),
    )
    receiver = ActorSystem.create(
        Behaviors.receive(receive),
        "metrics-receiver",
        remoting=config(receiver_endpoint, "metrics-sender", sender_endpoint),
    )
    try:
        association = sender.remoting.associate("metrics-receiver")
        remote = sender.remoting.resolve(locator(receiver))

        with pytest.raises(UnknownSerializerError):
            remote.tell("unregistered")
        remote.tell(ObservedMessage("first"))
        assert delivered.wait(1.0)
        association.close(1.0, detail="metrics reconnect")
        wait_until(lambda: not sender.remoting.associations)

        with pytest.raises(NoAssociationError):
            remote.tell(ObservedMessage("disconnected"))
        successor = sender.remoting.associate("metrics-receiver")
        remote.tell(ObservedMessage("second"))
        wait_until(lambda: len(received) == 2)

        assert sender.remoting.metrics == RemotingMetrics(
            accepted_delivery_attempts=2,
            rejected_delivery_attempts=2,
            dead_letter_count=1,
            serialization_rejections=1,
            deserialization_rejections=0,
            sequence_violations=0,
            reconnect_count=1,
        )
        assert receiver.remoting.metrics.accepted_delivery_attempts == 2
        successor.close(1.0, detail="evict prior history")
        wait_until(lambda: not sender.remoting.associations)
        assert len(sender.remoting.association_history) == 1
        assert sender.remoting.association_history[0].association_uid == successor.association_uid
        assert sender.remoting.metrics.accepted_delivery_attempts == 2
    finally:
        sender.stop()
        receiver.stop()


def test_health_event_window_reports_drops_and_drains_after_shutdown() -> None:
    sender_endpoint = endpoint()
    receiver_endpoint = endpoint()
    sender = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "events-sender",
        remoting=config(
            sender_endpoint,
            "events-receiver",
            receiver_endpoint,
            health_event_capacity=1,
            health_event_max_subscriptions=1,
        ),
    )
    receiver = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "events-receiver",
        remoting=config(receiver_endpoint, "events-sender", sender_endpoint),
    )
    subscription = sender.remoting.health_events.subscribe()
    sender_stopped = False
    try:
        with pytest.raises(RuntimeError, match="subscription limit"):
            sender.remoting.health_events.subscribe()

        association = sender.remoting.associate("events-receiver")
        association.close(1.0, detail="health event close")
        wait_until(lambda: not sender.remoting.associations)

        assert subscription.dropped_count == 1
        event = subscription.get_nowait()
        assert event.kind is RemotingHealthEventKind.ASSOCIATION_CLOSED
        assert event.association is not None
        assert event.association.state is AssociationState.CLOSED
        with pytest.raises(Empty):
            subscription.get_nowait()

        subscription.close()
        terminal = sender.remoting.health_events.subscribe()
        sender.remoting.associate("events-receiver")
        wait_until(lambda: bool(sender.remoting.associations))
        sender.stop()
        sender_stopped = True

        assert sender.remoting.health_events.closed
        assert terminal.dropped_count == 1
        final_event = terminal.get_nowait()
        assert final_event.kind is RemotingHealthEventKind.ASSOCIATION_CLOSED
        assert final_event.association is not None
        assert final_event.association.state is AssociationState.CLOSED
        with pytest.raises(RuntimeError, match="closed"):
            sender.remoting.health_events.subscribe()
    finally:
        subscription.close()
        if not sender_stopped:
            sender.stop()
        receiver.stop()


def test_listener_failure_cannot_publish_after_a_racing_activation(
    monkeypatch,
) -> None:
    sender_endpoint = endpoint()
    receiver_endpoint = endpoint()
    sender = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "event-race-sender",
        remoting=config(sender_endpoint, "event-race-receiver", receiver_endpoint),
    )
    receiver = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "event-race-receiver",
        remoting=config(receiver_endpoint, "event-race-sender", sender_endpoint),
    )
    subscription = sender.remoting.health_events.subscribe()
    activation_entered = Event()
    release_activation = Event()
    original_activated = sender.remoting._association_activated
    errors = []

    def blocked_activation(association) -> None:
        activation_entered.set()
        release_activation.wait(1.0)
        original_activated(association)

    def associate() -> None:
        try:
            sender.remoting.associate("event-race-receiver")
        except BaseException as error:
            errors.append(error)

    monkeypatch.setattr(sender.remoting, "_association_activated", blocked_activation)
    associator = Thread(target=associate)
    try:
        associator.start()
        assert activation_entered.wait(1.0)
        sender.remoting._listener._record_failure(OSError("activation race"))
        release_activation.set()
        associator.join(1.0)
        wait_until(lambda: not sender.remoting.associations)
        events = []
        wait_until(lambda: events.extend(subscription.drain()) or len(events) >= 2)

        assert not associator.is_alive()
        assert [event.kind for event in events] == [
            RemotingHealthEventKind.LISTENER_FAILED,
            RemotingHealthEventKind.ASSOCIATION_CLOSED,
        ]
        assert all(
            event.association is None or event.association.state is AssociationState.CLOSED
            for event in events
        )
    finally:
        release_activation.set()
        associator.join(1.0)
        subscription.close()
        sender.stop()
        receiver.stop()
