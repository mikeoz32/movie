from dataclasses import FrozenInstanceError
from queue import Empty
from uuid import UUID

import pytest

from movie.cluster.model import ClusterMember, MemberIdentity, MemberStatus, Reachability
from movie.cluster.observability import ClusterEvent, ClusterEventKind, ClusterEvents
from movie.remoting.transport import Endpoint


def cluster_member() -> ClusterMember:
    return ClusterMember(
        MemberIdentity("member", UUID(int=1)),
        Endpoint("127.0.0.1", 7001),
        MemberStatus.UP,
        Reachability.REACHABLE,
    )


def event(kind: ClusterEventKind, revision: int) -> ClusterEvent:
    return ClusterEvent(
        kind=kind,
        member=cluster_member(),
        revision=revision,
        timestamp=float(revision),
    )


def test_cluster_event_kinds_have_stable_values() -> None:
    assert [kind.value for kind in ClusterEventKind] == [
        "member_up",
        "member_leaving",
        "member_left",
        "member_reachable",
        "member_unreachable",
    ]


def test_cluster_event_is_frozen_slotted_and_keyword_only() -> None:
    value = event(ClusterEventKind.MEMBER_UP, 1)

    assert value.timestamp == 1.0
    assert not hasattr(value, "__dict__")
    with pytest.raises(FrozenInstanceError):
        value.revision = 2  # type: ignore[misc]
    with pytest.raises(TypeError):
        ClusterEvent(ClusterEventKind.MEMBER_UP, cluster_member(), 1)  # type: ignore[misc]


@pytest.mark.parametrize(
    ("capacity", "max_subscriptions"),
    [(0, 1), (-1, 1), (True, 1), (1.5, 1), (1, 0), (1, True), (1, 1.5)],
)
def test_cluster_events_validates_bounds(capacity: object, max_subscriptions: object) -> None:
    with pytest.raises(ValueError, match="positive integers"):
        ClusterEvents(capacity, max_subscriptions)  # type: ignore[arg-type]


def test_cluster_events_are_future_only_bounded_and_nonblocking() -> None:
    events = ClusterEvents(capacity=2, max_subscriptions=1)
    assert events._publish(event(ClusterEventKind.MEMBER_UP, 0))
    subscription = events.subscribe()

    with pytest.raises(RuntimeError, match="subscription limit"):
        events.subscribe()
    first = event(ClusterEventKind.MEMBER_REACHABLE, 1)
    second = event(ClusterEventKind.MEMBER_UNREACHABLE, 2)
    third = event(ClusterEventKind.MEMBER_REACHABLE, 3)
    assert events._publish(first)
    assert events._publish(second)
    assert events._publish(third)

    assert events.capacity == 2
    assert events.retained_count == 2
    assert subscription.dropped_count == 1
    assert subscription.poll() is second
    assert subscription.get_nowait() is third
    assert subscription.poll() is None
    with pytest.raises(Empty):
        subscription.get_nowait()


def test_cluster_event_subscription_drain_limit_and_close() -> None:
    events = ClusterEvents(capacity=3, max_subscriptions=1)
    subscription = events.subscribe()
    first = event(ClusterEventKind.MEMBER_UP, 1)
    second = event(ClusterEventKind.MEMBER_LEAVING, 2)
    third = event(ClusterEventKind.MEMBER_LEFT, 3)
    for value in (first, second, third):
        events._publish(value)

    assert subscription.drain(0) == []
    assert subscription.drain(2) == [first, second]
    assert subscription.drain() == [third]
    with pytest.raises(ValueError, match="nonnegative integer"):
        subscription.drain(-1)
    with pytest.raises(ValueError, match="nonnegative integer"):
        subscription.drain(True)

    subscription.close()
    subscription.close()
    assert subscription.closed
    assert subscription.poll() is None
    with pytest.raises(Empty):
        subscription.get_nowait()


def test_stream_close_rejects_work_but_subscribers_can_drain_retained_events() -> None:
    events = ClusterEvents(capacity=1, max_subscriptions=1)
    subscription = events.subscribe()
    retained = event(ClusterEventKind.MEMBER_LEFT, 1)
    events._publish(retained)

    events.close()
    events.close()

    assert events.closed
    assert not subscription.closed
    assert events.retained_count == 1
    assert not events._publish(event(ClusterEventKind.MEMBER_UP, 2))
    with pytest.raises(RuntimeError, match="closed"):
        events.subscribe()
    assert subscription.drain() == [retained]


def test_subscription_close_records_drops_and_releases_its_slot() -> None:
    events = ClusterEvents(capacity=1, max_subscriptions=1)
    subscription = events.subscribe()
    events._publish(event(ClusterEventKind.MEMBER_UP, 1))
    events._publish(event(ClusterEventKind.MEMBER_LEFT, 2))

    subscription.close()

    assert subscription.dropped_count == 1
    replacement = events.subscribe()
    with replacement:
        assert not replacement.closed
    assert replacement.closed
