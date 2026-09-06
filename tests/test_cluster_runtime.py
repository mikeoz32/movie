import socket
import time
from collections.abc import Callable
from dataclasses import replace
from queue import Queue
from threading import Event, Thread
from uuid import uuid4

import pytest

from movie.actor import ActorSystem, Behaviors
from movie.actor.impl.context import LocalActorContext
from movie.actor.impl.system import ActorSystemImpl
from movie.cluster import (
    ClusterConfig,
    ClusterEventKind,
    ClusterJoinError,
    ClusterMember,
    ClusterShutdownError,
    MemberIdentity,
    MemberStatus,
    Reachability,
    SeedContact,
)
from movie.cluster._protocol import _JoinConfirm, _JoinRequest
from movie.remoting import (
    Endpoint,
    RemotingConfig,
    SerializerRegistryBuilder,
    TcpTransport,
)
from movie.remoting.ref import RemoteActorRef


def endpoint() -> Endpoint:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return Endpoint("127.0.0.1", sock.getsockname()[1])
    finally:
        sock.close()


def wait_until(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("condition was not met before the deadline")
        time.sleep(0.005)


def cluster_config(seed_endpoint: Endpoint) -> ClusterConfig:
    return ClusterConfig(
        "test-cluster",
        SeedContact("cluster-seed", seed_endpoint),
        heartbeat_interval=0.02,
        unreachable_timeout=0.15,
        join_timeout=2.0,
    )


def remoting_config(local: Endpoint, peers: dict[str, Endpoint]) -> RemotingConfig:
    return RemotingConfig(
        local,
        peers,
        SerializerRegistryBuilder().build(),
        transport=TcpTransport(),
        association_timeout=1.0,
    )


def start_pair() -> tuple[ActorSystem, ActorSystem]:
    seed_endpoint = endpoint()
    member_endpoint = endpoint()
    config = cluster_config(seed_endpoint)
    behavior = Behaviors.receive(lambda context, message: Behaviors.same)
    seed = ActorSystem.create(
        behavior,
        "cluster-seed",
        remoting=remoting_config(
            seed_endpoint,
            {"cluster-member": member_endpoint},
        ),
        cluster=config,
    )
    try:
        member = ActorSystem.create(
            behavior,
            "cluster-member",
            remoting=remoting_config(
                member_endpoint,
                {"cluster-seed": seed_endpoint},
            ),
            cluster=config,
        )
    except BaseException:
        seed.stop()
        raise
    return seed, member


def member_by_name(system: ActorSystem, name: str):
    return next(
        member
        for member in system.cluster.members
        if member.identity.system_name == name
    )


def test_cluster_requires_remoting_and_a_matching_static_seed() -> None:
    behavior = Behaviors.receive(lambda context, message: Behaviors.same)
    seed_endpoint = endpoint()
    member_endpoint = endpoint()
    config = cluster_config(seed_endpoint)

    with pytest.raises(ValueError, match="requires remoting"):
        ActorSystem.create(behavior, "cluster-member", cluster=config)
    with pytest.raises(ValueError, match="allowlist"):
        ActorSystem.create(
            behavior,
            "cluster-member",
            remoting=remoting_config(member_endpoint, {}),
            cluster=config,
        )


def test_join_rejects_incompatible_cluster_protocol_settings() -> None:
    seed_endpoint = endpoint()
    member_endpoint = endpoint()
    seed_config = ClusterConfig(
        "test-cluster",
        SeedContact("cluster-seed", seed_endpoint),
        heartbeat_interval=0.02,
        unreachable_timeout=0.3,
        join_timeout=0.15,
    )
    member_config = replace(seed_config, member_limit=128)
    behavior = Behaviors.receive(lambda context, message: Behaviors.same)
    seed = ActorSystem.create(
        behavior,
        "cluster-seed",
        remoting=remoting_config(seed_endpoint, {"cluster-member": member_endpoint}),
        cluster=seed_config,
    )
    try:
        with pytest.raises(ClusterJoinError, match="timed out"):
            ActorSystem.create(
                behavior,
                "cluster-member",
                remoting=remoting_config(member_endpoint, {"cluster-seed": seed_endpoint}),
                cluster=member_config,
            )
        assert [member.identity.system_name for member in seed.cluster.members] == [
            "cluster-seed"
        ]
    finally:
        seed.stop()


def test_seed_forms_cluster_and_member_joins_with_incarnation_identity() -> None:
    seed, member = start_pair()
    try:
        assert seed.cluster.is_coordinator
        assert seed.cluster.is_joined
        assert not member.cluster.is_coordinator
        assert member.cluster.is_joined

        seed_members = seed.cluster.members
        member_members = member.cluster.members
        assert {value.identity.system_name for value in seed_members} == {
            "cluster-seed",
            "cluster-member",
        }
        assert member_members == seed_members
        assert member_by_name(seed, "cluster-member").identity.incarnation_uid == (
            member.incarnation_uid
        )
        assert member_by_name(member, "cluster-seed").identity.incarnation_uid == (
            seed.incarnation_uid
        )
        assert all(value.status is MemberStatus.UP for value in seed_members)
        assert all(value.reachability is Reachability.REACHABLE for value in seed_members)
    finally:
        member.stop()
        seed.stop()


@pytest.mark.parametrize("rejected_type", [_JoinRequest, _JoinConfirm])
def test_join_retries_only_locally_rejected_control_submission(
    monkeypatch,
    rejected_type: type[object],
) -> None:
    original_tell = RemoteActorRef.tell
    rejections: Queue[object] = Queue()

    def reject_twice(reference, message) -> None:
        if type(message) is rejected_type and rejections.qsize() < 2:
            rejections.put_nowait(object())
            raise RuntimeError("synthetic local admission rejection")
        original_tell(reference, message)

    monkeypatch.setattr(RemoteActorRef, "tell", reject_twice)
    seed_endpoint = endpoint()
    member_endpoint = endpoint()
    config = ClusterConfig(
        "test-cluster",
        SeedContact("cluster-seed", seed_endpoint),
        heartbeat_interval=1.0,
        unreachable_timeout=2.0,
        join_timeout=0.5,
        reassociation_timeout=0.1,
    )
    behavior = Behaviors.receive(lambda context, message: Behaviors.same)
    seed = ActorSystem.create(
        behavior,
        "cluster-seed",
        remoting=remoting_config(seed_endpoint, {"cluster-member": member_endpoint}),
        cluster=config,
    )
    member = ActorSystem.create(
        behavior,
        "cluster-member",
        remoting=remoting_config(member_endpoint, {"cluster-seed": seed_endpoint}),
        cluster=config,
    )
    try:
        assert rejections.qsize() == 2
        assert seed.cluster.is_joined
        assert member.cluster.is_joined
        assert member_by_name(seed, "cluster-member").status is MemberStatus.UP
    finally:
        member.stop()
        seed.stop()


def test_membership_updates_converge_and_events_remain_separate_from_remoting() -> None:
    seed_endpoint = endpoint()
    member_endpoint = endpoint()
    config = cluster_config(seed_endpoint)
    behavior = Behaviors.receive(lambda context, message: Behaviors.same)
    seed = ActorSystem.create(
        behavior,
        "cluster-seed",
        remoting=remoting_config(seed_endpoint, {"cluster-member": member_endpoint}),
        cluster=config,
    )
    subscription = seed.cluster.events.subscribe()
    try:
        member = ActorSystem.create(
            behavior,
            "cluster-member",
            remoting=remoting_config(member_endpoint, {"cluster-seed": seed_endpoint}),
            cluster=config,
        )
        try:
            event = subscription.get_nowait()
            assert event.kind is ClusterEventKind.MEMBER_UP
            assert event.member.identity.incarnation_uid == member.incarnation_uid
            assert seed.remoting.health_events is not seed.cluster.events
        finally:
            member.stop()
    finally:
        subscription.close()
        seed.stop()


def test_missing_heartbeats_mark_member_unreachable_without_removing_membership(
    monkeypatch,
) -> None:
    seed, member = start_pair()
    monkeypatch.setattr(member.cluster, "_attempt_reassociation", lambda now: None)
    subscription = seed.cluster.events.subscribe()
    try:
        association = next(
            association
            for association in member.remoting.associations
            if association.peer_system_name == "cluster-seed"
        )
        runtime_association = next(
            value
            for value in member.remoting._associations
            if value.association_uid == association.association_uid
        )
        runtime_association.close(1.0, detail="test heartbeat loss")

        wait_until(
            lambda: member_by_name(seed, "cluster-member").reachability
            is Reachability.UNREACHABLE
        )
        observed = subscription.drain()
        assert any(event.kind is ClusterEventKind.MEMBER_UNREACHABLE for event in observed)
        unreachable = member_by_name(seed, "cluster-member")
        assert unreachable.status is MemberStatus.UP
        assert len(seed.cluster.members) == 2
    finally:
        subscription.close()
        member.stop()
        seed.stop()


def test_cluster_reassociates_and_recovers_reachability_for_same_incarnation(
    monkeypatch,
) -> None:
    seed, member = start_pair()
    original_reassociate = member.cluster._attempt_reassociation
    monkeypatch.setattr(
        member.cluster,
        "_attempt_reassociation",
        lambda now: None,
    )
    try:
        member_revision = member.cluster.membership.revision
        runtime_association = next(iter(member.remoting._associations))
        runtime_association.close(1.0, detail="test recoverable disconnect")
        wait_until(
            lambda: member_by_name(seed, "cluster-member").reachability
            is Reachability.UNREACHABLE
        )
        wait_until(
            lambda: member_by_name(member, "cluster-seed").reachability
            is Reachability.UNREACHABLE
        )
        unreachable_revision = member.cluster.membership.revision
        assert unreachable_revision > member_revision

        monkeypatch.setattr(
            member.cluster,
            "_attempt_reassociation",
            original_reassociate,
        )
        wait_until(
            lambda: member_by_name(seed, "cluster-member").reachability
            is Reachability.REACHABLE
        )
        wait_until(
            lambda: member_by_name(member, "cluster-seed").reachability
            is Reachability.REACHABLE
        )
        assert member.cluster.membership.revision > unreachable_revision
        assert member_by_name(seed, "cluster-member").identity.incarnation_uid == (
            member.incarnation_uid
        )
    finally:
        member.stop()
        seed.stop()


def test_graceful_leave_publishes_leaving_and_left_before_remoting_stops() -> None:
    seed, member = start_pair()
    subscription = seed.cluster.events.subscribe()
    try:
        member.cluster.leave(timeout=1.0)

        wait_until(
            lambda: member_by_name(seed, "cluster-member").status
            is MemberStatus.LEFT
        )
        kinds = [event.kind for event in subscription.drain()]
        assert kinds == [
            ClusterEventKind.MEMBER_LEAVING,
            ClusterEventKind.MEMBER_LEFT,
        ]
        assert member.remoting.is_healthy
        assert not member.cluster.is_joined
    finally:
        subscription.close()
        member.stop()
        seed.stop()


def test_actor_system_stop_performs_cluster_leave_before_remoting_shutdown() -> None:
    seed, member = start_pair()
    stopped = False
    try:
        member.stop(timeout=2.0)
        stopped = True

        wait_until(
            lambda: member_by_name(seed, "cluster-member").status
            is MemberStatus.LEFT
        )
        assert member_by_name(seed, "cluster-member").reachability is Reachability.REACHABLE
    finally:
        if not stopped:
            member.stop()
        seed.stop()


def test_three_member_star_converges_without_participant_associations() -> None:
    seed_endpoint = endpoint()
    first_endpoint = endpoint()
    second_endpoint = endpoint()
    config = cluster_config(seed_endpoint)
    behavior = Behaviors.receive(lambda context, message: Behaviors.same)
    seed = ActorSystem.create(
        behavior,
        "cluster-seed",
        remoting=remoting_config(
            seed_endpoint,
            {
                "cluster-first": first_endpoint,
                "cluster-second": second_endpoint,
            },
        ),
        cluster=config,
    )
    first = None
    second = None
    try:
        first = ActorSystem.create(
            behavior,
            "cluster-first",
            remoting=remoting_config(first_endpoint, {"cluster-seed": seed_endpoint}),
            cluster=config,
        )
        second = ActorSystem.create(
            behavior,
            "cluster-second",
            remoting=remoting_config(second_endpoint, {"cluster-seed": seed_endpoint}),
            cluster=config,
        )
        expected = {"cluster-seed", "cluster-first", "cluster-second"}
        wait_until(
            lambda: {
                member.identity.system_name for member in first.cluster.members
            }
            == expected
        )

        assert {
            member.identity.system_name for member in seed.cluster.members
        } == expected
        assert {
            member.identity.system_name for member in second.cluster.members
        } == expected
        assert all(
            association.peer_system_name == "cluster-seed"
            for association in first.remoting.associations
        )
    finally:
        if second is not None:
            second.stop()
        if first is not None:
            first.stop()
        seed.stop()


def test_unconfirmed_join_cannot_leave_an_up_ghost_member(monkeypatch) -> None:
    seed_endpoint = endpoint()
    member_endpoint = endpoint()
    config = ClusterConfig(
        "test-cluster",
        SeedContact("cluster-seed", seed_endpoint),
        heartbeat_interval=0.02,
        unreachable_timeout=0.3,
        join_timeout=0.15,
    )
    behavior = Behaviors.receive(lambda context, message: Behaviors.same)
    seed = ActorSystem.create(
        behavior,
        "cluster-seed",
        remoting=remoting_config(seed_endpoint, {"cluster-ghost": member_endpoint}),
        cluster=config,
    )
    monkeypatch.setattr(
        seed.cluster,
        "_handle_join_confirm",
        lambda message, source: None,
    )
    try:
        with pytest.raises(ClusterJoinError, match="timed out"):
            ActorSystem.create(
                behavior,
                "cluster-ghost",
                remoting=remoting_config(member_endpoint, {"cluster-seed": seed_endpoint}),
                cluster=config,
            )

        wait_until(
            lambda: all(
                member.identity.system_name != "cluster-ghost"
                or member.status is MemberStatus.LEFT
                for member in seed.cluster.members
            )
        )
    finally:
        seed.stop()


def test_provisional_join_expiry_is_not_delayed_by_heartbeat_interval() -> None:
    seed_endpoint = endpoint()
    ghost_endpoint = endpoint()
    config = ClusterConfig(
        "test-cluster",
        SeedContact("cluster-seed", seed_endpoint),
        heartbeat_interval=1.0,
        unreachable_timeout=2.0,
        join_timeout=0.1,
    )
    seed = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "cluster-seed",
        remoting=remoting_config(seed_endpoint, {"cluster-ghost": ghost_endpoint}),
        cluster=config,
    )
    identity = MemberIdentity("cluster-ghost", uuid4())
    request_id = uuid4()
    try:
        with seed.cluster._condition:
            seed.cluster._replace_member_locked(
                ClusterMember(
                    identity,
                    ghost_endpoint,
                    MemberStatus.JOINING,
                    Reachability.REACHABLE,
                )
            )
            seed.cluster._pending_joins[identity] = (
                request_id,
                time.monotonic() + 0.05,
            )
        seed.cluster._control_queue.put_nowait(object())

        wait_until(
            lambda: identity not in {member.identity for member in seed.cluster.members},
            timeout=0.25,
        )
    finally:
        seed.stop()


def test_post_confirmation_join_timeout_rolls_back_coordinator_membership(
    monkeypatch,
) -> None:
    seed_endpoint = endpoint()
    member_endpoint = endpoint()
    config = ClusterConfig(
        "test-cluster",
        SeedContact("cluster-seed", seed_endpoint),
        heartbeat_interval=0.02,
        unreachable_timeout=0.3,
        join_timeout=0.15,
    )
    behavior = Behaviors.receive(lambda context, message: Behaviors.same)
    seed = ActorSystem.create(
        behavior,
        "cluster-seed",
        remoting=remoting_config(seed_endpoint, {"cluster-ghost": member_endpoint}),
        cluster=config,
    )
    monkeypatch.setattr(seed.cluster, "_broadcast_membership", lambda exclude=None: None)
    monkeypatch.setattr(
        seed.cluster,
        "_handle_heartbeat",
        lambda message, source: None,
    )
    try:
        with pytest.raises(ClusterJoinError, match="timed out"):
            ActorSystem.create(
                behavior,
                "cluster-ghost",
                remoting=remoting_config(member_endpoint, {"cluster-seed": seed_endpoint}),
                cluster=config,
            )

        wait_until(
            lambda: member_by_name(seed, "cluster-ghost").status is MemberStatus.LEFT
        )
    finally:
        seed.stop()


def test_explicit_down_allows_a_replacement_incarnation_to_join(monkeypatch) -> None:
    seed_endpoint = endpoint()
    member_endpoint = endpoint()
    config = ClusterConfig(
        "test-cluster",
        SeedContact("cluster-seed", seed_endpoint),
        heartbeat_interval=0.02,
        unreachable_timeout=0.15,
        join_timeout=2.0,
        member_limit=2,
    )
    behavior = Behaviors.receive(lambda context, message: Behaviors.same)
    seed = ActorSystem.create(
        behavior,
        "cluster-seed",
        remoting=remoting_config(seed_endpoint, {"cluster-member": member_endpoint}),
        cluster=config,
    )
    member = ActorSystem.create(
        behavior,
        "cluster-member",
        remoting=remoting_config(member_endpoint, {"cluster-seed": seed_endpoint}),
        cluster=config,
    )
    replacement = None
    try:
        monkeypatch.setattr(member.cluster, "_attempt_reassociation", lambda now: None)
        old_identity = member.cluster.self_identity
        runtime_association = next(iter(member.remoting._associations))
        runtime_association.close(1.0, detail="test abrupt loss")
        wait_until(
            lambda: member_by_name(seed, "cluster-member").reachability
            is Reachability.UNREACHABLE
        )

        seed.cluster.down(old_identity)
        assert member_by_name(seed, "cluster-member").status is MemberStatus.LEFT
        member.stop()

        replacement = ActorSystem.create(
            behavior,
            "cluster-member",
            remoting=remoting_config(member_endpoint, {"cluster-seed": seed_endpoint}),
            cluster=config,
        )
        assert replacement.incarnation_uid != old_identity.incarnation_uid
        assert replacement.cluster.is_joined
        assert len(seed.cluster.members) == 2
        assert old_identity in seed.cluster._retired_identities
        assert member_by_name(seed, "cluster-member").identity.incarnation_uid == (
            replacement.incarnation_uid
        )
    finally:
        if replacement is not None:
            replacement.stop()
        elif member.cluster is not None:
            member.stop()
        seed.stop()


@pytest.mark.parametrize("method_name", ["leave", "prepare_stop", "stop"])
def test_cluster_shutdown_rejects_actor_callback_to_avoid_dispatcher_self_starvation(
    method_name,
) -> None:
    seed_endpoint = endpoint()
    config = cluster_config(seed_endpoint)
    attempted = Event()
    errors: Queue[BaseException] = Queue()

    def receive(context, message):
        try:
            method = getattr(context.get_system().cluster, method_name)
            method(timeout=0.1)
        except BaseException as error:
            errors.put_nowait(error)
        finally:
            attempted.set()
        return Behaviors.same

    seed = ActorSystem.create(
        Behaviors.receive(receive),
        "cluster-seed",
        remoting=remoting_config(seed_endpoint, {}),
        cluster=config,
    )
    try:
        seed.tell("leave")
        assert attempted.wait(1.0)
        error = errors.get_nowait()
        assert isinstance(error, RuntimeError)
        assert "actor callback" in str(error)
        assert seed.cluster.is_joined
    finally:
        seed.stop()


def test_shutdown_wakes_worker_with_a_long_heartbeat_interval() -> None:
    seed_endpoint = endpoint()
    config = ClusterConfig(
        "test-cluster",
        SeedContact("cluster-seed", seed_endpoint),
        heartbeat_interval=60.0,
        unreachable_timeout=120.0,
        join_timeout=2.0,
    )
    seed = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "cluster-seed",
        remoting=remoting_config(seed_endpoint, {}),
        cluster=config,
    )

    started = time.monotonic()
    seed.stop(timeout=1.0)

    assert time.monotonic() - started < 1.0


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), -float("inf")])
def test_cluster_and_actor_system_reject_nonfinite_shutdown_timeouts(timeout: float) -> None:
    seed_endpoint = endpoint()
    seed = ActorSystem.create(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "cluster-seed",
        remoting=remoting_config(seed_endpoint, {}),
        cluster=cluster_config(seed_endpoint),
    )
    try:
        with pytest.raises(ValueError, match="positive"):
            seed.cluster.leave(timeout)
        with pytest.raises(ValueError, match="positive"):
            seed.stop(timeout)
    finally:
        seed.stop()


def test_missing_leave_ack_reserves_time_for_local_shutdown(monkeypatch) -> None:
    seed, member = start_pair()
    monkeypatch.setattr(
        seed.cluster,
        "_handle_leave",
        lambda message, source: None,
    )
    stopped = False
    try:
        started = time.monotonic()
        member.stop(timeout=1.0)
        stopped = True

        assert time.monotonic() - started < 1.0
    finally:
        if not stopped:
            member.stop()
        seed.stop()


def test_shutdown_interrupts_cluster_join_startup_without_leaking_worker(
    monkeypatch,
) -> None:
    seed_endpoint = endpoint()
    member_endpoint = endpoint()
    config = cluster_config(seed_endpoint)
    system = ActorSystemImpl(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "cluster-member",
        remoting=remoting_config(member_endpoint, {"cluster-seed": seed_endpoint}),
        cluster=config,
    )
    join_entered = Event()
    errors: Queue[BaseException] = Queue()

    def interrupted_join(_deadline: float) -> None:
        join_entered.set()
        with system.cluster._condition:
            while system.cluster._state.value == "starting":
                system.cluster._condition.wait()
        raise ClusterJoinError("cluster startup was interrupted")

    def start() -> None:
        try:
            system.start()
        except BaseException as error:
            errors.put_nowait(error)

    monkeypatch.setattr(system.cluster, "_join", interrupted_join)
    starter = Thread(target=start)
    starter.start()
    try:
        assert join_entered.wait(1.0)
        system.stop(timeout=1.0)
    finally:
        starter.join(1.0)

    assert not starter.is_alive()
    assert isinstance(errors.get_nowait(), ClusterJoinError)
    assert system.actor_count == 0


def test_shutdown_retry_still_waits_for_unsettled_cluster_startup(
    monkeypatch,
) -> None:
    seed_endpoint = endpoint()
    member_endpoint = endpoint()
    config = cluster_config(seed_endpoint)
    system = ActorSystemImpl(
        Behaviors.receive(lambda context, message: Behaviors.same),
        "cluster-member",
        remoting=remoting_config(member_endpoint, {"cluster-seed": seed_endpoint}),
        cluster=config,
    )
    join_entered = Event()
    release_join = Event()
    errors: Queue[BaseException] = Queue()

    def blocked_join(_deadline: float) -> None:
        join_entered.set()
        release_join.wait(1.0)
        raise ClusterJoinError("cluster startup was interrupted")

    def start() -> None:
        try:
            system.start()
        except BaseException as error:
            errors.put_nowait(error)

    monkeypatch.setattr(system.cluster, "_join", blocked_join)
    starter = Thread(target=start)
    starter.start()
    try:
        assert join_entered.wait(1.0)
        for _ in range(2):
            with pytest.raises(ClusterShutdownError, match="startup did not settle"):
                system.cluster._stop_before(time.monotonic() + 0.05)
    finally:
        release_join.set()
        starter.join(2.0)

    assert not starter.is_alive()
    assert isinstance(errors.get_nowait(), ClusterJoinError)
    assert system.actor_count == 0


def test_concurrent_start_retries_resolution_until_coordinator_control_is_ready(
    monkeypatch,
) -> None:
    seed_endpoint = endpoint()
    member_endpoint = endpoint()
    config = cluster_config(seed_endpoint)
    behavior = Behaviors.receive(lambda context, message: Behaviors.same)
    seed = ActorSystemImpl(
        behavior,
        "cluster-seed",
        remoting=remoting_config(seed_endpoint, {"cluster-member": member_endpoint}),
        cluster=config,
    )
    member = ActorSystemImpl(
        behavior,
        "cluster-member",
        remoting=remoting_config(member_endpoint, {"cluster-seed": seed_endpoint}),
        cluster=config,
    )
    cluster_start_entered = Event()
    release_cluster_start = Event()
    errors: Queue[BaseException] = Queue()
    original_start = seed.cluster.start

    def delayed_cluster_start() -> None:
        cluster_start_entered.set()
        if not release_cluster_start.wait(1.0):
            raise TimeoutError("test did not release coordinator cluster startup")
        original_start()

    def start(system: ActorSystemImpl) -> None:
        try:
            system.start()
        except BaseException as error:
            errors.put_nowait(error)

    monkeypatch.setattr(seed.cluster, "start", delayed_cluster_start)
    seed_thread = Thread(target=start, args=(seed,))
    member_thread = Thread(target=start, args=(member,))
    seed_thread.start()
    try:
        assert cluster_start_entered.wait(1.0)
        member_thread.start()
        wait_until(lambda: bool(seed.remoting.associations))
        time.sleep(0.05)
        release_cluster_start.set()
        seed_thread.join(2.0)
        member_thread.join(2.0)

        assert not seed_thread.is_alive()
        assert not member_thread.is_alive()
        assert errors.empty()
        assert seed.cluster.is_joined
        assert member.cluster.is_joined
    finally:
        release_cluster_start.set()
        seed_thread.join(1.0)
        if member_thread.ident is not None:
            member_thread.join(1.0)
        if member.actor_count:
            member.stop()
        if seed.actor_count:
            seed.stop()


def test_control_admission_is_ready_when_actor_path_becomes_resolvable(
    monkeypatch,
) -> None:
    seed_endpoint = endpoint()
    member_endpoint = endpoint()
    config = cluster_config(seed_endpoint)
    behavior = Behaviors.receive(lambda context, message: Behaviors.same)
    seed = ActorSystemImpl(
        behavior,
        "cluster-seed",
        remoting=remoting_config(seed_endpoint, {"cluster-member": member_endpoint}),
        cluster=config,
    )
    member = ActorSystemImpl(
        behavior,
        "cluster-member",
        remoting=remoting_config(member_endpoint, {"cluster-seed": seed_endpoint}),
        cluster=config,
    )
    control_actor_published = Event()
    release_control_actor = Event()
    errors: Queue[BaseException] = Queue()
    original_start = LocalActorContext.start

    def gated_start(context: LocalActorContext) -> None:
        if context._system is seed and context._ref.name == "_movie_cluster_control_v1":
            control_actor_published.set()
            if not release_control_actor.wait(1.0):
                raise TimeoutError("test did not release cluster control actor startup")
        original_start(context)

    def start(system: ActorSystemImpl) -> None:
        try:
            system.start()
        except BaseException as error:
            errors.put_nowait(error)

    monkeypatch.setattr(LocalActorContext, "start", gated_start)
    seed_thread = Thread(target=start, args=(seed,))
    member_thread = Thread(target=start, args=(member,))
    seed_thread.start()
    try:
        assert control_actor_published.wait(1.0)
        member_thread.start()
        wait_until(lambda: not seed.cluster._control_queue.empty())
        release_control_actor.set()
        seed_thread.join(2.0)
        member_thread.join(2.0)

        assert not seed_thread.is_alive()
        assert not member_thread.is_alive()
        assert errors.empty()
        assert seed.cluster.is_joined
        assert member.cluster.is_joined
    finally:
        release_control_actor.set()
        seed_thread.join(1.0)
        if member_thread.ident is not None:
            member_thread.join(1.0)
        if member.actor_count:
            member.stop()
        if seed.actor_count:
            seed.stop()
