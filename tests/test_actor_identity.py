import uuid

import pytest

from movie.actor import ActorSystem, Behaviors
from movie.actor.identity import ActorIdentity, new_incarnation_uid


def _behavior():
    return Behaviors.receive(lambda context, message: Behaviors.same)


def test_actor_identity_requires_nonzero_uuid_components() -> None:
    zero = uuid.UUID(int=0)
    nonzero = uuid.uuid4()

    with pytest.raises(ValueError, match="incarnation UID"):
        ActorIdentity(zero, nonzero)
    with pytest.raises(ValueError, match="Actor UID"):
        ActorIdentity(nonzero, zero)
    with pytest.raises(ValueError, match="incarnation UID"):
        ActorIdentity("not-a-uuid", nonzero)  # type: ignore[arg-type]


def test_generated_incarnation_uid_is_nonzero_and_128_bit() -> None:
    uid = new_incarnation_uid()

    assert isinstance(uid, uuid.UUID)
    assert 0 < uid.int < 2**128


def test_same_named_systems_have_distinct_incarnations_and_owned_identities() -> None:
    first = ActorSystem.create(_behavior(), "same-name")
    second = ActorSystem.create(_behavior(), "same-name")
    try:
        assert first.incarnation_uid != second.incarnation_uid
        assert first.identity.system_incarnation_uid == first.incarnation_uid
        assert first.id == first.identity.actor_uid
        assert isinstance(first.id, uuid.UUID)
        assert first.path == second.path

        assert first.lookup_actor_by_uid(first.id) is first._root_ref
        assert first.lookup_actor_by_path(first.path.canonical) is first._root_ref
        assert first.resolve_actor(first.identity, first.path) is first._root_ref
        assert first.resolve_actor(second.identity, first.path) is None
    finally:
        first.stop()
        second.stop()


def test_path_reuse_does_not_reuse_actor_identity() -> None:
    system = ActorSystem.create(_behavior(), "path-reuse")
    try:
        first = system.spawn(_behavior(), "worker")
        first_stopped = system.actor_stop_future(first)
        old_identity = first.identity
        old_path = first.path.canonical

        system.terminate(first)
        first_stopped.result(timeout=1.0)
        second = system.spawn(_behavior(), "worker")

        assert second.path.canonical == old_path
        assert second.id != first.id
        assert second.identity != old_identity
        assert system.lookup_actor_by_uid(first.id) is None
        assert system.lookup_actor_by_path(old_path) is second
        assert system.resolve_actor(old_identity, old_path) is None
        assert system.resolve_actor(second.identity, old_path) is second
    finally:
        system.stop()
