from uuid import uuid4

import pytest

from movie.actor.path import ActorPath, Address, RootActorPath, parse_actor_path


def test_add_children():
    root = RootActorPath(Address("local", "test-system"))
    c1 = root / ("child1#" + str(uuid4()))

    assert c1.elements() == ["/", "child1"]
    assert str(c1) == "local://test-system/child1"
    assert "#" not in c1.canonical


@pytest.mark.parametrize(
    "canonical",
    [
        "movie://test-system/",
        "movie://test-system/user/worker-1",
        "movie://test-system@localhost:8080/user/worker_1",
        "movie://test-system@127.0.0.1:65535/a~b/c-d",
        "movie://test-system@[2001:db8::1]:443/user/worker",
    ],
)
def test_canonical_actor_paths_round_trip(canonical: str) -> None:
    path = parse_actor_path(canonical)

    assert str(path) == canonical
    assert ActorPath.parse(canonical) == path
    assert "//" not in canonical.split("://", 1)[1].split("/", 1)[1]


@pytest.mark.parametrize(
    "value",
    [
        "movie://test-system",
        "movie://test-system//user",
        "movie://test-system/user/",
        "movie://test-system/user#00000000-0000-0000-0000-000000000001",
        "movie://test-system/us%65r",
        "movie://test-system/user?query",
        "movie://test-system/usér",
        "movie://test-system@2001:db8::1/user",
        "movie://test-system@host:0/user",
        "movie://test-system@host:65536/user",
        "movie://test-system@[2001:0db8::1]:443/user",
    ],
)
def test_parser_rejects_noncanonical_or_non_ascii_paths(value: str) -> None:
    with pytest.raises(ValueError):
        parse_actor_path(value)


def test_address_requires_a_valid_resolvable_ascii_authority() -> None:
    with pytest.raises(ValueError, match="port requires"):
        Address("movie", "system", port=8080)
    with pytest.raises(ValueError, match="system name"):
        Address("movie", "not a system")
    with pytest.raises(ValueError, match="host"):
        Address("movie", "system", "bad_host")
