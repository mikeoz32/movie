from uuid import uuid4

from movie.actor.path import Address, RootActorPath


def test_add_children():
    root = RootActorPath(Address("local", "test-system"))
    c1 = root / ("child1#" + str(uuid4()))

    assert c1.elements() == ["/", "child1"]
