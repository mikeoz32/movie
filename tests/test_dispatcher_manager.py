import pytest
from movie.actor.system import ActorSystem
from movie.dispatch.manager import DEFAULT_DISPATCHER_ID, DispatcherManager


def test_dispatcher_manager_init() -> None:
    system = ActorSystem.create(lambda ctx: None, "test-system")
    manager = DispatcherManager(system.config)
    assert manager is not None
    assert manager.default_dispatcher is not None
    assert manager.internal_dispatcher is not None
    assert manager.lookup(DEFAULT_DISPATCHER_ID) is manager.default_dispatcher
    assert manager.lookup("internal-dispatcher") is manager.internal_dispatcher
    with pytest.raises(ValueError):
        manager.lookup("non-existent-dispatcher")
