from movie.actor.system import ActorSystem
from movie.dispatch.manager import DispatcherManager


def test_dispatcher_manager_init() -> None:
    system = ActorSystem.create(lambda ctx: None, "test-system")
    manager = DispatcherManager(system.config)
    assert manager is not None
