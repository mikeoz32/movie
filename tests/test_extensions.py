from movie.actor import ActorSystem, Behaviors, AbstractBehavior, ActorContext
from movie.actor.extension import ExtensionId


class EmptyRoot(AbstractBehavior[None]):
    def receive(self, context: ActorContext, message: None):
        return None


class CounterExtension:
    def __init__(self) -> None:
        self.value = 0


class CounterExtensionId(ExtensionId[CounterExtension]):
    def __init__(self) -> None:
        self.create_calls = 0

    def create_extension(self, system):
        self.create_calls += 1
        ext = CounterExtension()
        ext.value = 41
        return ext


class NamedExtension:
    def __init__(self, name: str) -> None:
        self.name = name


class NamedExtensionId(ExtensionId[NamedExtension]):
    def create_extension(self, system):
        return NamedExtension("movie")


def test_extension_id_get_registers_singleton_extension():
    system = ActorSystem.create(Behaviors.setup(EmptyRoot), "extension-system")
    try:
        ext_id = CounterExtensionId()

        first = ext_id.get(system)
        second = ext_id.get(system)

        assert first is second
        assert first.value == 41
        assert ext_id.create_calls == 1
    finally:
        system.stop()


def test_extension_lookup_by_type_returns_registered_instance():
    system = ActorSystem.create(Behaviors.setup(EmptyRoot), "extension-system-by-type")
    try:
        ext_id = NamedExtensionId()
        ext = ext_id.get(system)

        by_type = system.extension(NamedExtension)

        assert by_type is ext
        assert by_type.name == "movie"
    finally:
        system.stop()
