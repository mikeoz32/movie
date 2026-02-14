from __future__ import annotations

from typing import TYPE_CHECKING, Generic, Protocol, TypeVar, cast

if TYPE_CHECKING:
    from movie.actor.system import ActorSystem, ExtendedActorSystem


class Extension(Protocol): ...


E = TypeVar("E", bound=Extension)


class ExtensionId(Generic[E]):
    """Akka-style extension identifier.

    Subclasses override :meth:`create_extension` and can call :meth:`get` to
    retrieve a lazily-created singleton extension for a given actor system.
    """

    def create_extension(self, system: "ExtendedActorSystem") -> E:
        raise NotImplementedError

    def get(self, system: "ActorSystem") -> E:
        extended = cast("ExtendedActorSystem", system)
        return extended.register_extension(self, self.create_extension)
