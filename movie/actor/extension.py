from __future__ import annotations

from collections.abc import Callable
from enum import Enum, auto
from threading import Condition, RLock, get_ident
from time import monotonic
from typing import TYPE_CHECKING, Generic, Protocol, TypeVar, cast, runtime_checkable

if TYPE_CHECKING:
    from movie.actor.system import ExtendedActorSystem


class Extension(Protocol):
    """One actor-system-scoped service."""


@runtime_checkable
class ManagedExtension(Extension, Protocol):
    def start(self) -> None: ...

    def stop(self, timeout: float) -> None: ...


E = TypeVar("E", bound=Extension)


class ExtensionId(Generic[E]):
    """Stable key and factory for one extension singleton per actor system."""

    __slots__ = ("_factory", "name")

    def __init__(
        self,
        name: str,
        factory: Callable[[ExtendedActorSystem], E],
    ) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError("Extension name must be a nonempty string")
        if not callable(factory):
            raise TypeError("Extension factory must be callable")
        self.name = name
        self._factory = factory

    def create_extension(self, system: ExtendedActorSystem) -> E:
        return self._factory(system)

    def get(self, system: ExtendedActorSystem) -> E:
        return system.extension(self)

    def __repr__(self) -> str:
        return f"ExtensionId({self.name!r})"


class _RegistryState(Enum):
    INACTIVE = auto()
    ACTIVE = auto()
    STOPPING = auto()
    STOPPED = auto()


class ExtensionRegistry:
    def __init__(self, system: ExtendedActorSystem) -> None:
        self._system = system
        self._condition = Condition(RLock())
        self._state = _RegistryState.INACTIVE
        self._instances: dict[ExtensionId[object], object] = {}
        self._initializing: dict[ExtensionId[object], int] = {}
        self._waiting_for: dict[int, int] = {}
        self._started: list[ExtensionId[object]] = []

    def activate(self) -> None:
        with self._condition:
            if self._state is not _RegistryState.INACTIVE:
                raise RuntimeError("Extension registry can only be activated once")
            self._state = _RegistryState.ACTIVE
            self._condition.notify_all()

    def get(self, extension_id: ExtensionId[E]) -> E:
        key = cast(ExtensionId[object], extension_id)
        owner = get_ident()
        with self._condition:
            while True:
                if self._state is not _RegistryState.ACTIVE:
                    raise RuntimeError("Actor system is not accepting extension lookups")
                existing = self._instances.get(key)
                if existing is not None:
                    return cast(E, existing)
                initializing_owner = self._initializing.get(key)
                if initializing_owner == owner:
                    raise RuntimeError(
                        f"Recursive initialization of extension {extension_id.name!r}"
                    )
                if initializing_owner is not None:
                    self._waiting_for[owner] = initializing_owner
                    try:
                        waiting_owner = initializing_owner
                        while waiting_owner in self._waiting_for:
                            waiting_owner = self._waiting_for[waiting_owner]
                            if waiting_owner == owner:
                                raise RuntimeError(
                                    "Cross-thread extension dependency cycle"
                                )
                        self._condition.wait()
                    finally:
                        self._waiting_for.pop(owner, None)
                    continue
                self._initializing[key] = owner
                break

        extension = None
        try:
            extension = extension_id.create_extension(self._system)
            if extension is None:
                raise TypeError("Extension factory returned None")
            if isinstance(extension, ManagedExtension):
                extension.start()
        except BaseException as error:
            if isinstance(extension, ManagedExtension):
                try:
                    extension.stop(1.0)
                except BaseException as cleanup_error:
                    error.add_note(
                        f"Extension startup cleanup failed: {cleanup_error!r}"
                    )
            with self._condition:
                self._initializing.pop(key, None)
                self._condition.notify_all()
            raise

        with self._condition:
            self._instances[key] = extension
            self._started.append(key)
            self._initializing.pop(key, None)
            active = self._state is _RegistryState.ACTIVE
            self._condition.notify_all()
        if not active:
            raise RuntimeError("Actor system stopped while extension was starting")
        return extension

    def stop_all(self, timeout: float) -> None:
        if timeout < 0:
            raise ValueError("Extension shutdown timeout must be nonnegative")
        deadline = monotonic() + timeout
        with self._condition:
            if self._state is _RegistryState.STOPPED:
                return
            self._state = _RegistryState.STOPPING
            self._condition.notify_all()
            while self._initializing:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError("Extensions did not stop before the deadline")
                self._condition.wait(remaining)

        while True:
            with self._condition:
                if not self._started:
                    self._instances.clear()
                    self._state = _RegistryState.STOPPED
                    self._condition.notify_all()
                    return
                key = self._started[-1]
                extension = self._instances[key]
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError("Extensions did not stop before the deadline")
            if isinstance(extension, ManagedExtension):
                extension.stop(remaining)
            with self._condition:
                if self._started and self._started[-1] is key:
                    self._started.pop()
                    self._instances.pop(key, None)


__all__ = [
    "Extension",
    "ExtensionId",
    "ExtensionRegistry",
    "ManagedExtension",
]
