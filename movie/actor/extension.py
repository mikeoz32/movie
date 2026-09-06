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


@runtime_checkable
class PreActorStopExtension(Extension, Protocol):
    """Extension that prepares while actors and dependent services are available."""

    def prepare_stop(self, timeout: float) -> None: ...


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
    PREPARING = auto()
    STOPPING = auto()
    STOPPED = auto()


class ExtensionRegistry:
    def __init__(self, system: ExtendedActorSystem) -> None:
        self._system = system
        self._condition = Condition(RLock())
        self._state = _RegistryState.INACTIVE
        self._instances: dict[ExtensionId[object], object] = {}
        self._configured: set[ExtensionId[object]] = set()
        self._initializing: dict[ExtensionId[object], int] = {}
        self._waiting_for: dict[int, int] = {}
        self._started: list[ExtensionId[object]] = []
        self._prepared: set[ExtensionId[object]] = set()
        self._stopped: set[ExtensionId[object]] = set()
        self._failed: dict[ExtensionId[object], BaseException] = {}

    def activate(self) -> None:
        with self._condition:
            if self._state is not _RegistryState.INACTIVE:
                raise RuntimeError("Extension registry can only be activated once")
            self._state = _RegistryState.ACTIVE
            self._condition.notify_all()

    def configure(self, extension_id: ExtensionId[E]) -> E:
        """Create an extension before activation without starting it."""
        key = cast(ExtensionId[object], extension_id)
        owner = get_ident()
        with self._condition:
            if self._state is not _RegistryState.INACTIVE:
                raise RuntimeError("Extensions can only be configured before activation")
            existing = self._instances.get(key)
            if existing is not None:
                return cast(E, existing)
            if key in self._initializing:
                raise RuntimeError(
                    f"Recursive configuration of extension {extension_id.name!r}"
                )
            self._initializing[key] = owner

        try:
            extension = extension_id.create_extension(self._system)
            if extension is None:
                raise TypeError("Extension factory returned None")
        except BaseException:
            with self._condition:
                self._initializing.pop(key, None)
                self._condition.notify_all()
            raise

        with self._condition:
            self._instances[key] = extension
            self._configured.add(key)
            self._initializing.pop(key, None)
            self._condition.notify_all()
        return extension

    def get(self, extension_id: ExtensionId[E]) -> E:
        key = cast(ExtensionId[object], extension_id)
        owner = get_ident()
        with self._condition:
            while True:
                if self._state in (_RegistryState.STOPPING, _RegistryState.STOPPED):
                    raise RuntimeError("Actor system is not accepting extension lookups")
                failure = self._failed.get(key)
                if failure is not None:
                    raise RuntimeError(
                        f"Extension {extension_id.name!r} failed during startup"
                    ) from failure
                existing = self._instances.get(key)
                if existing is not None and (
                    self._state is _RegistryState.INACTIVE or key in self._started
                ):
                    return cast(E, existing)
                if self._state is not _RegistryState.ACTIVE:
                    raise RuntimeError("Actor system is not accepting new extensions")
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

        extension = existing
        try:
            if extension is None:
                extension = extension_id.create_extension(self._system)
                if extension is None:
                    raise TypeError("Extension factory returned None")
                with self._condition:
                    self._instances[key] = extension
                    active = self._state is _RegistryState.ACTIVE
                    self._condition.notify_all()
                if not active:
                    raise RuntimeError("Actor system stopped while extension was starting")
            if isinstance(extension, ManagedExtension):
                extension.start()
        except BaseException as error:
            cleanup_succeeded = True
            if isinstance(extension, ManagedExtension):
                try:
                    extension.stop(1.0)
                except BaseException as cleanup_error:
                    cleanup_succeeded = False
                    error.add_note(
                        f"Extension startup cleanup failed: {cleanup_error!r}"
                    )
            with self._condition:
                if (
                    cleanup_succeeded
                    and key not in self._configured
                    and self._instances.get(key) is extension
                ):
                    self._instances.pop(key, None)
                else:
                    self._failed[key] = error
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

    def find(self, extension_id: ExtensionId[E]) -> E | None:
        key = cast(ExtensionId[object], extension_id)
        with self._condition:
            existing = self._instances.get(key)
            return cast(E, existing) if existing is not None else None

    def _lifecycle_order_locked(self) -> tuple[ExtensionId[object], ...]:
        return tuple(dict.fromkeys((*self._started, *self._instances)))

    def prepare_stop_all(self, timeout: float) -> None:
        if timeout < 0:
            raise ValueError("Extension preparation timeout must be nonnegative")
        deadline = monotonic() + timeout
        with self._condition:
            if self._state in (_RegistryState.STOPPING, _RegistryState.STOPPED):
                return
            self._state = _RegistryState.PREPARING
            self._condition.notify_all()

        while True:
            with self._condition:
                initializing = tuple(
                    key
                    for key in reversed(self._instances)
                    if key in self._initializing and key not in self._prepared
                )
                if not initializing:
                    if not self._initializing:
                        break
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        raise TimeoutError("Extensions did not prepare before the deadline")
                    self._condition.wait(remaining)
                    continue

            prepared_one = False
            for key in initializing:
                with self._condition:
                    extension = self._instances.get(key)
                if not isinstance(extension, PreActorStopExtension):
                    continue
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError("Extensions did not prepare before the deadline")
                extension.prepare_stop(remaining)
                with self._condition:
                    self._prepared.add(key)
                    prepared_one = True
            if not prepared_one:
                with self._condition:
                    if self._initializing:
                        remaining = deadline - monotonic()
                        if remaining <= 0:
                            raise TimeoutError(
                                "Extensions did not prepare before the deadline"
                            )
                        self._condition.wait(remaining)

        with self._condition:
            lifecycle_order = self._lifecycle_order_locked()
        for key in reversed(lifecycle_order):
            with self._condition:
                if key in self._prepared:
                    continue
                extension = self._instances.get(key)
            if not isinstance(extension, PreActorStopExtension):
                continue
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError("Extensions did not prepare before the deadline")
            extension.prepare_stop(remaining)
            with self._condition:
                self._prepared.add(key)

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
                key = next(
                    (
                        key
                        for key in reversed(self._lifecycle_order_locked())
                        if key not in self._stopped
                    ),
                    None,
                )
                if key is None:
                    self._prepared.clear()
                    self._state = _RegistryState.STOPPED
                    self._condition.notify_all()
                    return
                extension = self._instances[key]
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise TimeoutError("Extensions did not stop before the deadline")
            if isinstance(extension, ManagedExtension):
                extension.stop(remaining)
            with self._condition:
                self._failed.pop(key, None)
                self._prepared.discard(key)
                if key in self._configured:
                    self._stopped.add(key)
                else:
                    self._instances.pop(key, None)
                    if key in self._started:
                        self._started.remove(key)


__all__ = [
    "Extension",
    "ExtensionId",
    "ExtensionRegistry",
    "ManagedExtension",
    "PreActorStopExtension",
]
