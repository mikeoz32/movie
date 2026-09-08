from __future__ import annotations

from collections.abc import Callable
from enum import Enum, auto
from typing import Generic, TypeVar

from movie.persistence.model import OperationId

S = TypeVar("S")


class _EffectKind(Enum):
    NONE = auto()
    PERSIST = auto()
    DELETE = auto()


class DurableEffect(Generic[S]):
    __slots__ = ("_callbacks", "_kind", "_operation_id", "_state", "_stop")

    def __init__(self) -> None:
        raise TypeError("DurableEffect values are created by DurableStateBehavior")

    @classmethod
    def _create(
        cls,
        kind: _EffectKind,
        state: S | None = None,
        operation_id: OperationId | None = None,
        callbacks: tuple[Callable[[S], None], ...] = (),
        stop: bool = False,
    ) -> DurableEffect[S]:
        effect = object.__new__(cls)
        effect._kind = kind
        effect._state = state
        effect._operation_id = operation_id
        effect._callbacks = callbacks
        effect._stop = stop
        return effect

    def then_run(self, callback: Callable[[S], None]) -> DurableEffect[S]:
        if not callable(callback):
            raise TypeError("Durable State callback must be callable")
        return self._create(
            self._kind,
            self._state,
            self._operation_id,
            (*self._callbacks, callback),
            self._stop,
        )

    def then_stop(self) -> DurableEffect[S]:
        return self._create(
            self._kind,
            self._state,
            self._operation_id,
            self._callbacks,
            True,
        )


def _none_effect() -> DurableEffect[S]:
    return DurableEffect._create(_EffectKind.NONE)


def _persist_effect(state: S, operation_id: OperationId) -> DurableEffect[S]:
    if type(operation_id) is not OperationId:
        raise ValueError("persist requires an OperationId")
    return DurableEffect._create(_EffectKind.PERSIST, state, operation_id)


def _delete_effect(operation_id: OperationId) -> DurableEffect[S]:
    if type(operation_id) is not OperationId:
        raise ValueError("delete requires an OperationId")
    return DurableEffect._create(_EffectKind.DELETE, operation_id=operation_id)


__all__ = ["DurableEffect"]
