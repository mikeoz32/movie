from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass
from enum import Enum, auto
from typing import Generic, NoReturn, TypeVar, cast

from movie.actor.behaviour import AbstractBehavior, Behaviors
from movie.actor.context import ActorContext, InternalActorContext
from movie.actor.system import ActorSystem
from movie.persistence.codec import EncodedState, StateCodec
from movie.persistence.effect import (
    DurableEffect,
    _delete_effect,
    _EffectKind,
    _none_effect,
    _persist_effect,
)
from movie.persistence.errors import DurableStatePersistError, DurableStateRecoveryError
from movie.persistence.extension import DURABLE_STATE
from movie.persistence.model import (
    DurableStateRecord,
    OperationId,
    PersistenceId,
    WriteResult,
)
from movie.persistence.store import DurableStateStore

C = TypeVar("C")
S = TypeVar("S")


class _Phase(Enum):
    RECOVERING = auto()
    RUNNING = auto()
    PERSISTING = auto()
    STOPPING = auto()


@dataclass(frozen=True, slots=True)
class _RecoveryCompleted(ActorSystem.ControlMessage):
    generation: object
    record: DurableStateRecord | None = None
    error: Exception | None = None
    duplicate: bool = False

    def applies_to(self, behavior: object) -> bool:
        return getattr(behavior, "_generation", None) is self.generation

    def deliver(self, behavior: object, context: object) -> None:
        behavior._complete_recovery(self)


@dataclass(frozen=True, slots=True)
class _WriteCompleted(ActorSystem.ControlMessage):
    generation: object
    result: WriteResult | None = None
    error: Exception | None = None

    def applies_to(self, behavior: object) -> bool:
        return getattr(behavior, "_generation", None) is self.generation

    def deliver(self, behavior: object, context: object) -> None:
        behavior._complete_write(self)


@dataclass(slots=True)
class _PendingEffect(Generic[S]):
    state: S
    callbacks: tuple
    stop: bool


class DurableStateBehavior(AbstractBehavior[C], Generic[C, S]):
    """Actor behavior backed by one latest revisioned Durable State."""

    def __init__(
        self,
        context: ActorContext[C],
        persistence_id: PersistenceId,
        codec: StateCodec[S],
    ) -> None:
        super().__init__(context)
        if type(persistence_id) is not PersistenceId:
            raise ValueError("DurableStateBehavior requires a PersistenceId")
        if not callable(getattr(codec, "encode", None)) or not callable(
            getattr(codec, "decode", None)
        ):
            raise TypeError("DurableStateBehavior requires a StateCodec")
        self._persistence_id = persistence_id
        self._codec = codec
        self._internal_context = cast(InternalActorContext[C], context)
        self._generation = object()
        self._phase = _Phase.RECOVERING
        self._revision = 0
        self._state = cast(S, None)
        self._pending: _PendingEffect[S] | None = None
        extension = DURABLE_STATE.get(context.get_system())
        self._store: DurableStateStore = extension.store
        self._internal_context.suspend_user_messages()
        self._begin_recovery(duplicate=False)

    @property
    def persistence_id(self) -> PersistenceId:
        return self._persistence_id

    @property
    def revision(self) -> int:
        return self._revision

    def empty_state(self) -> S:
        raise NotImplementedError

    def handle_command(
        self,
        state: S,
        command: C,
        context: ActorContext[C],
    ) -> DurableEffect[S]:
        raise NotImplementedError

    def on_recovery_completed(self, state: S, revision: int) -> None:
        pass

    def on_recovery_failure(self, error: Exception) -> None:
        pass

    def on_persist_failure(self, error: Exception) -> None:
        pass

    def persist(self, state: S, operation_id: OperationId) -> DurableEffect[S]:
        return _persist_effect(state, operation_id)

    def delete(self, operation_id: OperationId) -> DurableEffect[S]:
        return _delete_effect(operation_id)

    def none(self) -> DurableEffect[S]:
        return _none_effect()

    def stop(self) -> DurableEffect[S]:
        return _none_effect().then_stop()

    def receive(self, context: ActorContext[C], message: C) -> AbstractBehavior | None:
        if self._phase is not _Phase.RUNNING:
            raise RuntimeError("Durable State command was invoked while recovery was suspended")
        self._process_command(message)
        return Behaviors.same

    def on_signal(
        self,
        context: ActorContext[C],
        message: ActorSystem.SystemMessage,
    ) -> None:
        if isinstance(message, ActorSystem.PostStop):
            self._phase = _Phase.STOPPING

    def actor_failed(self, error: Exception) -> None:
        self._phase = _Phase.STOPPING

    def _begin_recovery(self, *, duplicate: bool) -> None:
        self._phase = _Phase.RECOVERING
        try:
            future = self._store.load(self._persistence_id)
            self._pipe_completion(
                future,
                lambda result, error: _RecoveryCompleted(
                    self._generation,
                    result,
                    error,
                    duplicate,
                ),
            )
        except Exception as error:
            self._raise_recovery_failure(error)

    def _complete_recovery(self, completion: _RecoveryCompleted) -> None:
        if completion.error is not None:
            self._raise_recovery_failure(completion.error)
        try:
            record = completion.record
            if record is None:
                state = self._copy_state(self.empty_state())
                revision = 0
            elif record.deleted:
                state = self._copy_state(self.empty_state())
                revision = record.revision
            else:
                assert record.manifest is not None and record.payload is not None
                state = self._copy_state(
                    self._codec.decode(record.manifest, record.payload)
                )
                revision = record.revision
        except Exception as error:
            self._raise_recovery_failure(error)
        self._state = state
        self._revision = revision
        self.on_recovery_completed(self._copy_state(state), revision)
        if completion.duplicate:
            self._finish_pending_effect(apply_candidate=False)
        else:
            self._phase = _Phase.RUNNING
            self._internal_context.resume_user_messages()

    def _process_command(self, command: C) -> None:
        state = self._copy_state(self._state)
        effect = self.handle_command(state, command, self.context)
        if not isinstance(effect, DurableEffect):
            raise TypeError("Durable State command handler must return a DurableEffect")
        if effect._kind is _EffectKind.NONE:
            self._run_callbacks(effect._callbacks, self._state)
            if effect._stop:
                self._request_stop()
            return

        try:
            operation_id = effect._operation_id
            assert operation_id is not None
            if effect._kind is _EffectKind.PERSIST:
                encoded = self._codec.encode(effect._state)
                if not isinstance(encoded, EncodedState):
                    raise TypeError("StateCodec.encode() must return EncodedState")
                candidate = self._codec.decode(encoded.manifest, encoded.payload)
            else:
                candidate = self._copy_state(self.empty_state())
                encoded = None
            self._pending = _PendingEffect(candidate, effect._callbacks, effect._stop)
            self._phase = _Phase.PERSISTING
            self._internal_context.suspend_user_messages()
            if encoded is None:
                future = self._store.delete(
                    self._persistence_id,
                    expected_revision=self._revision,
                    operation_id=operation_id,
                )
            else:
                future = self._store.upsert(
                    self._persistence_id,
                    expected_revision=self._revision,
                    operation_id=operation_id,
                    manifest=encoded.manifest,
                    payload=encoded.payload,
                )
        except Exception as error:
            self._raise_persist_failure(error)
        self._pipe_completion(
            future,
            lambda result, error: _WriteCompleted(self._generation, result, error),
        )

    def _complete_write(self, completion: _WriteCompleted) -> None:
        if completion.error is not None:
            self._raise_persist_failure(completion.error)
        result = completion.result
        if result is None:
            self._raise_persist_failure(
                DurableStatePersistError("Durable State store returned no write result")
            )
        if result.duplicate:
            self._begin_recovery(duplicate=True)
            return
        if result.revision != self._revision + 1:
            self._raise_persist_failure(
                DurableStatePersistError("Durable State store returned an invalid revision")
            )
        self._revision = result.revision
        self._finish_pending_effect(apply_candidate=True)

    def _finish_pending_effect(self, *, apply_candidate: bool) -> None:
        pending = self._pending
        if pending is None:
            raise DurableStatePersistError("Durable State completion has no pending effect")
        self._pending = None
        if apply_candidate:
            self._state = pending.state
        self._phase = _Phase.RUNNING
        self._run_callbacks(pending.callbacks, self._state)
        if pending.stop:
            self._request_stop()
        else:
            self._internal_context.resume_user_messages()

    def _run_callbacks(self, callbacks: tuple, state: S) -> None:
        for callback in callbacks:
            callback(self._copy_state(state))

    def _copy_state(self, state: S) -> S:
        encoded = self._codec.encode(state)
        if not isinstance(encoded, EncodedState):
            raise TypeError("StateCodec.encode() must return EncodedState")
        return self._codec.decode(encoded.manifest, encoded.payload)

    def _request_stop(self) -> None:
        self._phase = _Phase.STOPPING
        self._internal_context.suspend_user_messages()
        self._internal_context.enqueue_control(ActorSystem.Stop())

    def _raise_recovery_failure(self, error: Exception) -> NoReturn:
        self._run_failure_hook(self.on_recovery_failure, error)
        raise DurableStateRecoveryError(
            f"Recovery failed for {self._persistence_id}"
        ) from error

    def _raise_persist_failure(self, error: Exception) -> NoReturn:
        self._run_failure_hook(self.on_persist_failure, error)
        raise DurableStatePersistError(
            f"Durable State commit failed for {self._persistence_id}"
        ) from error

    @staticmethod
    def _run_failure_hook(hook, error: Exception) -> None:
        try:
            hook(error)
        except BaseException as hook_error:
            error.add_note(f"Durable State failure hook also failed: {hook_error!r}")

    def _pipe_completion(self, future: Future, mapper) -> None:
        def complete(future: Future) -> None:
            try:
                result = future.result()
                message = mapper(result, None)
            except BaseException as error:
                if not isinstance(error, Exception):
                    wrapped = RuntimeError("Persistence operation raised BaseException")
                    wrapped.__cause__ = error
                    error = wrapped
                message = mapper(None, error)
            self._internal_context.enqueue_control(message)

        internal_callback = getattr(future, "add_internal_done_callback", None)
        if internal_callback is None:
            future.add_done_callback(complete)
        else:
            internal_callback(complete)


__all__ = ["DurableStateBehavior"]
