import logging
from collections import deque
from concurrent.futures import Future, InvalidStateError
from dataclasses import dataclass
from enum import Enum, auto
from threading import Lock
from typing import Callable, Generic, Iterable, List, Tuple, TypeVar

from movie.actor import AbstractBehavior, ActorContext, ActorRef, ActorSystem, Behaviors
from movie.future import RuntimeFuture

T = TypeVar("T")
G = TypeVar("G")


# Messages exchanged between stages


@dataclass(frozen=True)
class Subscribe:
    down: ActorRef
    wiring: "_GraphLifecycle | None" = None


@dataclass(frozen=True)
class SetUpstream:
    up: ActorRef
    wiring: "_GraphLifecycle | None" = None


@dataclass(frozen=True)
class Request:
    n: int


@dataclass(frozen=True)
class OnNext(Generic[T]):
    element: T


class OnComplete:
    pass


@dataclass(frozen=True)
class OnError:
    error: Exception


class Cancel:
    pass


# Keep the original spellings as aliases for existing callers.
Requiest = Request
OnEerror = OnError
Calncel = Cancel


StageBehaviorCommand = (
    Subscribe | SetUpstream | Request | OnNext | OnComplete | OnError | Cancel
)

_DROP = object()
_MIN_MAILBOX_CAPACITY = 32


def _try_set_result(future: Future, value) -> bool:
    try:
        future.set_result(value)
        return True
    except InvalidStateError:
        return False


def _try_set_exception(future: Future, error: Exception) -> bool:
    try:
        future.set_exception(error)
        return True
    except InvalidStateError:
        return False


def _submit_result(system: ActorSystem, future: Future, value) -> None:
    system._submit_completion(lambda: _try_set_result(future, value))


def _submit_exception(
    system: ActorSystem, future: Future, error: Exception
) -> None:
    system._submit_completion(lambda: _try_set_exception(future, error))


def _submit_cancel(system: ActorSystem, future: Future) -> None:
    system._submit_completion(future.cancel)


def _as_exception(error: BaseException, message: str) -> Exception:
    if isinstance(error, Exception):
        return error
    wrapped = RuntimeError(message)
    wrapped.__cause__ = error
    return wrapped


class _GraphState(Enum):
    PENDING = auto()
    READY = auto()
    FAILED = auto()
    CANCELLED = auto()
    COMPLETED = auto()


class _GraphLifecycle:
    def __init__(
        self,
        system: ActorSystem,
        ready: RuntimeFuture[None],
        materialized: RuntimeFuture | None,
    ) -> None:
        self._system = system
        self._ready = ready
        self._materialized = materialized
        self._refs: list[ActorRef] = []
        self._remaining: int | None = None
        self._expected_stops: set = set()
        self._lock = Lock()
        self._state = _GraphState.PENDING

    def spawn(
        self, factory: Callable[[], AbstractBehavior], name: str
    ) -> ActorRef:
        behavior = factory()
        with self._lock:
            if self._state is not _GraphState.PENDING:
                raise RuntimeError("Stream materialization was cancelled")
            actor_ref = self._system.spawn(behavior, name=name)
            self._refs.append(actor_ref)
            return actor_ref

    def seal(self) -> None:
        with self._lock:
            if self._state is not _GraphState.PENDING:
                raise RuntimeError("Stream materialization was cancelled")
            self._remaining = len(self._refs) - 1

    def acknowledge(self) -> None:
        complete = False
        with self._lock:
            if self._state is not _GraphState.PENDING:
                return
            if self._remaining is None:
                return
            self._remaining -= 1
            if self._remaining == 0:
                self._state = _GraphState.READY
                complete = True
        if complete:
            _submit_result(self._system, self._ready, None)

    def expect_stop(self, actor_ref: ActorRef) -> None:
        with self._lock:
            self._expected_stops.add(actor_ref.id)

    def stage_stopped(self, actor_ref: ActorRef) -> None:
        with self._lock:
            if actor_ref.id in self._expected_stops:
                if len(self._expected_stops) == len(self._refs):
                    self._state = _GraphState.COMPLETED
                return
            if self._state in (
                _GraphState.FAILED,
                _GraphState.CANCELLED,
                _GraphState.COMPLETED,
            ):
                return
        self.fail(RuntimeError("Stream stage terminated unexpectedly"))

    def fail(self, error: Exception) -> None:
        with self._lock:
            if self._state in (
                _GraphState.FAILED,
                _GraphState.CANCELLED,
                _GraphState.COMPLETED,
            ):
                return
            readiness_failed = self._state is _GraphState.PENDING
            self._state = _GraphState.FAILED
        errors = self._terminate_stages()
        if readiness_failed:
            _submit_exception(self._system, self._ready, error)
        if self._materialized is not None:
            _submit_exception(self._system, self._materialized, error)
        for cleanup_error in errors:
            error.add_note(f"Stream cleanup failed: {cleanup_error!r}")

    def cancel(self, raise_errors: bool) -> None:
        with self._lock:
            if self._state in (
                _GraphState.FAILED,
                _GraphState.CANCELLED,
                _GraphState.COMPLETED,
            ):
                return
            self._state = _GraphState.CANCELLED
        errors = self._terminate_stages()
        self._ready.cancel()
        if self._materialized is not None:
            self._materialized.cancel()
        if errors:
            group = ExceptionGroup("Failed to cancel all stream stages", errors)
            if raise_errors:
                raise group
            self._system._submit_callback(
                lambda: logging.getLogger("movie.streams").error(
                    "Stream cancellation cleanup failed", exc_info=group
                )
            )

    def _terminate_stages(self) -> list[Exception]:
        errors = []
        for actor_ref in reversed(self._refs):
            try:
                self._system.terminate(actor_ref)
            except Exception as error:
                errors.append(error)
        return errors


class StageBehavior(AbstractBehavior[StageBehaviorCommand]):
    def __init__(
        self, ctx: ActorContext, *, prefetch: int = 16, maxbuf: int = 256
    ) -> None:
        super().__init__(ctx)
        self._up: ActorRef | None = None
        self._down: ActorRef | None = None
        self._buf: deque = deque()
        self._demand: int = 0
        self._in_flight: int = 0
        self._up_closed: bool = False
        self._prefetch: int = prefetch
        self._maxbuf: int = maxbuf
        self._terminated = False
        self._completion_sent = False
        self._lifecycle: _GraphLifecycle | None = None
        if prefetch <= 0 or maxbuf <= 0 or prefetch > maxbuf:
            raise ValueError("Stream requires 0 < prefetch <= maxbuf")

    def transform(self, x):
        return x

    def _stop(self, context: ActorContext) -> AbstractBehavior:
        if self._lifecycle is not None:
            self._lifecycle.expect_stop(context.get_self())
        return Behaviors.stopped

    def actor_failed(self, error: Exception) -> None:
        if self._lifecycle is not None:
            self._lifecycle.fail(error)

    def _maybe_pull(self) -> None:
        if (
            self._up is None
            or self._down is None
            or self._up_closed
            or self._terminated
        ):
            return

        buffered = len(self._buf) + self._in_flight
        target = min(self._maxbuf, max(self._prefetch, self._demand))
        request = min(self._maxbuf - buffered, target - buffered)
        if request > 0:
            self._in_flight += request
            self._up.tell(Request(request))

    def _push(self) -> bool:
        while self._demand > 0 and self._buf and self._down is not None:
            elem = self._buf.popleft()
            self._down.tell(OnNext(elem))
            self._demand -= 1

        if (
            self._up_closed
            and not self._buf
            and self._down is not None
            and not self._completion_sent
        ):
            self._completion_sent = True
            self._terminated = True
            self._down.tell(OnComplete())
            return True

        self._maybe_pull()
        return False

    def _fail_stream(self, error: Exception) -> None:
        if self._terminated:
            return
        self._terminated = True
        self._buf.clear()
        if self._up is not None:
            self._up.tell(Cancel())
        if self._down is not None:
            self._down.tell(OnError(error))

    def receive(
        self, context: ActorContext[StageBehaviorCommand], message: StageBehaviorCommand
    ) -> AbstractBehavior[StageBehaviorCommand] | None:
        if self._terminated:
            return self._stop(context)

        match message:
            case SetUpstream(up, wiring):
                self._lifecycle = wiring
                self._up = up
                self._up.tell(Subscribe(context.get_self(), wiring))
                self._maybe_pull()
            case Subscribe(down, wiring):
                self._lifecycle = wiring
                self._down = down
                if wiring is not None:
                    wiring.acknowledge()
                self._maybe_pull()
            case Request(n) if n > 0:
                self._demand += n
                if self._push():
                    return self._stop(context)
            case Request():
                self._fail_stream(ValueError("Stream demand must be positive"))
                return self._stop(context)
            case OnNext(element):
                if self._in_flight > 0:
                    self._in_flight -= 1
                try:
                    y = self.transform(element)
                    if y is not _DROP:
                        if len(self._buf) >= self._maxbuf:
                            raise BufferError("Stream stage buffer capacity exceeded")
                        self._buf.append(y)
                    if self._push():
                        return self._stop(context)
                except BaseException as error:
                    error = _as_exception(error, "Stream transform raised BaseException")
                    self._fail_stream(error)
                    return self._stop(context)
            case OnComplete():
                self._up_closed = True
                self._in_flight = 0
                if self._push():
                    return self._stop(context)
            case OnError(error):
                self._terminated = True
                self._buf.clear()
                if self._down is not None:
                    self._down.tell(OnError(error))
                return self._stop(context)
            case Cancel():
                self._terminated = True
                self._buf.clear()
                if self._up is not None:
                    self._up.tell(Cancel())
                return self._stop(context)
        return self


# Sources


class SourceFromIterable(StageBehavior):
    def __init__(
        self,
        ctx: ActorContext,
        iterable: Iterable[T],
        *,
        prefetch: int = 16,
        maxbuf: int = 256,
    ) -> None:
        super().__init__(ctx, prefetch=prefetch, maxbuf=maxbuf)
        self._iter = iter(iterable)
        self._exhausted: bool = False

    @staticmethod
    def create(it: Iterable[T], prefetch: int = 16, maxbuf: int = 256):
        return Behaviors.setup(
            lambda ctx: SourceFromIterable(ctx, it, prefetch=prefetch, maxbuf=maxbuf)
        )

    def receive(
        self, context: ActorContext[StageBehaviorCommand], message: StageBehaviorCommand
    ) -> AbstractBehavior[StageBehaviorCommand] | None:
        if self._terminated:
            return self._stop(context)

        match message:
            case Subscribe(down, wiring):
                self._lifecycle = wiring
                self._down = down
                if wiring is not None:
                    wiring.acknowledge()
            case Request(n) if n > 0:
                sent = 0
                while sent < n and not self._exhausted and self._down is not None:
                    try:
                        elem = next(self._iter)
                        self._down.tell(OnNext(elem))
                        sent += 1
                    except StopIteration:
                        self._exhausted = True
                        self._terminated = True
                        self._down.tell(OnComplete())
                        return self._stop(context)
                    except BaseException as error:
                        error = _as_exception(
                            error, "Stream source iterator raised BaseException"
                        )
                        self._terminated = True
                        self._down.tell(OnError(error))
                        return self._stop(context)
            case Request():
                self._terminated = True
                if self._down is not None:
                    self._down.tell(OnError(ValueError("Stream demand must be positive")))
                return self._stop(context)
            case Cancel():
                self._terminated = True
                return self._stop(context)
        return self


# Flows
class FlowMap(StageBehavior, Generic[T, G]):
    def __init__(
        self,
        ctx: ActorContext,
        func: Callable[[T], G],
        *,
        prefetch: int = 16,
        maxbuf: int = 256,
    ) -> None:
        super().__init__(ctx, prefetch=prefetch, maxbuf=maxbuf)
        self._func: Callable[[T], G] = func

    @staticmethod
    def create(func: Callable[[T], G], prefetch: int = 16, maxbuf: int = 256):
        return Behaviors.setup(
            lambda ctx: FlowMap(ctx, func, prefetch=prefetch, maxbuf=maxbuf)
        )

    def transform(self, x: T) -> G:
        return self._func(x)


# Sinks
class SinkForEach(StageBehavior, Generic[T]):
    def __init__(
        self,
        ctx: ActorContext,
        func: Callable[[T], None],
        result_future: Future[None] | None = None,
        *,
        prefetch: int = 16,
        maxbuf: int = 256,
    ) -> None:
        super().__init__(ctx, prefetch=prefetch, maxbuf=maxbuf)
        self._func: Callable[[T], None] = func
        self._result_future = result_future

    @staticmethod
    def create(
        func: Callable[[T], None],
        result_future: Future[None] | None = None,
        prefetch: int = 16,
        maxbuf: int = 256,
    ):
        return Behaviors.setup(
            lambda ctx: SinkForEach(
                ctx,
                func,
                result_future,
                prefetch=prefetch,
                maxbuf=maxbuf,
            )
        )

    def receive(
        self, context: ActorContext[StageBehaviorCommand], message: StageBehaviorCommand
    ) -> AbstractBehavior[StageBehaviorCommand] | None:
        match message:
            case SetUpstream(up, wiring):
                self._lifecycle = wiring
                self._up = up
                self._up.tell(Subscribe(context.get_self(), wiring))
                if self._up is not None:
                    self._up.tell(Request(self._prefetch))

            case OnNext(element):
                try:
                    self._func(element)
                    if self._up is not None:
                        self._up.tell(Request(1))
                except BaseException as error:
                    error = _as_exception(error, "Stream sink raised BaseException")
                    if self._result_future is not None:
                        _submit_exception(
                            context.get_system(), self._result_future, error
                        )
                    if self._up is not None:
                        self._up.tell(Cancel())
                    return self._stop(context)
            case OnComplete():
                if self._result_future is not None:
                    _submit_result(context.get_system(), self._result_future, None)
                return self._stop(context)
            case OnError(error):
                if self._result_future is not None:
                    _submit_exception(
                        context.get_system(), self._result_future, error
                    )
                return self._stop(context)
            case Cancel():
                if self._up is not None:
                    self._up.tell(Cancel())
                if self._result_future is not None:
                    _submit_cancel(context.get_system(), self._result_future)
                return self._stop(context)
        return self


class SinkCollect(StageBehavior, Generic[T]):
    def __init__(
        self,
        ctx: ActorContext,
        result_future: Future[List[T]],
        *,
        prefetch: int = 16,
        maxbuf: int = 256,
    ) -> None:
        super().__init__(ctx, prefetch=prefetch, maxbuf=maxbuf)
        self._result_future = result_future
        self._items: List[T] = []

    @staticmethod
    def create(
        result_future: Future[List[T]], prefetch: int = 16, maxbuf: int = 256
    ):
        return Behaviors.setup(
            lambda ctx: SinkCollect(ctx, result_future, prefetch=prefetch, maxbuf=maxbuf)
        )

    def receive(
        self, context: ActorContext[StageBehaviorCommand], message: StageBehaviorCommand
    ) -> AbstractBehavior[StageBehaviorCommand] | None:
        match message:
            case SetUpstream(up, wiring):
                self._lifecycle = wiring
                self._up = up
                self._up.tell(Subscribe(context.get_self(), wiring))
                if self._up is not None:
                    self._up.tell(Request(self._prefetch))
            case OnNext(element):
                self._items.append(element)
                if self._up is not None:
                    self._up.tell(Request(1))
            case OnComplete():
                _submit_result(context.get_system(), self._result_future, self._items)
                return self._stop(context)
            case OnError(error):
                _submit_exception(context.get_system(), self._result_future, error)
                return self._stop(context)
            case Cancel():
                if self._up is not None:
                    self._up.tell(Cancel())
                _submit_cancel(context.get_system(), self._result_future)
                return self._stop(context)
        return self


# DSL
class Flow(Generic[T, G]):
    def __init__(
        self,
        behavior_factory: Callable[[], AbstractBehavior[StageBehaviorCommand]],
        name: str | None = None,
    ):
        self._name = name or f"Flow-{id(self)}"
        self._behavior_factory = behavior_factory

    @staticmethod
    def map(func: Callable[[T], G], name: str | None = None) -> "Flow[T, G]":
        return Flow(lambda: FlowMap.create(func), name)


class Sink(Generic[T]):
    def __init__(
        self,
        behavior_factory: Callable[[], AbstractBehavior[StageBehaviorCommand]],
        name: str | None = None,
        *,
        materialized_future: Future | None = None,
    ):
        self._name = name or f"Sink-{id(self)}"
        self._behavior_factory = behavior_factory
        self._materialized_future = materialized_future
        self._claim_lock = Lock()
        self._claimed = False

    def _claim(self) -> None:
        with self._claim_lock:
            if self._claimed:
                raise RuntimeError("Sink instances can only be materialized once")
            self._claimed = True

    @staticmethod
    def for_each(func: Callable[[T], None], name: str | None = None) -> "Sink[T]":
        return Sink(lambda: SinkForEach.create(func), name)

    @staticmethod
    def for_each_materialized(
        func: Callable[[T], None], name: str | None = None
    ) -> tuple["Sink[T]", Future[None]]:
        result_future: RuntimeFuture[None] = RuntimeFuture()
        return (
            Sink(
                lambda: SinkForEach.create(func, result_future),
                name,
                materialized_future=result_future,
            ),
            result_future,
        )

    @staticmethod
    def collect(name: str | None = None) -> tuple["Sink[T]", Future[List[T]]]:
        result_future: RuntimeFuture[List[T]] = RuntimeFuture()
        return (
            Sink(
                lambda: SinkCollect.create(result_future),
                name,
                materialized_future=result_future,
            ),
            result_future,
        )

    @property
    def materialized_future(self) -> Future | None:
        return self._materialized_future


class Source(Generic[T]):
    def __init__(
        self,
        behavior_factory: Callable[[], AbstractBehavior[StageBehaviorCommand]],
        name: str | None = None,
    ):
        self._name = name or f"Source-{id(self)}"
        self._behavior_factory = behavior_factory

    @staticmethod
    def from_iterable(it: Iterable[T], name: str | None = None) -> "Source[T]":
        return Source(lambda: SourceFromIterable.create(it), name)

    def via(self, flow: Flow[T, G]) -> "Chained[T, G]":
        return Chained(self, flow)

    def to(self, sink: Sink[T]) -> "RunnableGraph":
        return RunnableGraph(self, sink)


class Chained(Source[T], Generic[T, G]):
    def __init__(
        self,
        source: Source[T],
        flow: Flow[T, G],
    ):
        self._name = f"{source._name}->{flow._name}"
        self._source = source
        self._flow = flow
        self._behavior_factory = lambda: self._source._behavior_factory()

    @property
    def source(self) -> Source[T]:
        return self._source

    @property
    def flow(self) -> Flow[T, G]:
        return self._flow


@dataclass(frozen=True)
class RunResult:
    stages: List[ActorRef]
    sink: ActorRef
    materialized: Future | None
    system: ActorSystem
    ready: Future[None]
    _cancel: Callable[[bool], None]

    def cancel(self) -> None:
        self._cancel(True)


class RunnableGraph:
    def __init__(self, source: Source, sink: Sink):
        self._source = source
        self._sink = sink
        self._run_lock = Lock()
        self._has_run = False

    def run(self, system: ActorSystem) -> RunResult:
        with self._run_lock:
            if self._has_run:
                raise RuntimeError("RunnableGraph instances can only be run once")
            self._has_run = True

        self._sink._claim()
        materialized = self._sink.materialized_future
        ready: RuntimeFuture[None] = RuntimeFuture(system._submit_callback)
        lifecycle = _GraphLifecycle(system, ready, materialized)
        ready.set_cancel_hook(lambda: lifecycle.cancel(False))
        if materialized is not None:
            if not isinstance(materialized, RuntimeFuture):
                raise TypeError("Stream materialized Future must be a RuntimeFuture")
            materialized.bind_callback_submit(
                system._submit_callback,
                cancel_hook=lambda: lifecycle.cancel(False),
            )

        stages: List[
            Tuple[str, Callable[[], AbstractBehavior[StageBehaviorCommand]]]
        ] = []

        def unwind(node):
            if isinstance(node, Chained):
                unwind(node.source)
                stages.append((node.flow._name, node.flow._behavior_factory))
            else:
                stages.append((node._name, node._behavior_factory))

        unwind(self._source)

        sink_name, sink_factory = self._sink._name, self._sink._behavior_factory

        mailbox_capacity = system.config.get_int("movie.mailbox.default.capacity", 100_000)
        if mailbox_capacity is None or mailbox_capacity < _MIN_MAILBOX_CAPACITY:
            error = ValueError(
                f"Streams require mailbox capacity >= {_MIN_MAILBOX_CAPACITY}"
            )
            if materialized is not None:
                _try_set_exception(materialized, error)
            raise error

        refs = []
        sink_ref: ActorRef | None = None

        try:
            for name, factory in stages:
                actor_ref = lifecycle.spawn(factory, name)
                refs.append(actor_ref)

            sink_ref = lifecycle.spawn(sink_factory, sink_name)
            lifecycle.seal()
        except BaseException as cause:
            error = _as_exception(cause, "Stream stage factory raised BaseException")
            lifecycle.fail(error)
            if error is cause:
                raise
            raise error from cause

        assert sink_ref is not None
        all_refs = [*refs, sink_ref]

        result = RunResult(
            stages=refs,
            sink=sink_ref,
            materialized=materialized,
            system=system,
            ready=ready,
            _cancel=lifecycle.cancel,
        )

        startup_futures = [system.actor_start_future(ref) for ref in all_refs]
        startup_lock = Lock()
        remaining = len(startup_futures)

        for actor_ref in all_refs:
            stop_future = system.actor_stop_future(actor_ref)
            assert isinstance(stop_future, RuntimeFuture)
            stop_future.add_internal_done_callback(
                lambda future, actor_ref=actor_ref: lifecycle.stage_stopped(actor_ref)
            )

        def wire_graph() -> None:
            try:
                upstream: ActorRef | None = None
                for actor_ref in refs:
                    if upstream is not None:
                        actor_ref.tell(SetUpstream(up=upstream, wiring=lifecycle))
                    upstream = actor_ref
                if upstream is not None:
                    sink_ref.tell(SetUpstream(up=upstream, wiring=lifecycle))
            except Exception as error:
                lifecycle.fail(error)

        def stage_started(future: Future[None]) -> None:
            nonlocal remaining
            try:
                future.result()
            except Exception as error:
                lifecycle.fail(error)
                return

            should_wire = False
            with startup_lock:
                if ready.done():
                    return
                remaining -= 1
                should_wire = remaining == 0
            if should_wire:
                wire_graph()

        for startup_future in startup_futures:
            assert isinstance(startup_future, RuntimeFuture)
            startup_future.add_internal_done_callback(stage_started)

        return result
