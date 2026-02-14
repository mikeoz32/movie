from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Callable, Generic, Iterable, List, Tuple, TypeVar

from movie.actor import AbstractBehavior, ActorContext, ActorRef, ActorSystem, Behaviors

T = TypeVar("T")
G = TypeVar("G")


# Messages protocol betwenn stages


@dataclass(frozen=True)
class Subscribe:
    down: ActorRef


@dataclass(frozen=True)
class SetUpstream:
    up: ActorRef


@dataclass(frozen=True)
class Requiest:
    n: int


@dataclass(frozen=True)
class OnNext(Generic[T]):
    element: T


class OnComplete:
    pass


@dataclass(frozen=True)
class OnEerror:
    error: Exception


class Calncel:
    pass


StageBehaviorCommand = (
    Subscribe | SetUpstream | Requiest | OnNext | OnComplete | OnEerror | Calncel
)

_DROP = object()


class StageBehavior(AbstractBehavior[StageBehaviorCommand]):
    def __init__(
        self, ctx: ActorContext, *, prefetch: int = 16, maxbuf: int = 256
    ) -> None:
        super().__init__(ctx)
        self._up: ActorRef | None = None
        self._down: ActorRef | None = None
        self._buf: deque = deque()
        self._demand: int = 0
        self._up_closed: bool = False
        self._prefetch: int = prefetch
        self._maxbuf: int = maxbuf

    def transform(self, x):
        return x

    def _maybe_pull(self):
        if (
            self._up
            and self._down is not None
            and not self._up_closed
            and len(self._buf) < (self._maxbuf // 2)
        ):
            self._up.tell(Requiest(self._prefetch))

    def _push(self):
        while self._demand > 0 and self._buf and self._down is not None:
            elem = self._buf.popleft()
            if elem is not _DROP:
                self._down.tell(OnNext(elem))
                self._demand -= 1

        if self._up_closed and not self._buf and self._down is not None:
            self._down.tell(OnComplete())
        else:
            self._maybe_pull()

    def receive(
        self, context: ActorContext[StageBehaviorCommand], message: StageBehaviorCommand
    ) -> AbstractBehavior[StageBehaviorCommand] | None:
        match message:
            case SetUpstream(up):
                self._up = up
                self._up.tell(Subscribe(context.get_self()))
                self._maybe_pull()
            case Subscribe(down):
                self._down = down
                return self
            case Requiest(n):
                self._demand += n
                self._push()
                return self
            case OnNext(element):
                try:
                    y = self.transform(element)
                    if y is not _DROP:
                        self._buf.append(y)
                    else:
                        # Simple backpressure, do nothing
                        pass
                    self._push()
                except Exception as e:
                    if self._down is not None:
                        self._down.tell(OnEerror(e))
            case OnComplete():
                self._up_closed = True
                self._push()
            case OnEerror(error):
                if self._down is not None:
                    self._down.tell(OnEerror(error))
            case Calncel():
                # TODO: notify upstream
                pass


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
        match message:
            case Subscribe(down):
                self._down = down
                if self._down is not None:
                    self._down.tell(Requiest(self._prefetch))
            case Requiest(n):
                sent = 0
                while sent < n and not self._exhausted and self._down is not None:
                    try:
                        elem = next(self._iter)
                        self._down.tell(OnNext(elem))
                        sent += 1
                    except StopIteration:
                        self._exhausted = True
                        self._down.tell(OnComplete())
                        break


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
            case SetUpstream(up):
                self._up = up
                self._up.tell(Subscribe(context.get_self()))
                if self._up is not None:
                    self._up.tell(Requiest(self._prefetch))

            case OnNext(element):
                try:
                    self._func(element)
                    if self._up is not None:
                        self._up.tell(Requiest(1))
                except Exception as e:
                    if self._result_future is not None:
                        if not self._result_future.done():
                            self._result_future.set_exception(e)
                    if self._up is not None:
                        self._up.tell(OnEerror(e))
            case OnComplete():
                if self._result_future is not None:
                    if not self._result_future.done():
                        self._result_future.set_result(None)
            case OnEerror(error):
                if self._result_future is not None:
                    if not self._result_future.done():
                        self._result_future.set_exception(error)
                if self._up is not None:
                    self._up.tell(OnEerror(error))
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
            case SetUpstream(up):
                self._up = up
                self._up.tell(Subscribe(context.get_self()))
                if self._up is not None:
                    self._up.tell(Requiest(self._prefetch))
            case OnNext(element):
                self._items.append(element)
                if self._up is not None:
                    self._up.tell(Requiest(1))
            case OnComplete():
                if not self._result_future.done():
                    self._result_future.set_result(self._items)
            case OnEerror(error):
                if not self._result_future.done():
                    self._result_future.set_exception(error)
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

    @staticmethod
    def for_each(func: Callable[[T], None], name: str | None = None) -> "Sink[T]":
        return Sink(lambda: SinkForEach.create(func), name)

    @staticmethod
    def for_each_materialized(
        func: Callable[[T], None], name: str | None = None
    ) -> tuple["Sink[T]", Future[None]]:
        result_future: Future[None] = Future()
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
        result_future: Future[List[T]] = Future()
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


class RunnableGraph:
    def __init__(self, source: Source, sink: Sink):
        self._source = source
        self._sink = sink

    def run(self, system: ActorSystem) -> RunResult:
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

        refs = []
        prev_ref: ActorRef | None = None

        for name, factory in stages:
            actor_ref = system.spawn(factory(), name=name)
            refs.append(actor_ref)
            if prev_ref is not None:
                actor_ref.tell(SetUpstream(up=prev_ref))
            prev_ref = actor_ref

        sink_ref = system.spawn(sink_factory(), name=sink_name)

        if prev_ref is not None:
            sink_ref.tell(SetUpstream(up=prev_ref))

        return RunResult(
            stages=refs,
            sink=sink_ref,
            materialized=self._sink.materialized_future,
        )
