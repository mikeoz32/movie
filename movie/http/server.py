from __future__ import annotations

from collections import deque
from concurrent.futures import Future, InvalidStateError
from dataclasses import dataclass
from threading import Condition, Lock
from time import monotonic
from typing import Any

from movie.actor import AbstractBehavior, ActorContext, ActorRef, ActorSystem, Behaviors
from movie.actor.extension import ExtensionId
from movie.actor.system import ExtendedActorSystem
from movie.future import RuntimeFuture
from movie.http.codec import HttpProtocolError, HttpRequestParser, render_response
from movie.http.model import HttpRequest, HttpResponse
from movie.io import (
    TCP,
    Bound,
    Close,
    Closed,
    CommandFailed,
    Connected,
    ConnectionClosed,
    ListenerClosed,
    PeerClosed,
    Read,
    Received,
    Register,
    TcpEndpoint,
    Write,
    WriteCompleted,
)
from movie.streams import (
    Cancel,
    Flow,
    OnComplete,
    OnError,
    OnNext,
    Request,
    RunnableGraph,
    SetUpstream,
    Sink,
    Source,
    StageBehavior,
    Subscribe,
)

_DEFAULT_MAX_REQUEST_LINE_BYTES = 8 * 1024
_DEFAULT_MAX_HEADER_BYTES = 32 * 1024
_DEFAULT_MAX_HEADER_COUNT = 100
_DEFAULT_MAX_BODY_BYTES = 1024 * 1024
_DEFAULT_MAX_BUFFER_BYTES = 2 * 1024 * 1024
_DEFAULT_MAX_PIPELINED_REQUESTS = 32
_DEFAULT_RESPONSE_BUFFER = 16


def _positive_int(value: int | None, default: int, label: str) -> int:
    result = default if value is None else value
    if not isinstance(result, int) or isinstance(result, bool) or result <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return result


def _try_set_result(future: Future, value) -> None:
    try:
        future.set_result(value)
    except InvalidStateError:
        pass


def _try_set_exception(future: Future, error: BaseException) -> None:
    try:
        future.set_exception(error)
    except InvalidStateError:
        pass


@dataclass(frozen=True, slots=True)
class HttpServerSettings:
    max_request_line_bytes: int
    max_header_bytes: int
    max_header_count: int
    max_body_bytes: int
    max_buffer_bytes: int
    max_pipelined_requests: int
    response_buffer: int


@dataclass(frozen=True, slots=True)
class ServerBinding:
    local: TcpEndpoint
    _control: _BindingControl

    def unbind(self) -> Future[None]:
        return self._control.unbind()


class _BindingControl:
    def __init__(
        self,
        system: ActorSystem,
        server_control: _ServerControl,
    ) -> None:
        self._system = system
        self._server_control = server_control
        self._lock = Lock()
        self._completed: RuntimeFuture[None] | None = None
        self._terminal = False
        self._error: BaseException | None = None

    def unbind(self) -> Future[None]:
        with self._lock:
            if self._completed is not None:
                return self._completed
            completed: RuntimeFuture[None] = RuntimeFuture(self._system._submit_callback)
            self._completed = completed
            terminal = self._terminal
            error = self._error
        if terminal:
            if error is None:
                _try_set_result(completed, None)
            else:
                _try_set_exception(completed, error)
            return completed
        stopped = self._server_control.request_stop()

        def listener_stopped(future: Future[None]) -> None:
            try:
                future.result()
            except BaseException as error:
                self.complete(error)
            else:
                self.complete()

        stopped.add_done_callback(listener_stopped)
        return completed

    def complete(self, error: BaseException | None = None) -> None:
        with self._lock:
            if self._terminal:
                return
            self._terminal = True
            self._error = error
            completed = self._completed
        if completed is not None:
            if error is None:
                _try_set_result(completed, None)
            else:
                _try_set_exception(completed, error)


class _ServerControl:
    def __init__(self, tcp) -> None:
        self._tcp = tcp
        self._lock = Lock()
        self._listener: ActorRef[Any] | None = None
        self._stopping = False
        self._listener_stopped: Future[None] = Future()

    @property
    def listener_stopped(self) -> Future[None]:
        return self._listener_stopped

    def bind_listener(self, listener: ActorRef[Any]) -> None:
        with self._lock:
            self._listener = listener
            stopping = self._stopping
        if stopping:
            self._close_listener(listener)

    def request_stop(self) -> Future[None]:
        with self._lock:
            self._stopping = True
            listener = self._listener
        if listener is not None:
            self._close_listener(listener)
        return self._listener_stopped

    def complete(self, error: BaseException | None = None) -> None:
        with self._lock:
            if self._listener_stopped.done():
                return
            if error is None:
                self._listener_stopped.set_result(None)
            else:
                self._listener_stopped.set_exception(error)

    def _close_listener(self, listener: ActorRef[Any]) -> None:
        try:
            closed = self._tcp.close_listener(listener)
        except BaseException as error:
            self.complete(error)
            return

        def listener_closed(future: Future[None]) -> None:
            try:
                future.result()
            except BaseException as error:
                self.complete(error)
            else:
                self.complete()

        closed.add_done_callback(listener_closed)


@dataclass(frozen=True, slots=True)
class _Stop:
    pass


@dataclass(frozen=True, slots=True)
class _StartConnection:
    pass


@dataclass(frozen=True, slots=True)
class _ListenerStopped:
    error: BaseException | None = None


@dataclass(frozen=True, slots=True)
class _ShutdownConnection:
    pass


@dataclass(frozen=True, slots=True)
class _StreamStopped:
    connection_id: object


@dataclass(frozen=True, slots=True)
class _ResponseProduced:
    pass


@dataclass(frozen=True, slots=True)
class _RequestContext:
    method: str
    close: bool
    response_to: ActorRef[Any]


class _RequestContexts:
    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._items: deque[_RequestContext] = deque()
        self._lock = Lock()

    def append(self, context: _RequestContext) -> None:
        with self._lock:
            if len(self._items) >= self._capacity:
                raise BufferError("HTTP response context capacity exceeded")
            self._items.append(context)

    def popleft(self) -> _RequestContext:
        with self._lock:
            if not self._items:
                raise RuntimeError("HTTP handler emitted more responses than requests")
            return self._items.popleft()


class _StreamCompletion:
    def __init__(
        self,
        extension: HttpExtension,
        server: ActorRef[Any],
        stream_id: object,
        stage_count: int,
    ) -> None:
        self._extension = extension
        self._server = server
        self._stream_id = stream_id
        self._remaining = stage_count
        self._lock = Lock()

    def stage_stopped(self, future: Future[None]) -> None:
        with self._lock:
            self._remaining -= 1
            complete = self._remaining == 0
        if complete:
            self._extension._forget_stream(self._stream_id)
            try:
                self._server.tell(_StreamStopped(self._stream_id))
            except BaseException:
                pass


class _HttpRequestSource(AbstractBehavior[Any]):
    def __init__(
        self,
        context: ActorContext,
        tcp,
        connection: ActorRef[Any],
        settings: HttpServerSettings,
        contexts: _RequestContexts,
    ) -> None:
        super().__init__(context)
        self._tcp = tcp
        self._connection = connection
        self._settings = settings
        self._contexts = contexts
        self._parser = HttpRequestParser(
            max_request_line_bytes=settings.max_request_line_bytes,
            max_header_bytes=settings.max_header_bytes,
            max_header_count=settings.max_header_count,
            max_body_bytes=settings.max_body_bytes,
            max_buffer_bytes=settings.max_buffer_bytes,
        )
        self._down: ActorRef[Any] | None = None
        self._lifecycle = None
        self._requests: deque[HttpRequest] = deque()
        self._demand = 0
        self._read_pending = False
        self._stop_reading = False
        self._peer_closed = False
        self._completion_sent = False
        self._protocol_error: HttpProtocolError | None = None
        self._error_sent = False
        self._responses_in_flight = 0
        self._close_requested = False

    def _stop(self, context: ActorContext):
        if self._lifecycle is not None:
            self._lifecycle.expect_stop(context.get_self())
        return Behaviors.stopped

    def _close(self, context: ActorContext) -> None:
        if self._close_requested:
            return
        self._close_requested = True
        try:
            self._tcp.close_connection(self._connection, context.get_self())
        except BaseException:
            pass

    def _fail(self, context: ActorContext, error: Exception):
        self._stop_reading = True
        if self._down is not None and not self._error_sent:
            self._error_sent = True
            try:
                self._down.tell(OnError(error))
            except BaseException:
                pass
        self._close(context)
        return self

    def _record_protocol_error(self, error: HttpProtocolError) -> None:
        self._requests.extend(error.parsed_requests)
        self._protocol_error = error
        self._stop_reading = True

    def _load_buffered(self) -> None:
        if self._requests or self._protocol_error is not None or self._stop_reading:
            return
        available = self._settings.max_pipelined_requests
        try:
            self._requests.extend(self._parser.feed(b"", max_requests=available))
        except HttpProtocolError as error:
            self._record_protocol_error(error)

    def _finish_peer_input(self) -> None:
        if self._requests or self._protocol_error is not None:
            return
        available = self._settings.max_pipelined_requests
        try:
            self._requests.extend(self._parser.finish(max_requests=available))
        except HttpProtocolError as error:
            self._record_protocol_error(error)

    def _pump(self, context: ActorContext):
        while (
            self._demand > 0 and self._responses_in_flight < self._settings.max_pipelined_requests
        ):
            if not self._requests:
                if self._peer_closed:
                    self._finish_peer_input()
                else:
                    self._load_buffered()
                if not self._requests:
                    break
            request = self._requests.popleft()
            close = request.close_requested
            self._contexts.append(_RequestContext(request.method, close, context.get_self()))
            assert self._down is not None
            self._responses_in_flight += 1
            self._down.tell(OnNext(request))
            self._demand -= 1
            if close:
                self._stop_reading = True
                self._protocol_error = None
                self._requests.clear()
                break

        if (
            not self._requests
            and self._responses_in_flight == 0
            and self._protocol_error is not None
            and not self._error_sent
        ):
            self._error_sent = True
            assert self._down is not None
            self._down.tell(OnError(self._protocol_error))
        elif (
            not self._requests
            and self._peer_closed
            and self._protocol_error is None
            and not self._completion_sent
        ):
            self._completion_sent = True
            assert self._down is not None
            self._down.tell(OnComplete())
            return self._stop(context)
        elif (
            self._demand > 0
            and self._responses_in_flight < self._settings.max_pipelined_requests
            and not self._requests
            and not self._read_pending
            and not self._stop_reading
        ):
            self._read_pending = True
            self._connection.tell(Read())
        return self

    def receive(self, context: ActorContext, message: Any):
        try:
            if isinstance(message, Subscribe):
                self._down = message.down
                self._lifecycle = message.wiring
                self._connection.tell(Register(context.get_self(), pull_mode=True))
                if message.wiring is not None:
                    message.wiring.acknowledge()
                return self._pump(context)
            if isinstance(message, Request):
                if message.n <= 0:
                    return self._fail(
                        context,
                        ValueError("Stream demand must be positive"),
                    )
                self._demand += message.n
                return self._pump(context)
            if isinstance(message, Received) and message.connection == self._connection:
                self._read_pending = False
                available = self._settings.max_pipelined_requests - len(self._requests)
                try:
                    self._requests.extend(
                        self._parser.feed(message.data, max_requests=max(0, available))
                    )
                except HttpProtocolError as error:
                    self._record_protocol_error(error)
                return self._pump(context)
            if isinstance(message, PeerClosed) and message.connection == self._connection:
                self._read_pending = False
                self._peer_closed = True
                self._stop_reading = True
                self._finish_peer_input()
                return self._pump(context)
            if isinstance(message, _ResponseProduced):
                if self._responses_in_flight > 0:
                    self._responses_in_flight -= 1
                return self._pump(context)
            if isinstance(message, Closed) and message.connection == self._connection:
                return self._stop(context)
            if isinstance(message, CommandFailed):
                return self._fail(context, RuntimeError(message.detail))
            if isinstance(message, ConnectionClosed) and message.connection == self._connection:
                error = ConnectionError(message.cause or "HTTP connection closed")
                if self._down is not None and not self._completion_sent and not self._error_sent:
                    self._down.tell(OnError(error))
                return self._stop(context)
            if isinstance(message, (Cancel, _ShutdownConnection)):
                if isinstance(message, _ShutdownConnection) and self._down is not None:
                    self._down.tell(OnError(ConnectionAbortedError("HTTP server is stopping")))
                self._stop_reading = True
                self._close(context)
                return self._stop(context)
        except BaseException as error:
            if not isinstance(error, Exception):
                wrapped = RuntimeError("HTTP request source raised BaseException")
                wrapped.__cause__ = error
                error = wrapped
            return self._fail(context, error)
        return self

    def on_signal(self, context: ActorContext, message: ActorSystem.SystemMessage) -> None:
        if (
            isinstance(message, ActorSystem.PostStop)
            and not self._close_requested
            and not self._completion_sent
        ):
            self._close(context)


class _HttpResponseSink(AbstractBehavior[Any]):
    def __init__(
        self,
        context: ActorContext,
        tcp,
        connection: ActorRef[Any],
        contexts: _RequestContexts,
        response_buffer: int,
    ) -> None:
        super().__init__(context)
        self._tcp = tcp
        self._connection = connection
        self._contexts = contexts
        self._response_buffer = response_buffer
        self._up: ActorRef[Any] | None = None
        self._lifecycle = None
        self._pending_close: deque[bool] = deque()
        self._closing = False
        self._close_sent = False
        self._complete_after_pending = False

    def _stop(self, context: ActorContext):
        if self._lifecycle is not None:
            self._lifecycle.expect_stop(context.get_self())
        return Behaviors.stopped

    def _request_close(self, context: ActorContext):
        if self._close_sent:
            return self
        self._close_sent = True
        try:
            self._tcp.close_connection(self._connection, context.get_self())
        except BaseException:
            return self._stop(context)
        return self

    def _cancel(self, context: ActorContext):
        if self._up is not None:
            try:
                self._up.tell(Cancel())
            except BaseException:
                pass
        return self._request_close(context)

    def _write_response(
        self,
        context: ActorContext,
        response: HttpResponse,
        *,
        close: bool,
        head_request: bool = False,
    ):
        wire = render_response(response, close=close, head_request=head_request)
        self._pending_close.append(close)
        try:
            self._connection.tell(Write(wire, completion_to=context.get_self()))
        except BaseException:
            self._pending_close.pop()
            return self._cancel(context)
        self._closing = close
        return self

    def receive(self, context: ActorContext, message: Any):
        if isinstance(message, SetUpstream):
            self._up = message.up
            self._lifecycle = message.wiring
            self._up.tell(Subscribe(context.get_self(), message.wiring))
            self._up.tell(Request(self._response_buffer))
            return self
        if isinstance(message, OnNext):
            if self._closing:
                return self
            try:
                if not isinstance(message.element, HttpResponse):
                    raise TypeError("HTTP handler flow must emit HttpResponse values")
                request = self._contexts.popleft()
                request.response_to.tell(_ResponseProduced())
                close = request.close or message.element.close_requested
                return self._write_response(
                    context,
                    message.element,
                    close=close,
                    head_request=request.method == "HEAD",
                )
            except BaseException:
                return self._cancel(context)
        if isinstance(message, WriteCompleted) and message.connection == self._connection:
            if not self._pending_close:
                return self._cancel(context)
            close = self._pending_close.popleft()
            if close or (self._complete_after_pending and not self._pending_close):
                return self._request_close(context)
            if self._up is not None:
                self._up.tell(Request(1))
            return self
        if isinstance(message, CommandFailed):
            if isinstance(message.command, Close):
                return self._stop(context)
            return self._cancel(context)
        if isinstance(message, OnError):
            if isinstance(message.error, HttpProtocolError):
                body = (message.error.detail + "\n").encode("utf-8")
                return self._write_response(
                    context,
                    HttpResponse(
                        message.error.status,
                        (("Content-Type", "text/plain; charset=utf-8"),),
                        body,
                    ),
                    close=True,
                )
            return self._cancel(context)
        if isinstance(message, OnComplete):
            self._complete_after_pending = True
            if not self._pending_close:
                return self._request_close(context)
            return self
        if isinstance(message, Cancel):
            return self._cancel(context)
        if isinstance(message, Closed) and message.connection == self._connection:
            return self._stop(context)
        return self

    def on_signal(self, context: ActorContext, message: ActorSystem.SystemMessage) -> None:
        if isinstance(message, ActorSystem.PostStop) and not self._close_sent:
            try:
                self._tcp.close_connection(self._connection)
            except BaseException:
                pass


class _HttpConnectionMaterializer(AbstractBehavior[Any]):
    def __init__(
        self,
        context: ActorContext,
        extension: HttpExtension,
        connected: Connected,
        handler: Flow[HttpRequest, HttpResponse],
    ) -> None:
        super().__init__(context)
        self._extension = extension
        self._connected = connected
        self._handler = handler
        self._run = None
        self._stream_id = (context.get_self().id, connected.connection.id)
        self._started = False
        self._stopping = False
        self._stream_stopped = False

    def _materialize(self, context: ActorContext, connected: Connected) -> None:
        contexts = _RequestContexts(self._extension.settings.max_pipelined_requests)
        source = Source(
            lambda: Behaviors.setup(
                lambda source_context: _HttpRequestSource(
                    source_context,
                    self._extension._tcp,
                    connected.connection,
                    self._extension.settings,
                    contexts,
                )
            ),
            "http-request-source",
        )
        sink = Sink(
            lambda: Behaviors.setup(
                lambda sink_context: _HttpResponseSink(
                    sink_context,
                    self._extension._tcp,
                    connected.connection,
                    contexts,
                    self._extension.settings.response_buffer,
                )
            ),
            "http-response-sink",
        )
        graph: RunnableGraph = source.via(self._handler).to(sink)
        run = None
        tracked = False
        try:
            run = graph.run(context.get_system())
            self._run = run
            tracked = self._extension._track_stream(self._stream_id, run)
            all_stages = (*run.stages, run.sink)
            completion = _StreamCompletion(
                self._extension,
                context.get_self(),
                self._stream_id,
                len(all_stages),
            )
            for stage in all_stages:
                stopped = context.get_system().actor_stop_future(stage)
                if not isinstance(stopped, RuntimeFuture):
                    raise TypeError("HTTP stream stage stop future must be a RuntimeFuture")
                stopped.add_internal_done_callback(completion.stage_stopped)
            if not tracked:
                run.stages[0].tell(_ShutdownConnection())
        except BaseException:
            if tracked:
                self._extension._forget_stream(self._stream_id)
            if run is not None:
                try:
                    run.cancel()
                except BaseException:
                    pass
            self._run = None
            self._extension._tcp.close_connection(connected.connection)
            raise

    def _shutdown(self) -> None:
        run = self._run
        try:
            self._extension._tcp.close_connection(self._connected.connection)
        except BaseException:
            pass
        if run is None:
            return
        try:
            run.cancel()
        except BaseException:
            pass

    def receive(self, context: ActorContext, message: Any):
        if isinstance(message, _StartConnection):
            if self._started or self._stopping:
                return self
            self._started = True
            try:
                self._materialize(context, self._connected)
            except BaseException:
                return Behaviors.stopped
            return self
        if isinstance(message, _ShutdownConnection):
            self._stopping = True
            self._shutdown()
            if self._run is None:
                return Behaviors.stopped
            return self
        if isinstance(message, _StreamStopped) and message.connection_id == self._stream_id:
            self._stream_stopped = True
            self._run = None
            try:
                self._extension._tcp.close_connection(self._connected.connection)
            except BaseException:
                pass
            return Behaviors.stopped
        return self

    def on_signal(self, context: ActorContext, message: ActorSystem.SystemMessage) -> None:
        if isinstance(message, ActorSystem.PostStop):
            if self._run is not None and not self._stream_stopped:
                try:
                    self._run.cancel()
                except BaseException:
                    pass
            elif self._run is None and not self._stream_stopped:
                self._extension._tcp.close_connection(self._connected.connection)


class _HttpConnectionSource(StageBehavior):
    def __init__(
        self,
        context: ActorContext,
        extension: HttpExtension,
        server_control: _ServerControl,
    ) -> None:
        super().__init__(context, prefetch=16, maxbuf=1024)
        self._extension = extension
        self._server_control = server_control

    def _close_buffered(self) -> None:
        while self._buf:
            connected = self._buf.popleft()
            try:
                self._extension._tcp.close_connection(connected.connection)
            except BaseException:
                pass

    def receive(self, context: ActorContext, message: Any):
        if isinstance(message, Connected):
            if self._up_closed or self._terminated:
                self._extension._tcp.close_connection(message.connection)
            elif len(self._buf) >= self._maxbuf:
                self._extension._tcp.close_connection(message.connection)
            else:
                self._buf.append(message)
                if self._push():
                    return self._stop(context)
            return self
        if isinstance(message, _Stop):
            self._server_control.request_stop()
            return self
        if isinstance(message, _ListenerStopped):
            if message.error is not None:
                self._close_buffered()
                self._fail_stream(message.error)
                return self._stop(context)
            self._up_closed = True
            if self._push():
                return self._stop(context)
            return self
        if isinstance(message, ListenerClosed):
            error = RuntimeError(message.cause)
            self._server_control.complete(error)
            self._close_buffered()
            self._fail_stream(error)
            return self._stop(context)
        if isinstance(message, Cancel):
            self._server_control.request_stop()
            self._close_buffered()
        return super().receive(context, message)

    def on_signal(self, context: ActorContext, message: ActorSystem.SystemMessage) -> None:
        if isinstance(message, ActorSystem.PostStop):
            self._server_control.request_stop()
            self._close_buffered()
            self._extension._forget_server(context.get_self())


class HttpExtension:
    def __init__(self, system: ExtendedActorSystem) -> None:
        self._system = system
        self._tcp = TCP.get(system)
        config = system.config
        max_request_line_bytes = _positive_int(
            config.get_int(
                "movie.http.server.max-request-line-bytes",
                _DEFAULT_MAX_REQUEST_LINE_BYTES,
            ),
            _DEFAULT_MAX_REQUEST_LINE_BYTES,
            "HTTP maximum request-line bytes",
        )
        max_header_bytes = _positive_int(
            config.get_int("movie.http.server.max-header-bytes", _DEFAULT_MAX_HEADER_BYTES),
            _DEFAULT_MAX_HEADER_BYTES,
            "HTTP maximum header bytes",
        )
        max_header_count = _positive_int(
            config.get_int("movie.http.server.max-header-count", _DEFAULT_MAX_HEADER_COUNT),
            _DEFAULT_MAX_HEADER_COUNT,
            "HTTP maximum header count",
        )
        max_body_bytes = _positive_int(
            config.get_int("movie.http.server.max-body-bytes", _DEFAULT_MAX_BODY_BYTES),
            _DEFAULT_MAX_BODY_BYTES,
            "HTTP maximum body bytes",
        )
        max_buffer_bytes = _positive_int(
            config.get_int("movie.http.server.max-buffer-bytes", _DEFAULT_MAX_BUFFER_BYTES),
            _DEFAULT_MAX_BUFFER_BYTES,
            "HTTP maximum request buffer bytes",
        )
        max_pipelined_requests = _positive_int(
            config.get_int(
                "movie.http.server.max-pipelined-requests",
                _DEFAULT_MAX_PIPELINED_REQUESTS,
            ),
            _DEFAULT_MAX_PIPELINED_REQUESTS,
            "HTTP maximum pipelined requests",
        )
        response_buffer = _positive_int(
            config.get_int("movie.http.server.response-buffer", _DEFAULT_RESPONSE_BUFFER),
            _DEFAULT_RESPONSE_BUFFER,
            "HTTP response buffer",
        )
        if response_buffer > max_pipelined_requests:
            raise ValueError("HTTP response buffer cannot exceed maximum pipelined requests")
        HttpRequestParser(
            max_request_line_bytes=max_request_line_bytes,
            max_header_bytes=max_header_bytes,
            max_header_count=max_header_count,
            max_body_bytes=max_body_bytes,
            max_buffer_bytes=max_buffer_bytes,
        )
        self.settings = HttpServerSettings(
            max_request_line_bytes,
            max_header_bytes,
            max_header_count,
            max_body_bytes,
            max_buffer_bytes,
            max_pipelined_requests,
            response_buffer,
        )
        self._condition = Condition(Lock())
        self._closed = False
        self._servers: set[ActorRef[Any]] = set()
        self._server_controls: dict[ActorRef[Any], _ServerControl] = {}
        self._materializers: set[ActorRef[Any]] = set()
        self._streams: dict[object, Any] = {}
        self._binding_in_progress = 0
        self._next_server_id = 1
        self._next_connection_id = 1

    def start(self) -> None:
        pass

    def bind(
        self,
        endpoint: TcpEndpoint,
        handler: Flow[HttpRequest, HttpResponse],
    ) -> Future[ServerBinding]:
        if not isinstance(endpoint, TcpEndpoint):
            raise TypeError("HTTP endpoint must be a TcpEndpoint")
        if not isinstance(handler, Flow):
            raise TypeError("HTTP handler must be a Flow")
        binding: RuntimeFuture[ServerBinding] = RuntimeFuture(
            self._system._submit_callback,
            cancellable=False,
        )
        with self._condition:
            if self._closed:
                raise RuntimeError("HTTP extension is stopping")
            actor_id = self._next_server_id
            self._next_server_id += 1
            self._binding_in_progress += 1
        server_control = _ServerControl(self._tcp)
        try:
            connections = Source(
                lambda: Behaviors.setup(
                    lambda context: _HttpConnectionSource(
                        context,
                        self,
                        server_control,
                    )
                ),
                f"http-connection-source-{actor_id}",
            )
            materialize = Sink.for_each(
                lambda connected: self._start_connection(connected, handler),
                name=f"http-connection-materializer-{actor_id}",
            )
            server_run = connections.to(materialize).run(self._system)
            server = server_run.stages[0]
        except BaseException as error:
            with self._condition:
                self._binding_in_progress -= 1
                self._condition.notify_all()
            _try_set_exception(binding, error)
            return binding
        with self._condition:
            self._servers.add(server)
            self._server_controls[server] = server_control
            self._condition.notify_all()
        binding_control = _BindingControl(self._system, server_control)
        server_control.listener_stopped.add_done_callback(
            lambda future: self._listener_stopped(server, future)
        )
        stopped = self._system.actor_stop_future(server)
        stopped.add_done_callback(lambda future: self._server_stopped(server, binding))
        bound = self._tcp.bind_endpoint(endpoint, server)
        bound.add_done_callback(
            lambda future: self._binding_completed(
                server,
                server_control,
                binding_control,
                binding,
                future,
            )
        )
        return binding

    def _start_connection(
        self,
        connected: Connected,
        handler: Flow[HttpRequest, HttpResponse],
    ) -> None:
        try:
            with self._condition:
                if self._closed:
                    self._tcp.close_connection(connected.connection)
                    return
                actor_id = self._next_connection_id
                self._next_connection_id += 1
                materializer = self._system.spawn(
                    Behaviors.setup(
                        lambda context: _HttpConnectionMaterializer(
                            context,
                            self,
                            connected,
                            handler,
                        )
                    ),
                    f"http-connection-{actor_id}",
                )
                self._materializers.add(materializer)
                stopped = self._system.actor_stop_future(materializer)
        except BaseException:
            self._tcp.close_connection(connected.connection)
            raise
        if not isinstance(stopped, RuntimeFuture):
            self._system.terminate(materializer)
            self._materializer_stopped(materializer)
            self._tcp.close_connection(connected.connection)
            raise TypeError("HTTP materializer stop future must be a RuntimeFuture")
        stopped.add_internal_done_callback(
            lambda future, actor=materializer: self._materializer_stopped(actor)
        )
        try:
            materializer.tell(_StartConnection())
        except BaseException:
            self._system.terminate(materializer)
            self._tcp.close_connection(connected.connection)
            raise

    def stop(self, timeout: float) -> None:
        if timeout < 0:
            raise ValueError("HTTP shutdown timeout must be nonnegative")
        deadline = monotonic() + timeout
        with self._condition:
            self._closed = True
            while self._binding_in_progress:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError("HTTP binds did not settle before the deadline")
                self._condition.wait(remaining)
            servers = tuple(self._servers)
            controls = tuple(self._server_controls.values())
            materializers = tuple(self._materializers)
        stopped = [(server, self._system.actor_stop_future(server)) for server in servers]
        for control in controls:
            control.request_stop()
        for server, _ in stopped:
            try:
                server.tell(_Stop())
            except BaseException:
                pass
        for materializer in materializers:
            try:
                materializer.tell(_ShutdownConnection())
            except BaseException:
                self._system.terminate(materializer)
        for control in controls:
            control.listener_stopped.result(max(0.0, deadline - monotonic()))
        for _, future in stopped:
            future.result(max(0.0, deadline - monotonic()))
        requested_streams: set[object] = set()
        while True:
            with self._condition:
                pending = tuple(
                    (stream_id, run)
                    for stream_id, run in self._streams.items()
                    if stream_id not in requested_streams
                )
                if not self._streams:
                    break
            for stream_id, run in pending:
                requested_streams.add(stream_id)
                try:
                    run.cancel()
                except BaseException:
                    pass
            with self._condition:
                if not self._streams:
                    break
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError("HTTP connection streams did not stop before the deadline")
                self._condition.wait(remaining)
        with self._condition:
            while self._materializers:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "HTTP connection materializers did not stop before the deadline"
                    )
                self._condition.wait(remaining)

    def _forget_server(self, server: ActorRef[Any]) -> None:
        with self._condition:
            self._servers.discard(server)
            self._condition.notify_all()

    def _listener_stopped(
        self,
        server: ActorRef[Any],
        stopped: Future[None],
    ) -> None:
        try:
            stopped.result()
        except BaseException as error:
            message = _ListenerStopped(error)
        else:
            message = _ListenerStopped()
        try:
            server.tell(message)
        except BaseException:
            self._system.terminate(server)
        with self._condition:
            self._server_controls.pop(server, None)
            self._condition.notify_all()

    def _materializer_stopped(self, materializer: ActorRef[Any]) -> None:
        with self._condition:
            self._materializers.discard(materializer)
            self._condition.notify_all()

    def _track_stream(self, stream_id: object, run) -> bool:
        with self._condition:
            self._streams[stream_id] = run
            running = not self._closed
            self._condition.notify_all()
            return running

    def _forget_stream(self, stream_id: object) -> None:
        with self._condition:
            self._streams.pop(stream_id, None)
            self._condition.notify_all()

    def _server_stopped(
        self,
        server: ActorRef[Any],
        binding: Future[ServerBinding],
    ) -> None:
        self._forget_server(server)
        if not binding.done():
            _try_set_exception(binding, RuntimeError("HTTP server stopped before binding"))

    def _binding_completed(
        self,
        server: ActorRef[Any],
        server_control: _ServerControl,
        binding_control: _BindingControl,
        binding: Future[ServerBinding],
        completed: Future[Bound],
    ) -> None:
        try:
            bound = completed.result()
        except BaseException as error:
            server_control.complete(error)
            _try_set_exception(binding, error)
        else:
            server_control.bind_listener(bound.listener)
            with self._condition:
                stopping = self._closed
            if stopping:
                _try_set_exception(binding, RuntimeError("HTTP extension is stopping"))
                server_control.request_stop()
            else:
                _try_set_result(binding, ServerBinding(bound.local, binding_control))
        finally:
            with self._condition:
                self._binding_in_progress -= 1
                self._condition.notify_all()


HTTP: ExtensionId[HttpExtension] = ExtensionId("http", HttpExtension)


__all__ = ["HTTP", "HttpExtension", "HttpServerSettings", "ServerBinding"]
