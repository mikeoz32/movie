"""Transport-neutral remoting association state machine."""

from __future__ import annotations

import secrets
from collections import deque
from dataclasses import dataclass
from enum import Enum
from threading import Condition, Event, Lock, RLock, Thread, current_thread
from time import monotonic, sleep
from typing import TYPE_CHECKING
from uuid import UUID

from movie.actor.dead_letter import DeadLetterReason, RemoteAdmissionResult
from movie.actor.identity import ActorIdentity
from movie.remoting.errors import (
    DeserializationError,
    FrameTooLargeError,
    HandshakeError,
    NoAssociationError,
    ProtocolValidationError,
    RemotingCapacityError,
    ResolutionError,
    SerializerNegotiationError,
    UnknownSerializerError,
    UnsupportedFrameError,
    UnsupportedHeaderVersionError,
    UnsupportedManifestError,
    WireCodecError,
)
from movie.remoting.serialization import SerializerRoute, negotiate_serializers
from movie.remoting.transport import (
    ConnectionState,
    Endpoint,
    LogicalChannel,
    TransportCapacityError,
    TransportClosedError,
    TransportConnection,
    TransportFlowControlError,
    TransportLimits,
    TransportProtocolError,
    TransportRecord,
)
from movie.remoting.wire import (
    BOOTSTRAP_MAX_FRAME_BYTES,
    CONTROL_LANE_ID,
    MAX_U64,
    MINIMUM_GOAWAY_FRAME_BYTES,
    PROTOCOL_MAJOR,
    PROTOCOL_MINOR,
    AssociationRole,
    DeserializationRejected,
    GoAway,
    Hello,
    HelloAccept,
    HelloReject,
    ReasonCode,
    RecipientUnavailable,
    ResolveRejected,
    ResolveRequest,
    ResolveResponse,
    StreamKind,
    UserMessage,
    decode_frame,
    encode_frame,
    negotiate_capabilities,
)

_ACTIVE_RECEIVE_BATCH = 128

if TYPE_CHECKING:
    from movie.remoting.runtime import RemotingRuntime


class AssociationState(Enum):
    HANDSHAKING = "handshaking"
    ACTIVE = "active"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class AssociationMetrics:
    accepted_delivery_attempts: int
    rejected_delivery_attempts: int
    dead_letter_count: int
    serialization_rejections: int
    deserialization_rejections: int
    sequence_violations: int
    reconnect_count: int


@dataclass(frozen=True, slots=True)
class AssociationSnapshot:
    association_uid: UUID
    state: AssociationState
    close_reason: ReasonCode | None
    close_detail: str | None
    peer_system_name: str | None
    peer_incarnation_uid: UUID | None
    peer_endpoint: Endpoint | None
    lane_count: int | None
    maximum_frame_bytes: int | None
    sequence_violations_by_lane: tuple[int, ...] | None
    outbound_message_limit: int | None
    outbound_byte_limit: int | None
    inbound_message_limit: int | None
    inbound_byte_limit: int | None
    pending_outbound_messages: int
    pending_outbound_bytes: int
    pending_inbound_messages: int
    pending_inbound_bytes: int
    metrics: AssociationMetrics


@dataclass(slots=True)
class _PendingResolve:
    path: str
    completed: Event
    response: ResolveResponse | None = None
    error: ResolutionError | None = None


class _HandshakeFailure(Exception):
    def __init__(self, reason: ReasonCode, detail: str, *, reply: bool = True) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail
        self.reply = reply


class _ProtocolFailure(Exception):
    def __init__(self, reason: ReasonCode, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


class Association:
    """One handshake and active receive loop driven by a dedicated daemon thread."""

    def __init__(
        self,
        runtime: RemotingRuntime,
        connection: TransportConnection,
        *,
        initiator: bool,
        expected_system_name: str | None = None,
        expected_endpoint: Endpoint | None = None,
        handshake_timeout: float = 5.0,
    ) -> None:
        self._runtime = runtime
        self._connection = connection
        self._initiator = initiator
        self._expected_system_name = expected_system_name
        self._expected_endpoint = expected_endpoint
        self._handshake_timeout = handshake_timeout
        self._condition = Condition(Lock())
        self._send_lock = RLock()
        self._pending_lock = Lock()
        self._state = AssociationState.HANDSHAKING
        self._close_reason: ReasonCode | None = None
        self._close_detail: str | None = None
        self._handshake_error: HandshakeError | None = None
        self._stop_requested = Event()

        self._peer_system_name: str | None = None
        self._peer_incarnation_uid: UUID | None = None
        self._peer_endpoint: Endpoint | None = None
        self._accept: HelloAccept | None = None
        self._lane_count: int | None = None
        self._outbound_message_limit: int | None = None
        self._outbound_byte_limit: int | None = None
        self._inbound_message_limit: int | None = None
        self._inbound_byte_limit: int | None = None
        self._send_routes: dict[int, SerializerRoute] = {}
        self._receive_routes: dict[int, SerializerRoute] = {}
        self._serializer_minors: dict[int, int] = {}
        self._outbound_sequences: list[int] = []
        self._inbound_sequences: list[int] = []
        self._sequence_violations_by_lane: list[int] = []
        self._deferred_records: deque[TransportRecord] = deque()
        self._deferred_bytes = 0
        self._pending: dict[int, _PendingResolve] = {}
        self._inbound_requests: set[int] = set()
        self._next_correlation_id = secrets.randbits(64) or 1

        self._accepted_delivery_attempts = 0
        self._rejected_delivery_attempts = 0
        self._dead_letter_count = 0
        self._serialization_rejections = 0
        self._deserialization_rejections = 0
        self._sequence_violations = 0
        self._reconnect_count = 0
        self._counts_as_reconnect = False
        self._started = False
        self._closed_notified = False
        self._registration_order = 0
        self._close_deadline: float | None = None

        direction = "outbound" if initiator else "inbound"
        self._thread = Thread(
            target=self._run,
            name=f"movie-remoting-association-{connection.association_uid.hex}-{direction}",
            daemon=True,
        )

    @property
    def association_uid(self) -> UUID:
        return self._connection.association_uid

    @property
    def state(self) -> AssociationState:
        with self._condition:
            return self._state

    @property
    def peer_system_name(self) -> str | None:
        with self._condition:
            return self._peer_system_name

    @property
    def peer_incarnation_uid(self) -> UUID | None:
        with self._condition:
            return self._peer_incarnation_uid

    @property
    def peer_endpoint(self) -> Endpoint | None:
        with self._condition:
            return self._peer_endpoint

    @property
    def target_system_name(self) -> str | None:
        return self.peer_system_name or self._expected_system_name

    @property
    def initiator_rank(self) -> tuple[bytes, bytes]:
        if self._initiator:
            return self._runtime.incarnation_uid.bytes, self.association_uid.bytes
        peer_uid = self.peer_incarnation_uid
        if peer_uid is None:
            raise RuntimeError("association rank is unavailable before HELLO")
        return peer_uid.bytes, self.association_uid.bytes

    def start(self) -> None:
        with self._condition:
            if self._started:
                raise RuntimeError("association can only be started once")
            self._thread.start()
            self._started = True

    def wait_active(self, timeout: float) -> None:
        deadline = monotonic() + timeout
        with self._condition:
            while self._state is AssociationState.HANDSHAKING:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise HandshakeError("association handshake timed out")
                self._condition.wait(remaining)
            if self._state is AssociationState.ACTIVE:
                return
            if self._handshake_error is not None:
                raise self._handshake_error
            raise HandshakeError(self._close_detail or "association closed during handshake")

    def close(self, timeout: float, *, detail: str = "actor system shutdown") -> None:
        deadline = monotonic() + timeout
        with self._condition:
            if self._close_deadline is None or self._close_deadline <= monotonic():
                self._close_deadline = deadline
            else:
                self._close_deadline = min(self._close_deadline, deadline)
        self.request_close(ReasonCode.NORMAL_SHUTDOWN, detail)
        self._flush_outbound(min(deadline, monotonic() + 0.05))
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError("association did not close before the deadline")
        self._connection.close(timeout=remaining)
        self._runtime._connection_closed(self._connection)
        if self._thread is not current_thread() and self._started:
            self._thread.join(max(0.0, deadline - monotonic()))
            if self._thread.is_alive():
                raise TimeoutError("association thread did not stop before the deadline")
        self._mark_closed()

    def request_close(
        self,
        reason: ReasonCode,
        detail: str,
        *,
        notify_peer: bool = True,
    ) -> None:
        with self._condition:
            if self._state in (AssociationState.CLOSING, AssociationState.CLOSED):
                return
            self._close_reason = reason
            self._close_detail = detail
            self._transition_to_closing_locked(detail)
        if notify_peer:
            self._send_terminal_best_effort(GoAway(reason, detail))
        self._stop_requested.set()

    def send_user_message(self, recipient: ActorIdentity, message: object) -> None:
        try:
            binding = self._runtime.config.serializers.binding_for(type(message))
            route = self._send_routes.get(binding.serializer_id)
            if route is None or binding.manifest not in route.manifests:
                raise UnsupportedManifestError(
                    f"manifest {binding.manifest!r} is not negotiated for the remote peer"
                )
            serialized = self._runtime.config.serializers.serialize_for_route(
                message,
                route,
                self._serializer_minors[binding.serializer_id],
            )
        except Exception as error:
            self._increment_many(serialization=1, rejected=1)
            raise error

        with self._send_lock:
            self._require_active(recipient.system_incarnation_uid)
            lane_count = self._lane_count
            assert lane_count is not None
            lane_id = int.from_bytes(
                recipient.system_incarnation_uid.bytes + recipient.actor_uid.bytes,
                "big",
            ) % lane_count
            lane_sequence = self._outbound_sequences[lane_id]
            if lane_sequence > MAX_U64:
                self._increment("rejected")
                self.request_close(
                    ReasonCode.PROTOCOL_VIOLATION,
                    f"delivery lane {lane_id} sequence is exhausted",
                )
                raise RemotingCapacityError("delivery lane sequence is exhausted")
            frame = UserMessage(
                self.association_uid,
                lane_id,
                lane_sequence,
                self._runtime.incarnation_uid,
                recipient.system_incarnation_uid,
                recipient.actor_uid,
                serialized.serializer_id,
                serialized.manifest,
                serialized.payload,
            )
            try:
                payload = encode_frame(
                    frame,
                    maximum_frame_bytes=self._maximum_frame_bytes,
                    stream_kind=StreamKind.DELIVERY_LANE,
                )
            except FrameTooLargeError:
                self._increment("rejected")
                raise
            try:
                peer_name = self._peer_system_name
                assert peer_name is not None
                self._runtime._send_if_current(
                    self,
                    peer_name,
                    recipient.system_incarnation_uid,
                    TransportRecord(
                        LogicalChannel(StreamKind.DELIVERY_LANE, lane_id), payload
                    ),
                )
            except (TransportCapacityError, RemotingCapacityError) as error:
                self._increment("rejected")
                raise RemotingCapacityError("outbound association capacity is full") from error
            except TransportClosedError as error:
                self._increment("rejected", cumulative=False)
                raise NoAssociationError("association closed before admission") from error
            self._outbound_sequences[lane_id] = lane_sequence + 1
            self._increment("accepted")

    def resolve(self, actor_path: str, timeout: float) -> ResolveResponse:
        self._require_active()
        pending = _PendingResolve(actor_path, Event())
        with self._pending_lock:
            limit = self._outbound_message_limit
            assert limit is not None
            if len(self._pending) >= limit:
                raise RemotingCapacityError("pending remote request capacity is full")
            correlation_id = self._new_correlation_id_locked()
            self._pending[correlation_id] = pending
        try:
            self._send_frame(
                ResolveRequest(correlation_id, self._peer_system_name_or_raise(), actor_path)
            )
        except BaseException:
            with self._pending_lock:
                self._pending.pop(correlation_id, None)
            raise

        if not pending.completed.wait(timeout):
            with self._pending_lock:
                timed_out = self._pending.pop(correlation_id, None) is pending
            if timed_out:
                self.request_close(
                    ReasonCode.PROTOCOL_VIOLATION,
                    "resolution timed out with a live correlation ID",
                )
                raise ResolutionError("remote actor resolution timed out")
            pending.completed.wait()
        if pending.error is not None:
            raise pending.error
        assert pending.response is not None
        return pending.response

    def snapshot(self) -> AssociationSnapshot:
        transport = self._connection.snapshot()
        with self._condition:
            accept = self._accept
            metrics = AssociationMetrics(
                self._accepted_delivery_attempts,
                self._rejected_delivery_attempts,
                self._dead_letter_count,
                self._serialization_rejections,
                self._deserialization_rejections,
                self._sequence_violations,
                self._reconnect_count,
            )
            return AssociationSnapshot(
                self.association_uid,
                self._state,
                self._close_reason,
                self._close_detail,
                self._peer_system_name,
                self._peer_incarnation_uid,
                self._peer_endpoint,
                self._lane_count,
                accept.maximum_frame_bytes if accept is not None else None,
                (
                    tuple(self._sequence_violations_by_lane)
                    if accept is not None
                    else None
                ),
                self._outbound_message_limit,
                self._outbound_byte_limit,
                self._inbound_message_limit,
                self._inbound_byte_limit,
                transport.pending_outbound_messages,
                transport.pending_outbound_bytes,
                transport.pending_inbound_messages,
                transport.pending_inbound_bytes,
                metrics,
            )

    @property
    def _maximum_frame_bytes(self) -> int:
        accept = self._accept
        return accept.maximum_frame_bytes if accept is not None else BOOTSTRAP_MAX_FRAME_BYTES

    def _run(self) -> None:
        try:
            self._handshake()
            if self._connection.snapshot().state is not ConnectionState.OPEN:
                raise _HandshakeFailure(
                    ReasonCode.PROTOCOL_VIOLATION,
                    "transport failed before association activation",
                    reply=False,
                )
            if not self._runtime._activate_association(self):
                raise _HandshakeFailure(
                    ReasonCode.DUPLICATE_ASSOCIATION,
                    "a canonical duplicate association was retained",
                )
            with self._condition:
                if self._state is not AssociationState.HANDSHAKING:
                    return
                self._state = AssociationState.ACTIVE
                self._condition.notify_all()
            self._runtime._association_activated(self)
            self._active_loop()
        except _HandshakeFailure as failure:
            if failure.reply:
                if (
                    failure.reason is ReasonCode.DUPLICATE_ASSOCIATION
                    and self._accept is not None
                ):
                    self._send_terminal_best_effort(
                        GoAway(failure.reason, failure.detail)
                    )
                else:
                    self._send_bootstrap_best_effort(
                        HelloReject(failure.reason, failure.detail)
                    )
            self._set_close_reason(failure.reason, failure.detail)
        except _ProtocolFailure as failure:
            self._send_terminal_best_effort(GoAway(failure.reason, failure.detail))
            self._set_close_reason(failure.reason, failure.detail)
        except TransportFlowControlError as error:
            self._send_terminal_best_effort(
                GoAway(ReasonCode.FLOW_CONTROL_VIOLATION, str(error))
            )
            self._set_close_reason(ReasonCode.FLOW_CONTROL_VIOLATION, str(error))
        except UnsupportedFrameError as error:
            self._send_terminal_best_effort(
                GoAway(ReasonCode.UNSUPPORTED_FRAME, str(error))
            )
            self._set_close_reason(ReasonCode.UNSUPPORTED_FRAME, str(error))
        except TransportProtocolError as error:
            self._send_terminal_best_effort(
                GoAway(ReasonCode.PROTOCOL_VIOLATION, str(error))
            )
            self._set_close_reason(ReasonCode.PROTOCOL_VIOLATION, str(error))
        except (TransportClosedError, TimeoutError) as error:
            self._set_close_reason(None, str(error))
        except (WireCodecError, ProtocolValidationError) as error:
            self._send_terminal_best_effort(
                GoAway(ReasonCode.PROTOCOL_VIOLATION, str(error))
            )
            self._set_close_reason(ReasonCode.PROTOCOL_VIOLATION, str(error))
        except BaseException as error:
            self._set_close_reason(ReasonCode.PROTOCOL_VIOLATION, str(error))
        finally:
            self._stop_requested.set()
            with self._condition:
                if self._close_deadline is None:
                    self._close_deadline = (
                        monotonic() + self._runtime.config.association_timeout
                    )
            if self._close_reason is not None:
                cleanup_deadline = self._cleanup_deadline()
                flush_deadline = monotonic() + min(
                    0.05,
                    self._runtime.config.association_timeout,
                )
                flush_deadline = min(flush_deadline, cleanup_deadline)
                self._flush_outbound(flush_deadline)
            cleanup_deadline = self._cleanup_deadline()
            remaining = cleanup_deadline - monotonic()
            if remaining <= 0:
                self._runtime._defer_connection_close(
                    self._connection,
                    cleanup_deadline,
                )
                close_timeout = None
            else:
                close_timeout = min(
                    0.05,
                    remaining,
                    self._runtime.config.association_timeout,
                )
            if close_timeout is not None:
                try:
                    self._connection.close(timeout=close_timeout)
                except BaseException:
                    self._runtime._defer_connection_close(
                        self._connection,
                        cleanup_deadline,
                    )
                else:
                    self._runtime._connection_closed(self._connection)
            self._mark_closed()

    def _cleanup_deadline(self) -> float:
        runtime_deadline = self._runtime._association_cleanup_deadline()
        with self._condition:
            close_deadline = self._close_deadline
        assert close_deadline is not None
        if runtime_deadline is None:
            return close_deadline
        return min(runtime_deadline, close_deadline)

    def _handshake(self) -> None:
        deadline = monotonic() + self._handshake_timeout
        role = AssociationRole.INITIATOR if self._initiator else AssociationRole.RESPONDER
        local_hello = self._local_hello(role)
        if self._initiator:
            self._send_frame(local_hello, bootstrap=True, enforce_effective=False)
        peer_hello = self._receive_hello(deadline)
        self._validate_peer_hello(peer_hello, role)
        if not self._runtime._retain_handshake_candidate(self):
            raise _HandshakeFailure(
                ReasonCode.DUPLICATE_ASSOCIATION,
                "a canonical duplicate association was retained",
            )
        if not self._initiator:
            self._send_frame(local_hello, bootstrap=True, enforce_effective=False)

        accept = self._derive_accept(local_hello, peer_hello)
        self._prepare_negotiated(accept)
        self._send_frame(accept, bootstrap=True, enforce_effective=False)
        peer_accept = self._receive_handshake_frame(deadline, defer_delivery=True)
        if isinstance(peer_accept, HelloReject):
            raise _HandshakeFailure(peer_accept.reason, peer_accept.detail, reply=False)
        if not isinstance(peer_accept, HelloAccept):
            raise _HandshakeFailure(
                ReasonCode.PROTOCOL_VIOLATION,
                f"expected HELLO_ACCEPT, got {type(peer_accept).__name__}",
            )
        if peer_accept != accept:
            raise _HandshakeFailure(
                ReasonCode.PROTOCOL_VIOLATION,
                "peer HELLO_ACCEPT is not the canonical negotiated value",
            )
        self._activate_transport(accept)

    def _local_hello(self, role: AssociationRole) -> Hello:
        endpoint = self._runtime.endpoint
        limits = self._runtime.config.limits
        return Hello(
            PROTOCOL_MAJOR,
            PROTOCOL_MINOR,
            role,
            self._runtime.system_name,
            self._runtime.incarnation_uid,
            self.association_uid,
            endpoint.host,
            endpoint.port,
            limits.maximum_record_bytes,
            self._runtime.config.lane_count,
            limits.outbound_message_limit,
            limits.outbound_byte_limit,
            limits.inbound_message_limit,
            limits.inbound_byte_limit,
            serializers=self._runtime.config.serializers.descriptors,
        )

    def _receive_hello(self, deadline: float) -> Hello:
        frame = self._receive_handshake_frame(deadline)
        if isinstance(frame, HelloReject):
            raise _HandshakeFailure(frame.reason, frame.detail, reply=False)
        if not isinstance(frame, Hello):
            raise _HandshakeFailure(
                ReasonCode.PROTOCOL_VIOLATION,
                f"expected HELLO, got {type(frame).__name__}",
            )
        return frame

    def _receive_handshake_frame(
        self,
        deadline: float,
        *,
        defer_delivery: bool = False,
    ):
        while True:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise _HandshakeFailure(
                    ReasonCode.PROTOCOL_VIOLATION,
                    "handshake timed out",
                )
            try:
                record = self._connection.receive(timeout=remaining)
                frame = decode_frame(
                    record.payload,
                    maximum_frame_bytes=BOOTSTRAP_MAX_FRAME_BYTES,
                    stream_kind=record.channel.kind,
                )
            except TimeoutError as error:
                raise _HandshakeFailure(
                    ReasonCode.PROTOCOL_VIOLATION, "handshake timed out"
                ) from error
            except UnsupportedFrameError as error:
                raise _HandshakeFailure(
                    ReasonCode.UNSUPPORTED_FRAME,
                    str(error),
                ) from error
            except UnsupportedHeaderVersionError as error:
                raise _HandshakeFailure(
                    ReasonCode.INCOMPATIBLE_VERSION,
                    str(error),
                ) from error
            except WireCodecError as error:
                raise _HandshakeFailure(ReasonCode.PROTOCOL_VIOLATION, str(error)) from error
            if defer_delivery and isinstance(frame, UserMessage):
                self._defer_delivery_record(record)
                continue
            return frame

    def _validate_peer_hello(self, hello: Hello, local_role: AssociationRole) -> None:
        if hello.protocol_major != PROTOCOL_MAJOR:
            raise _HandshakeFailure(
                ReasonCode.INCOMPATIBLE_VERSION,
                f"protocol major {hello.protocol_major} is incompatible with {PROTOCOL_MAJOR}",
            )
        expected_role = (
            AssociationRole.RESPONDER
            if local_role is AssociationRole.INITIATOR
            else AssociationRole.INITIATOR
        )
        if hello.role is not expected_role:
            raise _HandshakeFailure(
                ReasonCode.PROTOCOL_VIOLATION, "peer HELLO has the wrong association role"
            )
        if hello.association_uid != self.association_uid:
            raise _HandshakeFailure(
                ReasonCode.PROTOCOL_VIOLATION,
                "peer HELLO association UID does not match the transport",
            )
        if hello.system_incarnation_uid == self._runtime.incarnation_uid:
            raise _HandshakeFailure(
                ReasonCode.PROTOCOL_VIOLATION,
                "peer and local incarnation UIDs must differ",
            )
        if any(
            value <= 0
            for value in (
                hello.maximum_frame_bytes,
                hello.lane_count,
                hello.outbound_message_limit,
                hello.outbound_byte_limit,
                hello.inbound_message_limit,
                hello.inbound_byte_limit,
            )
        ):
            raise _HandshakeFailure(
                ReasonCode.PROTOCOL_VIOLATION, "peer HELLO limits must be positive"
            )
        if hello.maximum_frame_bytes < MINIMUM_GOAWAY_FRAME_BYTES:
            raise _HandshakeFailure(
                ReasonCode.PROTOCOL_VIOLATION,
                "peer maximum frame bytes cannot encode the minimum GOAWAY frame",
            )
        try:
            endpoint = Endpoint(hello.endpoint_host, hello.endpoint_port)
        except Exception as error:
            raise _HandshakeFailure(
                ReasonCode.SYSTEM_NAME_MISMATCH, "peer advertised an invalid endpoint"
            ) from error
        configured = self._runtime.config.peers.get(hello.system_name)
        if (
            configured is None
            or endpoint != configured
            or (
                self._expected_system_name is not None
                and hello.system_name != self._expected_system_name
            )
            or (self._expected_endpoint is not None and endpoint != self._expected_endpoint)
        ):
            raise _HandshakeFailure(
                ReasonCode.SYSTEM_NAME_MISMATCH,
                "peer actor-system name or advertised endpoint is not allowlisted",
            )
        with self._condition:
            self._peer_system_name = hello.system_name
            self._peer_incarnation_uid = hello.system_incarnation_uid
            self._peer_endpoint = endpoint

    def _derive_accept(self, local: Hello, peer: Hello) -> HelloAccept:
        initiator, responder = (local, peer) if self._initiator else (peer, local)
        try:
            serializers = negotiate_serializers(
                local.system_incarnation_uid,
                local.serializers,
                peer.system_incarnation_uid,
                peer.serializers,
            )
        except SerializerNegotiationError as error:
            raise _HandshakeFailure(ReasonCode.PROTOCOL_VIOLATION, str(error)) from error
        self._serializer_minors = {
            serializer.serializer_id: serializer.protocol_minor
            for serializer in serializers.serializers
        }
        return HelloAccept(
            self.association_uid,
            min(local.protocol_minor, peer.protocol_minor),
            min(local.maximum_frame_bytes, peer.maximum_frame_bytes),
            min(local.lane_count, peer.lane_count),
            min(initiator.outbound_message_limit, responder.inbound_message_limit),
            min(initiator.outbound_byte_limit, responder.inbound_byte_limit),
            min(responder.outbound_message_limit, initiator.inbound_message_limit),
            min(responder.outbound_byte_limit, initiator.inbound_byte_limit),
            serializers.routes,
            negotiate_capabilities(local.capabilities, peer.capabilities),
        )

    def _prepare_negotiated(self, accept: HelloAccept) -> None:
        peer_uid = self.peer_incarnation_uid
        assert peer_uid is not None
        if self._initiator:
            outbound_messages = accept.outbound_message_limit
            outbound_bytes = accept.outbound_byte_limit
            inbound_messages = accept.inbound_message_limit
            inbound_bytes = accept.inbound_byte_limit
        else:
            outbound_messages = accept.inbound_message_limit
            outbound_bytes = accept.inbound_byte_limit
            inbound_messages = accept.outbound_message_limit
            inbound_bytes = accept.outbound_byte_limit
        with self._condition:
            self._accept = accept
            self._lane_count = accept.lane_count
            self._outbound_message_limit = outbound_messages
            self._outbound_byte_limit = outbound_bytes
            self._inbound_message_limit = inbound_messages
            self._inbound_byte_limit = inbound_bytes
            self._send_routes = {
                route.serializer_id: route
                for route in accept.serializer_routes
                if route.origin_incarnation_uid == self._runtime.incarnation_uid
            }
            self._receive_routes = {
                route.serializer_id: route
                for route in accept.serializer_routes
                if route.origin_incarnation_uid == peer_uid
            }
            self._outbound_sequences = [0] * accept.lane_count
            self._inbound_sequences = [0] * accept.lane_count
            self._sequence_violations_by_lane = [0] * accept.lane_count

    def _activate_transport(self, accept: HelloAccept) -> None:
        assert self._outbound_message_limit is not None
        assert self._outbound_byte_limit is not None
        assert self._inbound_message_limit is not None
        assert self._inbound_byte_limit is not None
        self._connection.activate(
            TransportLimits(
                accept.maximum_frame_bytes,
                self._outbound_message_limit,
                self._outbound_byte_limit,
                self._inbound_message_limit,
                self._inbound_byte_limit,
            )
        )

    def _defer_delivery_record(self, record: TransportRecord) -> None:
        message_limit = self._inbound_message_limit
        byte_limit = self._inbound_byte_limit
        assert message_limit is not None and byte_limit is not None
        if (
            len(self._deferred_records) >= message_limit
            or self._deferred_bytes + len(record.payload) > byte_limit
        ):
            raise TransportFlowControlError(
                "delivery records overtook handshake beyond inbound capacity"
            )
        self._deferred_records.append(record)
        self._deferred_bytes += len(record.payload)

    def _active_loop(self) -> None:
        while not self._stop_requested.is_set():
            if self._deferred_records:
                count = min(_ACTIVE_RECEIVE_BATCH, len(self._deferred_records))
                records = [self._deferred_records.popleft() for _ in range(count)]
                self._deferred_bytes -= sum(len(record.payload) for record in records)
            else:
                try:
                    receive_many = getattr(self._connection, "receive_many", None)
                    if receive_many is None:
                        records = [self._connection.receive(timeout=0.2)]
                    else:
                        records = receive_many(_ACTIVE_RECEIVE_BATCH, timeout=0.2)
                except TimeoutError:
                    continue
            pending_users: list[tuple[UserMessage, LogicalChannel]] = []

            def flush_users() -> None:
                if pending_users:
                    self._handle_user_messages(pending_users)
                    pending_users.clear()

            try:
                for record in records:
                    frame = decode_frame(
                        record.payload,
                        maximum_frame_bytes=self._maximum_frame_bytes,
                        stream_kind=record.channel.kind,
                    )
                    if isinstance(frame, UserMessage):
                        pending_users.append((frame, record.channel))
                        continue
                    flush_users()
                    if isinstance(frame, ResolveRequest):
                        self._handle_resolve_request(frame)
                    elif isinstance(frame, (ResolveResponse, ResolveRejected)):
                        self._handle_resolve_result(frame)
                    elif isinstance(frame, (RecipientUnavailable, DeserializationRejected)):
                        self._handle_advisory(frame)
                    elif isinstance(frame, GoAway):
                        self._set_close_reason(frame.reason, frame.detail)
                        return
                    else:
                        raise _ProtocolFailure(
                            ReasonCode.PROTOCOL_VIOLATION,
                            f"{type(frame).__name__} is not valid on an active association",
                        )
            finally:
                flush_users()

    def _handle_resolve_request(self, frame: ResolveRequest) -> None:
        if frame.correlation_id in self._inbound_requests:
            raise _ProtocolFailure(
                ReasonCode.PROTOCOL_VIOLATION, "peer reused a live correlation ID"
            )
        self._inbound_requests.add(frame.correlation_id)
        try:
            if frame.system_name != self._runtime.system_name:
                response = ResolveRejected(
                    frame.correlation_id,
                    ReasonCode.SYSTEM_NAME_MISMATCH,
                    "resolution actor-system name does not match",
                )
            else:
                result, actor = self._runtime._resolve_local_path(frame.actor_path)
                if result is RemoteAdmissionResult.ACCEPTED:
                    assert actor is not None
                    response = ResolveResponse(
                        frame.correlation_id,
                        self._runtime.incarnation_uid,
                        actor.id,
                        actor.path.remote_path,
                    )
                else:
                    reason = {
                        RemoteAdmissionResult.ACTOR_NOT_FOUND: ReasonCode.ACTOR_NOT_FOUND,
                        RemoteAdmissionResult.ACTOR_STOPPING: ReasonCode.ACTOR_STOPPING,
                    }.get(result, ReasonCode.INVALID_PATH)
                    response = ResolveRejected(frame.correlation_id, reason, result.name)
            self._send_frame(response)
        finally:
            self._inbound_requests.discard(frame.correlation_id)

    def _handle_resolve_result(self, frame: ResolveResponse | ResolveRejected) -> None:
        with self._pending_lock:
            pending = self._pending.pop(frame.correlation_id, None)
        if pending is None:
            raise _ProtocolFailure(
                ReasonCode.PROTOCOL_VIOLATION,
                "peer responded with an unknown correlation ID",
            )
        if isinstance(frame, ResolveResponse):
            if (
                frame.system_incarnation_uid != self.peer_incarnation_uid
                or frame.actor_path != pending.path
            ):
                pending.error = ResolutionError(
                    "resolution response identity or path does not match its request"
                )
                pending.completed.set()
                raise _ProtocolFailure(
                    ReasonCode.PROTOCOL_VIOLATION,
                    "resolution response identity or path does not match its request",
                )
            pending.response = frame
        else:
            pending.error = ResolutionError(
                f"remote actor resolution was rejected: "
                f"{frame.reason.name}: {frame.detail}"
            )
        pending.completed.set()

    def _set_reconnect_count(
        self,
        reconnect_count: int,
        *,
        counts_as_reconnect: bool,
    ) -> None:
        with self._condition:
            self._reconnect_count = reconnect_count
            self._counts_as_reconnect = counts_as_reconnect

    def _is_reconnect(self) -> bool:
        with self._condition:
            return self._counts_as_reconnect

    def _set_registration_order(self, registration_order: int) -> None:
        self._registration_order = registration_order

    def _prepare_user_message(
        self,
        frame: UserMessage,
        channel: LogicalChannel,
    ) -> tuple[ActorIdentity, object, dict[str, object], UserMessage] | None:
        lane_count = self._lane_count
        peer_uid = self._peer_incarnation_uid
        assert lane_count is not None and peer_uid is not None
        if (
            frame.association_uid != self.association_uid
            or frame.lane_id >= lane_count
            or (
                channel.kind is StreamKind.DELIVERY_LANE
                and channel.lane_id != frame.lane_id
            )
            or frame.sender_incarnation_uid != peer_uid
            or frame.recipient_incarnation_uid != self._runtime.incarnation_uid
        ):
            raise _ProtocolFailure(
                ReasonCode.PROTOCOL_VIOLATION,
                "user-message association, lane, or incarnation is invalid",
            )
        expected = self._inbound_sequences[frame.lane_id]
        if frame.lane_sequence != expected:
            self._increment_sequence_violation(frame.lane_id)
            raise _ProtocolFailure(
                ReasonCode.PROTOCOL_VIOLATION,
                f"delivery lane {frame.lane_id} expected sequence {expected}, "
                f"got {frame.lane_sequence}",
            )
        route = self._receive_routes.get(frame.serializer_id)
        if route is None or frame.manifest not in route.manifests:
            raise _ProtocolFailure(
                ReasonCode.PROTOCOL_VIOLATION,
                "user-message serializer route was not negotiated",
            )

        self._inbound_sequences[frame.lane_id] = expected + 1
        identity = ActorIdentity(self._runtime.incarnation_uid, frame.recipient_actor_uid)
        try:
            message = self._runtime.config.serializers.deserialize(
                frame.serializer_id,
                frame.manifest,
                frame.payload,
                self._serializer_minors[frame.serializer_id],
            )
        except (UnknownSerializerError, UnsupportedManifestError, DeserializationError) as error:
            self._increment_many(
                deserialization=1,
                rejected=1,
                dead_letter=1,
            )
            self._runtime._publish_remoting_dead_letter(
                identity,
                DeadLetterReason.DESERIALIZATION_REJECTED,
                association_uid=self.association_uid,
                lane_id=frame.lane_id,
                lane_sequence=frame.lane_sequence,
                serializer_id=frame.serializer_id,
                manifest=frame.manifest,
                payload_byte_length=len(frame.payload),
            )
            reason = (
                ReasonCode.UNKNOWN_SERIALIZER
                if isinstance(error, UnknownSerializerError)
                else (
                    ReasonCode.UNSUPPORTED_MANIFEST
                    if isinstance(error, UnsupportedManifestError)
                    else ReasonCode.MALFORMED_PAYLOAD
                )
            )
            self._send_advisory_best_effort(
                DeserializationRejected(
                    self.association_uid,
                    frame.lane_id,
                    frame.lane_sequence,
                    frame.recipient_actor_uid,
                    reason,
                    str(error),
                )
            )
            return None

        return (
            identity,
            message,
            {
                "association_uid": self.association_uid,
                "lane_id": frame.lane_id,
                "lane_sequence": frame.lane_sequence,
                "serializer_id": frame.serializer_id,
                "manifest": frame.manifest,
                "payload_byte_length": len(frame.payload),
            },
            frame,
        )

    def _handle_user_messages(
        self,
        frames: list[tuple[UserMessage, LogicalChannel]],
    ) -> None:
        prepared: list[
            tuple[ActorIdentity, object, dict[str, object], UserMessage]
        ] = []
        try:
            for frame, channel in frames:
                message = self._prepare_user_message(frame, channel)
                if message is not None:
                    prepared.append(message)
        finally:
            self._admit_prepared_user_messages(prepared)

    def _admit_prepared_user_messages(
        self,
        prepared: list[
            tuple[ActorIdentity, object, dict[str, object], UserMessage]
        ],
    ) -> None:
        accepted = 0
        for identity, message, metadata, frame in prepared:
            result = self._runtime._admit_remote_message(
                identity,
                message,
                **metadata,
            )
            if result is RemoteAdmissionResult.ACCEPTED:
                accepted += 1
                continue
            self._increment_many(rejected=1, dead_letter=1)
            reason = {
                RemoteAdmissionResult.ACTOR_NOT_FOUND: ReasonCode.ACTOR_NOT_FOUND,
                RemoteAdmissionResult.ACTOR_STOPPING: ReasonCode.ACTOR_STOPPING,
                RemoteAdmissionResult.MAILBOX_FULL: ReasonCode.MAILBOX_FULL,
            }[result]
            self._send_advisory_best_effort(
                RecipientUnavailable(
                    self.association_uid,
                    frame.lane_id,
                    frame.lane_sequence,
                    frame.recipient_actor_uid,
                    reason,
                    result.name,
                )
            )
        if accepted:
            self._increment("accepted", accepted)

    def _handle_advisory(
        self, frame: RecipientUnavailable | DeserializationRejected
    ) -> None:
        lane_count = self._lane_count
        peer_uid = self._peer_incarnation_uid
        assert lane_count is not None and peer_uid is not None
        if frame.association_uid != self.association_uid or frame.lane_id >= lane_count:
            raise _ProtocolFailure(
                ReasonCode.PROTOCOL_VIOLATION,
                "advisory association or lane does not match the active association",
            )
        identity = ActorIdentity(peer_uid, frame.recipient_actor_uid)
        if isinstance(frame, RecipientUnavailable):
            reason = {
                ReasonCode.ACTOR_NOT_FOUND: DeadLetterReason.ACTOR_NOT_FOUND,
                ReasonCode.ACTOR_STOPPING: DeadLetterReason.ACTOR_STOPPING,
                ReasonCode.MAILBOX_FULL: DeadLetterReason.MAILBOX_FULL,
            }[frame.reason]
        else:
            reason = DeadLetterReason.DESERIALIZATION_REJECTED
        self._increment("dead_letter")
        self._runtime._publish_remoting_dead_letter(
            identity,
            reason,
            association_uid=self.association_uid,
            lane_id=frame.lane_id,
            lane_sequence=frame.lane_sequence,
        )

    def _send_frame(
        self,
        frame,
        *,
        bootstrap: bool = False,
        enforce_effective: bool = True,
    ) -> None:
        stream_kind = (
            StreamKind.DELIVERY_LANE if isinstance(frame, UserMessage) else StreamKind.CONTROL
        )
        maximum = BOOTSTRAP_MAX_FRAME_BYTES if bootstrap else self._maximum_frame_bytes
        payload = encode_frame(frame, maximum_frame_bytes=maximum, stream_kind=stream_kind)
        lane_id = frame.lane_id if isinstance(frame, UserMessage) else CONTROL_LANE_ID
        with self._send_lock:
            record = TransportRecord(LogicalChannel(stream_kind, lane_id), payload)
            if enforce_effective and self._outbound_message_limit is not None:
                self._send_payload_locked(record)
            else:
                self._connection.send(record)

    def _send_payload_locked(self, record: TransportRecord) -> None:
        message_limit = self._outbound_message_limit
        byte_limit = self._outbound_byte_limit
        assert message_limit is not None and byte_limit is not None
        send_active = getattr(self._connection, "send_active", None)
        if send_active is None:
            self._connection.send_prevalidated(record, message_limit, byte_limit)
        else:
            send_active(record)

    def _send_bootstrap_best_effort(self, frame) -> None:
        try:
            self._send_frame(frame, bootstrap=True, enforce_effective=False)
        except Exception:
            pass

    def _send_control_best_effort(self, frame) -> None:
        try:
            self._send_frame(frame)
        except Exception:
            pass

    def _send_terminal_best_effort(self, frame) -> None:
        try:
            payload = encode_frame(
                frame,
                maximum_frame_bytes=self._maximum_frame_bytes,
                stream_kind=StreamKind.CONTROL,
            )
            with self._send_lock:
                self._connection.send_terminal(
                    TransportRecord(
                        LogicalChannel(StreamKind.CONTROL, CONTROL_LANE_ID),
                        payload,
                    )
                )
        except FrameTooLargeError:
            if isinstance(frame, GoAway) and frame.detail:
                try:
                    payload = encode_frame(
                        GoAway(frame.reason),
                        maximum_frame_bytes=self._maximum_frame_bytes,
                        stream_kind=StreamKind.CONTROL,
                    )
                    with self._send_lock:
                        self._connection.send_terminal(
                            TransportRecord(
                                LogicalChannel(
                                    StreamKind.CONTROL,
                                    CONTROL_LANE_ID,
                                ),
                                payload,
                            )
                        )
                except Exception:
                    pass
        except Exception:
            pass

    def _send_advisory_best_effort(self, frame) -> None:
        self._send_control_best_effort(frame)

    def _flush_outbound(self, deadline: float) -> None:
        while monotonic() < deadline:
            snapshot = self._connection.snapshot()
            if snapshot.pending_outbound_messages == 0:
                return
            if snapshot.state is ConnectionState.CLOSED:
                return
            sleep(0.001)

    def _require_active(self, expected_peer_uid: UUID | None = None) -> None:
        with self._condition:
            if self._state is not AssociationState.ACTIVE:
                raise NoAssociationError("association is not active")
            if (
                expected_peer_uid is not None
                and self._peer_incarnation_uid != expected_peer_uid
            ):
                raise NoAssociationError("association targets another actor-system incarnation")

    def _routing_state(self) -> tuple[AssociationState, UUID | None]:
        with self._condition:
            return self._state, self._peer_incarnation_uid

    def _new_correlation_id_locked(self) -> int:
        for _ in range(MAX_U64):
            candidate = self._next_correlation_id
            self._next_correlation_id = 1 if candidate == MAX_U64 else candidate + 1
            if candidate and candidate not in self._pending:
                return candidate
        raise RemotingCapacityError("no live correlation ID is available")

    def _peer_system_name_or_raise(self) -> str:
        name = self.peer_system_name
        if name is None:
            raise NoAssociationError("association peer is unknown")
        return name

    def _increment(
        self,
        counter: str,
        amount: int = 1,
        *,
        cumulative: bool = True,
    ) -> None:
        self._increment_many(cumulative=cumulative, **{counter: amount})

    def _increment_sequence_violation(self, lane_id: int) -> None:
        with self._condition:
            self._sequence_violations += 1
            self._sequence_violations_by_lane[lane_id] += 1
        self._runtime._record_metrics(sequence_violations=1)

    def _increment_many(self, *, cumulative: bool = True, **increments: int) -> None:
        with self._condition:
            for counter, amount in increments.items():
                if counter == "accepted":
                    self._accepted_delivery_attempts += amount
                elif counter == "rejected":
                    self._rejected_delivery_attempts += amount
                elif counter == "dead_letter":
                    self._dead_letter_count += amount
                elif counter == "serialization":
                    self._serialization_rejections += amount
                elif counter == "deserialization":
                    self._deserialization_rejections += amount
                elif counter == "sequence":
                    self._sequence_violations += amount
                else:
                    raise ValueError(f"unknown association metric {counter!r}")
        names = {
            "accepted": "accepted_delivery_attempts",
            "rejected": "rejected_delivery_attempts",
            "dead_letter": "dead_letter_count",
            "serialization": "serialization_rejections",
            "deserialization": "deserialization_rejections",
            "sequence": "sequence_violations",
        }
        if cumulative:
            self._runtime._record_metrics(
                **{names[counter]: amount for counter, amount in increments.items()}
            )

    def _set_close_reason(self, reason: ReasonCode | None, detail: str) -> None:
        with self._condition:
            if self._close_detail is None:
                self._close_reason = reason
                self._close_detail = detail
            if self._state is not AssociationState.CLOSED:
                self._transition_to_closing_locked(detail)
        self._stop_requested.set()

    def _transition_to_closing_locked(self, detail: str) -> None:
        was_handshaking = self._state is AssociationState.HANDSHAKING
        self._state = AssociationState.CLOSING
        if was_handshaking and self._handshake_error is None:
            self._handshake_error = HandshakeError(detail)
        self._condition.notify_all()

    def _mark_closed(self) -> None:
        with self._condition:
            if self._closed_notified:
                return
            self._closed_notified = True
            detail = self._close_detail or "association closed"
        error = ResolutionError(detail)
        with self._pending_lock:
            pending = tuple(self._pending.values())
            self._pending.clear()
        for request in pending:
            request.error = error
            request.completed.set()
        with self._condition:
            if self._state is AssociationState.HANDSHAKING and self._handshake_error is None:
                self._handshake_error = HandshakeError(
                    self._close_detail or "association closed during handshake"
                )
            self._state = AssociationState.CLOSED
            self._condition.notify_all()
        self._runtime._association_closed(self)


__all__ = [
    "Association",
    "AssociationMetrics",
    "AssociationSnapshot",
    "AssociationState",
]
