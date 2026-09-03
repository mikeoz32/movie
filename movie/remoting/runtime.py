"""Actor-system remoting runtime and association manager."""

from __future__ import annotations

import math
from collections import deque
from enum import Enum
from threading import Condition, Lock, Thread, current_thread
from time import monotonic, sleep
from uuid import UUID, uuid4

from movie.actor.dead_letter import DeadLetterReason, RemoteAdmissionResult
from movie.actor.identity import ActorIdentity
from movie.actor.path import ActorPath, parse_actor_path
from movie.remoting.association import Association, AssociationSnapshot, AssociationState
from movie.remoting.config import RemotingConfig, _validate_remote_name
from movie.remoting.errors import (
    HandshakeError,
    NoAssociationError,
    RemotingCapacityError,
    RemotingShutdownError,
    ResolutionError,
    StaleIncarnationError,
)
from movie.remoting.ref import RemoteActorRef
from movie.remoting.transport import (
    Endpoint,
    Transport,
    TransportConnection,
    TransportError,
    TransportLimits,
    TransportRecord,
)
from movie.remoting.wire import BOOTSTRAP_MAX_FRAME_BYTES, ReasonCode


class _RuntimeState(Enum):
    NEW = "new"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"


def _timeout(value: float | None, default: float, field: str) -> float:
    if value is None:
        return default
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{field} must be a finite positive number")
    return float(value)


class RemotingRuntime:
    """Owns one listener and the active allowlisted peer associations."""

    def __init__(self, system, config: RemotingConfig) -> None:
        if not isinstance(config, RemotingConfig):
            raise TypeError("config must be a RemotingConfig")
        _validate_remote_name(system.name, "local actor-system name")
        if system.name in config.peers:
            raise ValueError("the local actor-system name cannot appear in the peer allowlist")
        self._system = system
        self._config = config
        self._transport: Transport | None = config.transport
        self._condition = Condition(Lock())
        self._state = _RuntimeState.NEW
        self._startup_settled = False
        self._listener = None
        self._endpoint: Endpoint | None = None
        self._associations: set[Association] = set()
        self._active: dict[str, Association] = {}
        self._known_incarnations: dict[str, UUID] = {}
        self._known_generation: dict[str, int] = {}
        self._next_association_generation = 1
        self._reconnect_counts: dict[str, int] = {}
        self._pending_incoming: list[TransportConnection] = []
        self._outbound_setups = 0
        self._closing_connections: dict[int, TransportConnection] = {}
        self._closer_thread: Thread | None = None
        self._transport_closed = False
        self._history: deque[AssociationSnapshot] = deque(
            maxlen=config.association_history_limit
        )
        limits = config.limits
        self._bootstrap_limits = TransportLimits(
            BOOTSTRAP_MAX_FRAME_BYTES,
            max(limits.outbound_message_limit, 4),
            max(limits.outbound_byte_limit, 2 * BOOTSTRAP_MAX_FRAME_BYTES),
            max(limits.inbound_message_limit, 4),
            max(limits.inbound_byte_limit, 2 * BOOTSTRAP_MAX_FRAME_BYTES),
        )

    @property
    def config(self) -> RemotingConfig:
        return self._config

    @property
    def system_name(self) -> str:
        return self._system.name

    @property
    def incarnation_uid(self) -> UUID:
        return self._system.incarnation_uid

    @property
    def endpoint(self) -> Endpoint:
        with self._condition:
            return self._endpoint or self._config.local

    @property
    def associations(self) -> tuple[AssociationSnapshot, ...]:
        with self._condition:
            associations = tuple(self._associations)
        return tuple(
            sorted(
                (association.snapshot() for association in associations),
                key=lambda snapshot: snapshot.association_uid.bytes,
            )
        )

    @property
    def association_history(self) -> tuple[AssociationSnapshot, ...]:
        with self._condition:
            return tuple(self._history)

    def _resolve_transport(self) -> Transport:
        if self._transport is not None:
            return self._transport
        if self._config.transport_backend == "asyncio":
            from movie.io import ASYNCIO_IO
            from movie.remoting.asyncio_tcp import AsyncioTcpTransport

            transport = AsyncioTcpTransport(ASYNCIO_IO.get(self._system))
        else:
            from movie.remoting.tcp import TcpTransport

            transport = TcpTransport()
        self._transport = transport
        return transport

    def start(self) -> None:
        with self._condition:
            if self._state is not _RuntimeState.NEW:
                raise RuntimeError("remoting runtime can only be started once")
            self._state = _RuntimeState.STARTING
        try:
            transport = self._resolve_transport()
            listener = transport.listen(
                self._config.local,
                self._bootstrap_limits,
                self._on_connection,
            )
        except BaseException:
            with self._condition:
                self._startup_settled = True
                if self._state is _RuntimeState.STARTING:
                    self._state = _RuntimeState.STOPPED
                self._condition.notify_all()
            raise

        with self._condition:
            self._startup_settled = True
            if self._state is not _RuntimeState.STARTING:
                interrupted = True
                self._listener = listener
                self._endpoint = Endpoint(self._config.local.host, listener.endpoint.port)
                incoming = ()
            else:
                interrupted = False
                self._listener = listener
                self._endpoint = Endpoint(self._config.local.host, listener.endpoint.port)
                self._state = _RuntimeState.RUNNING
                incoming = tuple(self._pending_incoming)
                self._pending_incoming.clear()
            self._condition.notify_all()
        if interrupted:
            raise RemotingShutdownError("remoting startup was interrupted by shutdown")
        for connection in incoming:
            self._start_incoming(connection)

    def stop(self, timeout: float | None = None) -> None:
        stop_timeout = _timeout(
            timeout,
            self._config.association_timeout,
            "remoting shutdown timeout",
        )
        deadline = monotonic() + stop_timeout
        with self._condition:
            if self._state is _RuntimeState.STOPPED and self._transport_closed:
                return
            if self._state is _RuntimeState.NEW:
                starting = False
            else:
                starting = self._state is _RuntimeState.STARTING
            self._state = _RuntimeState.STOPPING
            while starting and not self._startup_settled:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "remoting startup did not settle before the shutdown deadline"
                    )
                self._condition.wait(remaining)
            while self._outbound_setups:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(remaining)
            outbound_setups_pending = self._outbound_setups > 0
            listener = self._listener
            incoming = tuple(self._pending_incoming)
            self._pending_incoming.clear()
            closing_connections = tuple(self._closing_connections.values())
            associations = tuple(self._associations)

        first_error: BaseException | None = None
        if outbound_setups_pending:
            first_error = TimeoutError(
                "outbound association setup did not settle before the shutdown deadline"
            )
        if listener is not None:
            try:
                listener.close(timeout=max(0.0, deadline - monotonic()))
            except BaseException as error:
                first_error = error
            else:
                with self._condition:
                    if self._listener is listener:
                        self._listener = None
        for connection in (*incoming, *closing_connections):
            try:
                connection.close(timeout=max(0.0, deadline - monotonic()))
            except BaseException as error:
                first_error = first_error or error
                with self._condition:
                    self._closing_connections[id(connection)] = connection
            else:
                with self._condition:
                    self._closing_connections.pop(id(connection), None)
        for association in associations:
            remaining = deadline - monotonic()
            if remaining <= 0:
                first_error = first_error or TimeoutError(
                    "remoting runtime did not stop before the deadline"
                )
                break
            try:
                association.close(remaining)
            except BaseException as error:
                first_error = first_error or error

        with self._condition:
            self._condition.notify_all()
            closer = self._closer_thread
        if closer is not None and closer is not current_thread():
            closer.join(max(0.0, deadline - monotonic()))
            if closer.is_alive():
                first_error = first_error or TimeoutError(
                    "remoting connection closer did not stop before the deadline"
                )
        if not self._transport_closed and self._transport is not None:
            try:
                self._transport.close(
                    timeout=max(0.0, deadline - monotonic())
                )
            except BaseException as error:
                first_error = first_error or error
            else:
                self._transport_closed = True
        elif self._transport is None:
            self._transport_closed = True

        with self._condition:
            alive = tuple(
                association
                for association in self._associations
                if association.state is not AssociationState.CLOSED
            )
            resources_pending = (
                self._listener is not None
                or self._outbound_setups > 0
                or bool(self._closing_connections)
                or not self._transport_closed
            )
            if first_error is None and not alive and not resources_pending:
                self._state = _RuntimeState.STOPPED
                self._active.clear()
                self._condition.notify_all()
            else:
                self._state = _RuntimeState.STOPPING
        if first_error is not None:
            raise first_error
        if alive:
            raise TimeoutError("remoting association threads did not stop before the deadline")
        if resources_pending:
            raise TimeoutError("remoting transport resources did not stop before the deadline")

    def associate(self, system_name: str, timeout: float | None = None) -> Association:
        association_timeout = _timeout(
            timeout,
            self._config.association_timeout,
            "association timeout",
        )
        endpoint = self._config.peers.get(system_name)
        if endpoint is None:
            raise HandshakeError(f"actor system {system_name!r} is not allowlisted")
        with self._condition:
            if self._state is not _RuntimeState.RUNNING:
                raise RemotingShutdownError("remoting runtime is not running")
            current = self._active.get(system_name)
            if current is not None and current.state is AssociationState.ACTIVE:
                return current
            capacity = len(self._config.peers) + self._config.pending_association_limit
            if (
                len(self._associations)
                + len(self._closing_connections)
                + self._outbound_setups
                >= capacity
            ):
                raise RemotingCapacityError("remoting association capacity is full")
            self._outbound_setups += 1

        deadline = monotonic() + association_timeout
        association_uid = uuid4()
        try:
            transport = self._transport
            assert transport is not None
            connection = transport.connect(
                endpoint,
                self._bootstrap_limits,
                association_uid,
                timeout=max(0.0, deadline - monotonic()),
            )
        except TransportError as error:
            self._release_outbound_setup()
            raise HandshakeError(
                f"could not connect to allowlisted actor system {system_name!r}"
            ) from error
        except BaseException:
            self._release_outbound_setup()
            raise
        try:
            association = Association(
                self,
                connection,
                initiator=True,
                expected_system_name=system_name,
                expected_endpoint=endpoint,
                handshake_timeout=max(0.0, deadline - monotonic()),
            )
        except BaseException:
            self._close_reserved_outbound(connection)
            raise
        if not self._register_and_start(association, outbound_reserved=True):
            raise RemotingShutdownError("remoting runtime stopped during association setup")
        try:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise HandshakeError("association timed out before its handshake started")
            association.wait_active(remaining)
        except HandshakeError:
            if association.snapshot().close_reason is ReasonCode.DUPLICATE_ASSOCIATION:
                retained = self._wait_for_retained_association(system_name, deadline)
                if retained is not None:
                    return retained
            association.request_close(
                ReasonCode.PROTOCOL_VIOLATION,
                "outbound association handshake did not complete",
            )
            raise
        retained = self._wait_for_retained_association(system_name, deadline)
        if retained is not None:
            return retained
        raise HandshakeError("association became inactive before setup completed")

    def resolve(
        self,
        locator: str | ActorPath,
        timeout: float | None = None,
    ) -> RemoteActorRef:
        resolution_timeout = _timeout(
            timeout,
            self._config.association_timeout,
            "resolution timeout",
        )
        path, system_name = self._validate_locator(locator)
        with self._condition:
            if self._state is not _RuntimeState.RUNNING:
                raise RemotingShutdownError("remoting runtime is not running")
            association = self._active.get(system_name)
            if association is None or association.state is not AssociationState.ACTIVE:
                raise NoAssociationError(
                    f"no active association exists for actor system {system_name!r}"
                )
        response = association.resolve(path.remote_path, resolution_timeout)
        peer_uid = association.peer_incarnation_uid
        with self._condition:
            known_uid = self._known_incarnations.get(system_name)
        if (
            peer_uid is None
            or response.system_incarnation_uid != peer_uid
            or known_uid != peer_uid
        ):
            association.request_close(
                ReasonCode.PROTOCOL_VIOLATION,
                "resolution response came from another actor-system incarnation",
            )
            raise ResolutionError("resolution response actor-system incarnation is invalid")
        identity = ActorIdentity(response.system_incarnation_uid, response.actor_uid)
        return RemoteActorRef(self, system_name, identity, path)

    def _tell(self, reference: RemoteActorRef, message: object) -> None:
        try:
            association = self._association_for_reference(reference)
        except StaleIncarnationError:
            self._publish_reference_dead_letter(
                reference,
                DeadLetterReason.STALE_INCARNATION,
            )
            raise
        except (NoAssociationError, RemotingShutdownError):
            self._publish_reference_dead_letter(
                reference,
                DeadLetterReason.NO_ASSOCIATION,
            )
            raise
        try:
            association.send_user_message(reference.identity, message)
        except StaleIncarnationError:
            self._publish_reference_dead_letter(
                reference,
                DeadLetterReason.STALE_INCARNATION,
            )
            raise
        except NoAssociationError as first_error:
            try:
                replacement = self._association_for_reference(reference)
            except StaleIncarnationError:
                self._publish_reference_dead_letter(
                    reference,
                    DeadLetterReason.STALE_INCARNATION,
                )
                raise
            except (NoAssociationError, RemotingShutdownError):
                self._publish_reference_dead_letter(
                    reference,
                    DeadLetterReason.NO_ASSOCIATION,
                )
                raise
            if replacement is association:
                self._publish_reference_dead_letter(
                    reference,
                    DeadLetterReason.NO_ASSOCIATION,
                )
                raise first_error
            try:
                replacement.send_user_message(reference.identity, message)
            except StaleIncarnationError:
                self._publish_reference_dead_letter(
                    reference,
                    DeadLetterReason.STALE_INCARNATION,
                )
                raise
            except NoAssociationError:
                self._publish_reference_dead_letter(
                    reference,
                    DeadLetterReason.NO_ASSOCIATION,
                )
                raise

    def _publish_reference_dead_letter(
        self,
        reference: RemoteActorRef,
        reason: DeadLetterReason,
    ) -> None:
        self._publish_remoting_dead_letter(
            reference.identity,
            reason,
            recipient_path=reference.path.canonical,
        )

    def _association_for_reference(self, reference: RemoteActorRef) -> Association:
        with self._condition:
            state = self._state
            known = self._known_incarnations.get(reference.system_name)
            association = self._active.get(reference.system_name)
            association_state, active_peer = (
                association._routing_state()
                if association is not None
                else (None, None)
            )
        if state is not _RuntimeState.RUNNING:
            raise RemotingShutdownError("remoting runtime is stopping")
        if (
            known is not None
            and known != reference.identity.system_incarnation_uid
        ) or (
            active_peer is not None
            and active_peer != reference.identity.system_incarnation_uid
        ):
            raise StaleIncarnationError(
                "remote actor reference targets a stale actor-system incarnation"
            )
        if association is None or association_state is not AssociationState.ACTIVE:
            raise NoAssociationError(
                f"no active association exists for actor system {reference.system_name!r}"
            )
        return association

    def _validate_locator(self, locator: str | ActorPath) -> tuple[ActorPath, str]:
        if isinstance(locator, ActorPath):
            path = locator
        elif isinstance(locator, str):
            try:
                path = parse_actor_path(locator)
            except (TypeError, ValueError) as error:
                raise ResolutionError("remote actor locator is not canonical") from error
        else:
            raise ResolutionError("remote actor locator must be a string or ActorPath")
        address = path.address
        if (
            address.protocol != "movie"
            or not address.has_global_scope
            or address.host is None
            or address.port is None
            or not path.is_remote_resolvable
        ):
            raise ResolutionError("remote actor locator must be canonical and globally scoped")
        configured = self._config.peers.get(address.system)
        if configured != Endpoint(address.host, address.port):
            raise ResolutionError(
                "remote actor locator endpoint does not exactly match the peer allowlist"
            )
        return path, address.system

    def _on_connection(self, connection: TransportConnection) -> None:
        with self._condition:
            if self._state is _RuntimeState.STARTING:
                if len(self._pending_incoming) < self._config.pending_association_limit:
                    self._pending_incoming.append(connection)
                    return
                running = False
            else:
                running = self._state is _RuntimeState.RUNNING
        if running:
            self._start_incoming(connection)
        else:
            self._close_connection_quickly(connection)

    def _start_incoming(self, connection: TransportConnection) -> None:
        association = Association(
            self,
            connection,
            initiator=False,
            handshake_timeout=self._config.association_timeout,
        )
        self._register_and_start(association)

    def _register_and_start(
        self,
        association: Association,
        *,
        outbound_reserved: bool = False,
    ) -> bool:
        with self._condition:
            if outbound_reserved:
                self._outbound_setups -= 1
                self._condition.notify_all()
            if (
                self._state is not _RuntimeState.RUNNING
                or len(self._associations)
                + len(self._closing_connections)
                + self._outbound_setups
                >= len(self._config.peers) + self._config.pending_association_limit
                or sum(
                    association.state is AssociationState.HANDSHAKING
                    for association in self._associations
                )
                >= self._config.pending_association_limit
            ):
                accepted = False
                if outbound_reserved:
                    self._closing_connections[id(association._connection)] = (
                        association._connection
                    )
                    self._ensure_closer_locked()
            else:
                association._set_registration_order(
                    self._next_association_generation
                )
                self._next_association_generation += 1
                self._associations.add(association)
                try:
                    association.start()
                except BaseException:
                    self._associations.discard(association)
                    if outbound_reserved:
                        self._closing_connections[id(association._connection)] = (
                            association._connection
                        )
                        self._ensure_closer_locked()
                    self._condition.notify_all()
                    raise
                else:
                    accepted = True
                    self._condition.notify_all()
        if not accepted and not outbound_reserved:
            self._close_connection_quickly(association._connection)
        return accepted

    def _send_if_current(
        self,
        association: Association,
        peer_name: str,
        peer_incarnation_uid: UUID,
        record: TransportRecord,
    ) -> None:
        with self._condition:
            if self._active.get(peer_name) is not association:
                raise NoAssociationError("association is no longer active")
            if self._known_incarnations.get(peer_name) != peer_incarnation_uid:
                raise StaleIncarnationError(
                    "remote actor reference targets a stale actor-system incarnation"
                )
            association._send_payload_locked(record)

    def _activate_association(self, candidate: Association) -> bool:
        peer_name = candidate.peer_system_name
        peer_uid = candidate.peer_incarnation_uid
        assert peer_name is not None and peer_uid is not None
        replaced: Association | None = None
        with self._condition:
            if self._state is not _RuntimeState.RUNNING:
                return False
            current = self._active.get(peer_name)
            known_uid = self._known_incarnations.get(peer_name)
            known_generation = self._known_generation.get(peer_name, 0)
            if (
                known_uid is not None
                and known_uid != peer_uid
                and candidate._registration_order < known_generation
            ):
                return False
            if current is not None and current.state not in (
                AssociationState.CLOSING,
                AssociationState.CLOSED,
            ):
                current_uid = current.peer_incarnation_uid
                if current_uid == peer_uid:
                    if current.initiator_rank <= candidate.initiator_rank:
                        return False
                    replaced = current
                else:
                    replaced = current
            reconnect_count = self._reconnect_counts.get(peer_name, 0)
            if (
                (current is None or current.state is AssociationState.CLOSING)
                and known_uid == peer_uid
            ):
                reconnect_count += 1
            elif known_uid != peer_uid:
                reconnect_count = 0
            candidate._set_reconnect_count(reconnect_count)
            self._reconnect_counts[peer_name] = reconnect_count
            self._active[peer_name] = candidate
            self._known_incarnations[peer_name] = peer_uid
            self._known_generation[peer_name] = candidate._registration_order
            self._condition.notify_all()
        if replaced is not None:
            replaced.request_close(
                ReasonCode.DUPLICATE_ASSOCIATION,
                "a canonical replacement association was retained",
            )
        return True

    def _association_activated(self, association: Association) -> None:
        with self._condition:
            if self._active.get(association.peer_system_name) is association:
                self._condition.notify_all()

    def _wait_for_retained_association(
        self, system_name: str, deadline: float
    ) -> Association | None:
        quiet_deadline = min(deadline, monotonic() + 0.05)
        with self._condition:
            while True:
                unsettled = any(
                    association.state is AssociationState.HANDSHAKING
                    and association.target_system_name == system_name
                    for association in self._associations
                )
                retained = self._active.get(system_name)
                if not unsettled:
                    quiet_remaining = quiet_deadline - monotonic()
                    if quiet_remaining <= 0:
                        if retained is not None and retained.state is AssociationState.ACTIVE:
                            return retained
                        return None
                    self._condition.wait(quiet_remaining)
                    continue
                quiet_deadline = min(deadline, monotonic() + 0.05)
                remaining = deadline - monotonic()
                if remaining <= 0:
                    return None
                self._condition.wait(remaining)

    def _association_closed(self, association: Association) -> None:
        snapshot = association.snapshot()
        with self._condition:
            self._associations.discard(association)
            self._history.append(snapshot)
            peer_name = association.peer_system_name
            if peer_name is not None and self._active.get(peer_name) is association:
                self._active.pop(peer_name, None)
            self._condition.notify_all()

    def _resolve_local_path(
        self, actor_path: str
    ) -> tuple[RemoteAdmissionResult, object | None]:
        return self._system.resolve_remote_path(actor_path)

    def _admit_remote_message(self, identity: ActorIdentity, message: object, **metadata):
        return self._system.admit_remote_message(identity, message, **metadata)

    def _publish_remoting_dead_letter(
        self,
        identity: ActorIdentity,
        reason: DeadLetterReason,
        **metadata,
    ) -> None:
        if metadata.get("recipient_path") is None:
            if identity.system_incarnation_uid == self.incarnation_uid:
                actor = self._system.lookup_actor_by_uid(identity.actor_uid)
                path = actor.path.canonical if actor is not None else None
            else:
                path = None
            if path is not None:
                metadata["recipient_path"] = path
        self._system._publish_dead_letter(identity, reason, **metadata)

    def _close_connection_quickly(self, connection: TransportConnection) -> None:
        connection_id = id(connection)
        with self._condition:
            if connection_id not in self._closing_connections:
                capacity = (
                    len(self._config.peers)
                    + self._config.pending_association_limit
                )
                if (
                    len(self._associations)
                    + len(self._closing_connections)
                    + self._outbound_setups
                    >= capacity
                ):
                    raise TimeoutError(
                        "remoting closing-connection capacity is full"
                    )
                self._closing_connections[connection_id] = connection
                self._condition.notify_all()
        try:
            connection.close(timeout=min(0.5, self._config.association_timeout))
        except (TimeoutError, TransportError):
            with self._condition:
                self._ensure_closer_locked()
                self._condition.notify_all()
            return
        except BaseException:
            with self._condition:
                self._closing_connections.pop(connection_id, None)
                self._condition.notify_all()
            raise
        else:
            with self._condition:
                self._closing_connections.pop(connection_id, None)
                self._condition.notify_all()

    def _release_outbound_setup(self) -> None:
        with self._condition:
            self._outbound_setups -= 1
            self._condition.notify_all()

    def _close_reserved_outbound(self, connection: TransportConnection) -> None:
        with self._condition:
            self._outbound_setups -= 1
            self._closing_connections[id(connection)] = connection
            self._ensure_closer_locked()
            self._condition.notify_all()

    def _ensure_closer_locked(self) -> None:
        if self._closer_thread is not None and self._closer_thread.is_alive():
            return
        self._closer_thread = Thread(
            target=self._close_connections,
            name=f"movie-remoting-connection-closer-{self.system_name}",
            daemon=True,
        )
        self._closer_thread.start()

    def _close_connections(self) -> None:
        try:
            while True:
                with self._condition:
                    if self._closing_connections:
                        connection = next(iter(self._closing_connections.values()))
                    elif self._state in (_RuntimeState.STOPPING, _RuntimeState.STOPPED):
                        return
                    else:
                        self._condition.wait(0.1)
                        continue
                try:
                    connection.close(
                        timeout=min(0.5, self._config.association_timeout)
                    )
                except BaseException:
                    sleep(0.05)
                else:
                    with self._condition:
                        self._closing_connections.pop(id(connection), None)
                        self._condition.notify_all()
        finally:
            with self._condition:
                if self._closer_thread is current_thread():
                    self._closer_thread = None
                if self._closing_connections:
                    self._ensure_closer_locked()
                self._condition.notify_all()


__all__ = ["RemotingRuntime"]
