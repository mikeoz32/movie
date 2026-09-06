"""Actor System-owned coordinated cluster membership runtime."""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, replace
from enum import Enum
from queue import Empty, Full, Queue
from threading import Condition, Event, Lock, Thread, current_thread
from time import monotonic
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from movie.actor.behaviour import Behaviors
from movie.actor.dead_letter import RemoteAdmissionResult
from movie.actor.identity import ActorIdentity
from movie.actor.path import Address, RootActorPath
from movie.actor.ref import ActorRef
from movie.cluster._protocol import (
    _Heartbeat,
    _HeartbeatAck,
    _JoinAccepted,
    _JoinConfirm,
    _JoinRequest,
    _Leave,
    _LeaveAck,
    _MembershipUpdate,
    _WireMember,
)
from movie.cluster.config import ClusterConfig, _compatibility_fingerprint
from movie.cluster.errors import ClusterError, ClusterJoinError, ClusterShutdownError
from movie.cluster.model import (
    ClusterMember,
    MemberIdentity,
    MembershipSnapshot,
    MemberStatus,
    Reachability,
)
from movie.cluster.observability import ClusterEvent, ClusterEventKind, ClusterEvents
from movie.remoting.association import AssociationState
from movie.remoting.config import RemotingConfig
from movie.remoting.ref import RemoteActorRef
from movie.remoting.runtime import RemotingRuntime
from movie.remoting.transport import Endpoint

if TYPE_CHECKING:
    from movie.actor.impl.system import ActorSystemImpl

_CONTROL_ACTOR_NAME = "_movie_cluster_control_v1"
_STOP_WORKER = object()
_WAKE_WORKER = object()


@dataclass(frozen=True, slots=True)
class _InboundControl:
    message: object
    source: MemberIdentity
    source_endpoint: Endpoint


class _RuntimeState(Enum):
    NEW = "new"
    STARTING = "starting"
    RUNNING = "running"
    FAILED = "failed"
    STOPPING = "stopping"
    STOPPED = "stopped"


class ClusterRuntime:
    """One volatile cluster membership view for an Actor System Incarnation."""

    def __init__(
        self,
        system: ActorSystemImpl,
        config: ClusterConfig,
        remoting_config: RemotingConfig,
        remoting: RemotingRuntime,
    ) -> None:
        self._is_coordinator = self._validate_configuration(
            system,
            config,
            remoting_config,
        )
        self._system = system
        self._config = config
        self._config_fingerprint = _compatibility_fingerprint(config)
        self._remoting_config = remoting_config
        self._remoting = remoting
        self._endpoint = remoting_config.local
        self._self_identity = MemberIdentity(system.name, system.incarnation_uid)

        self._condition = Condition(Lock())
        self._stop_lock = Lock()
        self._state = _RuntimeState.NEW
        self._startup_settled = False
        self._failure: BaseException | None = None
        self._revision = 0
        self._view_revision = 0
        self._members: dict[MemberIdentity, ClusterMember] = {
            self._self_identity: ClusterMember(
                self._self_identity,
                self._endpoint,
                MemberStatus.JOINING,
                Reachability.REACHABLE,
            )
        }
        self._events = ClusterEvents(
            config.event_capacity,
            config.event_max_subscriptions,
        )
        self._control_queue: Queue[object] = Queue(config.control_queue_capacity)
        self._stop_requested = Event()
        self._worker: Thread | None = None
        self._control_ref: ActorRef | None = None

        self._coordinator_identity: MemberIdentity | None = None
        self._coordinator_ref: RemoteActorRef | None = None
        self._membership_token: UUID | None = None
        self._pending_join_request: UUID | None = None
        self._pending_join_submission: _JoinRequest | _JoinConfirm | None = None
        self._join_deadline = 0.0
        self._join_retry_at = 0.0
        self._join_send_lock = Lock()
        self._join_complete = False
        self._pending_leave_request: UUID | None = None
        self._leave_complete = False
        self._heartbeat_sequence = 0
        self._last_heartbeat_ack = -1
        self._next_reassociation = 0.0
        self._reassociation_failures = 0

        self._member_tokens: dict[MemberIdentity, UUID] = {}
        self._member_refs: dict[MemberIdentity, RemoteActorRef] = {}
        self._last_heartbeat_sequence: dict[MemberIdentity, int] = {}
        self._last_evidence: dict[MemberIdentity, float] = {
            self._self_identity: monotonic()
        }
        self._pending_joins: dict[MemberIdentity, tuple[UUID, float]] = {}
        self._left_order: deque[MemberIdentity] = deque()
        self._retired_identities: set[MemberIdentity] = set()
        self._authoritative_snapshot: tuple[_WireMember, ...] | None = None

    @staticmethod
    def _validate_configuration(
        system: ActorSystemImpl,
        config: ClusterConfig,
        remoting_config: RemotingConfig,
    ) -> bool:
        if not isinstance(config, ClusterConfig):
            raise ValueError("cluster must be a ClusterConfig")
        MemberIdentity(system.name, system.incarnation_uid)
        if remoting_config.local.port == 0:
            raise ValueError("cluster remoting endpoint port must be positive")
        is_coordinator = system.name == config.seed.system_name
        if is_coordinator:
            if remoting_config.local != config.seed.endpoint:
                raise ValueError("membership coordinator endpoint must match its seed contact")
        elif remoting_config.peers.get(config.seed.system_name) != config.seed.endpoint:
            raise ValueError("seed contact must match the remoting peer allowlist")
        return is_coordinator

    @property
    def name(self) -> str:
        return self._config.name

    @property
    def self_identity(self) -> MemberIdentity:
        return self._self_identity

    @property
    def is_coordinator(self) -> bool:
        return self._is_coordinator

    @property
    def is_joined(self) -> bool:
        with self._condition:
            member = self._members[self._self_identity]
            return member.status is MemberStatus.UP

    @property
    def failure(self) -> BaseException | None:
        with self._condition:
            return self._failure

    @property
    def events(self) -> ClusterEvents:
        return self._events

    @property
    def membership(self) -> MembershipSnapshot:
        with self._condition:
            return MembershipSnapshot(
                self._config.name,
                self._view_revision,
                self._self_identity,
                tuple(self._members.values()),
            )

    @property
    def members(self) -> tuple[ClusterMember, ...]:
        return self.membership.members

    def start(self) -> None:
        with self._condition:
            if self._state is not _RuntimeState.NEW:
                raise RuntimeError("cluster runtime can only be started once")
            self._state = _RuntimeState.STARTING
        deadline = monotonic() + self._config.join_timeout
        try:
            control_ref = self._system.spawn(
                Behaviors.receive(lambda context, message: Behaviors.same),
                _CONTROL_ACTOR_NAME,
            )
            with self._condition:
                self._control_ref = control_ref
                if self._state is not _RuntimeState.STARTING:
                    raise ClusterJoinError("cluster startup was interrupted")
            self._system.wait_for_actor_start(
                control_ref,
                max(0.0, deadline - monotonic()),
            )
            if self._is_coordinator:
                self._form_cluster()
            worker = Thread(
                target=self._run,
                name=f"movie-cluster-{self._system.name}",
                daemon=True,
            )
            worker.start()
            with self._condition:
                self._worker = worker
                if self._state is not _RuntimeState.STARTING:
                    raise ClusterJoinError("cluster startup was interrupted")
            if not self._is_coordinator:
                self._join(deadline)
        except BaseException as error:
            with self._condition:
                self._failure = error
                if self._state is _RuntimeState.STARTING:
                    self._state = _RuntimeState.FAILED
                self._startup_settled = True
                self._condition.notify_all()
            raise
        with self._condition:
            if self._state is not _RuntimeState.STARTING:
                self._startup_settled = True
                self._condition.notify_all()
                raise ClusterJoinError("cluster startup was interrupted")
            self._state = _RuntimeState.RUNNING
            self._startup_settled = True
            self._condition.notify_all()

    def leave(self, timeout: float | None = None) -> None:
        if self._system._is_in_actor_callback():
            raise RuntimeError("ClusterRuntime.leave() cannot be called from an actor callback")
        if timeout is None:
            timeout = self._config.join_timeout
        if (
            not isinstance(timeout, (int, float))
            or isinstance(timeout, bool)
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ValueError("cluster leave timeout must be positive")
        self._stop_before(monotonic() + float(timeout))

    def down(self, identity: MemberIdentity) -> None:
        if type(identity) is not MemberIdentity:
            raise ValueError("downing requires a MemberIdentity")
        if not self._is_coordinator:
            raise ClusterError("only the Membership Coordinator can down a member")
        if identity == self._self_identity:
            raise ClusterError("the Membership Coordinator cannot down itself")
        with self._condition:
            if self._state is not _RuntimeState.RUNNING:
                raise ClusterError("cluster runtime is not running")
            member = self._members.get(identity)
            if member is None or member.status is MemberStatus.LEFT:
                return
            if member.status is not MemberStatus.UP:
                raise ClusterError("only an up member can be downed")
            if member.reachability is not Reachability.UNREACHABLE:
                raise ClusterError("only an unreachable member can be downed")
            self._revision += 1
            self._replace_member_locked(replace(member, status=MemberStatus.LEFT))
            self._revoke_member_locked(identity)
        self._broadcast_membership(exclude=identity)

    def _stop_before(self, deadline: float) -> None:
        remaining = deadline - monotonic()
        if remaining <= 0 or not self._stop_lock.acquire(timeout=remaining):
            raise ClusterShutdownError("cluster runtime did not stop before the deadline")
        try:
            self._stop_before_locked(deadline)
        finally:
            self._stop_lock.release()

    def _stop_before_locked(self, deadline: float) -> None:
        with self._condition:
            if self._state is _RuntimeState.STOPPED:
                return
            if self._state is _RuntimeState.NEW:
                self._startup_settled = True
            self._state = _RuntimeState.STOPPING
            self._condition.notify_all()
            while not self._startup_settled:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise ClusterShutdownError(
                        "cluster startup did not settle before the shutdown deadline"
                    )
                self._condition.wait(remaining)
            member = self._members[self._self_identity]
            can_leave = member.status is MemberStatus.UP or (
                not self._is_coordinator
                and member.status is MemberStatus.JOINING
                and self._membership_token is not None
                and self._coordinator_ref is not None
            )
            self._condition.notify_all()

        if can_leave:
            if self._is_coordinator:
                self._coordinator_leave()
            else:
                self._member_leave(deadline)

        self._stop_requested.set()
        try:
            self._control_queue.put_nowait(_STOP_WORKER)
        except Full:
            pass
        worker = self._worker
        if worker is not None and worker is not current_thread():
            remaining = deadline - monotonic()
            if remaining > 0:
                worker.join(remaining)
            if worker.is_alive():
                raise ClusterShutdownError("cluster worker did not stop before the deadline")

        control_ref = self._control_ref
        if control_ref is not None:
            self._system.terminate(control_ref)
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise ClusterShutdownError("cluster control actor did not stop before the deadline")
            self._system.actor_stop_future(control_ref).result(timeout=remaining)
            self._control_ref = None

        with self._condition:
            member = self._members[self._self_identity]
            if member.status is not MemberStatus.LEFT:
                self._replace_member_locked(replace(member, status=MemberStatus.LEFT))
            self._state = _RuntimeState.STOPPED
            self._condition.notify_all()
        self._system._clear_cluster_admission(self)
        self._events.close()

    def _admit_remote_control(
        self,
        identity: ActorIdentity,
        message: object,
        association_uid: object,
        recipient_path: str | None,
    ) -> RemoteAdmissionResult | None:
        with self._condition:
            control_ref = self._control_ref
            targets_control_actor = (
                identity == control_ref.identity
                if control_ref is not None
                else recipient_path == f"/{_CONTROL_ACTOR_NAME}"
                and self._state is _RuntimeState.STARTING
            )
        if not targets_control_actor:
            return None
        if not isinstance(association_uid, UUID):
            return RemoteAdmissionResult.ACTOR_NOT_FOUND
        association = next(
            (
                snapshot
                for snapshot in self._remoting.associations
                if snapshot.association_uid == association_uid
                and snapshot.state is AssociationState.ACTIVE
                and snapshot.peer_system_name is not None
                and snapshot.peer_incarnation_uid is not None
                and snapshot.peer_endpoint is not None
            ),
            None,
        )
        if association is None:
            return RemoteAdmissionResult.ACTOR_NOT_FOUND
        with self._condition:
            accepts_stopping_reply = self._state is _RuntimeState.STOPPING and type(
                message
            ) in (_LeaveAck, _MembershipUpdate)
            if self._state not in (
                _RuntimeState.STARTING,
                _RuntimeState.RUNNING,
            ) and not accepts_stopping_reply:
                return RemoteAdmissionResult.ACTOR_STOPPING
        try:
            self._control_queue.put_nowait(
                _InboundControl(
                    message,
                    MemberIdentity(
                        association.peer_system_name,
                        association.peer_incarnation_uid,
                    ),
                    association.peer_endpoint,
                )
            )
        except Full:
            return RemoteAdmissionResult.MAILBOX_FULL
        return RemoteAdmissionResult.ACCEPTED

    def _run(self) -> None:
        next_tick = monotonic() + self._config.heartbeat_interval
        try:
            while not self._stop_requested.is_set():
                now = monotonic()
                with self._condition:
                    provisional_deadline = min(
                        (
                            deadline
                            for _, deadline in self._pending_joins.values()
                        ),
                        default=next_tick,
                    )
                    join_retry = (
                        self._join_retry_at
                        if self._pending_join_submission is not None
                        else next_tick
                    )
                timeout = max(
                    0.0,
                    min(next_tick, provisional_deadline, join_retry) - now,
                )
                try:
                    message = self._control_queue.get(timeout=timeout)
                except Empty:
                    message = None
                if message is _STOP_WORKER:
                    return
                if message is not None and message is not _WAKE_WORKER:
                    self._handle_control(message)
                now = monotonic()
                if self._is_coordinator:
                    self._expire_provisional_joins(now)
                elif now >= self._join_retry_at:
                    self._submit_pending_join_control()
                if now >= next_tick:
                    self._tick(now)
                    next_tick = now + self._config.heartbeat_interval
        except BaseException as error:
            with self._condition:
                if self._state not in (_RuntimeState.STOPPING, _RuntimeState.STOPPED):
                    self._failure = error
                    self._state = _RuntimeState.FAILED
                self._condition.notify_all()

    def _handle_control(self, inbound: object) -> None:
        if type(inbound) is not _InboundControl:
            return
        message = inbound.message
        source = inbound.source
        source_endpoint = inbound.source_endpoint
        if type(message) is _JoinRequest:
            self._handle_join_request(message, source, source_endpoint)
        elif type(message) is _JoinAccepted:
            self._handle_join_accepted(message, source)
        elif type(message) is _JoinConfirm:
            self._handle_join_confirm(message, source)
        elif type(message) is _Heartbeat:
            self._handle_heartbeat(message, source)
        elif type(message) is _HeartbeatAck:
            self._handle_heartbeat_ack(message, source)
        elif type(message) is _MembershipUpdate:
            self._handle_membership_update(message, source)
        elif type(message) is _Leave:
            self._handle_leave(message, source)
        elif type(message) is _LeaveAck:
            self._handle_leave_ack(message, source)

    def _form_cluster(self) -> None:
        with self._condition:
            self._revision += 1
            member = replace(
                self._members[self._self_identity],
                status=MemberStatus.UP,
            )
            self._replace_member_locked(member)
            self._join_complete = True
            self._condition.notify_all()

    def _join(self, deadline: float) -> None:
        try:
            association = self._associate_coordinator_until(deadline)
            peer_uid = association.peer_incarnation_uid
            if peer_uid is None:
                raise ClusterJoinError("seed association has no peer incarnation")
            coordinator_identity = MemberIdentity(
                self._config.seed.system_name,
                peer_uid,
            )
            coordinator_ref = self._resolve_coordinator_until(deadline)
            request_id = uuid4()
            request = _JoinRequest(
                self._config.name,
                _compatibility_fingerprint(self._config),
                self._system.name,
                self._system.incarnation_uid,
                self._endpoint.host,
                self._endpoint.port,
                self._control_ref.id,
                peer_uid,
                request_id,
            )
            with self._condition:
                self._coordinator_identity = coordinator_identity
                self._coordinator_ref = coordinator_ref
                self._pending_join_request = request_id
                self._pending_join_submission = request
                self._join_deadline = deadline
            self._submit_pending_join_control()
        except ClusterJoinError:
            raise
        except BaseException as error:
            raise ClusterJoinError(
                f"could not contact membership coordinator {self._config.seed.system_name!r}"
            ) from error

        with self._condition:
            while (
                not self._join_complete
                and self._failure is None
                and self._state is _RuntimeState.STARTING
            ):
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise ClusterJoinError("cluster join timed out")
                self._condition.wait(remaining)
            if self._failure is not None:
                raise ClusterJoinError("cluster worker failed during join") from self._failure
            if not self._join_complete:
                raise ClusterJoinError("cluster startup was interrupted")

    def _associate_coordinator_until(self, deadline: float):
        last_error: BaseException | None = None
        while True:
            with self._condition:
                if self._state is not _RuntimeState.STARTING:
                    raise ClusterJoinError("cluster startup was interrupted")
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise ClusterJoinError("cluster join timed out") from last_error
            try:
                return self._remoting.associate(
                    self._config.seed.system_name,
                    timeout=min(self._config.reassociation_timeout, remaining),
                )
            except BaseException as error:
                last_error = error
                self._stop_requested.wait(min(0.01, remaining))

    def _resolve_coordinator_until(self, deadline: float) -> RemoteActorRef:
        path = self._control_path(
            self._config.seed.system_name,
            self._config.seed.endpoint,
        )
        last_error: BaseException | None = None
        while True:
            with self._condition:
                if self._state is not _RuntimeState.STARTING:
                    raise ClusterJoinError("cluster startup was interrupted")
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise ClusterJoinError("cluster join timed out") from last_error
            try:
                return self._remoting.resolve(
                    path,
                    timeout=min(self._config.reassociation_timeout, remaining),
                )
            except BaseException as error:
                last_error = error
                self._stop_requested.wait(min(0.01, remaining))
                self._associate_coordinator_until(deadline)

    def _submit_pending_join_control(self) -> None:
        if not self._join_send_lock.acquire(blocking=False):
            return
        try:
            with self._condition:
                message = self._pending_join_submission
                reference = self._coordinator_ref
                deadline = self._join_deadline
                if (
                    message is None
                    or reference is None
                    or self._state is not _RuntimeState.STARTING
                    or monotonic() >= deadline
                ):
                    return
            for attempt in range(2):
                try:
                    reference.tell(message)
                except BaseException:
                    if attempt == 0 and self._attempt_reassociation(monotonic()):
                        continue
                    with self._condition:
                        if self._pending_join_submission is message:
                            self._join_retry_at = min(
                                deadline,
                                max(monotonic() + 0.01, self._next_reassociation),
                            )
                    try:
                        self._control_queue.put_nowait(_WAKE_WORKER)
                    except Full:
                        pass
                    return
                with self._condition:
                    if self._pending_join_submission is message:
                        self._pending_join_submission = None
                        self._join_retry_at = 0.0
                return
        finally:
            self._join_send_lock.release()

    def _handle_join_request(
        self,
        message: _JoinRequest,
        source: MemberIdentity,
        source_endpoint: Endpoint,
    ) -> None:
        if (
            not self._is_coordinator
            or not self._targets_self(message.cluster_name, message.target_incarnation_uid)
            or message.config_fingerprint != self._config_fingerprint
        ):
            return
        endpoint = Endpoint(message.source_host, message.source_port)
        identity = MemberIdentity(message.source_system_name, message.source_incarnation_uid)
        if (
            source != identity
            or source_endpoint != endpoint
            or self._remoting_config.peers.get(message.source_system_name) != endpoint
        ):
            return

        with self._condition:
            if self._state not in (_RuntimeState.STARTING, _RuntimeState.RUNNING):
                return
            same_name = any(
                member.identity.system_name == identity.system_name
                and member.identity != identity
                and member.status is not MemberStatus.LEFT
                for member in self._members.values()
            )
            existing = self._members.get(identity)
            pending = self._pending_joins.get(identity)
            if (
                same_name
                or identity in self._retired_identities
                or (
                    existing is not None
                    and existing.status is not MemberStatus.JOINING
                )
                or (pending is not None and pending[0] != message.request_id)
            ):
                return
            if existing is None and not self._reserve_member_capacity_locked():
                return
            reference = RemoteActorRef(
                self._remoting,
                identity.system_name,
                ActorIdentity(
                    identity.incarnation_uid,
                    message.source_control_actor_uid,
                ),
                self._control_path(identity.system_name, endpoint),
            )
            token = self._member_tokens.get(identity)
            if token is None:
                token = uuid4()
                self._member_tokens[identity] = token
            self._member_refs[identity] = reference
            self._last_evidence[identity] = monotonic()
            self._last_heartbeat_sequence.setdefault(identity, -1)
            if existing is None:
                self._revision += 1
                member = ClusterMember(
                    identity,
                    endpoint,
                    MemberStatus.JOINING,
                    Reachability.REACHABLE,
                )
                self._replace_member_locked(member)
            if pending is None:
                self._pending_joins[identity] = (
                    message.request_id,
                    monotonic() + self._config.join_timeout,
                )
            revision, members = self._wire_snapshot_locked()
        try:
            reference.tell(
                _JoinAccepted(
                    self._config.name,
                    self._system.name,
                    self._system.incarnation_uid,
                    identity.incarnation_uid,
                    message.request_id,
                    token,
                    revision,
                    members,
                )
            )
        except BaseException:
            self._expire_provisional_join(identity, message.request_id)
            return

    def _handle_join_accepted(
        self,
        message: _JoinAccepted,
        source: MemberIdentity,
    ) -> None:
        with self._condition:
            if (
                self._is_coordinator
                or not self._valid_coordinator_message_locked(
                    message.cluster_name,
                    message.source_system_name,
                    message.source_incarnation_uid,
                    message.target_incarnation_uid,
                    source,
                )
                or message.request_id != self._pending_join_request
            ):
                return
            if not self._apply_wire_snapshot_locked(message.revision, message.members):
                return
            self._membership_token = message.membership_token
            self._last_evidence[self._coordinator_identity] = monotonic()
            coordinator = self._coordinator_identity
            if coordinator is not None:
                self._pending_join_submission = _JoinConfirm(
                    self._config.name,
                    self._system.name,
                    self._system.incarnation_uid,
                    coordinator.incarnation_uid,
                    message.request_id,
                    message.membership_token,
                )
        if coordinator is None:
            return
        self._submit_pending_join_control()

    def _attempt_reassociation(self, now: float) -> bool:
        with self._condition:
            identity = self._coordinator_identity
            if (
                identity is None
                or self._state not in (_RuntimeState.STARTING, _RuntimeState.RUNNING)
                or now < self._next_reassociation
            ):
                return False
            failures = self._reassociation_failures
            delay = min(
                self._config.unreachable_timeout,
                max(0.05, self._config.heartbeat_interval) * (2 ** min(failures, 6)),
            )
            self._next_reassociation = now + delay
        try:
            association = self._remoting.associate(
                self._config.seed.system_name,
                timeout=self._config.reassociation_timeout,
            )
        except BaseException:
            with self._condition:
                self._reassociation_failures += 1
            return False
        if association.peer_incarnation_uid != identity.incarnation_uid:
            with self._condition:
                self._reassociation_failures += 1
            return False
        with self._condition:
            self._reassociation_failures = 0
            self._next_reassociation = 0.0
        return True

    def _handle_join_confirm(
        self,
        message: _JoinConfirm,
        source: MemberIdentity,
    ) -> None:
        if (
            not self._is_coordinator
            or not self._targets_self(message.cluster_name, message.target_incarnation_uid)
        ):
            return
        identity = MemberIdentity(message.source_system_name, message.source_incarnation_uid)
        if identity != source:
            return
        with self._condition:
            if self._state not in (_RuntimeState.STARTING, _RuntimeState.RUNNING):
                return
            member = self._members.get(identity)
            pending = self._pending_joins.get(identity)
            if (
                member is None
                or member.status is not MemberStatus.JOINING
                or pending is None
                or pending[0] != message.request_id
                or pending[1] < monotonic()
                or self._member_tokens.get(identity) != message.membership_token
            ):
                return
            self._pending_joins.pop(identity, None)
            self._last_evidence[identity] = monotonic()
            self._revision += 1
            self._replace_member_locked(replace(member, status=MemberStatus.UP))
        self._broadcast_membership()

    def _tick(self, now: float) -> None:
        with self._condition:
            if self._state not in (_RuntimeState.STARTING, _RuntimeState.RUNNING):
                return
            status = self._members[self._self_identity].status
        if self._is_coordinator:
            if status is MemberStatus.UP:
                self._detect_unreachable_members(now)
        else:
            self._submit_pending_join_control()
            if status in (MemberStatus.JOINING, MemberStatus.UP):
                self._send_heartbeat()
            if status is MemberStatus.UP:
                self._detect_unreachable_coordinator(now)

    def _send_heartbeat(self) -> None:
        with self._condition:
            reference = self._coordinator_ref
            identity = self._coordinator_identity
            token = self._membership_token
            if reference is None or identity is None or token is None:
                return
            sequence = self._heartbeat_sequence
            self._heartbeat_sequence += 1
        try:
            reference.tell(
                _Heartbeat(
                    self._config.name,
                    self._system.name,
                    self._system.incarnation_uid,
                    identity.incarnation_uid,
                    token,
                    sequence,
                )
            )
        except BaseException:
            self._attempt_reassociation(monotonic())

    def _handle_heartbeat(
        self,
        message: _Heartbeat,
        source: MemberIdentity,
    ) -> None:
        if (
            not self._is_coordinator
            or not self._targets_self(message.cluster_name, message.target_incarnation_uid)
        ):
            return
        identity = MemberIdentity(message.source_system_name, message.source_incarnation_uid)
        if identity != source:
            return
        with self._condition:
            member = self._members.get(identity)
            reference = self._member_refs.get(identity)
            if (
                member is None
                or member.status is not MemberStatus.UP
                or self._member_tokens.get(identity) != message.membership_token
                or reference is None
            ):
                return
            previous_sequence = self._last_heartbeat_sequence.get(identity, -1)
            if message.sequence <= previous_sequence:
                return
            self._last_heartbeat_sequence[identity] = message.sequence
            self._last_evidence[identity] = monotonic()
            became_reachable = member.reachability is Reachability.UNREACHABLE
            if became_reachable:
                self._revision += 1
                self._replace_member_locked(
                    replace(member, reachability=Reachability.REACHABLE)
                )
            revision, members = self._wire_snapshot_locked()
        try:
            reference.tell(
                _HeartbeatAck(
                    self._config.name,
                    self._system.name,
                    self._system.incarnation_uid,
                    identity.incarnation_uid,
                    message.membership_token,
                    message.sequence,
                    revision,
                    members,
                )
            )
        except BaseException:
            return
        if became_reachable:
            self._broadcast_membership(exclude=identity)

    def _handle_heartbeat_ack(
        self,
        message: _HeartbeatAck,
        source: MemberIdentity,
    ) -> None:
        with self._condition:
            if (
                self._is_coordinator
                or not self._valid_coordinator_message_locked(
                    message.cluster_name,
                    message.source_system_name,
                    message.source_incarnation_uid,
                    message.target_incarnation_uid,
                    source,
                    message.membership_token,
                )
                or message.sequence <= self._last_heartbeat_ack
                or message.sequence >= self._heartbeat_sequence
            ):
                return
            if not self._apply_wire_snapshot_locked(message.revision, message.members):
                return
            self._last_heartbeat_ack = message.sequence
            self._reassociation_failures = 0
            self._next_reassociation = 0.0
            self._last_evidence[self._coordinator_identity] = monotonic()
            self._mark_coordinator_reachable_locked()
            self._complete_join_if_up_locked()

    def _detect_unreachable_members(self, now: float) -> None:
        changed = False
        with self._condition:
            for identity, member in tuple(self._members.items()):
                if (
                    identity == self._self_identity
                    or member.status is not MemberStatus.UP
                    or member.reachability is Reachability.UNREACHABLE
                ):
                    continue
                if now - self._last_evidence.get(identity, now) < self._config.unreachable_timeout:
                    continue
                self._revision += 1
                self._replace_member_locked(
                    replace(member, reachability=Reachability.UNREACHABLE)
                )
                changed = True
        if changed:
            self._broadcast_membership()

    def _detect_unreachable_coordinator(self, now: float) -> None:
        with self._condition:
            identity = self._coordinator_identity
            if identity is None:
                return
            member = self._members.get(identity)
            if (
                member is None
                or member.status is not MemberStatus.UP
                or member.reachability is Reachability.UNREACHABLE
                or now - self._last_evidence.get(identity, now)
                < self._config.unreachable_timeout
            ):
                return
            self._replace_member_locked(
                replace(member, reachability=Reachability.UNREACHABLE)
            )

    def _handle_membership_update(
        self,
        message: _MembershipUpdate,
        source: MemberIdentity,
    ) -> None:
        with self._condition:
            if not self._valid_coordinator_message_locked(
                message.cluster_name,
                message.source_system_name,
                message.source_incarnation_uid,
                message.target_incarnation_uid,
                source,
                message.membership_token,
            ):
                return
            if self._apply_wire_snapshot_locked(message.revision, message.members):
                self._last_evidence[self._coordinator_identity] = monotonic()
                self._mark_coordinator_reachable_locked()
                self._complete_join_if_up_locked()

    def _member_leave(self, deadline: float) -> None:
        with self._condition:
            member = self._members[self._self_identity]
            self._replace_member_locked(replace(member, status=MemberStatus.LEAVING))
            reference = self._coordinator_ref
            identity = self._coordinator_identity
            token = self._membership_token
            request_id = uuid4()
            self._pending_leave_request = request_id
            self._leave_complete = False
        if reference is not None and identity is not None and token is not None:
            try:
                reference.tell(
                    _Leave(
                        self._config.name,
                        self._system.name,
                        self._system.incarnation_uid,
                        identity.incarnation_uid,
                        token,
                        request_id,
                    )
                )
            except BaseException:
                pass
            else:
                leave_deadline = monotonic() + max(
                    0.0,
                    (deadline - monotonic()) / 2,
                )
                with self._condition:
                    while not self._leave_complete:
                        remaining = leave_deadline - monotonic()
                        if remaining <= 0:
                            break
                        self._condition.wait(remaining)
        with self._condition:
            member = self._members[self._self_identity]
            if member.status is not MemberStatus.LEFT:
                self._replace_member_locked(replace(member, status=MemberStatus.LEFT))

    def _handle_leave(
        self,
        message: _Leave,
        source: MemberIdentity,
    ) -> None:
        if (
            not self._is_coordinator
            or not self._targets_self(message.cluster_name, message.target_incarnation_uid)
        ):
            return
        identity = MemberIdentity(message.source_system_name, message.source_incarnation_uid)
        if identity != source:
            return
        with self._condition:
            member = self._members.get(identity)
            reference = self._member_refs.get(identity)
            if (
                member is None
                or self._member_tokens.get(identity) != message.membership_token
                or reference is None
            ):
                return
            if member.status is MemberStatus.UP:
                self._revision += 1
                self._replace_member_locked(replace(member, status=MemberStatus.LEAVING))
                leaving = True
            else:
                leaving = False
        if leaving:
            self._broadcast_membership()
        with self._condition:
            member = self._members[identity]
            if member.status is not MemberStatus.LEFT:
                self._revision += 1
                self._replace_member_locked(replace(member, status=MemberStatus.LEFT))
            revision, members = self._wire_snapshot_locked()
            self._revoke_member_locked(identity)
        try:
            reference.tell(
                _LeaveAck(
                    self._config.name,
                    self._system.name,
                    self._system.incarnation_uid,
                    identity.incarnation_uid,
                    message.request_id,
                    revision,
                    members,
                )
            )
        except BaseException:
            pass
        self._broadcast_membership(exclude=identity)

    def _handle_leave_ack(
        self,
        message: _LeaveAck,
        source: MemberIdentity,
    ) -> None:
        with self._condition:
            if (
                self._is_coordinator
                or not self._valid_coordinator_message_locked(
                    message.cluster_name,
                    message.source_system_name,
                    message.source_incarnation_uid,
                    message.target_incarnation_uid,
                    source,
                )
                or message.request_id != self._pending_leave_request
            ):
                return
            self._apply_wire_snapshot_locked(message.revision, message.members)
            self._leave_complete = True
            self._pending_leave_request = None
            self._condition.notify_all()

    def _coordinator_leave(self) -> None:
        with self._condition:
            member = self._members[self._self_identity]
            self._revision += 1
            self._replace_member_locked(replace(member, status=MemberStatus.LEAVING))
        self._broadcast_membership()
        with self._condition:
            member = self._members[self._self_identity]
            self._revision += 1
            self._replace_member_locked(replace(member, status=MemberStatus.LEFT))
        self._broadcast_membership()

    def _broadcast_membership(self, exclude: MemberIdentity | None = None) -> None:
        with self._condition:
            revision, members = self._wire_snapshot_locked()
            targets = tuple(
                (identity, reference, self._member_tokens.get(identity))
                for identity, reference in self._member_refs.items()
                if identity != exclude
            )
        for identity, reference, token in targets:
            if token is None:
                continue
            try:
                reference.tell(
                    _MembershipUpdate(
                        self._config.name,
                        self._system.name,
                        self._system.incarnation_uid,
                        identity.incarnation_uid,
                        token,
                        revision,
                        members,
                    )
                )
            except BaseException:
                pass

    def _apply_wire_snapshot_locked(
        self,
        revision: int,
        members: tuple[_WireMember, ...],
    ) -> bool:
        if revision < self._revision or len(members) > self._config.member_limit:
            return False
        if revision == self._revision and self._authoritative_snapshot is not None:
            return members == self._authoritative_snapshot
        converted: dict[MemberIdentity, ClusterMember] = {}
        for wire_member in members:
            identity = MemberIdentity(
                wire_member.system_name,
                wire_member.incarnation_uid,
            )
            endpoint = Endpoint(wire_member.host, wire_member.port)
            if identity == self._self_identity:
                if endpoint != self._endpoint:
                    return False
            elif (
                identity.system_name == self._config.seed.system_name
                and endpoint != self._config.seed.endpoint
            ):
                return False
            converted[identity] = ClusterMember(
                identity,
                endpoint,
                MemberStatus(wire_member.status),
                Reachability(wire_member.reachability),
            )
        self_member = converted.get(self._self_identity)
        coordinator = self._coordinator_identity
        if self_member is None or coordinator is None or coordinator not in converted:
            return False
        newly_retired = {
            identity
            for identity, member in self._members.items()
            if member.status is MemberStatus.LEFT and identity not in converted
        }
        if len(self._retired_identities | newly_retired) > self._config.retired_identity_limit:
            return False
        if any(
            member.status is not MemberStatus.LEFT
            and (
                identity in self._retired_identities
                or self._members.get(identity) is not None
                and self._members[identity].status is MemberStatus.LEFT
            )
            for identity, member in converted.items()
        ):
            return False
        current_self = self._members[self._self_identity]
        if current_self.status in (MemberStatus.LEAVING, MemberStatus.LEFT):
            converted[self._self_identity] = replace(
                self_member,
                status=current_self.status,
            )
        previous = self._members
        if previous != converted:
            self._view_revision += 1
        self._members = converted
        self._revision = revision
        self._retired_identities.update(newly_retired)
        self._authoritative_snapshot = members
        self._publish_snapshot_changes_locked(previous, converted)
        return True

    def _complete_join_if_up_locked(self) -> None:
        if (
            self._pending_join_request is not None
            and self._members[self._self_identity].status is MemberStatus.UP
        ):
            self._pending_join_request = None
            self._join_complete = True
            self._condition.notify_all()

    def _reserve_member_capacity_locked(self) -> bool:
        while len(self._members) >= self._config.member_limit and self._left_order:
            identity = self._left_order.popleft()
            member = self._members.get(identity)
            if member is not None and member.status is MemberStatus.LEFT:
                if (
                    identity not in self._retired_identities
                    and len(self._retired_identities)
                    >= self._config.retired_identity_limit
                ):
                    return False
                self._retired_identities.add(identity)
                self._members.pop(identity, None)
                self._view_revision += 1
                self._revoke_member_locked(identity)
        return len(self._members) < self._config.member_limit

    def _expire_provisional_joins(self, now: float) -> None:
        with self._condition:
            expired = tuple(
                (identity, request_id)
                for identity, (request_id, deadline) in self._pending_joins.items()
                if deadline <= now
            )
        for identity, request_id in expired:
            self._expire_provisional_join(identity, request_id)

    def _expire_provisional_join(
        self,
        identity: MemberIdentity,
        request_id: UUID,
    ) -> None:
        with self._condition:
            pending = self._pending_joins.get(identity)
            member = self._members.get(identity)
            if (
                pending is None
                or pending[0] != request_id
                or member is None
                or member.status is not MemberStatus.JOINING
            ):
                return
            self._pending_joins.pop(identity, None)
            self._members.pop(identity, None)
            self._view_revision += 1
            self._revoke_member_locked(identity)
            self._revision += 1

    def _revoke_member_locked(self, identity: MemberIdentity) -> None:
        self._pending_joins.pop(identity, None)
        self._member_tokens.pop(identity, None)
        self._member_refs.pop(identity, None)
        self._last_heartbeat_sequence.pop(identity, None)
        self._last_evidence.pop(identity, None)
        member = self._members.get(identity)
        if member is not None and member.status is MemberStatus.LEFT:
            self._left_order.append(identity)

    def _publish_snapshot_changes_locked(
        self,
        previous: dict[MemberIdentity, ClusterMember],
        current: dict[MemberIdentity, ClusterMember],
    ) -> None:
        for identity in sorted(
            current,
            key=lambda value: (value.system_name, value.incarnation_uid.bytes),
        ):
            self._publish_member_changes_locked(previous.get(identity), current[identity])

    def _replace_member_locked(self, member: ClusterMember) -> None:
        previous = self._members.get(member.identity)
        if previous == member:
            return
        self._view_revision += 1
        self._members[member.identity] = member
        self._publish_member_changes_locked(previous, member)

    def _publish_member_changes_locked(
        self,
        previous: ClusterMember | None,
        member: ClusterMember,
    ) -> None:
        if previous is None:
            if member.status is MemberStatus.UP:
                self._publish_locked(ClusterEventKind.MEMBER_UP, member)
            return
        if previous.status is not member.status:
            kind = {
                MemberStatus.UP: ClusterEventKind.MEMBER_UP,
                MemberStatus.LEAVING: ClusterEventKind.MEMBER_LEAVING,
                MemberStatus.LEFT: ClusterEventKind.MEMBER_LEFT,
            }.get(member.status)
            if kind is not None:
                self._publish_locked(kind, member)
        if previous.reachability is not member.reachability:
            kind = (
                ClusterEventKind.MEMBER_REACHABLE
                if member.reachability is Reachability.REACHABLE
                else ClusterEventKind.MEMBER_UNREACHABLE
            )
            self._publish_locked(kind, member)

    def _publish_locked(self, kind: ClusterEventKind, member: ClusterMember) -> None:
        self._events._publish(
            ClusterEvent(
                kind=kind,
                member=member,
                revision=self._view_revision,
            )
        )

    def _mark_coordinator_reachable_locked(self) -> None:
        identity = self._coordinator_identity
        if identity is None:
            return
        member = self._members.get(identity)
        if member is not None and member.reachability is Reachability.UNREACHABLE:
            self._replace_member_locked(
                replace(member, reachability=Reachability.REACHABLE)
            )

    def _valid_coordinator_message_locked(
        self,
        cluster_name: str,
        source_system_name: str,
        source_incarnation_uid: UUID,
        target_incarnation_uid: UUID,
        source: MemberIdentity,
        membership_token: UUID | None = None,
    ) -> bool:
        identity = self._coordinator_identity
        return (
            self._targets_self(cluster_name, target_incarnation_uid)
            and identity is not None
            and source == identity
            and source_system_name == identity.system_name
            and source_incarnation_uid == identity.incarnation_uid
            and (membership_token is None or membership_token == self._membership_token)
        )

    def _targets_self(self, cluster_name: str, target_incarnation_uid: UUID) -> bool:
        return (
            cluster_name == self._config.name
            and target_incarnation_uid == self._system.incarnation_uid
        )

    def _wire_snapshot_locked(self) -> tuple[int, tuple[_WireMember, ...]]:
        members = tuple(
            _WireMember(
                member.identity.system_name,
                member.identity.incarnation_uid,
                member.endpoint.host,
                member.endpoint.port,
                member.status.value,
                member.reachability.value,
            )
            for member in sorted(
                self._members.values(),
                key=lambda value: (
                    value.identity.system_name,
                    value.identity.incarnation_uid.bytes,
                ),
            )
        )
        return self._revision, members

    @staticmethod
    def _control_path(system_name: str, endpoint: Endpoint):
        return RootActorPath(
            Address("movie", system_name, endpoint.host, endpoint.port)
        ).child(_CONTROL_ACTOR_NAME)


__all__ = ["ClusterRuntime"]
