# Movie Actor Runtime

Movie provides volatile actors that exchange messages within an actor system or across explicitly associated actor systems. Optional cluster membership remains separate from remoting associations and does not imply durable delivery.

## Language

**Actor System**:
A named runtime boundary that owns actors and their lifecycle.
_Avoid_: Node, cluster member

**Actor System Incarnation**:
One execution lifetime of an actor system, distinguished from every previous or future execution with the same name.
_Avoid_: Node session, system instance

**Actor Identity**:
The immutable combination of an actor system incarnation and an actor UID. A path alone is not an identity.
_Avoid_: Actor path, actor name

**Actor Path**:
A human-readable hierarchical name used to resolve an actor within one actor system incarnation.
_Avoid_: Actor identity, network address

**Endpoint**:
A configured network location at which an actor system accepts or initiates remoting associations.
_Avoid_: Node, actor address

**Association**:
A temporary communication relationship between two specific actor system incarnations. A reconnect creates a new association.
_Avoid_: Session, cluster membership

**Transport Backend**:
A bounded connection implementation that carries association control records and logical delivery lanes. TCP is the default backend; the association model does not depend on TCP sockets.
_Avoid_: Association, actor address

**Remote Actor Reference**:
A capability that targets one actor identity through a known remote actor system.
_Avoid_: Remote path, proxy actor

**Delivery Attempt**:
One submission of a user message to a local or remote actor reference. A successful attempt does not imply that the recipient processed the message.
_Avoid_: Delivery, transaction

**Delivery Lane**:
An ordered sequence of remote delivery attempts that share a FIFO boundary.
_Avoid_: Connection, queue

**Serializer Manifest**:
A stable payload type and schema-version identifier interpreted by a registered serializer.
_Avoid_: Python class name, pickle type

**Dead Letter**:
A delivery attempt that the runtime knows cannot reach its intended actor identity.
_Avoid_: Failed message, retry

**Remoting Health Event**:
A volatile local notification that a remoting listener failed or an association became active or closed. The bounded event window is diagnostic, not durable lifecycle delivery.
_Avoid_: Cluster event, delivery acknowledgement

**Remoting Metrics Snapshot**:
An immutable view of cumulative remoting counters for one actor system incarnation. It does not imply remote receipt or actor processing.
_Avoid_: Delivery confirmation, durable metric

**Cluster**:
A named volatile membership domain containing accepted actor system incarnations.
_Avoid_: Actor system, association, durable registry

**Cluster Member**:
One specific actor system incarnation accepted into a cluster. Restarting the actor system creates a different member.
_Avoid_: Endpoint, actor system name, process

**Member Identity**:
The immutable combination of an actor system name and actor system incarnation UID within one cluster.
_Avoid_: Endpoint, actor identity, actor system name alone

**Membership Coordinator**:
The statically configured cluster member that serializes membership changes in the initial cluster protocol.
_Avoid_: Association initiator, remoting listener, elected leader

**Seed Contact**:
The configured actor system name and endpoint used to contact the membership coordinator.
_Avoid_: Member identity, discovery service, endpoint alone

**Member Status**:
A cluster member's membership lifecycle state: joining, up, leaving, or left.
_Avoid_: Reachability, association state

**Reachability**:
A failure detector's local observation that a cluster member is reachable or unreachable. An unreachable member remains a cluster member.
_Avoid_: Member status, proof of failure, association state

**Heartbeat**:
A bounded cluster-control exchange that provides recent evidence of reachability.
_Avoid_: Delivery acknowledgement, user-message acknowledgement

**Failure Detector**:
A local time-based mechanism that changes reachability observations when heartbeat evidence is absent. It cannot prove failure or resolve a network partition.
_Avoid_: Membership coordinator, split-brain resolver

**Downing**:
An explicit membership decision that transitions one exact unreachable member to left so a replacement incarnation may join.
_Avoid_: Failure detection, proof of failure, automatic removal

**Graceful Leave**:
A bounded best-effort membership transition that is attempted before remoting shuts down. It does not drain actor mailboxes or guarantee that every member observed the departure.
_Avoid_: Actor system shutdown, durable acknowledgement

**Cluster Membership Event**:
A volatile local notification about a membership or reachability transition. Its bounded event window is not a durable event log.
_Avoid_: Remoting health event, consensus result

**Persistence Identity**:
A stable application-defined identity for one logical durable state, independent of Actor Identity, Actor Path, Actor System, and Actor System Incarnation.
_Avoid_: Actor identity, actor path, database key

**Operation Identity**:
A stable token for one intended durable-state mutation within one Persistence Identity. Reusing it distinguishes an uncertain retry from a new mutation.
_Avoid_: Delivery attempt, actor message ID, transaction ID

**Durable State**:
The latest successfully committed application state or tombstone for one Persistence Identity and Revision.
_Avoid_: Actor state, event journal, durable mailbox

**Revision**:
A monotonically increasing storage concurrency coordinate for one Persistence Identity. It is not a domain version or evidence that an actor processed a message.
_Avoid_: Schema version, sequence number, acknowledgement

**Tombstone**:
A durable deleted-state marker that preserves the current Revision and Operation Identity history.
_Avoid_: Missing row, physical purge

**Recovery**:
The attempt to load one Durable State before its behavior processes user commands. Recovery does not restore volatile mailbox contents.
_Avoid_: Actor restart, message replay, cluster rejoin

**Durable State Change**:
An immutable committed state snapshot or tombstone appended for one nonduplicate Durable State mutation.
_Avoid_: Domain event, actor message, current durable state

**Projection**:
A resumable asynchronous consumer that transforms ordered Durable State Changes into an application read model or external side effect.
_Avoid_: Actor, query, event handler

**Projection Identity**:
A stable application-defined identity for one logical Projection checkpoint and source definition.
_Avoid_: Persistence identity, projection process, handler name alone

**Projection Offset**:
A monotonically increasing change-feed position stored for one Projection Identity. It records source progress, not successful processing by another system.
_Avoid_: Revision, operation identity, delivery acknowledgement

**Change Feed**:
The ordered retained sequence of Durable State Changes from which Projections resume after a Projection Offset.
_Avoid_: Durable State table, actor mailbox, event journal
