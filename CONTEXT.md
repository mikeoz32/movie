# Movie Actor Runtime

Movie provides volatile actors that exchange messages within an actor system or across explicitly associated actor systems. The model preserves actor identity and message-ordering boundaries without implying durable delivery or clustering.

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
