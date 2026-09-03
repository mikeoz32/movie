# Remoting v1 Contract

Status: implemented.

This document defines the interoperability and runtime semantics for Movie remoting v1. Canonical domain terms are defined in [`CONTEXT.md`](../CONTEXT.md), and the architectural boundary is recorded in [ADR-0001](adr/0001-remoting-v1-boundaries.md).

## Scope

Remoting v1 provides:

- direct associations between explicitly configured actor-system endpoints;
- resolution of an actor path to an actor identity;
- remote actor references for user-message delivery attempts;
- at-most-once delivery with a per-lane FIFO boundary;
- bounded transport queues and explicit serializer negotiation;
- a transport-neutral association layer with TCP as the required v1 backend.

Remoting v1 does not provide:

- discovery, cluster membership, leader election, or split-brain handling;
- durable messages, application-level retries, deduplication, or processing acknowledgements;
- remote spawning, cross-system parent-child relationships, remote supervision, or death watch;
- peer authentication, authorization, or an Internet-safe trust boundary;
- location transparency for an actor that moves between actor-system incarnations.

## Identity And Addressing

Each actor system generates a nonzero random 128-bit incarnation UID before opening an endpoint. Reusing an actor-system name after restart does not reuse its incarnation UID.

Each actor has a nonzero random 128-bit actor UID. Its actor identity is:

```text
(actor-system-incarnation-uid, actor-uid)
```

An actor path is a locator and diagnostic name, not an identity. A canonical locator has this form:

```text
movie://<system-name>@<host>:<port>/<actor-path>
```

System names and path segments contain only ASCII URI-unreserved characters: letters, digits, `.`, `_`, `~`, and `-`. Each is 1 to 255 bytes. A path starts with one `/`, has no empty segments, and never contains an actor UID. Local actors whose names do not satisfy this grammar are not remotely resolvable in v1.

Resolving a locator returns the current actor identity and a remote actor reference bound to that identity. If the target actor system restarts, the old reference becomes stale even when the new incarnation contains the same path and actor name. The runtime must never silently retarget a stale reference.

A remote actor reference remains valid across a new association to the same actor-system incarnation. Reconnect resets its ordering boundary but does not require path resolution again. An association to a different incarnation invalidates the reference.

An endpoint is configured out of band. The host and port in a locator must exactly match an allowlisted endpoint for the named actor system; they never authorize an arbitrary outbound connection. Remoting v1 does not discover endpoints from actor paths, DNS service records, or other actor systems.

## Association Lifecycle

The protocol identifier is `movie-remoting/1`. Protocol major is `1`, protocol minor starts at `0`, frame header version is `1`, and the bootstrap frame limit is 1 MiB. A future QUIC backend uses the protocol identifier as its ALPN value. One active association represents one pair of actor-system incarnations.

The initiator chooses a nonzero random 128-bit association UID, opens one transport connection, writes its connection preamble, and sends `HELLO`. The responder validates it, echoes that association UID in its own `HELLO`, and sends it on the same connection. Each peer sends one `HELLO_ACCEPT` after validating the other peer's `HELLO`. The association becomes active after both accepts are received.

`HELLO` contains:

- protocol major and minor versions;
- actor-system name and 128-bit incarnation UID;
- the 128-bit association UID chosen by the initiator;
- a role byte identifying initiator or responder;
- the configured endpoint advertised by the sender;
- maximum frame size, delivery-lane count, and queue limits;
- registered serializer descriptors and readable/writable manifests;
- optional protocol capabilities.

`HELLO_ACCEPT` contains the association UID, negotiated protocol minor, maximum frame size, delivery-lane count, queue limits, directional serializer routes, and capabilities. Both peers derive the same canonical result from both `HELLO` frames; a mismatch is a protocol violation.

If both peers initiate concurrently, both retain the connection with the lexicographically smaller `(initiator-incarnation-uid, association-uid)` tuple and close the other with `DUPLICATE_ASSOCIATION`.

An association ends on graceful `GOAWAY`, transport connection loss, protocol violation, incompatible negotiation, or actor-system shutdown. Reconnecting creates a new association UID, new delivery lanes, and new ordering boundaries. Frames from a previous association are never replayed into its successor.

## Transport Mapping

The association layer exchanges complete bounded records on logical channels:

- one bidirectional control channel;
- a negotiated fixed number of unidirectional delivery lanes in each direction.

The transport interface accepts control and delivery-lane records only after local queue admission. It must preserve record boundaries and ordering within each logical channel, provide bounded pending-message and pending-byte capacity, reject new records without evicting accepted records when full, and report connection closure without replaying records. Before negotiated limits are activated, a stream-multiplexed transport exposes only control records to the association and leaves delivery streams flow-controlled and unread. Activation occurs only after both `HELLO_ACCEPT` frames are validated, preventing a delivery stream from overtaking the handshake.

TCP is the required v1 backend. It uses one full-duplex ordered byte stream per association. The initiator writes this preamble once before the first frame:

| Field | Size | Meaning |
| --- | ---: | --- |
| Magic | 4 bytes | ASCII `MOV1` |
| Channel kind | 1 byte | `2` multiplexed connection |
| Association UID | 16 bytes | Owning association |
| Lane ID | 2 bytes | `0xffff` for a multiplexed connection |

After the preamble, complete protocol frames are concatenated. Their existing length prefix restores record boundaries. `USER_MESSAGE` carries its logical lane ID and sequence; all other frames belong to the control channel. One bounded writer fairly selects accepted records from control and delivery-lane queues. TCP's global ordering is stronger than the public contract and does not create an ordering guarantee between different logical lanes.

An optional stream-multiplexed transport may map the control channel and each delivery lane to separate streams. Such streams use channel kind `0` for control or `1` for a delivery lane in the same preamble format. A duplicate lane ID, wrong stream direction, or preamble association mismatch closes the association. QUIC, if added, must use ALPN `movie-remoting/1` and satisfy the same bounded-record interface; QUIC-specific objects are not exposed to the association layer.

The v1 public `tell` API is senderless. The sender maps all delivery attempts targeting the same recipient actor identity to the same lane for the lifetime of an association, regardless of which equivalent remote actor reference object was used. Concurrent calls are linearized when the outbound lane accepts their envelopes.

Frames on one lane are ordered by a monotonically increasing unsigned 64-bit lane sequence. The first sequence is zero. A duplicate, regression, gap, or overflow is a protocol violation because remoting v1 never retries or intentionally skips a committed lane frame.

Different lanes have no relative ordering guarantee. Different recipient identities can therefore observe delivery independently.

## Frame Encoding

All integers use unsigned big-endian encoding. `string16` is a 2-byte length followed by UTF-8 bytes; `bytes32` is a 4-byte length followed by opaque bytes; and every list starts with a 2-byte element count. Strings must be valid UTF-8 and normalized to NFC.

Every frame begins with:

| Field | Size | Meaning |
| --- | ---: | --- |
| Frame length | 4 bytes | Bytes following this field |
| Frame type | 1 byte | Protocol frame identifier |
| Flags | 1 byte | Frame-specific flags |
| Header version | 2 bytes | Version of this frame header |
| Correlation ID | 8 bytes | Request/response correlation, or zero |

Flags are zero in v1. Header version is one. Nonzero flags or another header version are unsupported mandatory features and close the association.

The negotiated maximum frame size includes the complete frame. Before negotiation, `HELLO`, `HELLO_ACCEPT`, and `HELLO_REJECT` use the 1 MiB bootstrap limit. A peer must reject an oversized frame before allocating its declared body.

Frame types reserved by v1 are:

| Value | Name | Logical channel |
| ---: | --- | --- |
| `0x01` | `HELLO` | Control |
| `0x02` | `HELLO_ACCEPT` | Control |
| `0x03` | `GOAWAY` | Control |
| `0x04` | `HELLO_REJECT` | Control |
| `0x10` | `RESOLVE_REQUEST` | Control |
| `0x11` | `RESOLVE_RESPONSE` | Control |
| `0x12` | `RESOLVE_REJECTED` | Control |
| `0x20` | `USER_MESSAGE` | Delivery lane |
| `0x30` | `RECIPIENT_UNAVAILABLE` | Control |
| `0x31` | `DESERIALIZATION_REJECTED` | Control |

Unknown frame types close the association with `UNSUPPORTED_FRAME`.

### Handshake Bodies

`HELLO` uses correlation ID zero and contains, in order:

1. protocol major `u16` and minor `u16`;
2. role `u8`, where zero is initiator and one is responder;
3. actor-system name `string16`;
4. actor-system incarnation UID `16 bytes`;
5. association UID `16 bytes`;
6. endpoint host `string16` and port `u16`;
7. maximum frame bytes `u32` and delivery-lane count `u16`;
8. outbound message limit `u32`, outbound byte limit `u64`, inbound message limit `u32`, and inbound byte limit `u64`;
9. serializer descriptor count `u16` and serializer descriptors;
10. capability count `u16` and capability names as `string16` values.

A serializer descriptor contains serializer ID `u32`, stable name `string16`, protocol major `u16`, protocol minor `u16`, readable-manifest count and `string16` values, then writable-manifest count and `string16` values.

`HELLO_ACCEPT` uses correlation ID zero and contains the association UID, negotiated protocol minor `u16`, maximum frame bytes `u32`, lane count `u16`, four negotiated queue limits in the same order as `HELLO`, serializer-route count and routes, then accepted capability count and `string16` names. Protocol minor, maximum frame bytes, and lane count are the minimum offered by both peers. The `outbound` fields are the initiator-to-responder limits, each computed from the initiator's outbound offer and responder's inbound offer. The `inbound` fields are the responder-to-initiator limits, computed in the reverse direction. This role-relative encoding gives both peers an identical body.

A serializer route contains origin actor-system incarnation UID `16 bytes`, serializer ID `u32`, manifest count `u16`, and sendable manifests as `string16` values. Routes are sorted by origin UID, serializer ID, then manifest bytes, making both peers' `HELLO_ACCEPT` bodies identical.

`HELLO_REJECT` and `GOAWAY` use correlation ID zero and contain reason code `u16` followed by detail `string16`. Detail is diagnostic and must not be parsed for behavior.

### Resolution Bodies

`RESOLVE_REQUEST` requires a unique nonzero correlation ID and contains system name `string16` and canonical actor path `string16`.

`RESOLVE_RESPONSE` echoes that correlation ID and contains system incarnation UID `16 bytes`, actor UID `16 bytes`, and canonical path `string16`.

`RESOLVE_REJECTED` echoes that correlation ID and contains reason code `u16` and detail `string16`.

Correlation IDs are unique among outstanding requests from one peer. Reusing a live ID or responding with an unknown ID is a protocol violation.

### Advisory Bodies

`RECIPIENT_UNAVAILABLE` and `DESERIALIZATION_REJECTED` use correlation ID zero and contain association UID `16 bytes`, lane ID `u16`, lane sequence `u64`, recipient actor UID `16 bytes`, reason code `u16`, and detail `string16`.

Reason codes reserved by v1 are:

| Value | Name |
| ---: | --- |
| `0x0000` | `NORMAL_SHUTDOWN` |
| `0x0001` | `INCOMPATIBLE_VERSION` |
| `0x0002` | `SYSTEM_NAME_MISMATCH` |
| `0x0003` | `DUPLICATE_ASSOCIATION` |
| `0x0004` | `UNSUPPORTED_FRAME` |
| `0x0005` | `PROTOCOL_VIOLATION` |
| `0x0006` | `FLOW_CONTROL_VIOLATION` |
| `0x0010` | `ACTOR_NOT_FOUND` |
| `0x0011` | `ACTOR_STOPPING` |
| `0x0012` | `MAILBOX_FULL` |
| `0x0013` | `INVALID_PATH` |
| `0x0020` | `UNKNOWN_SERIALIZER` |
| `0x0021` | `UNSUPPORTED_MANIFEST` |
| `0x0022` | `MALFORMED_PAYLOAD` |

## User Message Envelope

The `USER_MESSAGE` body contains fields in this order:

| Field | Size | Meaning |
| --- | ---: | --- |
| Association UID | 16 bytes | Association that owns the attempt |
| Lane ID | 2 bytes | Negotiated delivery lane |
| Lane sequence | 8 bytes | FIFO sequence within the lane |
| Sender system incarnation UID | 16 bytes | Sender actor system |
| Recipient system incarnation UID | 16 bytes | Expected target incarnation |
| Recipient actor UID | 16 bytes | Immutable target actor UID |
| Serializer ID | 4 bytes | Negotiated payload serializer |
| Manifest length | 2 bytes | UTF-8 serializer manifest length |
| Payload length | 4 bytes | Serialized payload length |
| Serializer manifest | Variable | Stable payload type and schema version |
| Payload | Variable | Opaque serializer output |

The manifest limit is 1,024 bytes. Payload and frame limits are negotiated during the handshake. Actor paths and Python class names are not included in a user-message envelope.

Before deserialization, a receiver must verify that:

- the association UID is current;
- the lane ID matches the logical delivery channel and negotiated range;
- lane sequence is the next expected value;
- sender incarnation UID equals the associated peer;
- recipient incarnation UID equals the local actor-system incarnation;
- serializer ID and manifest were negotiated for the receiving direction;
- declared lengths fit the negotiated limits and frame body exactly.

Failure of an identity, lane, sequence, or length check closes the association with `PROTOCOL_VIOLATION`.

Protocol system messages are never encoded as user messages. Parent-child lifecycle, supervision, and actor-system control remain local to one actor system.

## Path Resolution

`RESOLVE_REQUEST` contains an actor-system name and actor path. The receiver returns:

- `RESOLVE_RESPONSE` with its incarnation UID, actor UID, and canonical path; or
- `RESOLVE_REJECTED` with `SYSTEM_NAME_MISMATCH`, `ACTOR_NOT_FOUND`, `ACTOR_STOPPING`, or `INVALID_PATH`.

A resolution response is accepted only from the association that owns its correlation ID. The resulting remote actor reference is identity-bound, not association-bound, and may use a later association to the same actor-system incarnation. Resolution does not reserve the actor or guarantee that it remains alive after the response.

## Serialization

Serializers are registered explicitly before an association becomes active. A registry entry contains:

- an unsigned 32-bit serializer ID greater than zero;
- a stable serializer name;
- a serializer protocol version;
- the manifests that the serializer can read and write.

Serializer IDs are stable application protocol identifiers and are never remapped per association. Serializer ID zero is reserved for protocol control data.

Two serializer descriptors are compatible when their IDs, stable names, and protocol majors are equal. The negotiated protocol minor is the lower minor. For each direction, the sendable manifest set is the intersection of the sender's writable manifests and receiver's readable manifests. An ID/name or protocol-major conflict rejects the handshake. An empty intersection omits only that directional route; the reverse route remains independent.

Movie passes the negotiated serializer protocol minor into every serializer call. Calls to one registered serializer are serialized by a reentrant registry lock, so serializer implementations do not need to provide their own free-threading lock. Serializer callbacks must not invoke a different serializer or serializer registry; payload conversion is a leaf operation.

A sender may use only a serializer ID accepted by both peers and a manifest in the sendable set for that direction. This check occurs before serialization and outbound queue admission.

A serializer manifest identifies the payload contract and schema version. It must not be derived from a Python module path or class name.

`pickle`, `cloudpickle`, `marshal`, and equivalent arbitrary-code object formats are forbidden for remote payloads.

Serialization completes before a delivery attempt is accepted into an outbound queue. A serialization error is reported synchronously to the caller and creates no delivery attempt.

## Delivery Semantics

Remote delivery is at-most-once.

A successful remote `tell` means that:

1. the target reference belongs to the active remote actor-system incarnation;
2. a compatible serializer produced the payload;
3. the local bounded outbound lane accepted the envelope.

It does not mean that the bytes reached the peer, entered the remote mailbox, or were processed by the actor.

TCP can retransmit transport packets within one connection. This does not constitute application-level retry and cannot create a second `USER_MESSAGE` frame.

There is no positive acknowledgement for a user message. On association loss, all envelopes not known to have been completely written are dropped. Envelopes with uncertain remote status are also dropped. Neither category is replayed after reconnect.

FIFO is guaranteed only for delivery attempts that:

- originate from the same sending actor system and target the same recipient actor identity;
- are accepted by the same association;
- map to the same delivery lane.

Equivalent remote actor reference objects targeting the same identity share this boundary. Reconnect, either actor-system restart, and different recipient identities are ordering boundaries.

The receiver consumes each delivery lane serially and attempts remote mailbox admission in lane-sequence order. Successfully admitted user messages retain that order through actor processing. A rejected admission does not block later lane frames, but later admitted messages cannot overtake an earlier admitted message.

Remote mailbox admission is an atomic internal operation with one of four results: `ACCEPTED`, `ACTOR_NOT_FOUND`, `ACTOR_STOPPING`, or `MAILBOX_FULL`. The implementation must not infer admission from the current public `ActorRef.tell` return value. Non-accepted results create a receiver-side dead letter and an advisory `RECIPIENT_UNAVAILABLE` frame; they never invoke the actor.

## Capacity And Flow Control

Every association has bounded limits for:

- outbound messages and bytes;
- inbound undecoded bytes;
- decoded messages awaiting mailbox submission;
- frame size and concurrent delivery lanes.

The transport reserves one additional control record and at most one maximum-frame worth of bytes for terminal `GOAWAY`. Normal control and user records cannot consume this reserve. Closing an association never evicts an accepted user record merely to report diagnostics; if the transport cannot flush the reserve within the shutdown deadline, it closes without replay.

TCP socket backpressure does not replace runtime queue limits. A full outbound lane rejects `tell` synchronously with a remoting capacity error. It must not evict an older accepted envelope. The transport writer blocks only its dedicated I/O thread, never an actor dispatcher worker.

A peer that exceeds negotiated frame or inbound limits receives `GOAWAY` with `FLOW_CONTROL_VIOLATION`, and the association closes.

## Failure Mapping

| Condition | Required result |
| --- | --- |
| No active association | Publish a local dead letter; do not buffer for a future association |
| Stale target incarnation | Publish a local dead letter and invalidate the remote reference |
| Unknown recipient actor UID | Peer sends advisory `RECIPIENT_UNAVAILABLE`; sender publishes a dead letter event |
| Recipient actor stopping | Reject admission with advisory `ACTOR_STOPPING` and publish a receiver-side dead letter |
| Recipient mailbox full | Reject admission with advisory `MAILBOX_FULL` and publish a receiver-side dead letter |
| Unknown serializer before send | Reject `tell` synchronously |
| Payload decode failure | Peer sends advisory `DESERIALIZATION_REJECTED`; recipient actor is not invoked |
| Lane sequence violation | Close association with `PROTOCOL_VIOLATION` |
| Outbound capacity exhausted | Reject `tell` synchronously without evicting accepted work |
| Association loss | Drop pending and uncertain envelopes without replay |
| Protocol version mismatch | Reject handshake with `INCOMPATIBLE_VERSION` |

Advisory rejection frames are diagnostic. They do not turn user messages into acknowledged delivery and can themselves be lost when an association closes.

A dead-letter event contains the target actor identity, reason code, and timestamp. Association UID, lane ID, lane sequence, serializer ID, manifest, and payload byte length are optional and present only when known at the failure point. Payload bytes are excluded by default. Advisory frames do not require the sender to retain historical envelope metadata after an attempt leaves its bounded queues. The actor system must expose a bounded local subscription API for these events before remoting is enabled.

## Trust Model

The required v1 TCP backend is unencrypted and does not authenticate its peer. Actor-system names and incarnation UIDs asserted during the handshake are not credentials. TLS wrapping, mutual TLS, application tokens, and certificate provisioning are outside the v1 transport contract.

This model is suitable only for a trusted network or an externally authenticated and encrypted tunnel. It is not safe to expose directly to an untrusted network.

## Observability

An implementation must expose, at minimum:

- association state and close reason;
- active peer incarnation UID and endpoint;
- outbound and inbound queue utilization;
- accepted, rejected, and dead-letter delivery-attempt counts;
- serialization and deserialization rejection counts;
- per-lane sequence violations;
- reconnect count without implying message retry.

## Runtime Foundations

The implementation provides these local foundations:

- actor-system incarnation UIDs and nonzero actor UIDs are first-class identity values;
- actor paths have canonical single-slash rendering and the remote-resolvable name grammar above;
- actor lookup by UID is distinct from path resolution;
- actor cells expose atomic remote mailbox-admission results;
- the actor system exposes bounded dead-letter subscriptions;
- serializer registries are immutable while an association is active;
- a remote actor reference cannot be used as a parent or receive local lifecycle system messages.

Tracing capability names, a TLS transport option, and an optional QUIC backend can be added later without changing this wire or delivery contract.

`RemotingConfig` owns its transport object for exactly one actor-system incarnation. A transport must not be shared between actor systems. Actor-system shutdown closes the listener, associations, pending connections, resolver workers, and finally the transport within the one global shutdown deadline.
