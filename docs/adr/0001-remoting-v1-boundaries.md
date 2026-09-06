---
status: accepted
---

# Use transport-neutral direct associations with at-most-once delivery

Remoting v1 connects explicitly configured actor-system endpoints through a transport-neutral association layer. TCP is the required v1 transport; optional transports such as QUIC can implement the same bounded transport interface later. User messages use at-most-once delivery and an explicit serializer registry with stable manifests; reconnects never replay uncertain messages, pickle is forbidden, and discovery, cluster membership, persistence, and cross-system parent-child relationships remain outside the boundary.

## Considered Options

- At-least-once delivery was rejected because retry requires durable intent, deduplication, and recipient acknowledgement semantics that the volatile actor model does not provide.
- TCP was selected because it is available on every supported Python platform and its ordered byte stream is sufficient for the v1 protocol. Logical delivery lanes preserve the protocol's FIFO boundaries without requiring transport-level streams.
- Binding association semantics directly to QUIC was rejected. QUIC remains a possible optional transport behind the same interface when a production-quality free-threaded Python implementation is available.
- Discovery and clustering were deferred so an association remains a direct relationship between two known actor-system incarnations rather than membership in a distributed runtime.
- Cluster membership was subsequently added as a separate application protocol in [ADR-0003](0003-layer-coordinated-cluster-membership-over-remoting.md); it does not change the remoting association boundary.
- Transport encryption and mutual authentication were deferred. V1 trusts the deployment network and the identity claimed by its peers.

## Consequences

- A successful remote `tell` means only that the local runtime accepted a delivery attempt; it does not acknowledge remote processing.
- A disconnect can lose in-flight messages, and a new association starts a new ordering boundary.
- Remote actor identity includes the target actor-system incarnation, so a restarted system never inherits references from its predecessor.
- TCP provides one globally ordered byte stream. The association writer fairly multiplexes bounded logical lanes onto it; this stronger physical ordering does not expand the public per-lane ordering guarantee.
- The association layer depends only on bounded transport records, so adding QUIC does not change actor references, serializers, or delivery semantics.
- Deployments must not expose remoting v1 to an untrusted network without an external security boundary.
