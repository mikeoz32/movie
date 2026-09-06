---
status: accepted
---

# Layer coordinated cluster membership over remoting

The first cluster protocol is a separate volatile application protocol carried by ordinary remoting delivery lanes. Remoting and cluster membership are Actor System extensions identified by stable `REMOTING` and `CLUSTER` extension IDs, and the cluster extension resolves remoting as an explicit dependency. One statically configured Membership Coordinator serializes joins and graceful leaves; all participating actor systems remain reciprocally allowlisted in `RemotingConfig`. Cluster Member identity includes the Actor System Incarnation UID, while reachability remains independent from membership status. Heartbeats provide local failure evidence but never automatically remove an unreachable member.

## Considered Options

- Adding cluster records to the remoting control protocol was rejected because association establishment and cluster membership have different compatibility, lifecycle, and delivery semantics.
- Dynamically authorizing endpoints learned during join was deferred because it would widen remoting's trust boundary before authentication and admission policy exist.
- Gossip membership and coordinator election were deferred until the centralized protocol establishes the public membership model and failure semantics.
- Treating association activation or closure as membership changes was rejected because associations are temporary and may reconnect without changing the participating incarnations.

## Consequences

- The Membership Coordinator is a deliberate availability limit: new joins and authoritative membership changes stop while it is unavailable.
- Cluster control payloads use a stable explicitly negotiated serializer and retain remoting's at-most-once, non-replay delivery semantics.
- Every participant and the Membership Coordinator must reciprocally allowlist each other before startup; participants do not need direct associations with one another.
- Graceful leave is bounded and best effort. Abrupt loss changes reachability to unreachable but does not down or remove a member.
- Replacing an abruptly lost incarnation requires an explicit exact-identity downing decision at the Membership Coordinator.
- Reverse extension lifecycle order makes cluster membership prepare its graceful leave while remoting remains active, then stops cluster before remoting and its I/O dependency.
- A later gossip or consensus protocol may replace coordination without changing Member Identity, Member Status, Reachability, or Association semantics.
