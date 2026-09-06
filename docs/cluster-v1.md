# Cluster Membership v1

## Scope

Cluster v1 is a volatile membership protocol layered over Movie remoting. It provides:

- one statically configured Membership Coordinator;
- bounded join attempts with incarnation-specific membership;
- a local immutable membership view;
- periodic heartbeats and reachable/unreachable observations;
- bounded best-effort graceful leave before remoting shutdown;
- explicit downing of one exact unreachable Member Identity;
- bounded local membership events;
- bounded same-incarnation reassociation attempts for cluster control traffic.

It does not provide dynamic endpoint admission, discovery, gossip, coordinator election or failover, automatic downing, consensus, split-brain resolution, durable membership, actor placement, cluster singleton, sharding, or distributed data. Cluster traffic inherits remoting v1's trusted-network requirement and at-most-once, non-replay delivery semantics.

## Topology

`ClusterConfig.seed` identifies the Membership Coordinator by actor-system name and Endpoint. The coordinator and every participant must reciprocally list each other in `RemotingConfig.peers`. Participants do not need to allowlist or associate directly with one another; membership snapshots learned from the coordinator never mutate the remoting allowlist.

The coordinator is an availability boundary. Existing local actors continue running when it is unavailable, but joins, graceful membership changes, and authoritative membership dissemination cannot complete until it returns. There is no automatic coordinator replacement in v1.

Every cluster endpoint must use a positive advertised port. Port-zero remoting listeners cannot participate because their Endpoint is not statically known before startup.

## Identity

A Member Identity is the pair of actor-system name and Actor System Incarnation UID within one named Cluster. An Endpoint is metadata and an Association UID is temporary transport state; neither identifies a member.

Restarting an actor system creates a new Member Identity even when its name and Endpoint are reused. UUID ordering does not establish which incarnation is newer. At most one non-`LEFT` incarnation of an actor-system name may appear in a valid membership view.

## Startup

Cluster configuration requires remoting configuration. `REMOTING` and `CLUSTER` are stable Actor System `ExtensionId` values; the Actor System preconfigures both extensions so the extension registry, rather than the Actor System implementation, owns their lifecycle. The cluster extension resolves the remoting extension as an explicit dependency. Movie adds the internal cluster serializer to the immutable application serializer registry before constructing either extension or claiming the Transport Backend. A serializer ID or name collision fails before remoting starts.

Actor System startup order is:

1. Start the local actor hierarchy.
2. Start the remoting extension.
3. Start the cluster extension and its library-owned control actor.
4. Form or join the Cluster.
5. Mark the Actor System running.

The configured coordinator forms the first one-member view. A participant associates with the coordinator, resolves its control actor, and performs the join protocol before `ActorSystem.create()` returns.

Configured remoting and cluster extensions start automatically. Starting the cluster extension from an actor callback is rejected because forming or joining the Cluster is a blocking Actor System startup operation; use `system.cluster` after `ActorSystem.create()` returns.

## Control Protocol

Cluster records are ordinary typed remoting user payloads on a delivery lane, not remoting control frames. The stable serializer name is `movie-cluster-control`, protocol version `1.0`; `ClusterConfig.serializer_id` selects its deployment-wide nonzero serializer ID.

The v1 records are:

- `JoinRequest`
- `JoinAccepted`
- `JoinConfirm`
- `MembershipUpdate`
- `Heartbeat`
- `HeartbeatAck`
- `Leave`
- `LeaveAck`

Every record carries the cluster name and exact source and target incarnations. A join request also carries a deterministic compatibility fingerprint covering the seed contact, heartbeat interval, unreachable timeout, member and retired-identity limits, and serializer ID; incompatible settings cannot create membership. Token-bearing records use a random membership token issued for one admitted incarnation. The receiving runtime binds each record to the actual Association that carried it before placing it in the bounded cluster queue; an allowlisted peer cannot claim another active peer merely by changing payload fields.

Payloads use strict canonical JSON with stable manifests. Decoding rejects malformed UTF-8/JSON, duplicate or unknown fields, noncanonical UUIDs, invalid endpoints, invalid status values, oversized payloads, and membership snapshots above 256 entries.

## Join

Join is a bounded three-step transition:

1. The participant sends `JoinRequest` with its incarnation, Endpoint, request ID, and cluster control actor UID.
2. The coordinator records a bounded provisional `JOINING` entry and replies with `JoinAccepted` and a membership token.
3. The participant sends token-bound `JoinConfirm`; only then does the coordinator transition that Member Identity to `UP` and publish a newer snapshot.

The participant becomes locally `UP` only after it receives the confirmed coordinator snapshot through `MembershipUpdate` or `HeartbeatAck`. Repeated copies of the same pending request may receive another response but never extend the original provisional deadline. Unconfirmed entries expire and release their capacity. If participant startup times out after receiving a token, startup rollback attempts a token-bound leave so the coordinator does not retain an `UP` ghost.

Member capacity includes the coordinator and all visible records. Terminal `LEFT` tombstones are retained until their capacity is needed by a later join; evicted identities move into a bounded fail-closed retired-identity fence and can never rejoin that coordinator incarnation. If `retired_identity_limit` is exhausted, the coordinator rejects further capacity reclamation rather than forgetting a terminal identity. Protocol credentials and cached references are revoked as soon as an identity becomes `LEFT`.

## Membership Views

`system.cluster.membership` returns a `MembershipSnapshot` containing the local Member Identity, a local monotonic view revision, and deterministically ordered member records. The local revision increases whenever visible member status, reachability, or membership contents change.

The wire revision is separately serialized by the Membership Coordinator. Participants reject lower revisions and reject a conflicting snapshot that reuses an accepted wire revision. A coordinator snapshot may describe participants that are not in the local remoting allowlist; those records are membership information and do not authorize an Association.

Member Status and Reachability are independent:

- valid status progression is `JOINING`, `UP`, `LEAVING`, `LEFT`;
- `LEFT` is terminal for one Member Identity;
- reachability is `REACHABLE` or `UNREACHABLE`;
- an `UNREACHABLE` member remains a member and normally remains `UP`.

## Heartbeats

Participants periodically send monotonically sequenced heartbeats to the coordinator. The coordinator validates the Member Identity and membership token, records recent local monotonic evidence, and replies with the current membership snapshot. Heartbeat acknowledgement means that the cluster application protocol processed that heartbeat; it does not acknowledge arbitrary user messages.

The coordinator observes participant reachability, while each participant independently observes coordinator reachability. A missed timeout produces an `UNREACHABLE` observation only. Later valid heartbeat evidence restores `REACHABLE` without changing Member Identity or status.

After an Association interruption, a participant makes backoff-controlled attempts bounded individually by `reassociation_timeout` to re-associate with the same coordinator incarnation. It retries only cluster control submissions that remoting rejected before local admission, submits new heartbeat attempts, and never replays a previously accepted record. A different coordinator incarnation is not adopted automatically.

## Leave And Downing

`ClusterRuntime.leave()` and `ActorSystem.stop()` attempt graceful leave while remoting is still active. During Actor System shutdown, the extension registry prepares extensions in reverse startup order before stopping local actors. The cluster extension therefore leaves first while the remoting extension remains active. After local actors stop, the registry stops cluster, remoting, and its I/O dependency in reverse startup order under the same Actor System deadline. A participant publishes local `LEAVING`, sends token-bound `Leave`, and waits only within a sub-budget that reserves time for local cleanup. The coordinator serializes `LEAVING` and `LEFT`, acknowledges the request, revokes the token, and disseminates the terminal snapshot. Missing acknowledgements do not prevent bounded local shutdown.

Graceful leave does not drain actor mailboxes, prove that every peer observed departure, or provide durable acknowledgement.

Abrupt loss never automatically removes a member. The Membership Coordinator may call `down(identity)` only for one exact `UP` and `UNREACHABLE` Member Identity. Downing transitions it to `LEFT` and permits a replacement incarnation with the same actor-system name to join. This is an operational membership decision, not a conclusion produced by the failure detector.

## Events And Failures

`system.cluster.events` publishes `MEMBER_UP`, `MEMBER_LEAVING`, `MEMBER_LEFT`, `MEMBER_REACHABLE`, and `MEMBER_UNREACHABLE`. Subscriptions observe only future events, never backpressure cluster work, retain a bounded window, and report overwritten events through `dropped_count`. Consumers must resynchronize from `system.cluster.membership` after a drop.

The event stream is volatile and closes after cluster shutdown. It is separate from remoting health events: Association activation or closure can trigger cluster activity but is never itself a membership transition.

Cluster worker failures are exposed through `system.cluster.failure`. They do not stop local actors automatically.
