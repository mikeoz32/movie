---
status: accepted
---

# Project durable state from a transactional change feed

Movie appends an immutable Durable State Change in the same transaction that updates the latest Durable State and Operation Identity history. Reading only the latest-state table was rejected because a lagging Projection could not reconstruct intermediate revisions; using actor messages as the source was rejected because mailboxes and delivery attempts are volatile.

## Consequences

- Projection Identity and Projection Offset are independent of Persistence Identity, Revision, and Operation Identity.
- At-least-once handlers run before their offset commits and must tolerate duplicate batches after uncertain failure.
- Exactly-once applies only when read-model writes and the Projection Offset commit through the same SQLite transaction abstraction. External side effects remain at-least-once and require idempotency or a transactional outbox.
- Change-feed compaction may advance only through the minimum checkpoint of every registered Projection. Registering a new Projection after compaction requires an application-defined baseline because removed history cannot be replayed.
- Projection v1 assumes one logical runner per Projection Identity across Actor Systems. Offset compare-and-set detects conflicts but is not a lease or fencing mechanism.
