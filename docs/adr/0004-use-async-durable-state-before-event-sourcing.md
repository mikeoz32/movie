---
status: accepted
---

# Use async durable state before event sourcing

Movie persistence begins with `DurableStateBehavior`, not event sourcing. An optional SQLite backend stores one latest state or tombstone per stable Persistence Identity using optimistic revisions and mandatory Operation Identities; database connections and operations run asynchronously through the Actor System's `ASYNCIO_IO` extension so actor dispatchers never perform blocking database I/O.

## Considered Options

- Event sourcing was deferred because journal replay, snapshots, domain-event retention, and event upcasting are unnecessary for the first durable-state contract. Durable State projections consume committed state snapshots rather than domain events.
- Running `sqlite3` calls on an asyncio event-loop thread was rejected because one blocking call could stall remoting and other I/O extensions; the SQLite backend uses an asynchronous driver and pins each connection to one I/O worker.
- Reusing the remoting serializer registry was rejected because live association negotiation cannot define compatibility with historical stored bytes. Persistence uses stable stored manifests and an independent codec contract.

## Consequences

- Command handlers expose candidate state only after a successful commit; ambiguous writes require Recovery and retry with the same Operation Identity.
- Actor mailboxes, cluster membership, remoting delivery attempts, and commands waiting during Recovery remain volatile.
- SQLite supplies local durable storage but not cross-system ownership. Applications must maintain one logical writer per Persistence Identity until a shared backend with fencing is introduced.
- PostgreSQL, event sourcing, snapshots, transactional outbox, sharding, and write fencing remain outside Durable State v1.
