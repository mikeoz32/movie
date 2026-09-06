---
status: accepted
---

# Use Actor System-owned asyncio TCP for remoting

Remoting v1 uses `AsyncioTcpTransport`, backed by the Actor System's `AsyncioIOExtension`, as its production TCP Transport Backend. This keeps socket tasks on the bounded Actor System-owned I/O pool and gives remoting one lifecycle boundary; the legacy threaded `TcpTransport` remains available through explicit transport injection for compatibility and comparison, but `RemotingConfig` no longer selects between built-in backends.
