# Kahuna.Core

`Kahuna.Core` is the embeddable server-side engine behind Kahuna. It owns the in-process actor system, lock manager, key-value manager, sequence manager, persistence integration, and Raft log restore/replication hooks.

The primary entry point is `KahunaManager`, which implements `IKahuna` and delegates work to the internal components:

- `Locks`: distributed lock state and lock replication.
- `KeyValues`: durable and ephemeral key-value operations, MVCC transaction state, and script execution.
- `Sequencer`: distributed sequence allocation backed by the key-value manager.
- `Persistence`: background storage writes and storage backend abstractions.
- `Replication`: serialization boundaries for Raft log records.
- `Embedding`: helpers for running Kahuna in-process.

Most callers should depend on `IKahuna` or `EmbeddedKahunaNode` instead of directly constructing component managers.
