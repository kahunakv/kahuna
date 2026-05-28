# Persistence

Persistence contains the abstraction and background write path for durable lock and key-value state.

`IPersistenceBackend` defines the storage contract. Current backend implementations support memory, SQLite, and RocksDB storage. `BackgroundWriterActor` receives committed state from lock/key-value actors and batches the actual backend writes.

The core actors treat persistence as asynchronous durability after consensus. Raft replication decides whether a mutation is committed; persistence stores the resulting committed state so nodes can recover after restart.

Storage-specific code belongs in `Persistence/Backend`. Protobuf records for storage formats belong in `Persistence/Protos`.
