# Persistence

Persistence contains the abstraction and background write path for durable lock and key-value state.

`IPersistenceBackend` defines the storage contract. Current backend implementations support memory, SQLite, and RocksDB storage. `BackgroundWriterActor` receives committed state from lock/key-value actors and batches the actual backend writes.

The core actors treat persistence as asynchronous durability after consensus. Raft replication decides whether a mutation is committed; persistence stores the resulting committed state so nodes can recover after restart.

Storage-specific code belongs in `Persistence/Backend`. Protobuf records for storage formats belong in `Persistence/Protos`.

## Backups and point-in-time recovery (`Persistence/Pitr`)

`Persistence/Pitr` implements full/incremental backup and point-in-time recovery (PITR). The base
image is a storage-engine checkpoint (`IPersistenceBackend.CreateCheckpoint`); incrementals and
restore work from committed WAL slices. A sliding **retention window** (`PitrWindow`, default 1h,
max 6h; `BaseSnapshotInterval`, default 30m) bounds how far back recovery reaches.

Retention-horizon invariants for anyone editing this code:

- **The WAL floor is what makes PITR possible.** `BackgroundWriterActor.UpdatePitrHorizon` computes a
  protected index (`PitrHorizon`, ≈ `now − PitrWindow − BaseSnapshotInterval`) per partition and calls
  `IRaft.SetMinRetainIndex`, which stops compaction from trimming the WAL below the window. The floor
  is in-memory and **resets on restart**, so it is re-asserted on the first flush tick. Pass a
  non-positive value (not `0`) when the index is not yet computable — `0` would suppress all
  compaction.
- **Full backups must read the committed-max `M` before flushing, then checkpoint** (`BackupDriver`).
  Reversing the order can record an `M` the checkpoint does not contain; since a full backup carries
  no WAL slice, those changes would be lost on restore. The production flush hook must be supplied
  (`KahunaManager.FlushPersistenceAsync`) or the guarantee is lost.
- **Restore replays committed entries with the HLC stop-predicate (`Time ≤ T`)** via the shared
  `KeyValueMessageDecoder` (the same decode used by `KeyValueRestorer`). Keep decode in that one place
  so PITR restore can never diverge from live log replay.
- **Coordinated cluster `T`** is capped per partition; the cut is consistent only for a safe `T`
  (`SnapshotCoordinator` picks one strictly below the earliest in-flight commit). It prevents cutting
  an actively-committing transaction; it does not, alone, prevent an already-committed cross-shard
  straddle — an unconditional guarantee needs a single coordinated commit HLC.

For a conceptual, beginner-friendly walkthrough, see
[`docs/backups-and-point-in-time-recovery-guide.md`](../../docs/backups-and-point-in-time-recovery-guide.md).
