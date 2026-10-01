# Persistence

Persistence contains the abstraction and background write path for durable lock and key-value state.

`IPersistenceBackend` defines the storage contract. Current backend implementations support memory, SQLite, and RocksDB storage. `BackgroundWriterActor` receives committed state from lock/key-value actors and batches the actual backend writes.

Backend persistence runs asynchronously after consensus. With a durable WAL and appropriate sync
settings, consensus durability precedes backend flush; an in-memory WAL/backend does not survive
process loss. The application-durability floor retains log entries until their derived rows and
required store snapshots are durable. Raft replication decides commitment; the backend stores the
resulting projection for recovery.

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
  negative value (for example `-1`) when the index is not yet computable — `0` would suppress all
  compaction.
- **Full backups must read the committed-max `M` before flushing, then checkpoint** (`BackupDriver`).
  Reversing the order can record an `M` the checkpoint does not contain; since a full backup carries
  no WAL slice, those changes would be lost on restore. The production flush hook must be supplied
  (`KahunaManager.FlushPersistenceAsync`) or the guarantee is lost.
- **Restore cuts key/value mutations on their commit HLC**, rather than blindly using the WAL
  entry time. The shared `KeyValueMessageDecoder` classifies value-carrying and by-reference records;
  prepared-intent settlement deltas can also materialize rows during replay. Keep classification
  and decoding aligned with live apply and restart restore. See the
  [durable settlement guide](../../docs/durable-settlement-guide.md).
- **Coordinated cluster `T`** is capped per partition; the cut is consistent only for a safe `T`
  (`SnapshotCoordinator` picks one strictly below the earliest in-flight commit). It prevents cutting
  an actively-committing transaction; it does not, alone, prevent an already-committed cross-shard
  straddle — durable transactions use a single commit HLC minted above participant staging stamps. This does
  not remove backup coverage, retained-history, exact-checkpoint or topology-stability requirements.

For a conceptual, beginner-friendly walkthrough, see
[`docs/backups-and-point-in-time-recovery-guide.md`](../../docs/backups-and-point-in-time-recovery-guide.md).

Whole-partition catch-up uses a separate state-transfer protocol. Its export is an at-least Raft
boundary rather than an exact backup cut; installation replaces owned rows and store slices with
streamed batches. See the [snapshot recovery guide](../../docs/snapshot-and-raft-recovery-guide.md)
for memory limits, deadlines and interrupted-install behavior.
