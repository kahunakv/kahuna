# Snapshot installation and Raft recovery guide

Kahuna registers whole-partition state transfer with Kommander. A replica that needs history below the
available Raft log floor can be seeded from its leader's snapshot and replay the retained tail. This
supports catch-up and repair under both full replication and per-partition placement; it is separate
from operator-triggered [backups and PITR](backups-and-point-in-time-recovery-guide.md).

For replica moves and staging-cap sizing, see the
[replication factor guide](replication-factor-guide.md#6-how-a-replica-move-proceeds). For detection and
containment of a divergent replica, see the
[leadership fencing guide](leadership-fencing-and-apply-fingerprint-guide.md#containment).

## What a data-partition snapshot carries

`PartitionStateTransfer` exports key/value rows, persistent lock state, completion receipts,
transaction records, prepared intents and the committed-head/settled-transaction ledger for the
partition. Ownership is derived from the range map or hash routing over the node-global backend.
Export first drains queued backend writes so the rows include state applied through the requested
boundary. This is an **at-least boundary**, not an exact point-in-time image: pages and store slices
can include later applies, and the receiver replays the retained tail idempotently. It is unsuitable
as an operator backup cut. It writes paged,
checksummed rows and store payloads into pooled segments rather than a growing contiguous array.
The complete exported snapshot still occupies memory on the sender: this is not a zero-buffer export.

Partition 0 uses `MetaSystemStateTransfer` instead: it transfers the range map and snapshot-floor hold
registry together. The data-partition streaming-install description below does not apply to that
separate metadata decoder.

## Verification, installation and failure

The receiver stages the complete transfer before installing it. Data-partition import makes two passes
through the seekable staged stream:

1. **Verify before changing state.** Check the partition id, every page/store checksum, and decoding of
   receipts, records and intents. Truncation, checksum failure or undecodable store entries leave the
   prior state untouched. When `OnePhaseApplyTimeValidation` is enabled, a snapshot lacking the
   committed-head ledger is refused.
2. **Replace the owned state.** Mark the install incomplete; drain queued persistence; purge the
   partition's old backend rows and receipt/record slices; stream in the new rows, records and receipts;
   replace the intent/ledger slice. Persist the durable-store snapshots before acknowledging success,
   invalidate resident key/value and lock state, and then clear the incomplete marker.

Backend batches flush at 4,096 rows or after accumulated value bytes reach 16 MiB. The byte threshold
can be exceeded by the final row; it is not a maximum value size. Records and receipts are decoded one
entry at a time. The intent/ledger section is still decoded as a whole for replacement, and the final
installed state must fit the node. Streaming reduces temporary working memory; it does not make total
memory independent of partition size. A non-seekable input is copied once into pooled memory segments.
Kommander's staged receive streams are seekable, including disk-backed streams.

This is **recoverable replacement, not a backend transaction over the entire partition**. A storage or
invalidation-hook failure after mutation starts can leave partial rows. The incomplete marker remains;
a retry purges that attempt's owned state before installing again. The marker records interruption on disk,
but there is currently no production caller of `IsInstallIncomplete`; the marker alone does not
establish a startup serving fence. Automatic crash recovery from that marker must not be assumed. A second import of the same
partition while one runs is refused, rather than queued. The in-process install flag is released on success or failure;
other partitions have independent install gates. Install and un-host purge of one partition cannot
interleave. Cancellation or loss of a sender acknowledgement does not roll back an install already
running through Kommander's installer.

Transaction-record and receipt snapshot files are also decoded entry by entry at startup. Prepared
intent snapshots are parsed directly from a file stream but still retain the decoded intent state.

## Receive staging and deadlines

The server exposes these flags; embedded options with the `Raft` prefix are nullable and leave the
underlying default unchanged unless set:

| Server flag | Embedded option | Default | Meaning |
|---|---|---|---|
| `--raft-snapshot-max-pending-bytes` | `RaftSnapshotMaxPendingBytes` | 512 MiB | Total staged bytes across receive sessions, on disk and in memory together. |
| `--raft-snapshot-max-pending-sessions` | `RaftSnapshotMaxPendingSessions` | 8 | Concurrent receive sessions. |
| `--raft-snapshot-staging-directory` | `RaftSnapshotStagingDirectory` | See below | Private directory for spilling received bytes. |
| `--raft-snapshot-staging-memory-bytes` | `RaftSnapshotStagingMemoryBytes` | 64 MiB | Resident staged-byte budget with a staging directory; zero stages entirely on disk. Ignored without a directory. |
| `--raft-snapshot-chunk-ack-timeout` | `RaftSnapshotChunkAckTimeout` | 15 s | Wait for a chunk acknowledgement, including the final chunk's install. |
| `--raft-snapshot-transfer-step-timeout` | `RaftSnapshotTransferStepTimeout` | 120 s | No-progress bound for an export, stream read or chunk send, not a total transfer deadline. |
| `--raft-reseed-request-timeout` | No corresponding option | 180 s | Bounds a requested repair's apply hold and the leader's pending checkpoint request. |

Server durations in this table are **milliseconds** on the command line. Embedded durations are
`TimeSpan`. Pending-byte/session caps must be positive and staging-memory bytes must be nonnegative.

The server default is no staging directory, so all staged bytes remain in memory. Embedded nodes with
`Storage = "sqlite"` or `"rocksdb"` and a `StoragePath` default to
`{StoragePath}/snapshot-staging_{StorageRevision}`; other embedded nodes default to memory staging.
An explicit embedded staging directory overrides this choice. `EmbeddedKahunaCluster.CreateInMemoryAsync`
gives each member a subdirectory named for that member under an explicit base staging directory. Without a fixed `StorageRevision`, the automatic staging
directory gets a fresh GUID on each construction; use a stable revision when reopening stored state.

Directories must be private to one node: Kommander deletes spill files there at startup. Distinct
storage revisions provide distinct automatic directories when hosts share a storage path. Spill-to-disk
limits staged-byte residency, not the decoded stores, export buffers, backend caches or total heap.
A spill-file write failure fails that receive session and causes sender retries. Size the pending cap above the largest expected partition snapshot even when staging on disk.

The final chunk is acknowledged after installation finishes. An install longer than the acknowledgement
budget can cause a new export/transfer while the earlier import still runs. The effective chunk deadline
is the smaller of chunk-ack and transfer-step timeouts. Embedded validation rejects an explicitly set
chunk-ack timeout above the effective step timeout. Raise both on all nodes when installs may exceed
120 seconds; a larger staging cap alone does not fix slow-install retries.

For example, configure a persistent embedded host's receive path (other options omitted):

```csharp
var options = new EmbeddedKahunaOptions
{
    Storage = "rocksdb",
    StoragePath = "/var/lib/kahuna/node-a",
    StorageRevision = "node-a",
    RaftSnapshotMaxPendingBytes = 1024L * 1024 * 1024,
    RaftSnapshotStagingMemoryBytes = 32L * 1024 * 1024,
    RaftSnapshotChunkAckTimeout = TimeSpan.FromMinutes(3),
    RaftSnapshotTransferStepTimeout = TimeSpan.FromMinutes(4)
};
```

The equivalent server staging/deadline settings require an explicit directory:

```bash
kahuna-server \
  --raft-snapshot-staging-directory /var/lib/kahuna/node-a/snapshot-staging \
  --raft-snapshot-max-pending-bytes 1073741824 \
  --raft-snapshot-staging-memory-bytes 33554432 \
  --raft-snapshot-chunk-ack-timeout 180000 \
  --raft-snapshot-transfer-step-timeout 240000
```

## Replication scheduling and log retention

The following settings describe Kahuna's pinned **Kommander 1.9.3** behavior; a newer sibling checkout
can contain recovery changes that this build does not consume. These **server** options are passed to `RaftConfiguration`. They do not have matching
properties on `EmbeddedKahunaOptions` today; do not assume server/embedded configuration parity.

| Flag | Default | Semantics |
|---|---|---|
| `--raft-fan-out-before-local-write` | `true` | Queue the leader's proposed write and send to followers concurrently. Quorum completion still requires the leader's local write to be durable. If that write fails after fan-out, followers may hold the proposal: the outcome can be unknown, rather than proof of rejection. |
| `--raft-follower-apply-in-own-turn` | `true` | Acknowledge follower appends before delivering committed entries to the application in separate executor turns. Apply remains ordered; a replication acknowledgement is not proof that the follower's application has caught up. The system partition still applies inline. |
| `--raft-follower-apply-turn-time` | 100 µs | Cooperative time budget per follower apply turn; at least one entry is delivered. `0` leaves only the entry budget. A long callback can exceed the time budget. |
| `--raft-follower-apply-turn-budget` | 1024 | Entry budget per follower turn; nonpositive disables the count bound. A large backlog can increase the effective budget. |
| `--raft-compaction-live-replica-lag-window` | 180000 ms | Time-sized recent history that raises the live-replica retention depth. `0` uses the entry budget alone. |
| `--raft-compaction-live-replica-lag-cap` | 10000000 entries | Caps the window's increase; nonpositive disables that increase. Does not lower a larger entry budget. |

The effective retention depth is
`max(CompactionLiveReplicaLagBudget, min(recent-window entries, CompactionLiveReplicaLagCap))` when the
window is enabled. `--raft-compaction-live-replica-lag-budget` defaults to 1,000,000 entries. Retention
uses the leader's observed commit rate; a new leader initially uses the entry budget. It is a bounded
catch-up aid, **not a guarantee that a pause of that many milliseconds always avoids a snapshot**.
Liveness, the separate silent-peer retention window, checkpoints, application durability and PITR floors
also constrain compaction. A replica outside retained history still needs snapshot seeding.

`--raft-grpc-max-message-bytes` defaults to 16 MiB and bounds peer send/receive messages. Keep outbound
batch/backfill payload limits below it to allow protocol overhead. Raise receivers' message limits
before raising senders' payload sizes. These transport limits do not replace snapshot staging caps.
