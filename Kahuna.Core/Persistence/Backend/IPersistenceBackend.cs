
using Kahuna.Server.Locks;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence.Pitr;
using Kommander.Time;

namespace Kahuna.Server.Persistence.Backend;

/// <summary>
/// Defines an interface for persistence backend implementations, providing methods
/// for storing and retrieving locks and key-value pairs from a persistent storage system.
/// </summary>
internal interface IPersistenceBackend
{
    public bool StoreLocks(List<PersistenceRequestItem> items);

    /// <summary>
    /// Persists a batch of committed key-value records. <b>Monotonic current-head contract:</b> the
    /// durable current row per key is advanced by (revision, commit HLC) only — an older record adds
    /// its retained-history row but never replaces a newer current head, regardless of arrival order
    /// within or across batches. On a <c>true</c> return, every key touched by the batch therefore
    /// has a durable current head at or above the newest record for that key in the batch. Overlay
    /// cleanup, flush acknowledgements, and durability-floor advancement all rely on this guarantee:
    /// the same committed mutation reaches this store from more than one producer (the owning actor
    /// and the Raft consumer), and a delayed older duplicate regressing the current row once caused
    /// committed data to become invisible to scans. Retained-history rows keyed by (key, revision)
    /// advance by commit HLC only, because delete/extend records legitimately reuse a revision.
    /// </summary>
    public bool StoreKeyValues(List<PersistenceRequestItem> items);

    /// <summary>
    /// Durably records per-partition application-durability floors: the highest Raft WAL log index
    /// whose delivered entries are all persisted in this backend. Written strictly <b>after</b> the
    /// flush batches the floors certify, so a crash between the two leaves the floor stale-low
    /// (safe: entries are redelivered on restore) and never ahead of the data it vouches for.
    /// The default keeps backends and test fakes that do not participate at "no opinion".
    /// </summary>
    public bool StoreDurabilityFloors(IReadOnlyList<(int PartitionId, long Floor)> floors) => true;

    /// <summary>
    /// Reads a partition's persisted application-durability floor, or -1 when none was recorded.
    /// Consulted at startup, before any WAL entry has been delivered.
    /// </summary>
    public long GetDurabilityFloor(int partitionId) => -1;

    /// <summary>
    /// Removes a partition's persisted application-durability floor. Called when this node stops
    /// being one of the partition's replicas: the floor vouches for applies whose local rows are
    /// being purged, so if the partition is ever hosted here again the stale floor would either
    /// pin WAL retention (stale-low) or — worse — suppress replay of entries the purged copy no
    /// longer reflects (stale-high after a re-seed). The default keeps non-participating backends
    /// at "no opinion".
    /// </summary>
    public bool RemoveDurabilityFloor(int partitionId) => true;

    /// <summary>
    /// Attempts to bring the storage engine back into service after repeated store failures.
    /// RocksDB latches a background error after a failed WAL append (for example ENOSPC): every
    /// later write returns the cached error without any new I/O, so the background writer's
    /// retained-batch retries can never succeed — even after the operator frees disk space. The
    /// RocksDB backend overrides this to close and reopen the engine so retained batches can
    /// drain without a process restart. Returns <c>true</c> when the engine was reset and stores
    /// may be retried immediately; <c>false</c> when the backend performs no reset (the default)
    /// or a reset is not possible yet (for example, the volume is still full). Callers must
    /// treat <c>false</c> as "keep the retained batches and retry later", never as data loss.
    /// </summary>
    public bool TryRecoverFromStorageFailure() => false;

    public LockEntry? GetLock(string resource);

    public KeyValueEntry? GetKeyValue(string keyName);

    /// <summary>
    /// Batched point lookup: returns one current-revision entry (or <c>null</c>) per key,
    /// index-aligned with <paramref name="keyNames"/>. Semantically equivalent to calling
    /// <see cref="GetKeyValue"/> once per key — the default implementation does exactly that —
    /// but backends with a native multi-key read (RocksDB <c>MultiGet</c>) override it to serve
    /// the whole batch in one storage call. Used by the backend read scheduler to compress a
    /// drained burst of independent point reads into a single backend call.
    /// </summary>
    public KeyValueEntry?[] GetKeyValues(string[] keyNames)
    {
        KeyValueEntry?[] results = new KeyValueEntry?[keyNames.Length];

        for (int i = 0; i < keyNames.Length; i++)
            results[i] = GetKeyValue(keyNames[i]);

        return results;
    }


    public KeyValueEntry? GetKeyValueRevision(string keyName, long revision);

    /// <summary>
    /// Returns the highest-revision entry for <paramref name="keyName"/> where
    /// <c>revision ≤ maxRevision</c> and <c>LastModified ≤ readTimestamp</c>,
    /// or <c>null</c> when no such retained revision exists.
    /// </summary>
    public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp);

    public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName);

    /// <summary>
    /// Snapshot prefix scan: returns, per key under <paramref name="prefixKeyName"/>, the current
    /// head row and the key's as-of image at <paramref name="readTimestamp"/> in one backend
    /// operation. <c>Snapshot</c> is the current head when the head's <c>LastModified</c> is
    /// at-or-before the timestamp; otherwise it is the highest retained revision with
    /// <c>revision &lt; head revision</c> and <c>LastModified ≤ readTimestamp</c>. <c>Snapshot</c>
    /// is <c>null</c> when the key had no committed version at the timestamp. Deleted and expired
    /// as-of entries are returned with their state — callers apply tombstone and expiry policy.
    /// <para>
    /// The default implementation composes <see cref="GetKeyValueByPrefix"/> with one
    /// <see cref="GetKeyValueRevisionAtOrBefore"/> call per stale key. Backends that interleave
    /// revision history with head rows in one physical range (RocksDB) override it with a single
    /// sequential pass: the per-key composition re-reads that range once per stale key, which
    /// multiplies disk reads by the revision-chain depth.
    /// </para>
    /// <para>
    /// <paramref name="shouldAbort"/> is polled during the scan; once it returns <c>true</c> the
    /// backend may stop early and return an incomplete result. Callers pass it only for reads whose
    /// result is discarded after cancellation (an expired read continuation).
    /// </para>
    /// </summary>
    public List<(string Key, ReadOnlyKeyValueEntry Current, ReadOnlyKeyValueEntry? Snapshot)> GetKeyValueByPrefixAtOrBefore(
        string prefixKeyName, HLCTimestamp readTimestamp, Func<bool>? shouldAbort = null)
    {
        List<(string, ReadOnlyKeyValueEntry)> scanned = GetKeyValueByPrefix(prefixKeyName);

        List<(string, ReadOnlyKeyValueEntry, ReadOnlyKeyValueEntry?)> result = new(scanned.Count);

        foreach ((string key, ReadOnlyKeyValueEntry entry) in scanned)
        {
            if (shouldAbort is not null && shouldAbort())
                break;

            if (entry.LastModified.CompareTo(readTimestamp) <= 0)
            {
                result.Add((key, entry, entry));
                continue;
            }

            KeyValueEntry? snapshot = GetKeyValueRevisionAtOrBefore(key, entry.Revision - 1, readTimestamp);
            if (snapshot is null || snapshot.State is KeyValueState.Undefined)
            {
                result.Add((key, entry, null));
                continue;
            }

            result.Add((key, entry, new(snapshot.Value, snapshot.Revision,
                snapshot.Expires, snapshot.LastUsed, snapshot.LastModified, snapshot.State)));
        }

        return result;
    }

    public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit);

    /// <summary>
    /// Whole-family paged scan over every current key-value row, in bounded pages of at most
    /// <paramref name="limit"/> entries. Pass null to start; pass the previous page's
    /// <see cref="KeyValueScanPage.NextCursor"/> to resume — the cursor is opaque and
    /// backend-owned (a sharded backend encodes its shard position in it), so callers must never
    /// interpret or fabricate it. Iterate until the cursor is null; a short or empty page with a
    /// non-null cursor only means the backend advanced internally.
    /// <para>
    /// The scan covers rows that are present for the scan's whole duration exactly once; rows
    /// written or deleted concurrently may or may not appear. It reads the physical family only —
    /// committed writes still queued in the background writer are not visible, so callers needing
    /// completeness against the commit frontier must drain the writer first.
    /// </para>
    /// </summary>
    public KeyValueScanPage ScanKeyValues(string? cursor, int limit) =>
        throw new NotSupportedException("This persistence backend does not support whole-family scans.");

    /// <summary>
    /// Whole-family paged scan over every current lock row. Cursor and coverage semantics are
    /// identical to <see cref="ScanKeyValues"/>.
    /// </summary>
    public LockScanPage ScanLocks(string? cursor, int limit) =>
        throw new NotSupportedException("This persistence backend does not support whole-family scans.");

    /// <summary>
    /// Physically removes every row belonging to each key — the current row, all retained revision
    /// history and any no-revision provenance — as opposed to writing a tombstone. This is the
    /// whole-partition install/purge primitive (replica seeding replaces a partition's data;
    /// un-hosting reclaims it); it must never be used on the request path, where deletes are
    /// tombstone writes. Callers must ensure no writes for the affected keys are in flight
    /// (a queued background-writer flush landing after the removal would resurrect rows).
    /// </summary>
    public bool DeleteKeyValues(IReadOnlyList<string> keys) =>
        throw new NotSupportedException("This persistence backend does not support physical key removal.");

    /// <summary>
    /// Physically removes every row belonging to each lock resource. Same contract and caller
    /// constraints as <see cref="DeleteKeyValues"/>.
    /// </summary>
    public bool DeleteLocks(IReadOnlyList<string> resources) =>
        throw new NotSupportedException("This persistence backend does not support physical key removal.");

/// <summary>
    /// Prunes persisted key/value revision history according to retention policy.
    /// </summary>
    /// <param name="keys">Target keys to prune, or <c>null</c> for a backend-wide sweep.</param>
    /// <param name="retentionCount">Maximum revisions to keep per key; <c>0</c> disables count-based pruning.</param>
    /// <param name="retentionAge">Maximum revision age; <see cref="TimeSpan.Zero"/> disables age-based pruning.</param>
    /// <param name="batchSize">Maximum revision records to delete in this pass.</param>
    /// <param name="floorTimestamp">
    /// Snapshot floor: the highest-revision entry whose <c>LastModified ≤ floorTimestamp</c>
    /// (the floor-boundary revision) and every revision newer than it are protected from deletion.
    /// Pass <see cref="HLCTimestamp.Zero"/> to disable floor protection and use the standard
    /// retention policy only.
    /// </param>
    /// <param name="result">Statistics for the prune pass.</param>
    /// <returns><c>true</c> when the pass completed without error.</returns>
    public bool PruneKeyValueRevisions(
        IReadOnlyCollection<string>? keys,
        int retentionCount,
        TimeSpan retentionAge,
        int batchSize,
        HLCTimestamp floorTimestamp,
        out RevisionPruneResult result
    );

    /// <summary>
    /// Produces a crash-consistent base-image snapshot of the storage engine at
    /// <paramref name="destinationPath"/>.  The image is accompanied by a
    /// <see cref="CheckpointManifest"/> sidecar that records the WAL index and HLC
    /// at which the snapshot was taken so restore can locate the correct replay start.
    /// <para>
    /// <b>Content policy:</b> all implementations snapshot both KV and lock state.
    /// RocksDB and SQLite do so implicitly (full-DB copy); Memory serialises both tables
    /// to JSON. The restore path decides which tables to apply.
    /// </para>
    /// <para>
    /// All implementations write into a temp sibling directory first and rename it into
    /// place atomically so a failure mid-copy never leaves a partial checkpoint at
    /// <paramref name="destinationPath"/>.
    /// </para>
    /// <para>
    /// RocksDB creates hard-links to live SST files — fast, does not stall foreground writes.
    /// SQLite runs <c>VACUUM INTO</c> per shard under a per-shard exclusive lock — each shard
    /// copy stalls writes to that shard for its duration; callers should schedule off the
    /// hot write path. Memory serialises both tables to JSON.
    /// </para>
    /// </summary>
    public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime);

    /// <summary>
    /// Produces a checkpoint that reflects the store's state <b>as of</b> <paramref name="cut"/>:
    /// for each key the newest revision whose <c>LastModified ≤ cut</c>, omitting keys whose entire
    /// history is newer than <paramref name="cut"/>. This yields an exact base image that contains
    /// no committed state newer than the declared cut, so replaying incremental segments (which stop
    /// at the restore target) reconstructs the state at any point ≥ <paramref name="cut"/>.
    /// <para>
    /// The default implementation falls back to <see cref="CreateCheckpoint"/> — a physical copy that
    /// may include writes committed after <paramref name="cut"/> (a superset of the as-of image).
    /// Backends that retain per-revision history override this to produce the exact cut.
    /// </para>
    /// </summary>
    public CheckpointResult CreateCheckpointAsOf(
        string destinationPath, long appliedIndex, HLCTimestamp cut, CancellationToken ct = default) =>
        CreateCheckpoint(destinationPath, appliedIndex, cut);

    /// <summary>
    /// True when <see cref="CreateCheckpointAsOf"/> produces an exact as-of image (no state newer
    /// than the cut). False when it falls back to a physical copy that may over-include recent writes.
    /// </summary>
    public bool SupportsExactAsOfCheckpoint => false;

    /// <summary>
    /// The durable pruned-history floor: the highest HLC <c>W</c> such that revision history strictly
    /// below <c>W</c> may have been removed by retention pruning, so an as-of read at a cut below
    /// <c>W</c> can no longer be proven to return each key's true boundary. A full backup whose cut is
    /// below this floor cannot be reconstructed exactly and must fail closed.
    /// <para>
    /// Concretely, each prune advances the floor to the maximum, across the keys it deleted history
    /// from, of that key's <b>oldest surviving</b> revision timestamp: at or above that point every
    /// key's boundary is still present, below it a boundary may be gone. The value is monotonic and
    /// durable across restart, since the deleted history does not come back.
    /// </para>
    /// <para>
    /// The default is <see cref="HLCTimestamp.Zero"/> — no pruning has removed reconstructable
    /// history. Backends that never prune (e.g. the in-memory backend) keep this default; backends
    /// that prune override it and persist it alongside their data.
    /// </para>
    /// </summary>
    public HLCTimestamp GetPrunedHistoryFloor() => HLCTimestamp.Zero;
}