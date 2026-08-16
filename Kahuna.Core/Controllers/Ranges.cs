
using Kommander.Time;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;

namespace Kahuna;

/// <summary>
/// Key-range surface: the key-space registry, range routing, split/merge triggers, range
/// locks and the MVCC snapshot-floor holds.
/// </summary>
public sealed partial class KahunaManager
{
    public Task<bool> ReplicateKeyValueRangePageLocal(int partitionId, byte[] page, CancellationToken cancellationToken) =>
        keyValues.ReplicateKeyValueRangePageLocal(partitionId, page, cancellationToken);

    /// <summary>
    /// Removes all snapshot holds whose lease has expired. Exposed for tests so they can trigger
    /// a purge cycle without waiting for the periodic <see cref="SnapshotFloorReaperActor"/> timer.
    /// </summary>
    internal Task<int> PurgeExpiredSnapshotHoldsAsync(CancellationToken ct = default) =>
        keyValues.PurgeExpiredSnapshotHoldsAsync(ct);

    /// <summary>The replicated range-descriptor map.</summary>
    internal RangeMapStore RangeMapStore => keyValues.RangeMapStore;

    /// <summary>The replicated, refcounted, leased MVCC snapshot-floor registry.</summary>
    internal SnapshotFloorStore SnapshotFloorStore => keyValues.SnapshotFloorStore;

    /// <summary>The per-node key-space routing registry.</summary>
    internal KeySpaceRegistry KeySpaceRegistry => keyValues.KeySpaceRegistry;

    /// <summary>The quiesce store — for test inspection only.</summary>
    internal RangeQuiesceStore RangeQuiesceStore => keyValues.RangeQuiesceStore;

    /// <inheritdoc/>
    public void RegisterKeyRange(string keySpace) => keyValues.KeySpaceRegistry.RegisterKeyRange(keySpace);

    /// <inheritdoc/>
    public Task<bool> RegisterKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default) =>
        keyValues.RegisterKeyRangeAsync(keySpace, cancellationToken);

    /// <inheritdoc/>
    public Task<bool> RemoveKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default) =>
        keyValues.RemoveKeyRangeAsync(keySpace, cancellationToken);

    /// <summary>The key-range data-movement primitive; register with <c>IRaft.RegisterStateMachineTransfer</c>.</summary>
    internal KvStateMachineTransfer KvStateMachineTransfer => keyValues.KvStateMachineTransfer;

    /// <summary>Returns live range locks held on <paramref name="keySpace"/> in the local actor (export helper).</summary>
    internal Task<List<KeyValueRangeLock>> GetRangeLocksAsync(string keySpace) =>
        keyValues.GetRangeLocksAsync(keySpace);

    /// <summary>Injects clamped lock entries into the local actor for <paramref name="keySpace"/> (import helper).</summary>
    internal Task ImportRangeLocksAsync(string keySpace, List<KeyValueRangeLock> locks) =>
        keyValues.ImportRangeLocksAsync(keySpace, locks);

    // IKahuna surface for inter-node routing.
    public Task<List<KeyValueRangeLock>> GetRangeLocks(string keySpace) =>
        keyValues.GetRangeLocksAsync(keySpace);

    public Task ImportRangeLocks(string keySpace, List<KeyValueRangeLock> locks) =>
        keyValues.ImportRangeLocksAsync(keySpace, locks);

    /// <summary>Resolves a key to its owning <c>(partitionId, generation)</c> (key-order router).</summary>
    internal (int PartitionId, long Generation) LocateRange(string key) => keyValues.LocateRange(key);

    /// <summary>The split-transaction executor.</summary>
    internal RangeSplitter RangeSplitter => keyValues.RangeSplitter;

    /// <summary>The auto-split trigger (exposed for regression tests of <c>ExecuteSplitAsync</c>).</summary>
    internal RangeSplitTrigger RangeSplitTrigger => keyValues.RangeSplitTrigger;

    /// <summary>The merge-transaction executor.</summary>
    internal RangeMerger RangeMerger => keyValues.RangeMerger;

    /// <summary>
    /// Returns the data partition id that <paramref name="key"/> routes to under Kahuna's own
    /// consistent-hash assignment. Matches the routing used by <c>LocateAndTrySetKeyValue</c> and
    /// all other locating operations, so callers can find the right leader without guessing.
    /// </summary>
    public int GetDataPartitionForKey(string key) => keyValues.LocateRange(key).PartitionId;

    /// <summary>
    /// Checks every KeyRange descriptor and splits any that exceed the configured size threshold.
    /// Returns the number of splits performed. Only executes on the node that holds leadership
    /// of both the system partition (0) and meta partition (1).
    /// </summary>
    public Task<int> TriggerAutoSplitAsync(CancellationToken ct = default) =>
        keyValues.TriggerAutoSplitAsync(ct);

    /// <summary>
    /// Test-seam overload: runs the auto-split trigger with an explicit <paramref name="threshold"/>
    /// and <paramref name="minRangeSize"/> instead of the production config values.
    /// </summary>
    internal Task<int> TriggerAutoSplitAsync(int threshold, int minRangeSize, CancellationToken ct = default) =>
        keyValues.TriggerAutoSplitAsync(threshold, minRangeSize, ct);

    /// <summary>
    /// Scans all KeyRange spaces for adjacent under-min descriptor pairs and merges them.
    /// Returns the number of merges performed. Only executes on the dual-leader node.
    /// </summary>
    public Task<int> TriggerAutoMergeAsync(CancellationToken ct = default) =>
        keyValues.TriggerAutoMergeAsync(ct);

    /// <summary>
    /// Test-seam overload: runs the auto-merge trigger with an explicit <paramref name="minMergeSize"/>
    /// instead of the production config value.
    /// </summary>
    internal Task<int> TriggerAutoMergeAsync(int minMergeSize, CancellationToken ct = default) =>
        keyValues.TriggerAutoMergeAsync(minMergeSize, ct);

    // ── MVCC snapshot floor ─────────────────────────────────────────────────────────────────

    public Task<(KeyValueResponseType Type, string HoldId, HLCTimestamp LeaseExpiry)>
        LocateAndAcquireSnapshotHold(string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken ct) =>
        keyValues.AcquireSnapshotHold(holderId, timestamp, leaseMs, ct);

    public Task<(KeyValueResponseType Type, HLCTimestamp LeaseExpiry)>
        LocateAndRenewSnapshotHold(string holdId, int leaseMs, CancellationToken ct) =>
        keyValues.RenewSnapshotHold(holdId, leaseMs, ct);

    public Task<KeyValueResponseType>
        LocateAndReleaseSnapshotHold(string holdId, CancellationToken ct) =>
        keyValues.ReleaseSnapshotHold(holdId, ct);

    public Task<(KeyValueResponseType Type, HLCTimestamp EffectiveFloor, int LiveHolds)>
        GetSnapshotFloor(CancellationToken ct) =>
        keyValues.GetSnapshotFloor(ct);
}
