using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Administrative surface of <see cref="KeyValuesManager"/>: snapshot holds, key-space registration and range
/// lookup, the range state-transfer entry points the split/merge and replica-seeding paths call, the Raft
/// callbacks, and the manual split/merge triggers. Each member forwards to the collaborator that owns it.
/// </summary>
internal sealed partial class KeyValuesManager
{

    /// <summary>
    /// Acquires or renews a refcounted hold protecting all revisions at/after
    /// <paramref name="timestamp"/>. Idempotent by (holderId, timestamp).
    /// </summary>
    public Task<(KeyValueResponseType Type, string HoldId, HLCTimestamp LeaseExpiry)> AcquireSnapshotHold(
        string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken ct) =>
        snapshotHolds.AcquireSnapshotHold(holderId, timestamp, leaseMs, ct);

    /// <summary>Extends an existing hold's lease.</summary>
    public Task<(KeyValueResponseType Type, HLCTimestamp LeaseExpiry)> RenewSnapshotHold(
        string holdId, int leaseMs, CancellationToken ct) =>
        snapshotHolds.RenewSnapshotHold(holdId, leaseMs, ct);

    /// <summary>Drops a hold, letting the floor advance once no other hold pins it.</summary>
    public Task<KeyValueResponseType> ReleaseSnapshotHold(string holdId, CancellationToken ct) =>
        snapshotHolds.ReleaseSnapshotHold(holdId, ct);

    /// <summary>Reaps holds whose lease expired without an explicit release.</summary>
    internal Task<int> PurgeExpiredSnapshotHoldsAsync(CancellationToken ct = default) =>
        snapshotHolds.PurgeExpiredSnapshotHoldsAsync(ct);

    /// <summary>The effective floor implied by the live holds, and how many there are.</summary>
    public Task<(KeyValueResponseType Type, HLCTimestamp EffectiveFloor, int LiveHolds)> GetSnapshotFloor(CancellationToken ct) =>
        snapshotHolds.GetSnapshotFloor(ct);


    /// <summary>
    /// The key-range data-movement primitive. Registered with Kommander via
    /// <c>RegisterStateMachineTransfer</c>; the split transaction calls its native export/import directly.
    /// </summary>
    internal KvStateMachineTransfer KvStateMachineTransfer => kvStateMachineTransfer;

    /// <summary>
    /// The meta-partition whole-state transfer. Registered with Kommander via
    /// <c>RegisterSystemStateTransfer</c> so a node below the meta WAL compaction floor is repaired
    /// with both the range map and the snapshot-floor holds.
    /// </summary>
    internal MetaSystemStateTransfer MetaSystemStateTransfer => metaSystemStateTransfer;

    /// <summary>
    /// The per-node key-space routing registry. Row/index spaces are registered as
    /// key-range here; <c>{db}/meta</c> and system spaces stay hash-routed (the default).
    /// </summary>
    internal KeySpaceRegistry KeySpaceRegistry => keySpaceRegistry;

    /// <summary>
    /// Flips <paramref name="keySpace"/> to key-range routing on this node and, on the meta-partition
    /// leader, auto-seeds its initial whole-space descriptor if none exists yet. Idempotent.
    /// </summary>
    internal Task<bool> RegisterKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default) =>
        keySpaceAdmin.RegisterKeyRangeAsync(keySpace, cancellationToken);

    /// <summary>Removes a key space's range registration and its descriptors.</summary>
    internal Task<bool> RemoveKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default) =>
        keySpaceAdmin.RemoveKeyRangeAsync(keySpace, cancellationToken);

    /// <summary>Resolves a key to its data partition and the generation fencing that placement.</summary>
    internal (int PartitionId, long Generation) LocateRange(string key) => keySpaceAdmin.LocateRange(key);

    /// <summary>Acknowledges that a key's revision reached the backend.</summary>
    internal void NotifyFlushed(string key, long revision) => keySpaceAdmin.NotifyFlushed(key, revision);

    /// <summary>The split-transaction executor. Splits a key range at a given split key.</summary>
    internal RangeSplitter RangeSplitter => keySpaceAdmin.RangeSplitter;

    /// <summary>The auto-split trigger (exposed for regression tests of <c>ExecuteSplitAsync</c>).</summary>
    internal RangeSplitTrigger RangeSplitTrigger => keySpaceAdmin.RangeSplitTrigger;

    /// <summary>The merge-transaction executor. Merges adjacent under-min ranges.</summary>
    internal RangeMerger RangeMerger => keySpaceAdmin.RangeMerger;

    internal Task<List<KeyValueRangeLock>> GetRangeLocksAsync(string keySpace) =>
        rangeStateTransfer.GetRangeLocksAsync(keySpace);

    internal Task ImportRangeLocksAsync(string keySpace, List<KeyValueRangeLock> locks) =>
        rangeStateTransfer.ImportRangeLocksAsync(keySpace, locks);

    internal Task<List<KeyValueRangeLock>> GetRangeLocksFromPartitionLeaderAsync(
        string keySpace,
        int partitionId,
        CancellationToken cancellationToken) =>
        rangeStateTransfer.GetRangeLocksFromPartitionLeaderAsync(keySpace, partitionId, cancellationToken);

    internal Task ImportRangeLocksToPartitionLeaderAsync(
        string keySpace,
        int partitionId,
        List<KeyValueRangeLock> locks,
        CancellationToken cancellationToken) =>
        rangeStateTransfer.ImportRangeLocksToPartitionLeaderAsync(keySpace, partitionId, locks, cancellationToken);

    internal IReadOnlyCollection<CompletionReceiptRecord> GetLocalCompletionReceiptsForRange(string? startKey, string? endKey) =>
        rangeStateTransfer.GetLocalCompletionReceiptsForRange(startKey, endKey);

    internal IReadOnlyList<Transactions.Data.TransactionRecord> GetLocalTransactionRecordsForRange(string? startKey, string? endKey) =>
        rangeStateTransfer.GetLocalTransactionRecordsForRange(startKey, endKey);

    internal IReadOnlyList<Transactions.Data.PreparedIntent> GetLocalPreparedIntentsForRange(string? startKey, string? endKey) =>
        rangeStateTransfer.GetLocalPreparedIntentsForRange(startKey, endKey);

    internal Task<bool> ImportDurableTransactionStateToPartitionLeaderAsync(
        int partitionId,
        IReadOnlyList<Transactions.Data.TransactionRecord> records,
        IReadOnlyList<Transactions.Data.PreparedIntent> intents,
        CancellationToken cancellationToken) =>
        rangeStateTransfer.ImportDurableTransactionStateToPartitionLeaderAsync(partitionId, records, intents, cancellationToken);

    internal void ImportCompletionReceipts(IReadOnlyCollection<CompletionReceiptRecord> receiptsToImport) =>
        rangeStateTransfer.ImportCompletionReceipts(receiptsToImport);

    internal Task<bool> CopyRangeToPartitionAsync(
        string keySpace,
        string? startKey,
        string? endKey,
        HLCTimestamp snapshotTs,
        int sourcePartitionId,
        int destinationPartitionId,
        HLCTimestamp readerTransactionId,
        CancellationToken cancellationToken) =>
        rangeStateTransfer.CopyRangeToPartitionAsync(keySpace, startKey, endKey, snapshotTs, sourcePartitionId, destinationPartitionId, readerTransactionId, cancellationToken);

    internal Task<bool> ReplicateKeyValueRangePageToPartitionLeaderAsync(
        int partitionId, byte[] page, CancellationToken cancellationToken) =>
        rangeStateTransfer.ReplicateKeyValueRangePageToPartitionLeaderAsync(partitionId, page, cancellationToken);

    public Task<bool> ReplicateKeyValueRangePageLocal(int partitionId, byte[] page, CancellationToken cancellationToken) =>
        rangeStateTransfer.ReplicateKeyValueRangePageLocal(partitionId, page, cancellationToken);

    public Task<(bool Ok, List<CompletionReceiptRecord> Receipts, byte[] TransactionRecords, byte[] PreparedIntents)> GetRangeTransactionStateLocal(
        int partitionId, string? startKey, string? endKey, CancellationToken cancellationToken) =>
        rangeStateTransfer.GetRangeTransactionStateLocal(partitionId, startKey, endKey, cancellationToken);

    internal Task<(bool Ok, IReadOnlyCollection<CompletionReceiptRecord> Receipts, IReadOnlyList<TransactionRecord> Records, IReadOnlyList<PreparedIntent> Intents)> GetRangeTransactionStateFromPartitionLeaderAsync(
        int sourcePartitionId, string? startKey, string? endKey, CancellationToken cancellationToken) =>
        rangeStateTransfer.GetRangeTransactionStateFromPartitionLeaderAsync(sourcePartitionId, startKey, endKey, cancellationToken);

    internal Task<bool> ImportCompletionReceiptsReplicated(
        int partitionId,
        IReadOnlyCollection<CompletionReceiptRecord> receiptsToImport,
        CancellationToken cancellationToken) =>
        rangeStateTransfer.ImportCompletionReceiptsReplicated(partitionId, receiptsToImport, cancellationToken);

    internal Task<bool> ImportCompletionReceiptsToPartitionLeaderAsync(
        int partitionId,
        IReadOnlyCollection<CompletionReceiptRecord> receiptsToImport,
        CancellationToken cancellationToken) =>
        rangeStateTransfer.ImportCompletionReceiptsToPartitionLeaderAsync(partitionId, receiptsToImport, cancellationToken);

    internal Task<bool> ForgetCompletionReceiptsReplicated(
        int partitionId,
        IReadOnlyCollection<CompletionReceiptRecord> receiptsToForget,
        CancellationToken cancellationToken) =>
        rangeStateTransfer.ForgetCompletionReceiptsReplicated(partitionId, receiptsToForget, cancellationToken);

    internal Task<bool> ForgetCompletionReceiptsToPartitionLeaderAsync(
        int partitionId,
        IReadOnlyCollection<CompletionReceiptRecord> receiptsToForget,
        CancellationToken cancellationToken) =>
        rangeStateTransfer.ForgetCompletionReceiptsToPartitionLeaderAsync(partitionId, receiptsToForget, cancellationToken);


    /// <summary>Receives restore messages that haven't been checkpointed yet.</summary>
    public Task<bool> OnLogRestored(int partitionId, RaftLog log) => replicationDispatcher.OnLogRestored(partitionId, log);

    /// <summary>Applies a committed log entry to the key-value subsystem.</summary>
    public Task<bool> OnReplicationReceived(int partitionId, RaftLog log) => replicationDispatcher.OnReplicationReceived(partitionId, log);

    /// <summary>Invoked when a replication error occurs.</summary>
    public void OnReplicationError(RaftLog log) => replicationDispatcher.OnReplicationError(log);

    /// <summary>Invoked when a partition's leader changes.</summary>
    public Task<bool> OnLeaderChanged(int partitionId, string node) => replicationDispatcher.OnLeaderChanged(partitionId, node);

    /// <summary>Runs one auto-merge pass at the configured minimum range size.</summary>
    internal Task<int> TriggerAutoMergeAsync(CancellationToken ct = default) => keySpaceAdmin.TriggerAutoMergeAsync(ct);

    /// <summary>Runs one auto-merge pass at an explicit minimum range size.</summary>
    internal Task<int> TriggerAutoMergeAsync(int minMergeSize, CancellationToken ct = default) =>
        keySpaceAdmin.TriggerAutoMergeAsync(minMergeSize, ct);

    /// <summary>Runs one auto-split pass at the configured thresholds.</summary>
    internal Task<int> TriggerAutoSplitAsync(CancellationToken ct = default) => keySpaceAdmin.TriggerAutoSplitAsync(ct);

    /// <summary>Runs one auto-split pass at explicit thresholds.</summary>
    internal Task<int> TriggerAutoSplitAsync(int threshold, int minRangeSize, CancellationToken ct = default) =>
        keySpaceAdmin.TriggerAutoSplitAsync(threshold, minRangeSize, ct);

    /// <summary>Splits the range covering <paramref name="splitKey"/> at that key.</summary>
    internal Task<SplitOutcome> ForceSplitAtKeyAsync(
        string keySpace,
        string splitKey,
        Func<Task>? duringQuiesce = null,
        CancellationToken ct = default) =>
        keySpaceAdmin.ForceSplitAtKeyAsync(keySpace, splitKey, duringQuiesce, ct);


}
