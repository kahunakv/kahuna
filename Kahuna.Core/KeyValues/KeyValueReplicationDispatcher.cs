using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Persistence;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The Raft-facing side of the key-value subsystem: restore replay for entries not yet checkpointed,
/// the committed-entry apply path, replication errors, and leader-change notifications. Every log type
/// the subsystem replicates (<c>kv</c>, <c>rangemap</c>, <c>snapshotfloor</c> and the durable-2PC records)
/// is dispatched from here to the store or replicator that owns it.
/// </summary>
internal sealed class KeyValueReplicationDispatcher
{
    // The consumer-apply and restore paths return a Task<bool> per committed log entry;
    // Task.FromResult(bool) has no cached instances, so these two singletons keep the
    // per-entry apply from allocating a fresh task cluster-wide.
    private static readonly Task<bool> TrueTask = Task.FromResult(true);

    private static readonly Task<bool> FalseTask = Task.FromResult(false);

    private static Task<bool> BoolTask(bool value) => value ? TrueTask : FalseTask;

    private readonly KeyValuesRuntime runtime;

    private readonly KeyValueRestorer restorer;

    private readonly KeyValueReplicator replicator;

    internal KeyValueReplicationDispatcher(KeyValuesRuntime runtime, KeyValueRestorer restorer, KeyValueReplicator replicator)
    {
        this.runtime = runtime;
        this.restorer = restorer;
        this.replicator = replicator;
    }

    // Aliases matching the field names the moved bodies use, so those bodies stay byte-for-byte as they were.
    private IRaft raft => runtime.Raft;

    private ILogger<IKahuna> logger => runtime.Logger;

    private RangeMapStore rangeMapStore => runtime.RangeMapStore;

    private KeySpaceRegistry keySpaceRegistry => runtime.KeySpaceRegistry;

    private SnapshotFloorStore snapshotFloorStore => runtime.SnapshotFloorStore;

    private CompletionReceiptStore completionReceiptStore => runtime.CompletionReceiptStore;

    private TransactionRecordStore transactionRecordStore => runtime.TransactionRecordStore;

    private PreparedIntentStore preparedIntentStore => runtime.PreparedIntentStore;

    private DurableApplyResultLedger durableApplyResults => runtime.DurableApplyResults;

    private PartitionDurabilityTracker? durabilityTracker => runtime.DurabilityTracker;

    /// <summary>
    /// Reconciles the per-node key-space registry with the key spaces the committed range map covers.
    /// Static because the manager's constructor runs it before this dispatcher (which needs the restorer
    /// and replicator) can exist.
    /// </summary>
    internal static void SyncKeySpaceRegistryFromRangeMap(RangeMapStore rangeMapStore, KeySpaceRegistry keySpaceRegistry)
    {
        HashSet<string> live = [];

        foreach (RangeDescriptor descriptor in rangeMapStore.Current.Descriptors)
        {
            if (!string.IsNullOrEmpty(descriptor.KeySpace))
                live.Add(descriptor.KeySpace);
        }

        keySpaceRegistry.ReconcileTo(live);
    }



    
    /// <summary>
    /// Receives restore messages that haven't been checkpointed yet.
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="log"></param>
    /// <returns></returns>
    public Task<bool> OnLogRestored(int partitionId, RaftLog log)
    {
        if (log.LogType == ReplicationTypes.RangeMap)
        {
            RegisterSynchronousApply(partitionId, log.Id);
            bool result = rangeMapStore.Restore(partitionId, log);
            // A replayed range-descriptor entry may introduce key spaces that are not yet in the
            // per-node KeySpaceRegistry. Sync so that routing on this node matches the restored map.
            if (result)
            {
                SyncKeySpaceRegistryFromRangeMap(rangeMapStore, keySpaceRegistry);
                durabilityTracker?.Resolve(partitionId, log.Id);
            }
            return BoolTask(result);
        }

        if (log.LogType == ReplicationTypes.SnapshotFloor)
        {
            RegisterSynchronousApply(partitionId, log.Id);
            bool result = snapshotFloorStore.Restore(partitionId, log);
            if (result)
                durabilityTracker?.Resolve(partitionId, log.Id);
            return BoolTask(result);
        }

        if (log.LogType == ReplicationTypes.TransactionRecord)
        {
            durabilityTracker?.RegisterPending(partitionId, log.Id, DurabilityChannel.TransactionRecords);
            bool result = transactionRecordStore.Restore(partitionId, log);
            if (result)
                durabilityTracker?.MarkApplied(partitionId, log.Id, DurabilityChannel.TransactionRecords);
            return BoolTask(result);
        }

        if (log.LogType == ReplicationTypes.PreparedIntent)
        {
            durabilityTracker?.RegisterPending(partitionId, log.Id, DurabilityChannel.PreparedIntents);
            bool result = preparedIntentStore.Restore(partitionId, log);
            if (result)
                durabilityTracker?.MarkApplied(partitionId, log.Id, DurabilityChannel.PreparedIntents);
            return BoolTask(result);
        }

        if (log.LogType == ReplicationTypes.CompletionReceipt)
        {
            durabilityTracker?.RegisterPending(partitionId, log.Id, DurabilityChannel.Receipts);
            bool result = completionReceiptStore.Restore(partitionId, log);
            if (result)
                durabilityTracker?.MarkApplied(partitionId, log.Id, DurabilityChannel.Receipts);
            return BoolTask(result);
        }

        return BoolTask(log.LogType != ReplicationTypes.KeyValues || restorer.Restore(partitionId, log));
    }

    /// <summary>
    /// Registers a WAL entry whose apply persists synchronously (range map, snapshot floor): it is
    /// tracked pending across the apply so a failed apply leaves the durability floor below it, and
    /// resolves immediately on success. The Flush channel is only nominal — the resolve happens
    /// inline, never through a background flush.
    /// </summary>
    private void RegisterSynchronousApply(int partitionId, long logIndex) =>
        durabilityTracker?.RegisterPending(partitionId, logIndex, DurabilityChannel.Flush);

    /// <summary>
    /// Receives replication messages once they're committed to the Raft log.
    /// </summary>
    /// <param name="partitionId"></param>
    /// <param name="log"></param>
    /// <returns></returns>
    public Task<bool> OnReplicationReceived(int partitionId, RaftLog log)
    {
        if (log.LogType == ReplicationTypes.RangeMap)
        {
            RegisterSynchronousApply(partitionId, log.Id);
            bool result = rangeMapStore.Replicate(partitionId, log);
            // Keep the per-node KeySpaceRegistry in sync with every replicated range-descriptor
            // update. Without this, follower nodes that receive a new key-range descriptor via Raft
            // still route the corresponding key space via hash (the default), causing 2PC prepare
            // to be sent to the wrong partition and the transaction to be aborted.
            if (result)
            {
                SyncKeySpaceRegistryFromRangeMap(rangeMapStore, keySpaceRegistry);
                durabilityTracker?.Resolve(partitionId, log.Id);
            }
            return BoolTask(result);
        }

        if (log.LogType == ReplicationTypes.SnapshotFloor)
        {
            RegisterSynchronousApply(partitionId, log.Id);
            bool result = snapshotFloorStore.Replicate(partitionId, log);
            if (result)
                durabilityTracker?.Resolve(partitionId, log.Id);
            return BoolTask(result);
        }

        // Durable records apply here, in Raft commit order, on every node. On the leader the write scheduler's
        // completion applies the identical delta too, and the fast-path ticket release means either side can run
        // first — so whichever applies records the outcome against the log entry, and the other consumes it
        // instead of deserializing and applying the same delta a second time.
        if (log.LogType == ReplicationTypes.TransactionRecord)
        {
            durabilityTracker?.RegisterPending(partitionId, log.Id, DurabilityChannel.TransactionRecords);

            if (durableApplyResults.TryConsume(partitionId, log.Id, out bool recorded))
            {
                if (recorded)
                    durabilityTracker?.MarkApplied(partitionId, log.Id, DurabilityChannel.TransactionRecords);
                return BoolTask(recorded);
            }

            bool applied = transactionRecordStore.Replicate(partitionId, log);
            durableApplyResults.RecordApplied(partitionId, log.Id, applied);
            if (applied)
                durabilityTracker?.MarkApplied(partitionId, log.Id, DurabilityChannel.TransactionRecords);
            return BoolTask(applied);
        }

        if (log.LogType == ReplicationTypes.PreparedIntent)
        {
            durabilityTracker?.RegisterPending(partitionId, log.Id, DurabilityChannel.PreparedIntents);

            // The prepare acknowledgement is what a local producer needs from this apply; a prepare rejected on its
            // merits is still a successfully applied log entry, so it never fails replication.
            if (!durableApplyResults.TryConsume(partitionId, log.Id, out _))
                durableApplyResults.RecordApplied(partitionId, log.Id, preparedIntentStore.ApplyDeltaAckPrepares(log));

            // A rejected prepare is still an applied entry (the store recorded the rejection), so the
            // snapshot that covers this apply certifies it either way.
            durabilityTracker?.MarkApplied(partitionId, log.Id, DurabilityChannel.PreparedIntents);

            return TrueTask;
        }

        if (log.LogType == ReplicationTypes.CompletionReceipt)
        {
            durabilityTracker?.RegisterPending(partitionId, log.Id, DurabilityChannel.Receipts);
            bool result = completionReceiptStore.Replicate(partitionId, log);
            if (result)
                durabilityTracker?.MarkApplied(partitionId, log.Id, DurabilityChannel.Receipts);
            return BoolTask(result);
        }

        return BoolTask(log.LogType != ReplicationTypes.KeyValues || replicator.Replicate(partitionId, log));
    }

    /// <summary>
    /// Registers every key space that has a descriptor in the current range map into the local
    /// <see cref="KeySpaceRegistry"/>. This is the missing link between the replicated/restored
    /// range-descriptor map and the per-node routing-mode table: the map is shared across the
    /// cluster via Raft, but the registry is node-local and must be kept in sync explicitly.
    /// <para>
    /// Any key space present in the range map was placed there through <see cref="MutateAsync"/>,
    /// which in turn requires passing through <see cref="KeySpaceRegistry.RegisterKeyRange"/> on
    /// the seeding node. That path already enforces all validity rules (e.g. non-empty, not a
    /// reserved space). Re-applying those rules here would duplicate consumer-specific knowledge
    /// inside Kahuna; instead we call <c>RegisterKeyRange</c> unconditionally and let it enforce
    /// its own invariants if a descriptor ever arrives with an invalid key space.
    /// </para>
    /// </summary>
    /// <summary>
    /// Invoken when a replication error occurs.
    /// </summary>
    /// <param name="log"></param>
    public void OnReplicationError(RaftLog log)
    {
        logger.LogError("Replication error: #{Id} {Type}", log.Id, log.LogType);
    }

    /// <summary>
    /// Called by Kommander when partition leadership changes.
    ///
    /// <para>Intentionally a no-op. The per-key <c>InvalidateOrApply</c> messages from
    /// <see cref="KeyValueReplicator"/> keep every resident cache entry coherent as committed logs
    /// are applied on followers. Correctness at promotion depends on a node having applied all
    /// committed entries up to its commit frontier <em>before</em> it serves as leader — Kommander
    /// now guarantees exactly that (it drains pending committed applies, and applies entries it
    /// commits via the leader path, before advertising the node as leader). With that guarantee a
    /// newly promoted leader's cache is already current, so no additional sweep or serving gate is
    /// needed here. The promotion-races-apply invariant is exercised by
    /// <c>PromotedLeader_RacingCommit_ServesLatestRevision</c>.</para>
    /// </summary>
    public Task<bool> OnLeaderChanged(int partitionId, string node)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("KeyValues: leader for partition {PartitionId} is now {Node}", partitionId, node);

        return Task.FromResult(true);
    }
}
