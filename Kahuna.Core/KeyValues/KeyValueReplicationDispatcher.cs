using Kommander;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Data;
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

    // Per-partition apply progress: the highest log id this node's key-value subsystem applied, across restore
    // and the committed apply path, plus a version that is odd while an apply is in progress. Log ids are
    // shared across subsystems, so gaps prove nothing — but the value itself, logged in the leadership-change
    // fingerprint below, makes a node whose apply stream stalled (frozen id while peers advance) visible from
    // the node logs alone. The version lets the fingerprint read the applied id and the store counts as one
    // consistent snapshot without a lock on the apply path: applies are serialized per partition by Kommander,
    // so a reader that sees the same even version before and after its reads saw no apply in between.
    private readonly System.Collections.Concurrent.ConcurrentDictionary<int, ApplyProgress> applyProgressByPartition = new();

    private sealed class ApplyProgress
    {
        public long Version;

        public long LastApplied;
    }

    private ApplyProgress ProgressOf(int partitionId) =>
        applyProgressByPartition.GetOrAdd(partitionId, static _ => new ApplyProgress());

    /// <summary>Marks an apply in progress (odd version). Applies are serialized per partition, so no two overlap.</summary>
    private ApplyProgress BeginApply(int partitionId)
    {
        ApplyProgress progress = ProgressOf(partitionId);
        Interlocked.Increment(ref progress.Version);
        return progress;
    }

    /// <summary>Records the applied id (monotonic) and marks the apply complete (even version).</summary>
    private static void EndApply(ApplyProgress progress, long logId)
    {
        if (logId > Volatile.Read(ref progress.LastApplied))
            Volatile.Write(ref progress.LastApplied, logId);

        Interlocked.Increment(ref progress.Version);
    }

    // Partitions whose leader-change fingerprint comparison is in flight, so a burst of leadership changes
    // on one partition runs one comparison at a time instead of a pile of them. A change that lands while
    // one runs bumps the partition's generation, and the running comparison re-runs once for it.
    private readonly System.Collections.Concurrent.ConcurrentDictionary<int, byte> leaderChangeComparisonsInFlight = new();

    private readonly System.Collections.Concurrent.ConcurrentDictionary<int, long> leaderChangeGeneration = new();

    /// <summary>Bound on one comparison attempt, peers included.</summary>
    private const int ComparisonAttemptTimeoutMs = 5_000;

    /// <summary>
    /// How long a leader-change comparison keeps retrying while it cannot compare every peer — a peer that
    /// did not answer, or answered at another applied kv log id. Replicas of a partition whose leader just
    /// changed converge on the same applied id within a few hundred milliseconds once the tail is applied,
    /// and an indeterminate or partial comparison is not a pass: the divergences the fault soaks found were
    /// all caught at an exactly equal applied id, which a single attempt only sees by luck of timing.
    /// </summary>
    internal const int ComparisonRetryWindowMs = 10_000;

    /// <summary>Delay between comparison attempts inside the retry window.</summary>
    private const int ComparisonRetryDelayMs = 250;

    internal KeyValueReplicationDispatcher(KeyValuesRuntime runtime, KeyValueRestorer restorer, KeyValueReplicator replicator)
    {
        this.runtime = runtime;
        this.restorer = restorer;
        this.replicator = replicator;

        ApplyFingerprintProbe = new PartitionApplyFingerprintProbe(runtime.Raft, runtime.InterNodeCommunication, GetApplyFingerprint);
    }

    /// <summary>Compares this partition's apply fingerprint across replicas; shared with the split path.</summary>
    internal PartitionApplyFingerprintProbe ApplyFingerprintProbe { get; }

    /// <summary>Gates and relinquishes a partition whose local projection is proven incomplete.</summary>
    private PartitionDivergenceContainment containment => runtime.DivergenceContainment;

    /// <summary>
    /// This node's apply fingerprint for <paramref name="partitionId"/>, or null when the node does not host
    /// it. The applied log id is the subsystem's own high-water mark (0 before the first apply); the committed
    /// heads are the partition's ledger slice; the live intents are the prepared intents whose keys route to
    /// the partition. Both counts apply from the log alone, so replicas at the same applied id must agree.
    /// </summary>
    internal KeyValueApplyFingerprint? GetApplyFingerprint(int partitionId)
    {
        KeyValueApplyFingerprint? real = null;

        if (runtime.Raft.HostsPartition(partitionId))
            real = ReadConsistentFingerprint(partitionId);

        Func<int, KeyValueApplyFingerprint?, KeyValueApplyFingerprint?>? overrideForTesting = ApplyFingerprintOverrideForTesting;
        return overrideForTesting is null ? real : overrideForTesting(partitionId, real);
    }

    /// <summary>
    /// Wall-clock budget for a consistent fingerprint read under a continuous apply stream. The read is a
    /// diagnostic (promotion report, split gate, recovery cross-check), never a request path, so it can
    /// afford to outwait an apply that a loaded host descheduled mid-flight; a fixed spin count could not,
    /// and answered "could not read" on a healthy replica whenever the applying thread was slow.
    /// </summary>
    private const int FingerprintReadBudgetMs = 250;

    /// <summary>
    /// Reads the applied id and the two store counts as one snapshot: an apply that lands between the reads
    /// would pair one entry's counts with the previous entry's id and fake a divergence between replicas
    /// that agree. Null only when every attempt inside the budget raced an apply; the caller treats that as
    /// unknown, not as a count.
    /// </summary>
    private KeyValueApplyFingerprint? ReadConsistentFingerprint(int partitionId)
    {
        ApplyProgress progress = ProgressOf(partitionId);
        SpinWait spin = new();
        long deadline = Environment.TickCount64 + FingerprintReadBudgetMs;

        while (true)
        {
            long before = Volatile.Read(ref progress.Version);

            if ((before & 1) == 0)
            {
                long lastApplied = Volatile.Read(ref progress.LastApplied);
                int heads = runtime.PreparedIntentStore.CommittedHeadCountForPartition(partitionId);
                int intents = runtime.PreparedIntentStore.LiveIntentCountForPartition(partitionId);

                if (Volatile.Read(ref progress.Version) == before)
                    return new KeyValueApplyFingerprint(lastApplied, heads, intents);
            }

            if (Environment.TickCount64 >= deadline)
                return null;

            // Past the spin phase this yields and sleeps between attempts: a long wait must not burn a core.
            spin.SpinOnce();
        }
    }

    /// <summary>
    /// Test-only injection point that drops committed entries before they reach the stores while still
    /// recording them as applied — the shape of a snapshot install that raised the apply cursor without
    /// delivering the entries below it. Receives (partition, entry) and answers whether to drop it. The
    /// entry is acknowledged to Kommander as applied, exactly as the defective install did. Never wired in
    /// production paths.
    /// </summary>
    internal Func<int, RaftLog, bool>? ApplySkipForTesting { get; set; }

    /// <summary>
    /// Test-only injection point: receives (partition, real fingerprint or null when not hosted) and answers
    /// what this node reports in its place, on every reader — the inter-node surface and the local read the
    /// promotion and split comparisons make — so a fixture can make one replica report a diverged
    /// committed-head count without corrupting a real apply stream. Never wired in production paths.
    /// </summary>
    internal Func<int, KeyValueApplyFingerprint?, KeyValueApplyFingerprint?>? ApplyFingerprintOverrideForTesting { get; set; }

    /// <summary>Per-partition applied log ids for the gauge.</summary>
    internal IReadOnlyList<(int PartitionId, long AppliedLogId)> SnapshotAppliedLogIds()
    {
        List<(int, long)> applied = new(applyProgressByPartition.Count);
        foreach (KeyValuePair<int, ApplyProgress> entry in applyProgressByPartition)
            applied.Add((entry.Key, Volatile.Read(ref entry.Value.LastApplied)));
        return applied;
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
        ApplyProgress progress = BeginApply(partitionId);

        try
        {
            return RestoreCore(partitionId, log);
        }
        finally
        {
            EndApply(progress, log.Id);
        }
    }

    private Task<bool> RestoreCore(int partitionId, RaftLog log)
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
        ApplyProgress progress = BeginApply(partitionId);

        try
        {
            return ReplicateCore(partitionId, log);
        }
        finally
        {
            EndApply(progress, log.Id);
        }
    }

    private Task<bool> ReplicateCore(int partitionId, RaftLog log)
    {
        Func<int, RaftLog, bool>? skipForTesting = ApplySkipForTesting;
        if (skipForTesting is not null && skipForTesting(partitionId, log))
            return TrueTask;

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

        // Durable records apply HERE and only here, in Raft commit order, on every node — leader included. This
        // is the single live writer of the record and intent stores, which is what makes their state a pure
        // function of the partition's log. The write scheduler's completion for a locally proposed entry does
        // not apply the delta a second time (it could run ahead of this apply, or of the entries below it, or
        // long after it); it waits on the ledger for the result recorded here.
        if (log.LogType == ReplicationTypes.TransactionRecord)
        {
            durabilityTracker?.RegisterPending(partitionId, log.Id, DurabilityChannel.TransactionRecords);

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
            durableApplyResults.RecordApplied(partitionId, log.Id, preparedIntentStore.ApplyDeltaAckPrepares(partitionId, log));

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
    /// <para>Takes no corrective action. The per-key <c>InvalidateOrApply</c> messages from
    /// <see cref="KeyValueReplicator"/> keep every resident cache entry coherent as committed logs
    /// are applied on followers. Correctness at promotion depends on a node having applied all
    /// committed entries up to its commit frontier <em>before</em> it serves as leader — Kommander
    /// guarantees exactly that (it drains pending committed applies, and applies entries it
    /// commits via the leader path, before advertising the node as leader). With that guarantee a
    /// newly promoted leader's cache is already current, so no additional sweep or serving gate is
    /// needed here. The promotion-races-apply invariant is exercised by
    /// <c>PromotedLeader_RacingCommit_ServesLatestRevision</c>.</para>
    ///
    /// <para>What it does do is log the node's per-partition apply fingerprint: a violation of the
    /// promotion guarantee (a stalled apply stream, an empty committed-head memory) is silent in
    /// operation and only ever visible in hindsight, so every leadership change records the local
    /// evidence needed to attribute one from node logs.</para>
    /// </summary>
    public Task<bool> OnLeaderChanged(int partitionId, string node)
    {
        // A leadership change is exactly the moment a node's applied projection becomes (or stops being)
        // authoritative, and a stale projection at this moment is otherwise invisible: log the fingerprint —
        // the subsystem's highest applied log id for the partition plus the durable-transaction store sizes —
        // so that comparing this line across nodes localizes a silently stalled apply stream or an empty
        // committed-head memory without any metric scrape. One line per node per leadership change.
        if (logger.IsEnabled(LogLevel.Information))
        {
            long lastApplied = Volatile.Read(ref ProgressOf(partitionId).LastApplied);
            logger.LogInformation(
                "KeyValues: leader for partition {PartitionId} is now {Node} (local applied kv log id {LastApplied}, committed heads {CommittedHeads}, live intents {LiveIntents})",
                partitionId, node, lastApplied,
                runtime.PreparedIntentStore.CommittedHeadCountForPartition(partitionId), runtime.PreparedIntentStore.LiveIntentCountForPartition(partitionId));
        }

        bool leadingNow = node == runtime.Raft.GetLocalEndpoint();

        // The ordered-apply rendezvous parks a locally proposed entry's completion until this node's consumer
        // applies it, which is only worth waiting for while this node leads: an ex-leader's apply decides nothing
        // for the producer (the new leader applies and judges the same quorum-durable entry), and a stalled
        // ex-leader's apply may not advance for a long time. Release what is parked the moment leadership moves
        // away, and let completions park again once this node leads.
        if (leadingNow)
            durableApplyResults.NoteLeadershipRegained(partitionId);
        else
            ReleaseParkedCompletions(partitionId, "leader changed");

        // A leader change is the moment a divergent apply projection starts to serve as authoritative, and
        // the moment every replica can still be asked: every replica compares its fingerprint with its
        // peers' off the notification path. The comparison cannot veto the election — Kommander already
        // elected — so a short leader relinquishes right after it, and a short follower gates itself so a
        // later election of it is relinquished on the spot. The report and the counter stay the
        // error-level signal that makes a replica that silently dropped part of its apply stream visible.
        leaderChangeGeneration.AddOrUpdate(partitionId, 1, static (_, generation) => generation + 1);

        if (leaderChangeComparisonsInFlight.TryAdd(partitionId, 0))
            _ = RunLeaderChangeComparisonsAsync(partitionId);

        return Task.FromResult(true);
    }

    /// <summary>
    /// Runs the leader-change comparison for the partition, and again for every leader change that landed
    /// while it ran: the comparison takes seconds under retry, and the change it would have missed is
    /// typically the one that promoted this node.
    /// </summary>
    private async Task RunLeaderChangeComparisonsAsync(int partitionId)
    {
        while (true)
        {
            leaderChangeGeneration.TryGetValue(partitionId, out long seen);

            await CompareAtLeaderChangeAsync(partitionId).ConfigureAwait(false);

            leaderChangeGeneration.TryGetValue(partitionId, out long now);
            if (now != seen)
                continue;

            leaderChangeComparisonsInFlight.TryRemove(partitionId, out _);

            // A change that landed between the check and the removal found the slot taken and did not start
            // a run of its own: take the slot back for it, or leave it to the run that did.
            leaderChangeGeneration.TryGetValue(partitionId, out now);
            if (now == seen || !leaderChangeComparisonsInFlight.TryAdd(partitionId, 0))
                return;
        }
    }

    /// <summary>Marks the partition as no longer led here and releases every durable completion parked on its
    /// ordered apply (see <see cref="DurableApplyResultLedger.NoteLeadershipLost"/>). Called from the leader-changed
    /// and leadership-lost notifications alike; idempotent.</summary>
    internal void ReleaseParkedCompletions(int partitionId, string reason)
    {
        int released = durableApplyResults.NoteLeadershipLost(partitionId);
        if (released > 0)
            logger.LogDurableCompletionsReleasedOnLeadershipLoss(released, partitionId, reason);
    }

    private async Task CompareAtLeaderChangeAsync(int partitionId)
    {
        const string moment = "leader change";

        try
        {
            // A node already gated by earlier evidence does not need the probe to know it must not lead.
            if (await containment.OnPromotedAsync(partitionId).ConfigureAwait(false))
                return;

            string local = raft.GetLocalEndpoint();

            ApplyFingerprintComparison? comparison = await CompareUntilConclusiveAsync(partitionId, moment).ConfigureAwait(false);
            if (comparison is null || !comparison.HasDivergence)
                return;

            // Whether this node reports is decided by the comparison's own view of who leads, not by the
            // notification that started it: leadership can move again while the comparison retries.
            bool leadingNow = comparison.Leader == local;

            // Each replica's fingerprint is one consistent snapshot, but the leader's and a peer's are taken
            // moments apart, and containment moves leadership, which is not free: a divergence that indicts
            // this node is acted on only when a second, independent pass still indicts it.
            if (comparison.IsBehind(local))
            {
                ApplyFingerprintComparison? confirmation = await CompareUntilConclusiveAsync(partitionId, moment).ConfigureAwait(false);

                if (confirmation is null || !confirmation.IsBehind(local))
                {
                    logger.LogApplyDivergenceNotConfirmed(local, partitionId);

                    if (confirmation is { HasDivergence: true } && confirmation.Leader == local)
                        ReportDivergence(confirmation, moment);

                    return;
                }

                comparison = confirmation;
                leadingNow = comparison.Leader == local;
            }

            // One report per detection, from the leader: every replica compares, and the leader's report is
            // the one that names each peer once.
            if (leadingNow)
                ReportDivergence(comparison, moment);

            await ContainIfLocalBehindAsync(comparison, moment).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "KeyValues: apply fingerprint comparison at leader change of partition {PartitionId} did not complete", partitionId);
        }
    }

    /// <summary>
    /// Runs the comparison until it is conclusive (every peer compared at the leader's applied kv log id), a
    /// divergence is found, or the retry window closes. Returns the last determinate comparison — which may
    /// be partial, and is then logged and counted as inconclusive — or null when no attempt was determinate
    /// (routine at startup, when the cluster is still initializing).
    /// </summary>
    private async Task<ApplyFingerprintComparison?> CompareUntilConclusiveAsync(int partitionId, string moment)
    {
        long deadline = Environment.TickCount64 + ComparisonRetryWindowMs;
        ApplyFingerprintComparison? last = null;

        while (true)
        {
            using CancellationTokenSource timeout = new(ComparisonAttemptTimeoutMs);

            ApplyFingerprintComparison comparison = await ApplyFingerprintProbe
                .CompareWithReplicasAsync(partitionId, timeout.Token).ConfigureAwait(false);

            if (comparison.IsDeterminate)
            {
                last = comparison;

                if (comparison.IsConclusive || comparison.HasDivergence)
                    return comparison;
            }

            if (Environment.TickCount64 >= deadline)
                break;

            await Task.Delay(ComparisonRetryDelayMs).ConfigureAwait(false);
        }

        if (last is null)
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("KeyValues: apply fingerprint comparison at {Moment} of partition {PartitionId} was indeterminate for the whole retry window", moment, partitionId);
        }
        else
        {
            KeyValueApplyMetrics.InconclusiveComparisons.Add(1);
            logger.LogApplyFingerprintInconclusive(
                partitionId, moment, raft.GetLocalEndpoint(), ComparisonRetryWindowMs, last.PeersCompared, last.PeersAsked, last.PeersAnswered);
        }

        return last;
    }

    /// <summary>
    /// Logs every divergent peer of a determinate comparison at error level, naming the replica the
    /// evidence indicts, and counts it. Shared by the leader-change report and the split's pre-copy check
    /// so both moments produce the same evidence.
    /// </summary>
    internal void ReportDivergence(ApplyFingerprintComparison comparison, string moment)
    {
        if (!comparison.IsDeterminate)
        {
            logger.LogWarning(
                "KeyValues: apply fingerprint comparison of partition {PartitionId} at {Moment} is indeterminate (leader {Leader})",
                comparison.PartitionId, moment, comparison.Leader ?? "unresolved");
            return;
        }

        if (!comparison.HasDivergence)
            return;

        foreach (ApplyDivergentPeer divergent in comparison.Divergent)
        {
            KeyValueApplyMetrics.DivergenceDetected.Add(1);
            logger.LogApplyFingerprintDivergence(
                comparison.PartitionId, moment, comparison.Leader!,
                comparison.LeaderFingerprint.CommittedHeads, comparison.LeaderFingerprint.LiveIntents,
                divergent.Peer, divergent.Fingerprint.CommittedHeads, divergent.Fingerprint.LiveIntents,
                comparison.LeaderFingerprint.AppliedLogId, divergent.DescribeSide(comparison.Leader!));
        }
    }

    /// <summary>
    /// Hands a comparison that indicts this node to containment: gate the partition here and, when this
    /// node leads it, relinquish leadership to the fullest peer. A comparison that indicts only other
    /// replicas changes nothing here — each of them runs the same comparison at the same leader change and
    /// gates itself.
    /// </summary>
    internal async Task ContainIfLocalBehindAsync(ApplyFingerprintComparison comparison, string moment)
    {
        if (!comparison.IsDeterminate || comparison.Leader is null)
            return;

        string local = raft.GetLocalEndpoint();

        if (!comparison.IsBehind(local))
            return;

        string fuller;
        string evidence;

        if (local == comparison.Leader)
        {
            fuller = comparison.FullerPeerThanLeader()!;
            ApplyDivergentPeer witness = default;
            foreach (ApplyDivergentPeer divergent in comparison.Divergent)
            {
                if (divergent.Peer == fuller)
                {
                    witness = divergent;
                    break;
                }
            }

            evidence = $"as leader it holds {comparison.LeaderFingerprint.CommittedHeads} committed heads and {comparison.LeaderFingerprint.LiveIntents} live intents while replica {fuller} holds {witness.Fingerprint.CommittedHeads} heads and {witness.Fingerprint.LiveIntents} intents at the same applied kv log id {comparison.LeaderFingerprint.AppliedLogId}";
        }
        else
        {
            fuller = comparison.Leader;
            ApplyDivergentPeer self = default;
            foreach (ApplyDivergentPeer divergent in comparison.Divergent)
            {
                if (divergent.Peer == local)
                {
                    self = divergent;
                    break;
                }
            }

            evidence = $"as a follower it holds {self.Fingerprint.CommittedHeads} committed heads and {self.Fingerprint.LiveIntents} live intents while leader {fuller} holds {comparison.LeaderFingerprint.CommittedHeads} heads and {comparison.LeaderFingerprint.LiveIntents} intents at the same applied kv log id {comparison.LeaderFingerprint.AppliedLogId}";
        }

        await containment.ContainAsync(comparison.PartitionId, fuller, evidence, moment).ConfigureAwait(false);
    }

    /// <summary>
    /// A whole-partition install replaced this node's projection with one that reflects every entry at or
    /// below <paramref name="upToIndex"/>: the applied high-water mark moves there, since those entries never
    /// arrive through the apply path on this node. Wired as the transfer's installed-boundary observer.
    /// </summary>
    internal void NoteInstalledThrough(int partitionId, long upToIndex)
    {
        // The install ran on the partition's executor, where applies are serialized, so it overlaps no apply;
        // bracketing it like one keeps a concurrent fingerprint read from pairing the old id with the new counts.
        ApplyProgress progress = BeginApply(partitionId);
        EndApply(progress, upToIndex);
    }
}
