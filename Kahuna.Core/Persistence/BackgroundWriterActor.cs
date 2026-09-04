
using System.Collections.Concurrent;
using Nixie;
using Kommander;
using Kommander.WAL.IO;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Logging;
using Kahuna.Server.Persistence.Pitr;
using Kommander.System;
using Kommander.Time;
using Polly.Contrib.WaitAndRetry;
using Kahuna.Utils;

namespace Kahuna.Server.Persistence;

/// <summary>
/// Represents an actor responsible for handling background write operations, performing tasks
/// such as storing locks and key-value pairs, and triggering periodic flush operations.
///
/// Writes dirty lock/key-value objects from memory to disk in batches
/// before they are forced out by backend processes.
/// </summary>
/// <remarks>
/// The <c>BackgroundWriterActor</c> processes requests of type <see cref="BackgroundWriteRequest"/>
/// and executes operations based on the request type. It supports queueing locks and key-value write
/// requests, as well as flushing data at scheduled intervals.
/// </remarks>
internal sealed class BackgroundWriterActor : IActor<BackgroundWriteRequest>
{
    /// <summary>
    /// In case of failure, the number of retries to write the data to the persistence backend.
    /// </summary>
    private const int WriteRetries = 5;
    
    /// <summary>
    /// Maximum number of items to be written in a single batch.
    /// </summary>
    private const int MaxBatchSize = 1024;
    
    /// <summary>
    /// Maximum size of a packet to be written in a single batch.
    /// </summary>
    private const int MaxPacketSize = 1024 * 512;

    /// <summary>
    /// Upper bound on the number of keys queued for targeted revision cleanup.
    /// Keys added beyond this limit are dropped; the periodic sweep covers them instead.
    /// </summary>
    private const int MaxPendingCleanupKeys = 10_000;

    /// <summary>
    /// Consecutive flush cycles that must fail with a storage fault before the writer asks the
    /// backend to recover its storage engine. Each cycle already spends the full in-cycle retry
    /// schedule (<see cref="WriteRetries"/> attempts on a ~1s backoff), so this threshold
    /// represents sustained failure, not a blip. Kept as a mutable internal so tests can drive
    /// the recovery path without waiting through several real cycles.
    /// </summary>
    internal int StoreFailureCyclesBeforeRecovery = 3;

    /// <summary>
    /// Fixed queue key for every batch submitted to the dedicated writer scheduler. Batches aggregate keys
    /// across many partitions into one backend call, so there is no single "real" partition to key on; and
    /// because the writer scheduler is its own instance, nothing else shares its queues — the old collision
    /// with meta-partition WAL reads (from keying background writes onto Raft's shared read pool at partition
    /// 0) is gone by construction. Read-your-flush correctness comes from the FlushedRevision ack, never from
    /// queue ordering, so a single serialized writer queue is both correct and simplest.
    /// </summary>
    private const int WriterQueueKey = 0;

    private readonly IRaft raft;

    private readonly IRaftReadScheduler backendWriteScheduler;

    private readonly IPersistenceBackend persistenceBackend;

    private readonly SnapshotFloorStore? snapshotFloorStore;

    private readonly CompletionReceiptStore? completionReceiptStore;

    private readonly TransactionRecordStore? transactionRecordStore;

    private readonly PreparedIntentStore? preparedIntentStore;

    /// <summary>Per-partition application-durability floor tracker, shared node-wide. Null when the
    /// node does not participate in durability-floor reporting.</summary>
    private readonly PartitionDurabilityTracker? durabilityTracker;

    private readonly KahunaConfiguration configuration;

    private readonly ILogger<IKahuna> logger;

    private readonly FlushNotificationSink flushNotificationSink;

    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> self;
    
    /// <summary>
    /// Dirty locks that need to be written to the persistence backend.
    /// </summary>
    private readonly Queue<BackgroundWriteRequest> dirtyLocks = new();
    
    /// <summary>
    /// Dirty key-values that need to be written to the persistence backend.
    /// </summary>
    private readonly Queue<BackgroundWriteRequest> dirtyKeyValues = new();

    // Highest LastModified HLC enqueued for persistence per partition. Every committed key-value write
    // — from the leader's proposal completion and from follower replication alike — is sent here as a
    // QueueStoreKeyValue, so this is the single point that observes "applied and queued". A full backup
    // waits until this reaches each partition's captured ToHlc before flushing, so the checkpoint can
    // never miss a committed write that had not yet been applied/enqueued when the flush ran.
    private readonly ConcurrentDictionary<int, HLCTimestamp> maxEnqueuedHlc = new();
    
    /// <summary>
    /// Partitions awaiting a checkpoint, mapped to the moment each one first went dirty after its
    /// last checkpoint. The stamp is written once per checkpoint cycle and deliberately NOT moved
    /// forward by later writes: the checkpoint gate measures the interval from this stamp, so
    /// refreshing it on every write would turn <see cref="KahunaConfiguration.CheckpointInterval"/>
    /// into "this partition has been quiet for that long" — a condition a continuously-written
    /// partition never meets, leaving its WAL uncompactable forever. The entry is removed when the
    /// partition checkpoints, so the next write starts a fresh interval.
    /// </summary>
    private readonly Dictionary<int, DateTime> partitionDirtySince = [];
    
    private readonly Stopwatch stopwatch = Stopwatch.StartNew();
    
    /// <summary>
    /// If a write fails, these lock items are kept in memory until the next write attempt.
    /// </summary>
    private List<PersistenceRequestItem>? pendingLockItems;

    /// <summary>
    /// If a write fails, these key/value items are kept in memory until the next write attempt.
    /// </summary>
    private List<PersistenceRequestItem>? pendingKeyValuesItems;

    /// <summary>
    /// WAL indexes of the retained lock batch, resolved against the durability tracker when the
    /// retried flush finally lands. Travels with <see cref="pendingLockItems"/>.
    /// </summary>
    private List<(int PartitionId, long LogIndex)>? pendingLockIndexes;

    /// <summary>
    /// WAL indexes of the retained key/value batch, resolved against the durability tracker when
    /// the retried flush finally lands. Travels with <see cref="pendingKeyValuesItems"/>.
    /// </summary>
    private List<(int PartitionId, long LogIndex)>? pendingKeyValueIndexes;

    /// <summary>
    /// Lock batch list reused across flush cycles: the wide-struct backing array is allocated once
    /// and kept at its converged capacity, instead of a fresh list per batch that regrows through
    /// repeated doubling. Invariant: a list parked here is empty. The field is taken (and nulled) at
    /// batch start; on success the consumed batch is cleared and parked back; on failure the batch
    /// is surrendered to <see cref="pendingLockItems"/> for retry and this field stays null, so the
    /// next batch can never alias a parked retry batch.
    /// </summary>
    private List<PersistenceRequestItem>? reusableLockItems;

    /// <summary>
    /// Key/value analog of <see cref="reusableLockItems"/>; surrendered to
    /// <see cref="pendingKeyValuesItems"/> on failure.
    /// </summary>
    private List<PersistenceRequestItem>? reusableKeyValuesItems;

    /// <summary>
    /// Reused WAL-index list for lock batches; same invariant and surrender rule as
    /// <see cref="reusableLockItems"/>.
    /// </summary>
    private List<(int PartitionId, long LogIndex)>? reusableLockIndexes;

    /// <summary>
    /// Reused WAL-index list for key/value batches; same invariant and surrender rule as
    /// <see cref="reusableKeyValuesItems"/>.
    /// </summary>
    private List<(int PartitionId, long LogIndex)>? reusableKeyValueIndexes;

    /// <summary>
    /// Mutation identities already drained into the key/value batch being assembled, so the routine
    /// duplicate of the same committed mutation — queued once by the owning actor and once by the
    /// Raft consumer — is collapsed to one physical write before any backend I/O. WAL indexes travel
    /// beside the items, so a collapsed duplicate still advances the durability floor when the
    /// surviving write lands; its flush acknowledgement would be byte-identical to the survivor's.
    /// Cleared per assembled batch; duplicates split across batches are suppressed by the backend's
    /// monotonic current-head guard instead.
    /// </summary>
    private readonly HashSet<(string Key, long Revision, int LmNode, long LmPhysical, uint LmCounter, int State, bool NoRevision)> drainedKeyValueIdentities = new();

    /// <summary>
    /// Per-partition floors already persisted to the backend, so a flush cycle only writes floors
    /// that actually advanced.
    /// </summary>
    private readonly Dictionary<int, long> lastPersistedFloors = [];
    
    /// <summary>
    /// Whether a checkpoint operation is pending.
    /// </summary>
    private bool pendingCheckpoint;

    /// <summary>
    /// True when a store call has failed with a storage fault — a thrown storage exception or a
    /// <c>false</c> contract return — since the last recovery evaluation. Writer-queue
    /// backpressure does not set it: a saturated queue is not a storage-engine fault, and a
    /// close-and-reopen would not help it.
    /// </summary>
    private bool storeFaultSinceLastRecoveryCheck;

    /// <summary>
    /// True when any store call has succeeded since the last recovery evaluation. A single
    /// success proves the engine writes, so it resets the wedge count even when another store in
    /// the same cycle failed (that failure is then not an engine wedge).
    /// </summary>
    private bool storeSuccessSinceLastRecoveryCheck;

    /// <summary>
    /// Consecutive flush cycles in which stores only failed. At
    /// <see cref="StoreFailureCyclesBeforeRecovery"/> the backend is declared wedged and asked
    /// to recover. Internal read access is the operator/test-visible wedge counter.
    /// </summary>
    private int consecutiveStoreFailureCycles;

    /// <summary>Consecutive flush cycles whose stores only failed. Test/observability only.</summary>
    internal int ConsecutiveStoreFailureCycles => consecutiveStoreFailureCycles;

    /// <summary>
    /// Distinct keys touched by successful key-value flushes that are queued for targeted
    /// revision cleanup. Bounded by <see cref="MaxPendingCleanupKeys"/>.
    /// </summary>
    private readonly HashSet<string> pendingRevisionCleanupKeys = new(StringComparer.Ordinal);

    /// <summary>
    /// Tracks whether the full backend-wide revision sweep has a batch still in progress
    /// from a previous cycle that hit the batch size limit.
    /// </summary>
    private bool fullSweepBacklogPending;

    /// <summary>
    /// Wall-clock timestamp of the last completed (or attempted) full backend-wide sweep,
    /// used to enforce <see cref="KahunaConfiguration.PersistentRevisionCleanupInterval"/>.
    /// Initialised to <see cref="DateTime.UtcNow"/> so the first sweep is not triggered until
    /// at least one full interval has elapsed after startup.
    /// </summary>
    private DateTime lastFullSweepUtc = DateTime.UtcNow;

    /// <summary>
    /// Wall-clock timestamp of the last PITR horizon tick. Initialised to MinValue so the
    /// first tick fires immediately on startup (re-asserting the floor after a restart).
    /// </summary>
    private DateTime lastPitrTickUtc = DateTime.MinValue;

    /// <summary>
    /// Called on the scheduler thread immediately before the prune floor is sampled
    /// (<c>SnapshotFloorStore.BeginPrune</c>) during both targeted cleanup and full sweep. Null in
    /// production; set by tests to gate the prune task before the floor is sampled so that a hold
    /// acquired here is reflected in the sampled floor.
    /// </summary>
    /// <summary>
    /// The highest LastModified HLC that has been enqueued for persistence for <paramref name="partitionId"/>,
    /// or <see cref="HLCTimestamp.Zero"/> if none. Thread-safe. A full backup polls this to wait for the
    /// apply/enqueue pipeline to catch up to a captured watermark before flushing.
    /// </summary>
    internal HLCTimestamp GetMaxEnqueuedHlc(int partitionId) =>
        maxEnqueuedHlc.TryGetValue(partitionId, out HLCTimestamp v) ? v : HLCTimestamp.Zero;

    internal Action? BeforePruneSampleHook;

    /// <summary>
    /// Called on the scheduler thread after the prune floor is sampled and the prune-delete window
    /// is open, but immediately before the backend delete runs. Null in production; set by tests to
    /// acquire a hold inside the delete window and verify that acquisition fails closed rather than
    /// letting the delete remove a boundary the sample never saw.
    /// </summary>
    internal Action? AfterPruneSampleHook;

    /// <summary>
    /// Test seam replacing the meta-partition catch-up confirmation that gates destructive
    /// revision prunes. Null in production (the real <c>ConfirmLocalApplicationAsync</c> runs);
    /// set by tests to force the confirmed/unconfirmed outcome without controlling elections.
    /// </summary>
    internal Func<ValueTask<bool>>? ConfirmPruneFreshnessOverride;

    /// <summary>
    /// A destructive revision prune must never run from a possibly-stale local hold registry: a
    /// node whose meta-partition application lags the cluster (a fresh join before the P0 state
    /// transfer, or a partitioned node whose prune timer keeps firing) sees an empty or stale
    /// hold set and would delete revisions the cluster still holds. Confirm that this node's
    /// applied frontier covers the cluster's commit frontier before sampling the floor; on
    /// failure the cycle is skipped and retried later — disk retention grows until the node
    /// catches up, which is the safe direction. Holds committed after this confirmation are
    /// covered on the leader by the acquire/prune-window generation protocol; cross-node, the
    /// residual exposure narrows from unbounded staleness to the confirmation→sample window.
    /// With no floor store there are no holds to protect and no gate is needed.
    /// </summary>
    private ValueTask<bool> ConfirmFloorRegistryFreshness()
    {
        if (snapshotFloorStore is null)
            return ValueTask.FromResult(true);

        if (ConfirmPruneFreshnessOverride is not null)
            return ConfirmPruneFreshnessOverride();

        return raft.ConfirmLocalApplicationAsync(RangeMapStore.MetaPartitionId);
    }


    public BackgroundWriterActor(
        IActorContext<BackgroundWriterActor, BackgroundWriteRequest> context,
        IRaft raft,
        IRaftReadScheduler backendWriteScheduler,
        IPersistenceBackend persistenceBackend,
        SnapshotFloorStore? snapshotFloorStore,
        CompletionReceiptStore? completionReceiptStore,
        TransactionRecordStore? transactionRecordStore,
        PreparedIntentStore? preparedIntentStore,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger,
        FlushNotificationSink flushNotificationSink,
        PartitionDurabilityTracker? durabilityTracker = null
    )
    {
        this.raft = raft;
        this.backendWriteScheduler = backendWriteScheduler;
        this.persistenceBackend = persistenceBackend;
        this.snapshotFloorStore = snapshotFloorStore;
        this.completionReceiptStore = completionReceiptStore;
        this.transactionRecordStore = transactionRecordStore;
        this.preparedIntentStore = preparedIntentStore;
        this.durabilityTracker = durabilityTracker;
        this.configuration = configuration;
        this.logger = logger;
        this.flushNotificationSink = flushNotificationSink;
        this.self = context.Self;

        TimeSpan writerDelay = configuration.DirtyObjectsWriterDelay > 0
            ? TimeSpan.FromMilliseconds(configuration.DirtyObjectsWriterDelay)
            : TimeSpan.FromSeconds(5);

        context.ActorSystem.StartPeriodicTimer(
            context.Self,
            "flush-diry-objects",
            new(BackgroundWriteType.Flush),
            writerDelay,
            writerDelay
        );
    }
    
    public async Task Receive(BackgroundWriteRequest message)
    {
        switch (message.Type)
        {
            case BackgroundWriteType.QueueStoreLock:
                dirtyLocks.Enqueue(message);
                break;
            
            case BackgroundWriteType.QueueStoreKeyValue:
                dirtyKeyValues.Enqueue(message);
                if (message.PartitionId >= 0)
                    maxEnqueuedHlc.AddOrUpdate(message.PartitionId, message.LastModified,
                        (_, existing) => message.LastModified.CompareTo(existing) > 0 ? message.LastModified : existing);
                break;
            
            case BackgroundWriteType.Flush:
                // The checkpoint runs LAST, after the flush it depends on: it advances the WAL
                // retention floor, so it must only ever see state whose data has already landed in
                // the backend. Running it first made it self-defeating — under sustained load a tick
                // practically always begins with something queued, so the checkpoint bailed out on
                // its own dirty-queue guard and the flush that would have cleared that guard ran
                // immediately afterwards, one instant too late. A busy partition then never
                // checkpointed and its Raft WAL never compacted.
                await FlushLocks();
                await FlushKeyValues();
                // A backend wedged by a latched storage error (RocksDB after ENOSPC) can never be
                // un-wedged by the retry loops above — every retry re-collects the cached error.
                // After enough all-failure cycles, ask the backend to recover its engine; on
                // success, drain the retained batches immediately instead of waiting a tick.
                if (await MaybeRecoverPersistenceBackend())
                {
                    await FlushLocks();
                    await FlushKeyValues();
                }
                await AdvanceDurabilityFloors(forceSnapshotCapture: false);
                await CheckpointPartitions();
                await RunTargetedRevisionCleanup();
                await RunFullRevisionSweep();
                UpdatePitrHorizon();
                // Periodic backstop for the event-driven teardown below: a write applied just
                // before the partition was un-hosted can re-create bookkeeping after the explicit
                // teardown message ran, so every flush cycle re-validates against the committed map.
                ForgetUnhostedPartitions();
                // If either queue still has items after the time budget, reschedule immediately
                // instead of waiting for the next periodic timer tick.
                if (dirtyLocks.Count > 0 || dirtyKeyValues.Count > 0)
                    self.Send(new(BackgroundWriteType.Flush));
                break;

            case BackgroundWriteType.ForgetUnhostedPartitions:
                // Sent when the committed placement map stops listing this node as a replica of one
                // or more partitions: drop their per-partition bookkeeping promptly instead of
                // waiting for the next periodic flush tick.
                ForgetUnhostedPartitions();
                break;

            case BackgroundWriteType.FlushAndNotify:
                // Explicit pre-backup/pre-close flush: drain fully (ignore the time budget) so the
                // completion signal means every queued write has landed in storage. The completion
                // source MUST be completed on every path — success, partial persistence failure, or
                // an exception thrown mid-flush (e.g. the writer scheduler rejecting an enqueue during
                // shutdown). A flush that never signals would hang its awaiter (FlushPersistenceAsync)
                // forever, which stalls backups and node close.
                try
                {
                    bool locksFlushed = await FlushLocks(drainFully: true);
                    bool keyValuesFlushed = await FlushKeyValues(drainFully: true);

                    // An explicit flush against a wedged-but-recoverable engine should succeed in
                    // place rather than fail the backup/close: same recovery gate as the periodic
                    // tick, then one more full drain.
                    if ((!locksFlushed || !keyValuesFlushed) && await MaybeRecoverPersistenceBackend())
                    {
                        locksFlushed = await FlushLocks(drainFully: true);
                        keyValuesFlushed = await FlushKeyValues(drainFully: true);
                    }

                    await AdvanceDurabilityFloors(forceSnapshotCapture: true);
                    await RunTargetedRevisionCleanup();
                    if (locksFlushed && keyValuesFlushed)
                        message.CompletionSource?.TrySetResult(true);
                    else
                        message.CompletionSource?.TrySetException(new IOException(
                            "The background writer could not durably persist all committed locks/key-values; the flush did not complete."));
                }
                catch (Exception ex)
                {
                    message.CompletionSource?.TrySetException(ex);
                }
                break;

            default:
                throw new NotImplementedException();
        }
    }

    /// <summary>
    /// Drops every piece of per-partition bookkeeping — checkpoint tracking, enqueued-HLC watermark,
    /// persisted-floor cache, and the partition's durability-tracker state — for partitions the
    /// committed placement map no longer hosts on this node. Retained state would be a WAL retention
    /// leak (and a stale durability floor) if the range is ever hosted here again; the on-disk floor
    /// row is reclaimed when the partition's local data is purged. Consults <c>HostsPartition</c>
    /// directly: it reads the same committed map the placement events are derived from, so the sweep
    /// can never drop a partition that is still hosted. A no-op until the first partition map has
    /// been applied, since before that nothing is materialized and every partition would look
    /// un-hosted.
    /// </summary>
    private void ForgetUnhostedPartitions()
    {
        if (!raft.IsInitialized)
            return;

        HashSet<int>? toForget = null;

        foreach (int partitionId in partitionDirtySince.Keys)
            if (!raft.HostsPartition(partitionId))
                (toForget ??= []).Add(partitionId);

        foreach (int partitionId in maxEnqueuedHlc.Keys)
            if (!raft.HostsPartition(partitionId))
                (toForget ??= []).Add(partitionId);

        foreach (int partitionId in lastPersistedFloors.Keys)
            if (!raft.HostsPartition(partitionId))
                (toForget ??= []).Add(partitionId);

        if (durabilityTracker is not null)
        {
            foreach (int partitionId in durabilityTracker.ObservedPartitions)
                if (!raft.HostsPartition(partitionId))
                    (toForget ??= []).Add(partitionId);
        }

        if (toForget is null)
            return;

        foreach (int partitionId in toForget)
        {
            partitionDirtySince.Remove(partitionId);
            maxEnqueuedHlc.TryRemove(partitionId, out _);
            lastPersistedFloors.Remove(partitionId);
            durabilityTracker?.Forget(partitionId);

            logger.LogDroppedUnhostedPartitionBookkeeping(partitionId);
        }

        if (partitionDirtySince.Count == 0)
            pendingCheckpoint = false;
    }

    /// <summary>
    /// Whether any per-partition bookkeeping (checkpoint tracking, enqueued-HLC watermark, or
    /// persisted-floor cache) is currently retained for <paramref name="partitionId"/>.
    /// Test/observability only.
    /// </summary>
    internal bool HasPartitionBookkeeping(int partitionId) =>
        partitionDirtySince.ContainsKey(partitionId)
        || maxEnqueuedHlc.ContainsKey(partitionId)
        || lastPersistedFloors.ContainsKey(partitionId);

    /// <summary>
    /// Checkpoints the partitions whose checkpoint interval has elapsed since they went dirty, which
    /// is what lets Kommander compact their WAL: compaction is anchored on the last checkpoint, so a
    /// partition that never checkpoints keeps every log entry it ever wrote.
    /// <para>
    /// Runs at the end of the flush tick, never at its start, and skips a partition whose own writes
    /// are still queued or still being retried: a checkpoint advances the WAL retention floor, and
    /// entries whose data has not reached the backend must stay replayable. The gate is per-partition
    /// so one busy (or stuck) partition cannot hold every other partition's WAL open — the coarse
    /// "anything dirty at all" form of this check is what made checkpoints depend on catching the
    /// whole node idle.
    /// </para>
    /// If this node is no longer the leader for a partition, the partition is dropped from tracking;
    /// the leader that owns it checkpoints it instead.
    /// </summary>
    /// <returns>A value task representing the asynchronous checkpoint operation.</returns>
    private async ValueTask CheckpointPartitions()
    {
        if (!pendingCheckpoint)
            return;

        DateTime currentTime = DateTime.UtcNow;
        TimeSpan maxTime = configuration.CheckpointInterval;

        List<int>? duePartitions = null;

        foreach ((int partitionId, DateTime dirtySince) in partitionDirtySince)
        {
            if ((currentTime - dirtySince) >= maxTime)
                (duePartitions ??= []).Add(partitionId);
        }

        if (duePartitions is null)
            return;

        // Only computed when a partition is actually due — once per checkpoint interval per
        // partition — so scanning the queues never runs on the per-tick path.
        HashSet<int> unflushed = CollectPartitionsWithUnflushedWrites(duePartitions);

        HashSet<int> partitionsToRemove = [];

        foreach (int partitionId in duePartitions)
        {
            if (unflushed.Contains(partitionId))
                continue;

            if (!await raft.AmILeaderIfHosted(partitionId, CancellationToken.None))
            {
                //logger.LogWarning("No longer leader to checkpoint partition #{PartitionId}", partitionId);

                partitionsToRemove.Add(partitionId);
                continue;
            }

            // Capture this partition's receipts and decision records durably before its checkpoint advances the
            // WAL retention floor: past the floor the receipt/decision-bearing log entries are compacted and can
            // no longer be replayed. Every dirty key-value write of this partition has already flushed (guarded
            // above), so the snapshots are consistent with the backend. If either cannot be made durable, skip this
            // partition's checkpoint and retry next cycle rather than compacting away the only proof of a commit
            // or decision.
            if (!TryCaptureCheckpointSnapshots(partitionId))
            {
                logger.LogWarning("Skipping checkpoint of partition #{PartitionId}: snapshot capture not durable", partitionId);
                continue;
            }

            RaftReplicationResult result = await raft.ReplicateCheckpoint(partitionId);

            if (result.Success)
            {
                logger.LogSuccessfullyCheckpointedPartition(partitionId);

                partitionsToRemove.Add(partitionId);
            }
        }

        foreach (int partitionToRemove in partitionsToRemove)
            partitionDirtySince.Remove(partitionToRemove);

        if (partitionDirtySince.Count == 0)
            pendingCheckpoint = false;
    }

    /// <summary>
    /// The partitions holding writes that have not reached the backend yet: everything still queued
    /// after the flush cycle (the flush drains under a time budget, so a saturated tick can leave a
    /// tail behind) plus the partitions of a batch that exhausted its retries and is retained for the
    /// next cycle. Checkpointing such a partition would advance its WAL retention floor past entries
    /// whose only durable copy is still that log entry.
    /// <para>
    /// A retained failed batch keeps only the serialized items, which carry no partition, so every
    /// tracked partition is reported as unflushed while one is outstanding. That is deliberately
    /// conservative: it delays checkpoints for as long as the backend is rejecting writes, which is
    /// exactly when nothing should be compacted.
    /// </para>
    /// </summary>
    /// <param name="duePartitions">The partitions whose answer the caller actually needs. Once all of
    /// them are known to be unflushed the scan stops, since nothing it could still find would change
    /// a decision — that bound matters because a backlogged queue is scanned on every tick for as
    /// long as it keeps a due partition blocked. Pass <c>null</c> to scan the queues in full.</param>
    internal HashSet<int> CollectPartitionsWithUnflushedWrites(List<int>? duePartitions = null)
    {
        HashSet<int> unflushed = [];

        if (pendingLockItems is not null || pendingKeyValuesItems is not null)
        {
            foreach (int partitionId in partitionDirtySince.Keys)
                unflushed.Add(partitionId);

            return unflushed;
        }

        int dueRemaining = duePartitions?.Count ?? int.MaxValue;

        foreach (BackgroundWriteRequest request in dirtyLocks)
        {
            if (request.PartitionId >= 0 && unflushed.Add(request.PartitionId)
                && duePartitions is not null && duePartitions.Contains(request.PartitionId) && --dueRemaining == 0)
                return unflushed;
        }

        foreach (BackgroundWriteRequest request in dirtyKeyValues)
        {
            if (request.PartitionId >= 0 && unflushed.Add(request.PartitionId)
                && duePartitions is not null && duePartitions.Contains(request.PartitionId) && --dueRemaining == 0)
                return unflushed;
        }

        return unflushed;
    }

    /// <summary>
    /// Persists the partition-scoped snapshots a partition's WAL checkpoint must not advance past — completion
    /// receipts, canonical transaction records, and prepared intents — returning true only when <b>all</b> are
    /// durable. This is the sole gate before <c>ReplicateCheckpoint</c>, so a false return keeps the retention
    /// floor where it is and the entries remain replayable; without gating the record/intent snapshots, a
    /// checkpoint could compact away the only durable copy of a committed decision or an unresolved intent, and a
    /// cold restart could not reconstruct it. Every snapshot is attempted (no short-circuit) so a transient
    /// failure in one does not mask the others; the checkpoint is skipped unless all three succeed.
    /// </summary>
    internal bool TryCaptureCheckpointSnapshots(int partitionId)
    {
        // Capture each store's resolve ceiling before its snapshot: the snapshot covers every apply
        // completed so far, which includes everything at or below the ceiling, so a durable snapshot
        // certifies those WAL entries for the partition's application-durability floor.
        long receiptCeiling = durabilityTracker?.GetHighestApplied(partitionId, DurabilityChannel.Receipts) ?? -1;
        long recordCeiling = durabilityTracker?.GetHighestApplied(partitionId, DurabilityChannel.TransactionRecords) ?? -1;
        long intentCeiling = durabilityTracker?.GetHighestApplied(partitionId, DurabilityChannel.PreparedIntents) ?? -1;

        bool receipts = completionReceiptStore?.PersistSnapshot(partitionId) ?? true;

        // Intents (with the partition's committed-head ledger) BEFORE records, and the order is load-bearing:
        // a bundled commit replayed after a restart is judged again against the intent set and ledger the
        // snapshot restored, unless the restored record already holds its outcome. Capturing the intent side
        // first puts its position at or before the record side's, so for every replayed bundled commit either
        // the record snapshot already carries the outcome (commit, or the rejection memo) or the intent/ledger
        // state is exact at the commit's position once the entries between them are re-applied. The reverse
        // order could restore an intent set from after a later competitor took the key — a replay would then
        // refuse a commit the live apply admitted.
        bool intents = preparedIntentStore?.PersistSnapshot(partitionId) ?? true;
        bool records = transactionRecordStore?.PersistSnapshot(partitionId) ?? true;

        if (receipts)
            durabilityTracker?.ResolveUpTo(partitionId, DurabilityChannel.Receipts, receiptCeiling);
        if (records)
            durabilityTracker?.ResolveUpTo(partitionId, DurabilityChannel.TransactionRecords, recordCeiling);
        if (intents)
            durabilityTracker?.ResolveUpTo(partitionId, DurabilityChannel.PreparedIntents, intentCeiling);

        return receipts && records && intents;
    }

    /// <summary>
    /// Resolves the WAL indexes of a durably-stored flush batch against the durability tracker so
    /// the partitions' application-durability floors can advance past them.
    /// </summary>
    private void ResolveFlushedIndexes(List<(int PartitionId, long LogIndex)>? batchIndexes)
    {
        if (durabilityTracker is null || batchIndexes is null)
            return;

        foreach ((int partitionId, long logIndex) in batchIndexes)
            durabilityTracker.Resolve(partitionId, logIndex);
    }

    /// <summary>
    /// Monotonic timestamp (<see cref="Environment.TickCount64"/>) of the last snapshot-channel
    /// capture pass started from <see cref="AdvanceDurabilityFloors"/>; negative means "never".
    /// Capture rewrites each store's entire per-partition snapshot file, and under sustained
    /// durable-2PC load the stores mutate continuously while retaining minutes of settled outcomes
    /// — so a capture per flush tick rewrites a monotonically growing file every tick, an
    /// allocation and I/O cost that grows with the retention window and stalls the flush pipeline.
    /// This stamp throttles the periodic path to checkpoint cadence.
    /// </summary>
    private long lastSnapshotCaptureTicks = -1;

    /// <summary>
    /// Advances the application-durability floors after a flush cycle. Two steps:
    /// <para>
    /// <b>Snapshot-channel resolution.</b> Partitions whose floor is blocked on unresolved
    /// receipt / transaction-record / prepared-intent entries get those stores' snapshots persisted
    /// here — on followers as well as leaders. The leader's checkpoint gate also captures these
    /// snapshots, but only for partitions it leads; without this pass a follower's floor (and
    /// therefore its WAL retention) would freeze on the first durable-2PC entry it applies.
    /// Because each capture is a full-store snapshot rewrite whose cost scales with the retained
    /// record/receipt count, the periodic flush path
    /// (<paramref name="forceSnapshotCapture"/> = <c>false</c>) captures at most once per
    /// <see cref="KahunaConfiguration.CheckpointInterval"/> rather than on every flush tick. The
    /// floor merely lags by that bounded interval, which only widens restart replay by the same
    /// bounded amount — the exposure a checkpoint-cadence capture always had. An explicit flush
    /// (<paramref name="forceSnapshotCapture"/> = <c>true</c>: backup, node close) always
    /// captures, because its completion signal promises the durable state is as advanced as it can
    /// be made.
    /// </para>
    /// <para>
    /// <b>Floor persistence.</b> Every partition whose watermark advanced past its last persisted
    /// floor gets the new floor written to the backend — strictly after the flush batches it
    /// certifies, so the persisted floor can never lead the data it vouches for. The floor read at
    /// the next startup is what Kommander uses to widen restart replay. This step is never
    /// throttled: it only writes floors that actually advanced, which is cheap.
    /// </para>
    /// </summary>
    private async ValueTask AdvanceDurabilityFloors(bool forceSnapshotCapture)
    {
        if (durabilityTracker is null)
            return;

        long nowTicks = Environment.TickCount64;
        bool captureSnapshots = forceSnapshotCapture
            || lastSnapshotCaptureTicks < 0
            || (nowTicks - lastSnapshotCaptureTicks) >= (long)configuration.CheckpointInterval.TotalMilliseconds;

        if (captureSnapshots)
        {
            lastSnapshotCaptureTicks = nowTicks;

            foreach (int partitionId in durabilityTracker.ObservedPartitions)
            {
                if (!durabilityTracker.HasPendingSnapshotWork(partitionId))
                    continue;

                if (!TryCaptureCheckpointSnapshots(partitionId))
                    logger.LogWarning(
                        "Durability floor of partition #{PartitionId} is blocked on a store snapshot that could not be persisted; WAL retention cannot advance past it until a snapshot succeeds",
                        partitionId);
            }
        }

        List<(int PartitionId, long Floor)>? floorsToPersist = null;

        foreach (int partitionId in durabilityTracker.ObservedPartitions)
        {
            long watermark = durabilityTracker.GetWatermark(partitionId);

            if (watermark >= 0 && watermark > lastPersistedFloors.GetValueOrDefault(partitionId, -1))
                (floorsToPersist ??= []).Add((partitionId, watermark));
        }

        if (floorsToPersist is null)
            return;

        bool stored;

        try
        {
            stored = await backendWriteScheduler.EnqueueTask(WriterQueueKey, () => persistenceBackend.StoreDurabilityFloors(floorsToPersist));
        }
        catch (ReadBackpressureExceededException)
        {
            // Writer queue saturated: skip this cycle; the floors are retried on the next flush tick.
            return;
        }
        catch (Exception ex)
        {
            // Same skip-and-retry treatment for any storage failure (e.g. a full disk): the floor
            // list is recomputed from the tracker every cycle, so nothing is lost by returning —
            // while an escaped exception would abort the rest of the flush pass.
            storeFaultSinceLastRecoveryCheck = true;
            logger.LogError(ex, "Persisting {Count} advanced durability floors failed; retrying next flush cycle", floorsToPersist.Count);
            return;
        }

        if (!stored)
        {
            storeFaultSinceLastRecoveryCheck = true;
            logger.LogWarning("Failed to persist {Count} advanced durability floors; retrying next flush cycle", floorsToPersist.Count);
            return;
        }

        storeSuccessSinceLastRecoveryCheck = true;

        foreach ((int partitionId, long floor) in floorsToPersist)
        {
            lastPersistedFloors[partitionId] = floor;

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("Durability floor of partition #{PartitionId} advanced to {Floor}", partitionId, floor);
        }
    }

    /// <summary>
    /// Evaluates the wedge counter and, past the threshold, asks the backend to recover its
    /// storage engine (for RocksDB: close and reopen, which clears a latched background error
    /// such as ENOSPC after the operator frees space). The counter advances only on cycles whose
    /// stores exclusively failed with storage faults: one successful store proves the engine
    /// writes and resets it, and pure writer-queue backpressure never counts. A recovery request
    /// that returns <c>false</c> (volume still full, or a backend with no reset capability) keeps
    /// the counter, so the request repeats on later failed cycles; the retained batches are held
    /// throughout, so nothing is lost either way.
    /// </summary>
    /// <returns><c>true</c> when the engine was recovered and retained batches should be retried now.</returns>
    private async ValueTask<bool> MaybeRecoverPersistenceBackend()
    {
        if (storeSuccessSinceLastRecoveryCheck)
            consecutiveStoreFailureCycles = 0;
        else if (storeFaultSinceLastRecoveryCheck)
            consecutiveStoreFailureCycles++;

        storeFaultSinceLastRecoveryCheck = false;
        storeSuccessSinceLastRecoveryCheck = false;

        if (consecutiveStoreFailureCycles < StoreFailureCyclesBeforeRecovery)
            return false;

        logger.LogError(
            "Persistence backend has failed {Cycles} consecutive flush cycles; requesting a storage-engine recovery",
            consecutiveStoreFailureCycles);

        bool recovered;

        try
        {
            recovered = await backendWriteScheduler.EnqueueTask(WriterQueueKey, () => persistenceBackend.TryRecoverFromStorageFailure());
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Persistence backend storage-engine recovery attempt failed");
            return false;
        }

        if (recovered)
        {
            consecutiveStoreFailureCycles = 0;
            logger.LogWarning("Persistence backend storage engine recovered; retrying the retained batches");
            return true;
        }

        logger.LogWarning(
            "Persistence backend storage engine could not be recovered yet; retained batches continue to be retried");
        return false;
    }

    /// <summary>
    /// Attempts to flush all pending locks in the background writer to the persistence backend.
    /// Drains multiple batches per call until the queue is empty or the flush time budget is
    /// exhausted. On budget expiry the caller reschedules immediately via self-Send.
    /// If any batch fails all retries, it is retained in <see cref="pendingLockItems"/> for the
    /// next flush cycle.
    /// </summary>
    /// <param name="drainFully">When true, ignores the per-call time budget and drains the queue
    /// completely. Used by the explicit pre-backup/pre-close flush so its completion signal means
    /// every queued write has landed.</param>
    /// <returns>A task that represents the asynchronous operation of flushing locks.</returns>
    private async ValueTask<bool> FlushLocks(bool drainFully = false)
    {
        if (dirtyLocks.Count == 0 && pendingLockItems == null)
            return true;

        long budgetMs = configuration.DirtyObjectsWriterDelay > 0
            ? configuration.DirtyObjectsWriterDelay
            : 5000;

        long startTick = Environment.TickCount64;

        while (dirtyLocks.Count > 0 || pendingLockItems != null)
        {
            stopwatch.Restart();

            List<PersistenceRequestItem> items;
            List<(int PartitionId, long LogIndex)>? batchIndexes;

            if (pendingLockItems != null)
            {
                items = pendingLockItems;
                batchIndexes = pendingLockIndexes;
                pendingLockItems = null;
                pendingLockIndexes = null;
            }
            else
            {
                items = reusableLockItems ?? [];
                reusableLockItems = null;
                batchIndexes = null;

                long size = 0;
                int counter = 0;
                DateTime timestamp = DateTime.UtcNow;

                while (dirtyLocks.TryDequeue(out BackgroundWriteRequest? lockRequest))
                {
                    // Stamped only when the partition is not already awaiting a checkpoint: the stamp
                    // marks when this checkpoint interval started, not when the partition was last
                    // written (see partitionDirtySince).
                    if (lockRequest.PartitionId >= 0)
                        partitionDirtySince.TryAdd(lockRequest.PartitionId, timestamp);

                    items.Add(new(
                        lockRequest.Key,
                        lockRequest.Value,
                        lockRequest.Revision,
                        lockRequest.Expires.N,
                        lockRequest.Expires.L,
                        lockRequest.Expires.C,
                        lockRequest.LastUsed.N,
                        lockRequest.LastUsed.L,
                        lockRequest.LastUsed.C,
                        lockRequest.LastModified.N,
                        lockRequest.LastModified.L,
                        lockRequest.LastModified.C,
                        lockRequest.State
                    ));

                    // Carry the WAL index so the durability floor advances when this batch lands;
                    // travels beside the items so the retry path keeps the linkage.
                    if (lockRequest.LogIndex >= 0 && lockRequest.PartitionId >= 0)
                    {
                        if (batchIndexes is null)
                        {
                            batchIndexes = reusableLockIndexes ?? [];
                            reusableLockIndexes = null;
                        }

                        batchIndexes.Add((lockRequest.PartitionId, lockRequest.LogIndex));
                    }

                    if (lockRequest.Value is not null)
                        size += lockRequest.Value.Length;

                    // The request has been copied into the item struct and is no longer referenced
                    // (the retry path keeps the items, not the requests), so recycle it.
                    BackgroundWriteRequestPool.Return(lockRequest);

                    if (++counter >= MaxBatchSize || size >= MaxPacketSize)
                        break;
                }
            }

            LazyRetryDelays backoffDelays = new(TimeSpan.FromMilliseconds(1000), WriteRetries);

            bool success = false;
            for (int retryAttempt = 0; retryAttempt < WriteRetries; retryAttempt++)
            {
                try
                {
                    success = await backendWriteScheduler.EnqueueTask(WriterQueueKey, () => persistenceBackend.StoreLocks(items));

                    // A contract-honoring false return is a storage fault the backend already logged.
                    if (!success)
                        storeFaultSinceLastRecoveryCheck = true;
                }
                catch (ReadBackpressureExceededException)
                {
                    // Writer queue at its depth limit — treat as a failed attempt so the backoff loop retries,
                    // and the post-loop failure path keeps the batch in pendingLockItems. Never dropped.
                    // Not a storage fault: it never feeds the wedge counter.
                    success = false;
                }
                catch (Exception ex)
                {
                    // Any storage failure (e.g. a RocksDB write hitting a full disk) must take the
                    // same retry-then-repark path — an escaped exception would drop the dequeued
                    // batch and freeze the durability floor of every partition it references.
                    success = false;
                    storeFaultSinceLastRecoveryCheck = true;
                    logger.LogError(ex, "Storing a batch of {Count} locks failed", items.Count);
                }
                if (success)
                    break;

                logger.LogWarning("Coundn't store batch of {Count} locks. Waiting...", items.Count);
                if (backoffDelays.TryNext(out TimeSpan timeSpan)) await Task.Delay(timeSpan);
            }

            if (!success)
            {
                pendingLockItems = items;
                pendingLockIndexes = batchIndexes;
                logger.LogError("Coundn't store batch of {Count} locks", items.Count);
                return false;
            }

            logger.LogSuccessfullyStoredLocks(items.Count, stopwatch.ElapsedMilliseconds);

            storeSuccessSinceLastRecoveryCheck = true;

            ResolveFlushedIndexes(batchIndexes);

            // The batch is fully consumed — recycle its lists for the next cycle (a retried batch
            // that finally landed is adopted the same way). Clear drops the value references, so
            // nothing stays pinned between flushes.
            items.Clear();
            reusableLockItems = items;

            if (batchIndexes is not null)
            {
                batchIndexes.Clear();
                reusableLockIndexes = batchIndexes;
            }

            pendingCheckpoint = true;

            if (!drainFully && (Environment.TickCount64 - startTick) >= budgetMs)
                return true;
        }

        return true;
    }

    /// <summary>
    /// Flushes the queued key-value items by processing and storing them persistently.
    /// Drains multiple batches per call until the queue is empty or the flush time budget is
    /// exhausted. On budget expiry the caller reschedules immediately via self-Send.
    /// Successfully stored batches update the revision-cleanup set; failed batches are retained
    /// in <see cref="pendingKeyValuesItems"/> for the next flush cycle.
    /// </summary>
    /// <param name="drainFully">When true, ignores the per-call time budget and drains the queue
    /// completely. Used by the explicit pre-backup/pre-close flush so its completion signal means
    /// every queued write has landed.</param>
    /// <returns>A value task representing the asynchronous operation of flushing key-value items.</returns>
    private async ValueTask<bool> FlushKeyValues(bool drainFully = false)
    {
        if (dirtyKeyValues.Count == 0 && pendingKeyValuesItems == null)
            return true;

        long budgetMs = configuration.DirtyObjectsWriterDelay > 0
            ? configuration.DirtyObjectsWriterDelay
            : 5000;

        long startTick = Environment.TickCount64;

        while (dirtyKeyValues.Count > 0 || pendingKeyValuesItems != null)
        {
            stopwatch.Restart();

            List<PersistenceRequestItem> items;
            List<(int PartitionId, long LogIndex)>? batchIndexes;

            if (pendingKeyValuesItems != null)
            {
                items = pendingKeyValuesItems;
                batchIndexes = pendingKeyValueIndexes;
                pendingKeyValuesItems = null;
                pendingKeyValueIndexes = null;
            }
            else
            {
                items = reusableKeyValuesItems ?? [];
                reusableKeyValuesItems = null;
                batchIndexes = null;

                long size = 0;
                int counter = 0;

                DateTime timestamp = DateTime.UtcNow;

                drainedKeyValueIdentities.Clear();

                while (dirtyKeyValues.TryDequeue(out BackgroundWriteRequest? keyValueRequest))
                {
                    // Stamped only when the partition is not already awaiting a checkpoint: the stamp
                    // marks when this checkpoint interval started, not when the partition was last
                    // written (see partitionDirtySince).
                    if (keyValueRequest.PartitionId >= 0)
                        partitionDirtySince.TryAdd(keyValueRequest.PartitionId, timestamp);

                    // Collapse the leader-plus-consumer duplicate of one committed mutation to a
                    // single physical write. The WAL index below is still recorded for a collapsed
                    // duplicate: its durability is certified by the surviving identical write.
                    bool firstOccurrence = drainedKeyValueIdentities.Add((
                        keyValueRequest.Key,
                        keyValueRequest.Revision,
                        keyValueRequest.LastModified.N,
                        keyValueRequest.LastModified.L,
                        keyValueRequest.LastModified.C,
                        keyValueRequest.State,
                        keyValueRequest.NoRevision));

                    if (firstOccurrence)
                    {
                        items.Add(new(
                            keyValueRequest.Key,
                            keyValueRequest.Value,
                            keyValueRequest.Revision,
                            keyValueRequest.Expires.N,
                            keyValueRequest.Expires.L,
                            keyValueRequest.Expires.C,
                            keyValueRequest.LastUsed.N,
                            keyValueRequest.LastUsed.L,
                            keyValueRequest.LastUsed.C,
                            keyValueRequest.LastModified.N,
                            keyValueRequest.LastModified.L,
                            keyValueRequest.LastModified.C,
                            keyValueRequest.State,
                            keyValueRequest.NoRevision
                        ));

                        if (keyValueRequest.Value is not null)
                            size += keyValueRequest.Value.Length;
                    }

                    // Carry the WAL index so the durability floor advances when this batch lands;
                    // travels beside the items so the retry path keeps the linkage.
                    if (keyValueRequest.LogIndex >= 0 && keyValueRequest.PartitionId >= 0)
                    {
                        if (batchIndexes is null)
                        {
                            batchIndexes = reusableKeyValueIndexes ?? [];
                            reusableKeyValueIndexes = null;
                        }

                        batchIndexes.Add((keyValueRequest.PartitionId, keyValueRequest.LogIndex));
                    }

                    // The request has been copied into the item struct and is no longer referenced
                    // (the retry path keeps the items, not the requests), so recycle it.
                    BackgroundWriteRequestPool.Return(keyValueRequest);

                    if (firstOccurrence && (++counter >= MaxBatchSize || size >= MaxPacketSize))
                        break;
                }
            }

            LazyRetryDelays backoffDelays = new(TimeSpan.FromMilliseconds(1000), WriteRetries);

            bool success = false;
            for (int retryAttempt = 0; retryAttempt < WriteRetries; retryAttempt++)
            {
                try
                {
                    success = await backendWriteScheduler.EnqueueTask(WriterQueueKey, () => persistenceBackend.StoreKeyValues(items));

                    // A contract-honoring false return is a storage fault the backend already logged.
                    if (!success)
                        storeFaultSinceLastRecoveryCheck = true;
                }
                catch (ReadBackpressureExceededException)
                {
                    // Writer queue at its depth limit — treat as a failed attempt so the backoff loop retries,
                    // and the post-loop failure path keeps the batch in pendingKeyValuesItems. Never dropped.
                    // Not a storage fault: it never feeds the wedge counter.
                    success = false;
                }
                catch (Exception ex)
                {
                    // Any storage failure (e.g. a RocksDB write hitting a full disk) must take the
                    // same retry-then-repark path. An exception that escapes this loop loses the
                    // dequeued batch: its WAL indexes stay pending forever, which freezes the
                    // partition's durability floor and with it WAL compaction — a permanent wedge
                    // from a transient fault.
                    success = false;
                    storeFaultSinceLastRecoveryCheck = true;
                    logger.LogError(ex, "Storing a batch of {Count} key-values failed", items.Count);
                }
                if (success)
                    break;

                logger.LogWarning("Coundn't store batch of {Count} key-values. Waiting...", items.Count);
                if (backoffDelays.TryNext(out TimeSpan timeSpan)) await Task.Delay(timeSpan);
            }

            if (!success)
            {
                // Persistence failed after exhausting retries. Keep the items queued for a later retry,
                // but report the failure so a caller waiting on a full drain (e.g. a full backup) never
                // treats an incomplete flush as success and publishes a checkpoint missing committed data.
                pendingKeyValuesItems = items;
                pendingKeyValueIndexes = batchIndexes;
                logger.LogError("Coundn't store batch of {Count} key-values", items.Count);
                return false;
            }

            logger.LogSuccessfullyStoredKeyValues(items.Count, stopwatch.ElapsedMilliseconds);

            storeSuccessSinceLastRecoveryCheck = true;

            ResolveFlushedIndexes(batchIndexes);

            // Acknowledge each durably-stored key-value back to its owning actor so it can advance
            // FlushedRevision and release the entry for eviction. The actor ignores acks that don't
            // match a live entry at that revision, so stale/superseded acks are harmless.
            foreach (ref readonly PersistenceRequestItem item in CollectionsMarshal.AsSpan(items))
                flushNotificationSink.NotifyFlushed(item.Key, item.Revision);

            if (ConfigurationValidator.IsPersistentRevisionRetentionEnabled(configuration)
                && configuration.PersistentRevisionCleanupOnWrite)
            {
                foreach (ref readonly PersistenceRequestItem item in CollectionsMarshal.AsSpan(items))
                {
                    if (pendingRevisionCleanupKeys.Count < MaxPendingCleanupKeys)
                        pendingRevisionCleanupKeys.Add(item.Key);
                }
            }

            // The batch is fully consumed — recycle its lists for the next cycle (a retried batch
            // that finally landed is adopted the same way). Clear drops the value references, so
            // nothing stays pinned between flushes.
            items.Clear();
            reusableKeyValuesItems = items;

            if (batchIndexes is not null)
            {
                batchIndexes.Clear();
                reusableKeyValueIndexes = batchIndexes;
            }

            pendingCheckpoint = true;

            if (!drainFully && (Environment.TickCount64 - startTick) >= budgetMs)
                return true;
        }

        return true;
    }

    /// <summary>
    /// Drains the pending revision cleanup key set and calls the persistence backend to prune
    /// old revision records for those keys. Keys are re-queued when the batch limit is reached
    /// or the backend call fails, so cleanup is retried on the next flush cycle.
    /// Cleanup failure never propagates to the caller.
    /// </summary>
    private async ValueTask RunTargetedRevisionCleanup()
    {
        if (!ConfigurationValidator.IsPersistentRevisionRetentionEnabled(configuration))
            return;

        if (!configuration.PersistentRevisionCleanupOnWrite)
            return;

        if (pendingRevisionCleanupKeys.Count == 0)
            return;

        if (!await ConfirmFloorRegistryFreshness())
        {
            // Keys stay queued; the next flush cycle retries once catch-up can be confirmed.
            SnapshotFloorMetrics.PruneSkippedUnconfirmed.Add(1);
            logger.LogWarning(
                "Skipping targeted revision cleanup of {Count} key(s): meta-partition catch-up could not be confirmed, the local hold registry may be stale. backend={Backend}",
                pendingRevisionCleanupKeys.Count,
                configuration.Storage);
            return;
        }

        List<string> keysToClean = [..pendingRevisionCleanupKeys];
        pendingRevisionCleanupKeys.Clear();

        stopwatch.Restart();
        RevisionPruneResult pruneResult = default;

        SnapshotFloorStore? capturedFloorStore = snapshotFloorStore;
        IRaft capturedRaft = raft;

        try
        {
            Action? capturedHook = BeforePruneSampleHook;
            Action? capturedAfterHook = AfterPruneSampleHook;
            bool success = await backendWriteScheduler.EnqueueTask(WriterQueueKey, () =>
            {
                capturedHook?.Invoke();
                // Open a prune-delete window: the floor is sampled under the store's commit lock so
                // any hold committed before this point is reflected, and any hold that commits after
                // it observes the open window and fails closed rather than losing its boundary to
                // the delete below.
                HLCTimestamp floor;
                long pruneToken = 0;
                bool windowOpen = capturedFloorStore is not null;
                if (windowOpen)
                    (floor, pruneToken) = capturedFloorStore!.BeginPrune(capturedRaft);
                else
                    floor = HLCTimestamp.Zero;

                try
                {
                    capturedAfterHook?.Invoke();
                    bool ok = persistenceBackend.PruneKeyValueRevisions(
                        keysToClean,
                        configuration.PersistentRevisionRetentionCount,
                        configuration.PersistentRevisionRetentionAge,
                        configuration.PersistentRevisionCleanupBatchSize,
                        floor,
                        out RevisionPruneResult r);
                    pruneResult = r;
                    return ok;
                }
                finally
                {
                    if (windowOpen)
                        capturedFloorStore!.EndPrune(pruneToken);
                }
            });

            if (success)
            {
                if (pruneResult.RevisionsDeleted > 0 || pruneResult.BatchLimitReached)
                    logger.LogPrunedKeyValueRevisionsTargeted(
                        pruneResult.KeysVisited,
                        pruneResult.RevisionsDeleted,
                        pruneResult.BatchLimitReached,
                        stopwatch.ElapsedMilliseconds,
                        configuration.Storage,
                        configuration.PersistentRevisionRetentionCount,
                        configuration.PersistentRevisionRetentionAge
                    );

                if (pruneResult.FloorViolations > 0)
                {
                    SnapshotFloorMetrics.MissingProtectedVersion.Add(pruneResult.FloorViolations);
                    logger.LogError(
                        "Persistent prune deleted {Count} floor-protected boundary revision(s) — floor enforcement has a gap; this counter must stay 0. backend={Backend}",
                        pruneResult.FloorViolations,
                        configuration.Storage);
                }

                if (pruneResult.BatchLimitReached)
                {
                    // Requeue only keys that still have prunable revisions (or were never visited
                    // because the batch filled). Backends that don't report per-key backlog fall
                    // back to the full set, preserving the previous conservative behaviour.
                    foreach (string key in pruneResult.RemainingKeys ?? keysToClean)
                    {
                        if (pendingRevisionCleanupKeys.Count < MaxPendingCleanupKeys)
                            pendingRevisionCleanupKeys.Add(key);
                    }
                }
            }
            else
            {
                logger.LogWarning(
                    "Failed to prune key/value revisions for {Count} targeted keys; will retry. backend={Backend}",
                    keysToClean.Count,
                    configuration.Storage
                );

                foreach (string key in keysToClean)
                {
                    if (pendingRevisionCleanupKeys.Count < MaxPendingCleanupKeys)
                        pendingRevisionCleanupKeys.Add(key);
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Exception during targeted revision cleanup; will retry {Count} keys. backend={Backend}", keysToClean.Count, configuration.Storage);

            foreach (string key in keysToClean)
            {
                if (pendingRevisionCleanupKeys.Count < MaxPendingCleanupKeys)
                    pendingRevisionCleanupKeys.Add(key);
            }
        }
    }

    /// <summary>
    /// Runs a backend-wide revision sweep no more often than
    /// <see cref="KahunaConfiguration.PersistentRevisionCleanupInterval"/>.
    /// When a previous sweep hit the batch size limit the next eligible cycle resumes
    /// immediately rather than waiting for another full interval.
    /// Sweep failure is logged as a warning and never propagates to the caller.
    /// </summary>
    private async ValueTask RunFullRevisionSweep()
    {
        if (!ConfigurationValidator.IsPersistentRevisionRetentionEnabled(configuration))
            return;

        DateTime now = DateTime.UtcNow;

        bool intervalElapsed = (now - lastFullSweepUtc) >= configuration.PersistentRevisionCleanupInterval;

        if (!intervalElapsed && !fullSweepBacklogPending)
            return;

        if (!await ConfirmFloorRegistryFreshness())
        {
            // The interval is not consumed, so the sweep retries on the next eligible cycle once
            // catch-up can be confirmed.
            SnapshotFloorMetrics.PruneSkippedUnconfirmed.Add(1);
            logger.LogWarning(
                "Skipping full revision sweep: meta-partition catch-up could not be confirmed, the local hold registry may be stale. backend={Backend}",
                configuration.Storage);
            return;
        }

        lastFullSweepUtc = now;
        fullSweepBacklogPending = false;

        stopwatch.Restart();
        RevisionPruneResult pruneResult = default;

        SnapshotFloorStore? capturedSweepFloorStore = snapshotFloorStore;
        IRaft capturedSweepRaft = raft;

        Action? capturedSweepHook = BeforePruneSampleHook;
        Action? capturedSweepAfterHook = AfterPruneSampleHook;
        try
        {
            bool success = await backendWriteScheduler.EnqueueTask(WriterQueueKey, () =>
            {
                capturedSweepHook?.Invoke();
                // Open a prune-delete window: the floor is sampled under the store's commit lock so
                // any hold committed before this point is reflected, and any hold that commits after
                // it observes the open window and fails closed rather than losing its boundary to
                // the delete below.
                HLCTimestamp sweepFloor;
                long sweepToken = 0;
                bool sweepWindowOpen = capturedSweepFloorStore is not null;
                if (sweepWindowOpen)
                    (sweepFloor, sweepToken) = capturedSweepFloorStore!.BeginPrune(capturedSweepRaft);
                else
                    sweepFloor = HLCTimestamp.Zero;

                try
                {
                    capturedSweepAfterHook?.Invoke();
                    bool ok = persistenceBackend.PruneKeyValueRevisions(
                        null,
                        configuration.PersistentRevisionRetentionCount,
                        configuration.PersistentRevisionRetentionAge,
                        configuration.PersistentRevisionCleanupBatchSize,
                        sweepFloor,
                        out RevisionPruneResult r);
                    pruneResult = r;
                    return ok;
                }
                finally
                {
                    if (sweepWindowOpen)
                        capturedSweepFloorStore!.EndPrune(sweepToken);
                }
            });

            if (success)
            {
                if (pruneResult.RevisionsDeleted > 0 || pruneResult.BatchLimitReached)
                    logger.LogPrunedKeyValueRevisionsSweep(
                        pruneResult.KeysVisited,
                        pruneResult.RevisionsDeleted,
                        pruneResult.BatchLimitReached,
                        stopwatch.ElapsedMilliseconds,
                        configuration.Storage,
                        configuration.PersistentRevisionRetentionCount,
                        configuration.PersistentRevisionRetentionAge
                    );

                if (pruneResult.FloorViolations > 0)
                {
                    SnapshotFloorMetrics.MissingProtectedVersion.Add(pruneResult.FloorViolations);
                    logger.LogError(
                        "Persistent prune (sweep) deleted {Count} floor-protected boundary revision(s) — floor enforcement has a gap; this counter must stay 0. backend={Backend}",
                        pruneResult.FloorViolations,
                        configuration.Storage);
                }

                fullSweepBacklogPending = pruneResult.BatchLimitReached;
            }
            else
            {
                logger.LogWarning(
                    "Failed to run full revision sweep; will retry after next interval. backend={Backend}",
                    configuration.Storage
                );
                fullSweepBacklogPending = true;
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Exception during full revision sweep; will retry after next interval. backend={Backend}", configuration.Storage);
            fullSweepBacklogPending = true;
        }
    }

    /// <summary>
    /// Advances the PITR WAL-retention floor on every partition once per flush interval.
    /// <para>
    /// The protected boundary is <c>now − PitrWindow − BaseSnapshotInterval</c>: WAL entries
    /// committed before that point are covered by at least one base snapshot and can be
    /// compacted.  When the boundary cannot be mapped to a committed index (WAL empty or all
    /// entries newer than the boundary) a non-positive value is passed so Kommander leaves the
    /// existing floor unchanged rather than suppressing all compaction with a zero floor.
    /// </para>
    /// <para>
    /// Initialised to <see cref="DateTime.MinValue"/> so the first call fires immediately on
    /// startup — this re-asserts the in-memory floor after a process restart.
    /// Note: a compaction that fires between process start and this first tick sees no floor
    /// and may truncate up to the last checkpoint, shortening the recoverable window by up to
    /// one flush cadence. This is inherent to the in-memory-floor design; flush cadence is
    /// normally far shorter than compaction cadence so the gap is small in practice.
    /// </para>
    /// </summary>
    private void UpdatePitrHorizon()
    {
        if (configuration.PitrWindow <= TimeSpan.Zero)
            return;

        DateTime now = DateTime.UtcNow;
        TimeSpan tickInterval = configuration.BaseSnapshotInterval > TimeSpan.Zero
            ? configuration.BaseSnapshotInterval
            : configuration.PitrWindow;

        if ((now - lastPitrTickUtc) < tickInterval)
            return;

        lastPitrTickUtc = now;

        IReadOnlyList<RaftPartitionRange> partitions = raft.GetPartitionMap();
        foreach (RaftPartitionRange partition in partitions)
        {
            int partitionId = partition.PartitionId;

            // The committed map covers the whole cluster; the WAL adapter and the retention floor
            // are local, so only partitions materialized on this node get a horizon here (a range
            // hosted elsewhere has no local WAL to protect, and Kommander reclaims the WAL of a
            // range this node stopped hosting on its own).
            if (!raft.HostsPartition(partitionId))
                continue;

            long protectedIndex = PitrHorizon.ComputeProtectedIndex(
                raft.WalAdapter, partitionId, now,
                configuration.PitrWindow, configuration.BaseSnapshotInterval);

            // Pass a negative value when no committed entry exists before the boundary so Kommander
            // clears this floor. This is the legacy single-value floor; Kommander composes it with any
            // concurrent backup retention holds via minimum (IRaft.AcquireRetentionHold), so a backup
            // capturing a WAL prefix is no longer clobbered by a horizon advance.
            raft.SetMinRetainIndex(partitionId, protectedIndex > 0 ? protectedIndex : -1);

            // Recomputes the boundary for the log line; ComputeProtectedIndex derives it
            // internally but does not expose it in order to keep the return type simple.
            long logBoundaryMs = (long)((now - configuration.PitrWindow - configuration.BaseSnapshotInterval) - DateTime.UnixEpoch).TotalMilliseconds;
            logger.LogPitrHorizonUpdated(partitionId, protectedIndex, logBoundaryMs);
        }
    }
}
