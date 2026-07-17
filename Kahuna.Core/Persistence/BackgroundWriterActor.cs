
using Nixie;
using Kommander;
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
    
    private readonly IRaft raft;

    private readonly IPersistenceBackend persistenceBackend;

    private readonly SnapshotFloorStore? snapshotFloorStore;

    private readonly CompletionReceiptStore? completionReceiptStore;

    private readonly CoordinatorDecisionStore? coordinatorDecisionStore;

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
    
    /// <summary>
    /// Partition IDs that are being tracked for checkpointing.
    /// </summary>
    private readonly Dictionary<int, DateTime> partitionIds = [];
    
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
    /// Whether a checkpoint operation is pending.
    /// </summary>
    private bool pendingCheckpoint;

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
    /// Called on the scheduler thread immediately before <c>GetFloorForPrune</c> during both
    /// targeted cleanup and full sweep. Null in production; set by tests to gate the prune task
    /// before the floor is sampled so that concurrent hold acquisitions can be observed.
    /// </summary>
    internal Action? BeforePruneSampleHook;
    
    public BackgroundWriterActor(
        IActorContext<BackgroundWriterActor, BackgroundWriteRequest> context,
        IRaft raft,
        IPersistenceBackend persistenceBackend,
        SnapshotFloorStore? snapshotFloorStore,
        CompletionReceiptStore? completionReceiptStore,
        CoordinatorDecisionStore? coordinatorDecisionStore,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger,
        FlushNotificationSink flushNotificationSink
    )
    {
        this.raft = raft;
        this.persistenceBackend = persistenceBackend;
        this.snapshotFloorStore = snapshotFloorStore;
        this.completionReceiptStore = completionReceiptStore;
        this.coordinatorDecisionStore = coordinatorDecisionStore;
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
                break;
            
            case BackgroundWriteType.Flush:
                await CheckpointPartitions();
                await FlushLocks();
                await FlushKeyValues();
                await RunTargetedRevisionCleanup();
                await RunFullRevisionSweep();
                UpdatePitrHorizon();
                // If either queue still has items after the time budget, reschedule immediately
                // instead of waiting for the next periodic timer tick.
                if (dirtyLocks.Count > 0 || dirtyKeyValues.Count > 0)
                    self.Send(new(BackgroundWriteType.Flush));
                break;

            case BackgroundWriteType.FlushAndNotify:
                // Explicit pre-backup/pre-close flush: drain fully (ignore the time budget) so the
                // completion signal means every queued write has landed in storage.
                await FlushLocks(drainFully: true);
                await FlushKeyValues(drainFully: true);
                await RunTargetedRevisionCleanup();
                message.CompletionSource?.TrySetResult(true);
                break;

            default:
                throw new NotImplementedException();
        }
    }

    /// <summary>
    /// Performs a checkpoint operation on partitions to ensure their state is up-to-date and synchronized.
    /// This method checks if any pending checkpoints can be completed by iterating over tracked partitions.
    /// Partitions that have exceeded the allowed checkpoint interval are processed, ensuring they are in a consistent state.
    /// If the actor is no longer the leader for a partition, the partition is removed from tracking.
    /// Successfully checked partitions are updated, and the checkpoint status is cleared when no partitions remain.
    /// </summary>
    /// <returns>A value task representing the asynchronous checkpoint operation.</returns>
    private async ValueTask CheckpointPartitions()
    {
        if (dirtyLocks.Count > 0 || dirtyKeyValues.Count > 0)
            return;

        if (!pendingCheckpoint)
            return;

        HashSet<int> partitionsToRemove = [];

        DateTime currentTime = DateTime.UtcNow;
        TimeSpan maxTime = configuration.CheckpointInterval;

        foreach (KeyValuePair<int, DateTime> kv in partitionIds)
        {
            if ((currentTime - kv.Value) < maxTime)
                continue;

            if (!await raft.AmILeader(kv.Key, CancellationToken.None))
            {
                //logger.LogWarning("No longer leader to checkpoint partition #{PartitionId}", kv.Key);

                partitionsToRemove.Add(kv.Key);
                continue;
            }

            // Capture this partition's receipts and decision records durably before its checkpoint advances the
            // WAL retention floor: past the floor the receipt/decision-bearing log entries are compacted and can
            // no longer be replayed. Every dirty key-value write has already flushed (guarded above), so the
            // snapshots are consistent with the backend. If either cannot be made durable, skip this partition's
            // checkpoint and retry next cycle rather than compacting away the only proof of a commit or decision.
            if (!TryCaptureCheckpointSnapshots(kv.Key))
            {
                logger.LogWarning("Skipping checkpoint of partition #{PartitionId}: snapshot capture not durable", kv.Key);
                continue;
            }

            RaftReplicationResult result = await raft.ReplicateCheckpoint(kv.Key);

            if (result.Success)
            {
                logger.LogSuccessfullyCheckpointedPartition(kv.Key);

                partitionsToRemove.Add(kv.Key);
            }
        }

        foreach (int partitionToRemove in partitionsToRemove)
            partitionIds.Remove(partitionToRemove);

        if (partitionIds.Count == 0)
            pendingCheckpoint = false;
    }

    /// <summary>
    /// Persists the partition-scoped receipt and decision snapshots a partition's WAL checkpoint must not advance
    /// past, returning true only when both are durable. This is the sole gate before <c>ReplicateCheckpoint</c>,
    /// so a false return keeps the retention floor where it is and the entries remain replayable. Extracted so
    /// the gate is directly testable under injected snapshot failure.
    /// </summary>
    internal bool TryCaptureCheckpointSnapshots(int partitionId)
    {
        bool receiptsDurable = completionReceiptStore?.PersistSnapshot(partitionId) ?? true;
        bool decisionsDurable = coordinatorDecisionStore?.PersistSnapshot(partitionId) ?? true;
        return receiptsDurable && decisionsDurable;
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
    private async ValueTask FlushLocks(bool drainFully = false)
    {
        if (dirtyLocks.Count == 0 && pendingLockItems == null)
            return;

        long budgetMs = configuration.DirtyObjectsWriterDelay > 0
            ? configuration.DirtyObjectsWriterDelay
            : 5000;

        long startTick = Environment.TickCount64;

        while (dirtyLocks.Count > 0 || pendingLockItems != null)
        {
            stopwatch.Restart();

            List<PersistenceRequestItem> items;

            if (pendingLockItems != null)
            {
                items = pendingLockItems;
                pendingLockItems = null;
            }
            else
            {
                items = [];

                long size = 0;
                int counter = 0;
                DateTime timestamp = DateTime.UtcNow;

                while (dirtyLocks.TryDequeue(out BackgroundWriteRequest? lockRequest))
                {
                    if (lockRequest.PartitionId >= 0)
                    {
                        if (partitionIds.TryGetValue(lockRequest.PartitionId, out DateTime currentTimestamp))
                        {
                            if (timestamp >= currentTimestamp)
                                partitionIds[lockRequest.PartitionId] = timestamp;
                        }
                        else
                            partitionIds.Add(lockRequest.PartitionId, timestamp);
                    }

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

                    if (lockRequest.Value is not null)
                        size += lockRequest.Value.Length;

                    // The request has been copied into the item struct and is no longer referenced
                    // (the retry path keeps the items, not the requests), so recycle it.
                    BackgroundWriteRequestPool.Return(lockRequest);

                    if (++counter >= MaxBatchSize || size >= MaxPacketSize)
                        break;
                }
            }

            IEnumerable<TimeSpan> backoffDelays = Backoff.DecorrelatedJitterBackoffV2(
                medianFirstRetryDelay: TimeSpan.FromMilliseconds(1000),
                retryCount: WriteRetries
            );

            bool success = false;
            foreach (TimeSpan timeSpan in backoffDelays)
            {
                success = await raft.ReadScheduler.EnqueueTask(0, () => persistenceBackend.StoreLocks(items));
                if (success)
                    break;

                logger.LogWarning("Coundn't store batch of {Count} locks. Waiting...", items.Count);
                await Task.Delay(timeSpan);
            }

            if (!success)
            {
                pendingLockItems = items;
                logger.LogError("Coundn't store batch of {Count} locks", items.Count);
                return;
            }

            logger.LogSuccessfullyStoredLocks(items.Count, stopwatch.ElapsedMilliseconds);
            pendingCheckpoint = true;

            if (!drainFully && (Environment.TickCount64 - startTick) >= budgetMs)
                return;
        }
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
    private async ValueTask FlushKeyValues(bool drainFully = false)
    {
        if (dirtyKeyValues.Count == 0 && pendingKeyValuesItems == null)
            return;

        long budgetMs = configuration.DirtyObjectsWriterDelay > 0
            ? configuration.DirtyObjectsWriterDelay
            : 5000;

        long startTick = Environment.TickCount64;

        while (dirtyKeyValues.Count > 0 || pendingKeyValuesItems != null)
        {
            stopwatch.Restart();

            List<PersistenceRequestItem> items;

            if (pendingKeyValuesItems != null)
            {
                items = pendingKeyValuesItems;
                pendingKeyValuesItems = null;
            }
            else
            {
                items = [];

                long size = 0;
                int counter = 0;

                DateTime timestamp = DateTime.UtcNow;

                while (dirtyKeyValues.TryDequeue(out BackgroundWriteRequest? keyValueRequest))
                {
                    if (keyValueRequest.PartitionId >= 0)
                    {
                        if (partitionIds.TryGetValue(keyValueRequest.PartitionId, out DateTime currentTimestamp))
                        {
                            if (timestamp >= currentTimestamp)
                                partitionIds[keyValueRequest.PartitionId] = timestamp;
                        }
                        else
                            partitionIds.Add(keyValueRequest.PartitionId, timestamp);
                    }

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

                    // The request has been copied into the item struct and is no longer referenced
                    // (the retry path keeps the items, not the requests), so recycle it.
                    BackgroundWriteRequestPool.Return(keyValueRequest);

                    if (++counter >= MaxBatchSize || size >= MaxPacketSize)
                        break;
                }
            }

            IEnumerable<TimeSpan> backoffDelays = Backoff.DecorrelatedJitterBackoffV2(
                medianFirstRetryDelay: TimeSpan.FromMilliseconds(1000),
                retryCount: WriteRetries
            );

            bool success = false;
            foreach (TimeSpan timeSpan in backoffDelays)
            {
                success = await raft.ReadScheduler.EnqueueTask(0, () => persistenceBackend.StoreKeyValues(items));
                if (success)
                    break;

                logger.LogWarning("Coundn't store batch of {Count} key-values. Waiting...", items.Count);
                await Task.Delay(timeSpan);
            }

            if (!success)
            {
                pendingKeyValuesItems = items;
                logger.LogError("Coundn't store batch of {Count} key-values", items.Count);
                return;
            }

            logger.LogSuccessfullyStoredKeyValues(items.Count, stopwatch.ElapsedMilliseconds);

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

            pendingCheckpoint = true;

            if (!drainFully && (Environment.TickCount64 - startTick) >= budgetMs)
                return;
        }
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

        List<string> keysToClean = [..pendingRevisionCleanupKeys];
        pendingRevisionCleanupKeys.Clear();

        stopwatch.Restart();
        RevisionPruneResult pruneResult = default;

        SnapshotFloorStore? capturedFloorStore = snapshotFloorStore;
        IRaft capturedRaft = raft;

        try
        {
            Action? capturedHook = BeforePruneSampleHook;
            bool success = await raft.ReadScheduler.EnqueueTask(0, () =>
            {
                capturedHook?.Invoke();
                // Sample the floor here (inside the task) so any hold acquired while this task
                // was queued is still observed. A residual window remains: a hold acquired after
                // GetFloorForPrune returns but before PruneKeyValueRevisions completes is still
                // invisible. That window is now micro- to low-millisecond; full closure would
                // require mutual exclusion between acquire and the delete (deferred).
                HLCTimestamp floor = capturedFloorStore?.GetFloorForPrune(capturedRaft) ?? HLCTimestamp.Zero;

                bool ok = persistenceBackend.PruneKeyValueRevisions(
                    keysToClean,
                    configuration.PersistentRevisionRetentionCount,
                    configuration.PersistentRevisionRetentionAge,
                    configuration.PersistentRevisionCleanupBatchSize,
                    floor,
                    out RevisionPruneResult r);
                pruneResult = r;
                return ok;
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

        lastFullSweepUtc = now;
        fullSweepBacklogPending = false;

        stopwatch.Restart();
        RevisionPruneResult pruneResult = default;

        SnapshotFloorStore? capturedSweepFloorStore = snapshotFloorStore;
        IRaft capturedSweepRaft = raft;

        Action? capturedSweepHook = BeforePruneSampleHook;
        try
        {
            bool success = await raft.ReadScheduler.EnqueueTask(0, () =>
            {
                capturedSweepHook?.Invoke();
                // Sample the floor here (inside the task) so any hold acquired while this task
                // was queued is still observed. Residual window: a hold acquired after
                // GetFloorForPrune returns but before PruneKeyValueRevisions completes is still
                // invisible. That window is now micro- to low-millisecond; full closure deferred.
                HLCTimestamp sweepFloor = capturedSweepFloorStore?.GetFloorForPrune(capturedSweepRaft) ?? HLCTimestamp.Zero;

                bool ok = persistenceBackend.PruneKeyValueRevisions(
                    null,
                    configuration.PersistentRevisionRetentionCount,
                    configuration.PersistentRevisionRetentionAge,
                    configuration.PersistentRevisionCleanupBatchSize,
                    sweepFloor,
                    out RevisionPruneResult r);
                pruneResult = r;
                return ok;
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
            long protectedIndex = PitrHorizon.ComputeProtectedIndex(
                raft.WalAdapter, partitionId, now,
                configuration.PitrWindow, configuration.BaseSnapshotInterval);

            // Pass a negative value when no committed entry exists before the boundary so
            // Kommander clears the floor rather than setting it to 0 (which suppresses all compaction).
            raft.SetMinRetainIndex(partitionId, protectedIndex > 0 ? protectedIndex : -1);

            // Recomputes the boundary for the log line; ComputeProtectedIndex derives it
            // internally but does not expose it in order to keep the return type simple.
            long logBoundaryMs = (long)((now - configuration.PitrWindow - configuration.BaseSnapshotInterval) - DateTime.UnixEpoch).TotalMilliseconds;
            logger.LogPitrHorizonUpdated(partitionId, protectedIndex, logBoundaryMs);
        }
    }
}
