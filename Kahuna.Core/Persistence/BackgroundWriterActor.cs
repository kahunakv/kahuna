
using Nixie;
using Kommander;
using System.Diagnostics;
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Logging;
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

    private readonly KahunaConfiguration configuration;

    private readonly ILogger<IKahuna> logger;
    
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
    
    public BackgroundWriterActor(
        IActorContext<BackgroundWriterActor, BackgroundWriteRequest> context,
        IRaft raft,
        IPersistenceBackend persistenceBackend,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    )
    {
        this.raft = raft;
        this.persistenceBackend = persistenceBackend;
        this.configuration = configuration;
        this.logger = logger;
        
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
                break;

            case BackgroundWriteType.FlushAndNotify:
                await FlushLocks();
                await FlushKeyValues();
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
        TimeSpan maxTime = TimeSpan.FromSeconds(30);

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
    /// Attempts to flush all pending locks in the background writer to the persistence backend.
    /// If there are no pending locks, the method returns immediately.
    /// If any locks fail to flush, they may be retried up to the maximum retry count.
    /// Successfully flushed locks are removed from the queue, and any failed locks are retained for future retries.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation of flushing locks.</returns>
    private async ValueTask FlushLocks()
    {
        if (dirtyLocks.Count == 0)
            return;

        stopwatch.Restart();
        
        List<PersistenceRequestItem> items;

        if (pendingLockItems != null)
            items = pendingLockItems;
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
                
                if (++counter >= MaxBatchSize || size >= MaxPacketSize)
                    break;
            }
        }

        IEnumerable<TimeSpan> backoffDelays = Backoff.DecorrelatedJitterBackoffV2(
            medianFirstRetryDelay: TimeSpan.FromMilliseconds(1000),
            retryCount: WriteRetries
        );

        foreach (TimeSpan timeSpan in backoffDelays)
        {
            bool success = await raft.ReadScheduler.EnqueueTask(0, () => persistenceBackend.StoreLocks(items));
            if (!success)
            {
                logger.LogWarning("Coundn't store batch of {Count} locks. Waiting...", items.Count);

                await Task.Delay(timeSpan);
                continue;
            }

            logger.LogSuccessfullyStoredLocks(items.Count, stopwatch.ElapsedMilliseconds);
            
            pendingCheckpoint = true;
            pendingLockItems = null;
            return;
        }

        pendingLockItems = items;
        
        logger.LogError("Coundn't store batch of {Count} locks", items.Count);
    }

    /// <summary>
    /// Flushes the queued key-value items by processing and storing them persistently.
    /// If there are no queued key-value items, the method immediately returns.
    /// Batches are created from the queue, retrying on failure with exponential backoff.
    /// Successfully stored items are cleared, while failed batches are retained for further attempts.
    /// </summary>
    /// <returns>A value task representing the asynchronous operation of flushing key-value items.</returns>
    private async ValueTask FlushKeyValues()
    {
        if (dirtyKeyValues.Count == 0)
            return;
        
        stopwatch.Restart();

        List<PersistenceRequestItem> items;

        if (pendingKeyValuesItems != null)
            items = pendingKeyValuesItems;
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
                    keyValueRequest.State
                ));
                
                if (keyValueRequest.Value is not null)
                    size += keyValueRequest.Value.Length;
                
                if (++counter >= MaxBatchSize || size >= MaxPacketSize)
                    break;
            }
        }

        IEnumerable<TimeSpan> backoffDelays = Backoff.DecorrelatedJitterBackoffV2(
            medianFirstRetryDelay: TimeSpan.FromMilliseconds(1000),
            retryCount: WriteRetries
        );
        
        foreach (TimeSpan timeSpan in backoffDelays)
        {
            bool success = await raft.ReadScheduler.EnqueueTask(0, () => persistenceBackend.StoreKeyValues(items));
            if (!success)
            {
                logger.LogWarning("Coundn't store batch of {Count} key-values. Waiting...", items.Count);
                
                await Task.Delay(timeSpan);
                continue;
            }

            logger.LogSuccessfullyStoredKeyValues(items.Count, stopwatch.ElapsedMilliseconds);

            if (ConfigurationValidator.IsPersistentRevisionRetentionEnabled(configuration)
                && configuration.PersistentRevisionCleanupOnWrite)
            {
                foreach (PersistenceRequestItem item in items)
                {
                    if (pendingRevisionCleanupKeys.Count < MaxPendingCleanupKeys)
                        pendingRevisionCleanupKeys.Add(item.Key);
                }
            }

            pendingCheckpoint = true;
            pendingKeyValuesItems = null;
            return;
        }
        
        pendingKeyValuesItems = items;

        logger.LogError("Coundn't store batch of {Count} key-values", items.Count);
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

        try
        {
            bool success = await raft.ReadScheduler.EnqueueTask(0, () =>
            {
                bool ok = persistenceBackend.PruneKeyValueRevisions(
                    keysToClean,
                    configuration.PersistentRevisionRetentionCount,
                    configuration.PersistentRevisionRetentionAge,
                    configuration.PersistentRevisionCleanupBatchSize,
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

        try
        {
            bool success = await raft.ReadScheduler.EnqueueTask(0, () =>
            {
                bool ok = persistenceBackend.PruneKeyValueRevisions(
                    null,
                    configuration.PersistentRevisionRetentionCount,
                    configuration.PersistentRevisionRetentionAge,
                    configuration.PersistentRevisionCleanupBatchSize,
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
}
