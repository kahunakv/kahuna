
using Nixie;
using Kommander;
using System.Diagnostics;
using Kahuna.Server.Persistence.Backend;
using Polly.Contrib.WaitAndRetry;

namespace Kahuna.Server.Persistence;

/*
 * Writes dirty locks/key-values from memory to disk in batches
 * before they are forced out by backend processes.
 */
public sealed class BackgroundWriterActor : IActor<BackgroundWriteRequest>
{
    private const int WriteRetries = 5;
    
    private const int MaxBatchSize = 500;
    
    private const int MaxPacketSize = 1024 * 512;
    
    private readonly IRaft raft;

    private readonly IPersistenceBackend persistenceBackend;

    private readonly ILogger<IKahuna> logger;
    
    private readonly Queue<BackgroundWriteRequest> dirtyLocks = new();
    
    private readonly Queue<BackgroundWriteRequest> dirtyKeyValues = new();
    
    private readonly HashSet<int> partitionIds = [];
    
    private readonly Stopwatch stopwatch = Stopwatch.StartNew();
    
    private List<PersistenceRequestItem>? pendingLockItems;
    
    private List<PersistenceRequestItem>? pendingKeyValuesItems;
    
    private bool pendingCheckpoint = true;
    
    public BackgroundWriterActor(
        IActorContext<BackgroundWriterActor, BackgroundWriteRequest> context,
        IRaft raft,
        IPersistenceBackend persistenceBackend,
        ILogger<IKahuna> logger
    )
    {
        this.raft = raft;
        this.persistenceBackend = persistenceBackend;
        this.logger = logger;
        
        context.ActorSystem.StartPeriodicTimer(
            context.Self,
            "flush-locks",
            new(BackgroundWriteType.Flush),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromMilliseconds(200)
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
                break;
            
            default:
                throw new NotImplementedException();
        }
    }

    private async ValueTask CheckpointPartitions()
    {
        if (dirtyLocks.Count > 0 || dirtyKeyValues.Count > 0)
            return;
        
        /*if (pendingCheckpoint)
        {
            foreach (int partitionId in partitionIds)
                await raft.ReplicateCheckpoint(partitionId);
            
            partitionIds.Clear();
            pendingCheckpoint = false;
        }*/

        await Task.CompletedTask;
    }

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

            while (dirtyLocks.TryDequeue(out BackgroundWriteRequest? lockRequest))
            {
                if (lockRequest.PartitionId >= 0)
                    partitionIds.Add(lockRequest.PartitionId);

                items.Add(new(
                    lockRequest.Key,
                    lockRequest.Value,
                    lockRequest.Revision,
                    lockRequest.Expires.L,
                    lockRequest.Expires.C,
                    lockRequest.LastUsed.L,
                    lockRequest.LastUsed.C,
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
            bool success = await raft.WriteThreadPool.EnqueueTask(() => persistenceBackend.StoreLocks(items));
            if (!success)
            {
                logger.LogWarning("Coundn't store batch of {Count} locks. Waiting...", items.Count);

                await Task.Delay(timeSpan);
                continue;
            }

            logger.LogDebug("Successfully stored batch of {Count} locks in {Elapsed}ms", items.Count, stopwatch.ElapsedMilliseconds);
            
            pendingCheckpoint = true;
            pendingLockItems = null;
            return;
        }

        pendingLockItems = items;
        
        logger.LogError("Coundn't store batch of {Count} locks", items.Count);
    }

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

            while (dirtyKeyValues.TryDequeue(out BackgroundWriteRequest? keyValueRequest))
            {
                if (keyValueRequest.PartitionId >= 0)
                    partitionIds.Add(keyValueRequest.PartitionId);

                items.Add(new(
                    keyValueRequest.Key,
                    keyValueRequest.Value,
                    keyValueRequest.Revision,
                    keyValueRequest.Expires.L,
                    keyValueRequest.Expires.C,
                    keyValueRequest.LastUsed.L,
                    keyValueRequest.LastUsed.C,
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
            bool success = await raft.WriteThreadPool.EnqueueTask(() => persistenceBackend.StoreKeyValues(items));
            if (!success)
            {
                logger.LogWarning("Coundn't store batch of {Count} key-values. Waiting...", items.Count);
                
                await Task.Delay(timeSpan);
                continue;
            }

            logger.LogDebug("Successfully stored batch of {Count} key-values in {Elapsed}ms", items.Count, stopwatch.ElapsedMilliseconds);
            
            pendingCheckpoint = true;
            pendingKeyValuesItems = null;
            return;
        }
        
        pendingKeyValuesItems = items;
        
        logger.LogError("Coundn't store batch of {Count} key-values", items.Count);
    }
}