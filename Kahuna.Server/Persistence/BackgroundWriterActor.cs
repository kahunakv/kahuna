
using System.Diagnostics;
using Nixie;
using Nixie.Routers;

using Kommander;

namespace Kahuna.Server.Persistence;

/*
 * Writes dirty locks/key-values from memory to disk in batches before they are forced out by backend processes.
 */
public sealed class BackgroundWriterActor : IActor<BackgroundWriteRequest>
{
    private readonly IRaft raft;
    
    private readonly IActorRef<RoundRobinActor<PersistenceActor, PersistenceRequest, PersistenceResponse>, PersistenceRequest, PersistenceResponse> persistenceActorRouter;

    private readonly ILogger<IKahuna> logger;
    
    private readonly Queue<BackgroundWriteRequest> dirtyLocks = new();
    
    private readonly Queue<BackgroundWriteRequest> dirtyKeyValues = new();
    
    private readonly HashSet<int> partitionIds = [];
    
    private readonly Stopwatch stopwatch = Stopwatch.StartNew();
    
    private bool pendingCheckpoint = true;
    
    public BackgroundWriterActor(
        IActorContext<BackgroundWriterActor, BackgroundWriteRequest> context,
        IRaft raft,
        IActorRef<RoundRobinActor<PersistenceActor, PersistenceRequest, PersistenceResponse>, PersistenceRequest, PersistenceResponse> persistenceActorRouter,
        ILogger<IKahuna> logger
    )
    {
        this.raft = raft;
        this.persistenceActorRouter = persistenceActorRouter;
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
                throw new ArgumentOutOfRangeException();
        }
    }

    private async ValueTask CheckpointPartitions()
    {
        if (dirtyLocks.Count == 0 && dirtyKeyValues.Count == 0)
        {
            if (pendingCheckpoint)
            {
                foreach (int partitionId in partitionIds)
                    await raft.ReplicateCheckpoint(partitionId);
                
                partitionIds.Clear();
                pendingCheckpoint = false;
            }
        }
    }

    private async ValueTask FlushLocks()
    {
        if (dirtyLocks.Count == 0)
            return;
        
        stopwatch.Restart();
                
        List<PersistenceRequestItem> items = [];
                
        while (dirtyLocks.TryDequeue(out BackgroundWriteRequest? keyValueRequest))
        {
            if (keyValueRequest.PartitionId >= 0)
                partitionIds.Add(keyValueRequest.PartitionId);
            
            items.Add(new(
                keyValueRequest.Key, 
                keyValueRequest.Value, 
                keyValueRequest.Revision, 
                keyValueRequest.Expires.L, 
                keyValueRequest.Expires.C, 
                keyValueRequest.State
            ));
        }
        
        PersistenceResponse? response = await persistenceActorRouter.Ask(new(PersistenceRequestType.StoreLock, items));
        
        if (response == null)
        {
            logger.LogError("Coundn't store batch of {Count} locks", items.Count);
            return;
        }

        if (response.Type == PersistenceResponseType.Failed)
        {
            logger.LogError("Coundn't store batch of {Count} locks", items.Count);
            return;
        }
        
        logger.LogDebug("Successfully stored batch of {Count} locks in {Elapsed}ms", items.Count, stopwatch.ElapsedMilliseconds);
        
        pendingCheckpoint = true;
    }
    
    private async ValueTask FlushKeyValues()
    {
        if (dirtyKeyValues.Count == 0)
            return;
        
        stopwatch.Restart();

        List<PersistenceRequestItem> items = [];
                
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
                keyValueRequest.State
            ));
        }
        
        PersistenceResponse? response = await persistenceActorRouter.Ask(new(PersistenceRequestType.StoreKeyValue, items));

        if (response == null)
        {
            logger.LogError("Coundn't store batch of {Count} key-values", items.Count);
            return;
        }

        if (response.Type == PersistenceResponseType.Failed)
        {
            logger.LogError("Coundn't store batch of {Count} key-values", items.Count);
            return;
        }
        
        logger.LogDebug("Successfully stored batch of {Count} key-values in {Elapsed}ms", items.Count, stopwatch.ElapsedMilliseconds);
        
        pendingCheckpoint = true;
    }
}