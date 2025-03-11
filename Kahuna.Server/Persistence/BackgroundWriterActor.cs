
using Nixie;
using Kahuna.Persistence;
using Kommander;
using Nixie.Routers;

namespace Kahuna.Locks;

/*
 * Writes dirty locks from memory to disk before they are forced out by backend processes.
 */
public sealed class BackgroundWriterActor : IActor<BackgroundWriteRequest>
{
    private readonly IRaft raft;
    
    private readonly IActorRef<ConsistentHashActor<PersistenceActor, PersistenceRequest, PersistenceResponse>, PersistenceRequest, PersistenceResponse> persistenceActorRouter;

    private readonly ILogger<IKahuna> logger;
    
    private readonly Queue<BackgroundWriteRequest> dirtyLocks = new();
    
    private readonly Queue<BackgroundWriteRequest> dirtyKeyValues = new();
    
    private readonly HashSet<int> partitionIds = [];
    
    public BackgroundWriterActor(
        IActorContext<BackgroundWriterActor, BackgroundWriteRequest> context,
        IRaft raft,
        IActorRef<ConsistentHashActor<PersistenceActor, PersistenceRequest, PersistenceResponse>, PersistenceRequest, PersistenceResponse> persistenceActorRouter,
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
                await FlushLocks();
                await FlushKeyValues();
                break;
        }
    }

    private async ValueTask FlushLocks()
    {
        if (dirtyLocks.Count == 0)
            return;
                
        while (dirtyLocks.TryDequeue(out BackgroundWriteRequest? lockRequest))
        {
            partitionIds.Add(lockRequest.PartitionId);
                    
            PersistenceResponse? response = await persistenceActorRouter.Ask(new(
                PersistenceRequestType.StoreLock,
                lockRequest.Resource,
                lockRequest.Owner,
                lockRequest.FencingToken,
                lockRequest.Expires.L,
                lockRequest.Expires.C,
                lockRequest.Consistency,
                lockRequest.State
            ));

            if (response == null)
                break;

            if (response.Type == PersistenceResponseType.Failed)
                break;
        }

        if (partitionIds.Count == 0)
            return;
        
        foreach (int partitionId in partitionIds)
            await raft.ReplicateCheckpoint(partitionId);
                
        partitionIds.Clear();
    }
    
    private async ValueTask FlushKeyValues()
    {
        if (dirtyLocks.Count == 0)
            return;
                
        while (dirtyLocks.TryDequeue(out BackgroundWriteRequest? lockRequest))
        {
            partitionIds.Add(lockRequest.PartitionId);
                    
            PersistenceResponse? response = await persistenceActorRouter.Ask(new(
                PersistenceRequestType.StoreKeyValue,
                lockRequest.Resource,
                lockRequest.Owner,
                lockRequest.FencingToken,
                lockRequest.Expires.L,
                lockRequest.Expires.C,
                lockRequest.Consistency,
                lockRequest.State
            ));

            if (response == null)
                break;

            if (response.Type == PersistenceResponseType.Failed)
                break;
        }

        if (partitionIds.Count == 0)
            return;
        
        foreach (int partitionId in partitionIds)
            await raft.ReplicateCheckpoint(partitionId);
                
        partitionIds.Clear();
    }
}