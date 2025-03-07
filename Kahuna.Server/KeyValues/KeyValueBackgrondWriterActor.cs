using Kahuna.Persistence;
using Kommander;
using Nixie;
using Nixie.Routers;

namespace Kahuna.KeyValues;

/*
 * Writes dirty keyValues from memory to disk before they are forced out by backend processes.
 */
public sealed class KeyValueBackgroundWriterActor : IActor<KeyValueBackgroundWriteRequest>
{
    private readonly IRaft raft;
    
    private readonly IActorRef<ConsistentHashActor<PersistenceActor, PersistenceRequest, PersistenceResponse>, PersistenceRequest, PersistenceResponse> persistenceActorRouter;

    private readonly ILogger<IKahuna> logger;
    
    private readonly Queue<KeyValueBackgroundWriteRequest> dirtyKeyValues = new();
    
    public KeyValueBackgroundWriterActor(
        IActorContext<KeyValueBackgroundWriterActor, KeyValueBackgroundWriteRequest> context,
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
            "flush-keyValues",
            new(KeyValueBackgroundWriteType.Flush),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromMilliseconds(200)
        );    
    }
    
    public async Task Receive(KeyValueBackgroundWriteRequest message)
    {
        switch (message.Type)
        {
            case KeyValueBackgroundWriteType.Queue:
                dirtyKeyValues.Enqueue(message);
                break;
            
            case KeyValueBackgroundWriteType.Flush:
                
                if (dirtyKeyValues.Count == 0)
                    return;
                
                HashSet<int> partitionIds = [];
                
                while (dirtyKeyValues.TryDequeue(out KeyValueBackgroundWriteRequest? keyValueRequest))
                {
                    partitionIds.Add(keyValueRequest.PartitionId);
                    
                    /*await persistenceActorRouter.Ask(new(
                        PersistenceRequestType.Store,
                        keyValueRequest.Key,
                        keyValueRequest.Value,
                        keyValueRequest.Expires.L,
                        keyValueRequest.Expires.C,
                        keyValueRequest.Consistency,
                        keyValueRequest.State
                    ));*/
                }
                
                foreach (int partitionId in partitionIds)
                    await raft.ReplicateCheckpoint(partitionId);
                
                break;
        }
    }
}