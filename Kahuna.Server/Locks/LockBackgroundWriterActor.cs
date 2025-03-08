
using Nixie;
using Kahuna.Persistence;
using Kommander;
using Nixie.Routers;

namespace Kahuna.Locks;

/*
 * Writes dirty locks from memory to disk before they are forced out by backend processes.
 */
public sealed class LockBackgroundWriterActor : IActor<LockBackgroundWriteRequest>
{
    private readonly IRaft raft;
    
    private readonly IActorRef<ConsistentHashActor<PersistenceActor, PersistenceRequest, PersistenceResponse>, PersistenceRequest, PersistenceResponse> persistenceActorRouter;

    private readonly ILogger<IKahuna> logger;
    
    private readonly Queue<LockBackgroundWriteRequest> dirtyLocks = new();
    
    public LockBackgroundWriterActor(
        IActorContext<LockBackgroundWriterActor, LockBackgroundWriteRequest> context,
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
            new(LockBackgroundWriteType.Flush),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromMilliseconds(200)
        );    
    }
    
    public async Task Receive(LockBackgroundWriteRequest message)
    {
        switch (message.Type)
        {
            case LockBackgroundWriteType.Queue:
                dirtyLocks.Enqueue(message);
                break;
            
            case LockBackgroundWriteType.Flush:
                
                if (dirtyLocks.Count == 0)
                    return;
                
                HashSet<int> partitionIds = [];
                
                while (dirtyLocks.TryDequeue(out LockBackgroundWriteRequest? lockRequest))
                {
                    partitionIds.Add(lockRequest.PartitionId);
                    
                    await persistenceActorRouter.Ask(new(
                        PersistenceRequestType.StoreLock,
                        lockRequest.Resource,
                        lockRequest.Owner,
                        lockRequest.FencingToken,
                        lockRequest.Expires.L,
                        lockRequest.Expires.C,
                        (int)lockRequest.Consistency,
                        (int)lockRequest.State
                    ));
                }
                
                foreach (int partitionId in partitionIds)
                    await raft.ReplicateCheckpoint(partitionId);
                
                break;
        }
    }
}