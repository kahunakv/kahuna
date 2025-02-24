
using Kommander;
using Nixie;
using Nixie.Routers;

namespace Kahuna;

/// <summary>
/// LockManager is a singleton class that manages lock actors.
/// </summary>
public sealed class LockManager : IKahuna
{
    private readonly ActorSystem actorSystem;
    
    private readonly IActorRefStruct<ConsistentHashActorStruct<LockActor, LockRequest, LockResponse>, LockRequest, LockResponse> locksRouter;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="logger"></param>
    public LockManager(ActorSystem actorSystem, IRaft raft, ILogger<IKahuna> logger)
    {
        this.actorSystem = actorSystem;

        int workers = Environment.ProcessorCount * 2;
        
        List<IActorRefStruct<LockActor, LockRequest, LockResponse>> responderInstances = new(workers);

        for (int i = 0; i < workers; i++)
            responderInstances.Add(actorSystem.SpawnStruct<LockActor, LockRequest, LockResponse>(null, logger));

        locksRouter = actorSystem.CreateConsistentHashRouterStruct(responderInstances);
    }
    
    /// <summary>
    /// Passes a TryLock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="lockName"></param>
    /// <param name="lockId"></param>
    /// <param name="expiresMs"></param>
    /// <returns></returns>
    public async Task<(LockResponseType, long)> TryLock(string lockName, string lockId, int expiresMs)
    {
        if (expiresMs <= 0)
            return (LockResponseType.Errored, -1);
        
        LockRequest request = new(LockRequestType.TryLock, lockName, lockId, expiresMs);

        LockResponse response = await locksRouter.Ask(request);
        return (response.Type, response.FencingToken);
    }
    
    /// <summary>
    /// Passes a TryExtendLock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="lockName"></param>
    /// <param name="lockId"></param>
    /// <param name="expiresMs"></param>
    /// <returns></returns>
    public async Task<LockResponseType> TryExtendLock(string lockName, string lockId, int expiresMs)
    {
        if (expiresMs <= 0)
            return LockResponseType.Errored;
        
        LockRequest request = new(LockRequestType.TryExtendLock, lockName, lockId, expiresMs);

        LockResponse response = await locksRouter.Ask(request);
        return response.Type;
    }

    /// <summary>
    /// Passes a TryUnlock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="lockName"></param>
    /// <param name="lockId"></param>
    /// <returns></returns>
    public async Task<LockResponseType> TryUnlock(string lockName, string lockId)
    {
        LockRequest request = new(LockRequestType.TryUnlock, lockName, lockId, 0);

        LockResponse response = await locksRouter.Ask(request);
        return response.Type;
    }
    
    /// <summary>
    /// Passes a Get request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="lockName"></param>
    /// <param name="lockId"></param>
    /// <returns></returns>
    public async Task<(LockResponseType, ReadOnlyLockContext?)> GetLock(string lockName)
    {
        LockRequest request = new(LockRequestType.Get, lockName, "", 0);

        LockResponse response = await locksRouter.Ask(request);
        return (response.Type, response.Context);
    }
}