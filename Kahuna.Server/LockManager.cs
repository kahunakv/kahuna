
using Nixie;
using System.Collections.Concurrent;

namespace Kahuna;

/// <summary>
/// LockManager is a singleton class that manages lock actors.
/// </summary>
public sealed class LockManager : IKahuna
{
    private readonly ActorSystem actorSystem;
    
    private readonly ConcurrentDictionary<string, Lazy<IActorRefStruct<LockActor, LockRequest, LockResponse>>> locks = new();
    
    public LockManager(ActorSystem actorSystem)
    {
        this.actorSystem = actorSystem;
    }
    
    /// <summary>
    /// Passes a TryLock request to the locker actor for the given lock name.
    /// </summary>
    /// <param name="lockName"></param>
    /// <param name="lockId"></param>
    /// <param name="expiresMs"></param>
    /// <returns></returns>
    public async Task<LockResponseType> TryLock(string lockName, string lockId, int expiresMs)
    {
        if (expiresMs <= 0)
            return LockResponseType.Errored;
        
        Lazy<IActorRefStruct<LockActor, LockRequest, LockResponse>> lazyLocker = locks.GetOrAdd(lockName, GetOrCreateLocker);
        IActorRefStruct<LockActor, LockRequest, LockResponse> greeter = lazyLocker.Value;
        
        LockRequest request = new(LockRequestType.TryLock, lockId, expiresMs);

        LockResponse response = await greeter.Ask(request);
        return response.Type;
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
        
        Lazy<IActorRefStruct<LockActor, LockRequest, LockResponse>> lazyLocker = locks.GetOrAdd(lockName, GetOrCreateLocker);
        IActorRefStruct<LockActor, LockRequest, LockResponse> locker = lazyLocker.Value;
        
        LockRequest request = new(LockRequestType.TryExtendLock, lockId, expiresMs);

        LockResponse response = await locker.Ask(request);
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
        Lazy<IActorRefStruct<LockActor, LockRequest, LockResponse>> lazyLocker = locks.GetOrAdd(lockName, GetOrCreateLocker);
        IActorRefStruct<LockActor, LockRequest, LockResponse> locker = lazyLocker.Value;
        
        LockRequest request = new(LockRequestType.TryUnlock, lockId, 0);

        LockResponse response = await locker.Ask(request);
        return response.Type;
    }
    
    /// <summary>
    /// Gets or creates a locker actor for the given lock name.
    /// </summary>
    /// <param name="lockName"></param>
    /// <returns></returns>
    private Lazy<IActorRefStruct<LockActor, LockRequest, LockResponse>> GetOrCreateLocker(string lockName)
    {
        return new(() => actorSystem.SpawnStruct<LockActor, LockRequest, LockResponse>(lockName));
    }
}