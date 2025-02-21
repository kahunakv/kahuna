
using Nixie;
using System.Collections.Concurrent;

namespace Kahuna;

public sealed class LockManager
{
    private readonly ActorSystem actorSystem;
    
    private readonly ConcurrentDictionary<string, Lazy<IActorRefStruct<LockActor, LockRequest, LockResponse>>> locks = new();
    
    public LockManager(ActorSystem actorSystem)
    {
        this.actorSystem = actorSystem;
    }
    
    public async Task<LockResponseType> TryLock(string lockName, string lockId, int expiresMs)
    {
        if (expiresMs <= 0)
            return LockResponseType.Errored;
        
        Lazy<IActorRefStruct<LockActor, LockRequest, LockResponse>> lazyGreeter = locks.GetOrAdd(lockName, GetOrCreateLocker);
        IActorRefStruct<LockActor, LockRequest, LockResponse> greeter = lazyGreeter.Value;
        
        LockRequest request = new(LockRequestType.TryLock, lockId, expiresMs);

        LockResponse response = await greeter.Ask(request);
        return response.Type;
    }

    public async Task<LockResponseType> TryUnlock(string lockName, string lockId)
    {
        Lazy<IActorRefStruct<LockActor, LockRequest, LockResponse>> lazyGreeter = locks.GetOrAdd(lockName, GetOrCreateLocker);
        IActorRefStruct<LockActor, LockRequest, LockResponse> greeter = lazyGreeter.Value;
        
        LockRequest request = new(LockRequestType.TryUnlock, lockId, 0);

        LockResponse response = await greeter.Ask(request);
        return response.Type;
    }
    
    private Lazy<IActorRefStruct<LockActor, LockRequest, LockResponse>> GetOrCreateLocker(string lockName)
    {
        return new(() => actorSystem.SpawnStruct<LockActor, LockRequest, LockResponse>(lockName));
    }
}