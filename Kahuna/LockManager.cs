
using Nixie;
using System.Collections.Concurrent;

namespace Kahuna;

public sealed class LockManager
{
    private readonly ActorSystem actorSystem;
    
    private readonly ConcurrentDictionary<string, Lazy<IActorRef<LockActor, LockRequest, LockResponse>>> locks = new();
    
    public LockManager(ActorSystem actorSystem)
    {
        this.actorSystem = actorSystem;
    }
    
    public async Task<LockResponseType> TryLock(string lockName, string lockId)
    {
        Lazy<IActorRef<LockActor, LockRequest, LockResponse>> lazyGreeter = locks.GetOrAdd(lockName, GetOrCreateLocker);
        IActorRef<LockActor, LockRequest, LockResponse> greeter = lazyGreeter.Value;
        
        LockRequest request = new(LockRequestType.TryLock, lockId);

        LockResponse? response = await greeter.Ask(request);
        if (response is null)
            return LockResponseType.Errored;

        return response.Type;
    }

    public async Task<LockResponseType> TryUnlock(string lockName, string lockId)
    {
        Lazy<IActorRef<LockActor, LockRequest, LockResponse>> lazyGreeter = locks.GetOrAdd(lockName, GetOrCreateLocker);
        IActorRef<LockActor, LockRequest, LockResponse> greeter = lazyGreeter.Value;
        
        LockRequest request = new(LockRequestType.TryUnlock, lockId);

        LockResponse? response = await greeter.Ask(request);
        if (response is null)
            return LockResponseType.Busy;

        return response.Type;
    }
    
    private Lazy<IActorRef<LockActor, LockRequest, LockResponse>> GetOrCreateLocker(string lockName)
    {
        return new(() => actorSystem.Spawn<LockActor, LockRequest, LockResponse>(lockName));
    }
}