namespace Kahuna.Client;

public sealed class KahunaLock : IAsyncDisposable
{
    private readonly KahunaClient locks;

    private readonly KahunaLockAdquireResult result;

    private readonly string resource;
    
    private readonly string? lockId;

    private bool disposed;

    public bool IsAcquired => result == KahunaLockAdquireResult.Success;

    public KahunaLock(KahunaClient locks, string resource, (KahunaLockAdquireResult result, string? lockId) lockInfo)
    {
        this.locks = locks;
        this.resource = resource;
        this.result = lockInfo.result;
        this.lockId = lockInfo.lockId;
    }

    public async ValueTask DisposeAsync()
    {
        disposed = true;

        GC.SuppressFinalize(this);

        if (!string.IsNullOrEmpty(lockId))
            await locks.Unlock(resource, lockId);
    }

    ~KahunaLock()
    {
        //if (!disposed)
        //    locks.logger.LogError("Lock was not disposed: {Resource}", resource);

        if (!disposed)
            Console.WriteLine("Lock was not disposed: {0}", resource);
    }
}