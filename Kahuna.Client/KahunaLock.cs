
namespace Kahuna.Client;

/// <summary>
/// Represents a lock acquired from the Kahuna service.
/// </summary>
public sealed class KahunaLock : IAsyncDisposable
{
    private readonly KahunaClient locks;

    private readonly KahunaLockAcquireResult result;

    private readonly string resource;
    
    private readonly string? lockId;

    private bool disposed;

    public bool IsAcquired => result == KahunaLockAcquireResult.Success;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="locks"></param>
    /// <param name="resource"></param>
    /// <param name="lockInfo"></param>
    public KahunaLock(KahunaClient locks, string resource, (KahunaLockAcquireResult result, string? lockId) lockInfo)
    {
        this.locks = locks;
        this.resource = resource;
        this.result = lockInfo.result;
        this.lockId = lockInfo.lockId;
    }

    /// <summary>
    /// Frees the lock after it's no longer needed
    /// </summary>
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