
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Shared.Locks;
// ReSharper disable ConvertToAutoProperty

namespace Kahuna.Client;

/// <summary>
/// Represents a lock acquired from the Kahuna service.
/// </summary>
public sealed class KahunaLock : IAsyncDisposable
{
    private readonly KahunaClient locks;

    private readonly KahunaLockAcquireResult result;

    private readonly long fencingToken;

    private readonly string resource;
    
    private readonly byte[]? owner;

    private readonly LockDurability durability;

    private bool disposed;

    public bool IsAcquired => result == KahunaLockAcquireResult.Success;
    
    public long FencingToken => fencingToken;
    
    public byte[] Owner => owner ?? throw new KahunaException("Lock was not acquired", LockResponseType.Errored);

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="locks"></param>
    /// <param name="resource"></param>
    /// <param name="lockInfo"></param>
    public KahunaLock(KahunaClient locks, string resource, (KahunaLockAcquireResult result, byte[]? owner, LockDurability durability, long fencingToken) lockInfo)
    {
        this.locks = locks;
        this.resource = resource;
        this.result = lockInfo.result;
        this.owner = lockInfo.owner;
        this.durability = lockInfo.durability;
        this.fencingToken = lockInfo.fencingToken;
    }
    
    /// <summary>
    /// Try to extend the lock by the specified duration.
    /// Returns true if the lock was successfully extended, false otherwise.
    /// </summary>
    /// <param name="duration"></param>
    /// <returns></returns>
    /// <exception cref="KahunaException"></exception>
    public async Task<(bool, long)> TryExtend(TimeSpan duration)
    {
        if (!IsAcquired || owner is null)
            throw new KahunaException("Lock was not acquired", LockResponseType.Errored);

        return await locks.TryExtend(resource, owner, duration, durability);
    }
    
    /// <summary>
    /// Try to extend the lock by the specified duration.
    /// Returns true if the lock was successfully extended, false otherwise.
    /// </summary>
    /// <param name="durationMs"></param>
    /// <returns></returns>
    /// <exception cref="KahunaException"></exception>
    public async Task<(bool, long)> TryExtend(int durationMs)
    {
        if (!IsAcquired || owner is null)
            throw new KahunaException("Lock was not acquired", LockResponseType.Errored);

        return await locks.TryExtend(resource, owner, durationMs, durability);
    }
    
    /// <summary>
    /// Obtains information about the lock (even if the lock wasn't adquired)
    /// </summary>
    /// <returns></returns>
    /// <exception cref="KahunaException"></exception>
    public async Task<KahunaLockInfo?> GetInfo()
    {
        return await locks.GetLockInfo(resource, durability);
    }

    /// <summary>
    /// Frees the lock after it's no longer needed
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        disposed = true;

        GC.SuppressFinalize(this);

        if (IsAcquired && owner is not null)
            await locks.Unlock(resource, owner, durability);
    }

    ~KahunaLock()
    {
        //if (!disposed)
        //    locks.logger.LogError("Lock was not disposed: {Resource}", resource);

        if (!disposed)
            Console.WriteLine("Lock was not disposed: {0}", resource);
    }
}