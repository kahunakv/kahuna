
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
    private readonly KahunaClient client;

    private readonly KahunaLockAcquireResult result;

    private readonly long fencingToken;

    private readonly string resource;
    
    private readonly byte[]? owner;

    private readonly LockDurability durability;
    
    private readonly string? servedFrom;

    private bool disposed;

    public bool IsAcquired => result == KahunaLockAcquireResult.Success;
    
    public long FencingToken => fencingToken;
    
    public byte[] Owner => owner ?? throw new KahunaException("Lock was not acquired", LockResponseType.Errored);

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="client"></param>
    /// <param name="resource"></param>
    /// <param name="result"></param>
    /// <param name="owner"></param>
    /// <param name="durability"></param>
    /// <param name="fencingToken"></param>
    /// <param name="servedFrom"></param>
    public KahunaLock(
        KahunaClient client, 
        string resource, 
        KahunaLockAcquireResult result, 
        byte[]? owner, 
        LockDurability durability, 
        long fencingToken, 
        string? servedFrom
    )
    {
        this.client = client;
        this.resource = resource;
        this.result = result;
        this.owner = owner;
        this.servedFrom = servedFrom;
        this.durability = durability;
        this.fencingToken = fencingToken;
    }
    
    /// <summary>
    /// Try to extend the lock by the specified duration.
    /// Returns true if the lock was successfully extended, false otherwise. 
    /// </summary>
    /// <param name="duration"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaException"></exception>
    public async Task<(bool, long)> TryExtend(TimeSpan duration, CancellationToken cancellationToken = default)
    {
        if (!IsAcquired || owner is null)
            throw new KahunaException("Lock was not acquired", LockResponseType.Errored);

        if (string.IsNullOrEmpty(servedFrom) || !client.UpgradeUrls)
            return await client.TryExtend(resource, owner, duration, durability, cancellationToken);
        
        return await client.Communication.TryExtend(servedFrom, resource, owner, (int)duration.TotalMilliseconds, durability, cancellationToken);
    }
    
    /// <summary>
    /// Try to extend the lock by the specified duration.
    /// Returns true if the lock was successfully extended, false otherwise. 
    /// </summary>
    /// <param name="durationMs"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    /// <exception cref="KahunaException"></exception>
    public async Task<(bool, long)> TryExtend(int durationMs, CancellationToken cancellationToken = default)
    {
        if (!IsAcquired || owner is null)
            throw new KahunaException("Lock was not acquired", LockResponseType.Errored);

        if (string.IsNullOrEmpty(servedFrom) || !client.UpgradeUrls)
            return await client.TryExtend(resource, owner, durationMs, durability, cancellationToken);
        
        return await client.Communication.TryExtend(servedFrom, resource, owner, durationMs, durability, cancellationToken);
    }
    
    /// <summary>
    /// Obtains information about the lock (even if the lock wasn't adquired)
    /// </summary>
    /// <returns></returns>
    /// <exception cref="KahunaException"></exception>
    public async Task<KahunaLockInfo?> GetInfo(CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(servedFrom) || !client.UpgradeUrls)
            return await client.GetLockInfo(resource, durability, cancellationToken);
        
        return await client.Communication.Get(servedFrom, resource, durability, cancellationToken);
    }

    /// <summary>
    /// Frees the lock after it's no longer needed
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        disposed = true;

        GC.SuppressFinalize(this);

        if (IsAcquired && owner is not null)
        {
            if (string.IsNullOrEmpty(servedFrom) || !client.UpgradeUrls)
            {
                await client.Unlock(resource, owner, durability);
                return;
            }

            await client.Communication.TryUnlock(servedFrom, resource, owner, durability, CancellationToken.None);
        }
    }

    ~KahunaLock()
    {
        //if (!disposed)
        //    locks.logger.LogError("Lock was not disposed: {Resource}", resource);

        if (!disposed)
            Console.WriteLine("Lock was not disposed: {0}", resource);
    }
}