
// ReSharper disable ConvertToAutoProperty

/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using System.Text.Json;
using Kahuna.Shared.Locks;

namespace Kahuna.Client;

/// <summary>
/// Represents a distributed lock managed by the Kahuna client library.
/// Provides functionality to acquire, extend, and release a lock with fencing token support.
/// </summary>
public sealed class KahunaLock : IAsyncDisposable
{
    private static readonly JsonSerializerOptions DefaultJsonSerializerOptions = new() { WriteIndented = false };
    
    private readonly KahunaClient client;

    private readonly KahunaLockAcquireResult result;

    private readonly long fencingToken;

    private readonly string resource;
    
    private readonly byte[]? owner;

    private readonly LockDurability durability;
    
    private readonly string? servedFrom;

    private bool disposed;

    /// <summary>
    /// Indicates whether the lock has been successfully acquired.
    /// Returns true if the lock acquisition operation completed with a success result; otherwise, false.
    /// </summary>
    public bool IsAcquired => result == KahunaLockAcquireResult.Success;

    /// <summary>
    /// Represents the fencing token assigned to this lock.
    /// The fencing token is a monotonically increasing number that ensures sequential consistency
    /// in distributed systems by preventing stale writes or operations from older lock holders.
    /// </summary>
    public long FencingToken => fencingToken;

    /// <summary>
    /// Gets the owner identifier of the lock.
    /// The owner is represented as a byte array and uniquely identifies the entity
    /// that currently holds the lock. Throws a KahunaException if the lock has not been acquired.
    /// </summary>
    public byte[] Owner => owner ?? throw new KahunaException("Lock was not acquired", LockResponseType.Errored);

    /// <summary>
    /// Gets the owner of the lock as a string representation.
    /// If the lock owner information is available, it returns the owner's string value; otherwise, an empty string.
    /// </summary>
    public string OwnerAsString => owner is not null ? Encoding.UTF8.GetString(owner) : "";

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

        if (string.IsNullOrEmpty(servedFrom) || !client.Options.UpgradeUrls)
            return await client.TryExtendLock(resource, owner, duration, durability, cancellationToken);
        
        return await client.Communication.TryExtendLock(servedFrom, resource, owner, (int)duration.TotalMilliseconds, durability, cancellationToken);
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

        if (string.IsNullOrEmpty(servedFrom) || !client.Options.UpgradeUrls)
            return await client.TryExtendLock(resource, owner, durationMs, durability, cancellationToken);
        
        return await client.Communication.TryExtendLock(servedFrom, resource, owner, durationMs, durability, cancellationToken);
    }
    
    /// <summary>
    /// Obtains information about the lock (even if the lock wasn't Acquired)
    /// </summary>
    /// <returns></returns>
    /// <exception cref="KahunaException"></exception>
    public async Task<KahunaLockInfo?> GetInfo(CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(servedFrom) || !client.Options.UpgradeUrls)
            return await client.GetLockInfo(resource, durability, cancellationToken);
        
        return await client.Communication.GetLock(servedFrom, resource, durability, cancellationToken);
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
            if (string.IsNullOrEmpty(servedFrom) || !client.Options.UpgradeUrls)
            {
                await client.Unlock(resource, owner, durability);
                return;
            }

            await client.Communication.TryUnlock(servedFrom, resource, owner, durability, CancellationToken.None);
        }
    }

    /// <summary>
    /// Serializes the lock's state, including the resource name, acquisition status, fencing token,
    /// and owner, into a JSON string format.
    /// </summary>
    /// <returns>
    /// A JSON string representation of the lock's state.
    /// </returns>
    public string ToJson()
    {
        return JsonSerializer.Serialize(new
        {
            resource,
            isAcquired = IsAcquired,
            fencingToken = FencingToken,
            owner = OwnerAsString
            //durability = durability.ToString()
        }, DefaultJsonSerializerOptions);
    }

    ~KahunaLock()
    {
        //if (!disposed)
        //    locks.logger.LogError("Lock was not disposed: {Resource}", resource);

        if (!disposed)
            Console.WriteLine("Lock was not disposed: {0}", resource);
    }    
}