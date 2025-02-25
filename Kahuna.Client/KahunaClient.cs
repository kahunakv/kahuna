
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using Flurl.Http;
using Kahuna.Client.Communication;
using Microsoft.Extensions.Logging;

namespace Kahuna.Client;

/// <summary>
/// Client for the Kahuna service
/// </summary>
public class KahunaClient
{
    private readonly string url;

    private readonly ILogger<KahunaClient>? logger;

    private readonly HttpCommunication communication;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="url"></param>
    /// <param name="logger"></param>
    public KahunaClient(string url, ILogger<KahunaClient>? logger)
    {
        this.url = url;
        this.logger = logger;
        this.communication = new(logger);
    }
    
    private async Task<KahunaLockAcquireResult> TryAcquireLock(string resource, string lockId, TimeSpan expiryTime, KahunaLockConsistency consistency)
    {
        return await communication.TryAcquireLock(url, resource, lockId, (int)expiryTime.TotalMilliseconds, consistency).ConfigureAwait(false);
    }
    
    private async Task<(KahunaLockAcquireResult, string?)> PeriodicallyTryAcquireLock(
        string resource, 
        TimeSpan expiryTime, 
        TimeSpan wait, 
        TimeSpan retry,
        KahunaLockConsistency consistency
    )
    {
        try
        {
            string owner = Guid.NewGuid().ToString("N");
            
            Stopwatch stopWatch = Stopwatch.StartNew();
            KahunaLockAcquireResult result = KahunaLockAcquireResult.Error;

            while (stopWatch.Elapsed < wait)
            {
                result = await TryAcquireLock(resource, owner, expiryTime, consistency).ConfigureAwait(false);

                if (result != KahunaLockAcquireResult.Success)
                {
                    await Task.Delay(retry).ConfigureAwait(false);
                    continue;
                }

                return (result, owner);
            }

            return (result, null);
        }
        catch (Exception ex)
        {
            logger?.LogError("Error locking lock instance: {Message}", ex.Message);

            return (KahunaLockAcquireResult.Error, null);
        }
    }
    
    /// <summary>
    /// Tries to acquire a lock on a resource with a given expiry time
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="expiryTime"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    private async Task<(KahunaLockAcquireResult, string?)> SingleTimeTryAcquireLock(string resource, TimeSpan expiryTime, KahunaLockConsistency consistency)
    {
        try
        {
            string owner = Guid.NewGuid().ToString("N");

            KahunaLockAcquireResult result = await TryAcquireLock(resource, owner, expiryTime, consistency).ConfigureAwait(false);

            return (result, owner);
        }
        catch (Exception ex)
        {
            logger?.LogError("Error locking lock instance: {Message}", ex.Message);

            return (KahunaLockAcquireResult.Error, null);
        }
    }
    
    /// <summary>
    /// Gets or creates a lock on a resource with a given expiry time.
    /// If the lock can't be acquired immediately, it will try to acquire it periodically 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="expiryTime"></param>
    /// <param name="waitTime"></param>
    /// <param name="retryTime"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<KahunaLock> GetOrCreateLock(string resource, int expiryTime = 30000, int waitTime = 0, int retryTime = 0, KahunaLockConsistency consistency = KahunaLockConsistency.Ephemeral)
    {
        TimeSpan expiry = TimeSpan.FromMilliseconds(expiryTime);
        TimeSpan wait = TimeSpan.FromMilliseconds(waitTime);
        TimeSpan retry = TimeSpan.FromMilliseconds(retryTime);

        return await GetOrCreateLock(resource, expiry, wait, retry, consistency).ConfigureAwait(false);
    }

    /// <summary>
    /// Gets or creates a lock on a resource with a given expiry time.
    /// If the lock can't be acquired immediately, it will try to acquire it periodically 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="expiry"></param>
    /// <param name="wait"></param>
    /// <param name="retry"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    /// <exception cref="KahunaException"></exception>
    public async Task<KahunaLock> GetOrCreateLock(string resource, TimeSpan expiry, TimeSpan wait, TimeSpan retry, KahunaLockConsistency consistency = KahunaLockConsistency.Ephemeral)
    {        
        if (wait == TimeSpan.Zero)
            return new(this, resource, await SingleTimeTryAcquireLock(resource, expiry, consistency).ConfigureAwait(false));
        
        if (retry == TimeSpan.Zero)
            throw new KahunaException("Retry cannot be zero");
        
        return new(this, resource, await PeriodicallyTryAcquireLock(resource, expiry, wait, retry, consistency).ConfigureAwait(false));
    }

    /// <summary>
    /// Gets or creates a lock on a resource with a given expiry time.
    /// Gives up immediately if the lock is not available 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="expiry"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<KahunaLock> GetOrCreateLock(string resource, TimeSpan expiry, KahunaLockConsistency consistency = KahunaLockConsistency.Ephemeral)
    {
        return new(this, resource, await SingleTimeTryAcquireLock(resource, expiry, consistency).ConfigureAwait(false));
    }
    
    /// <summary>
    /// Tried to extend the lock by the specified duration
    /// Returns true if the lock was successfully extended, false otherwise 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="duration"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<bool> TryExtend(string resource, string owner, TimeSpan duration, KahunaLockConsistency consistency = KahunaLockConsistency.Ephemeral)
    {
        try
        {
            return await communication.TryExtend(url, resource, owner, duration.TotalMilliseconds, consistency).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger?.LogInformation("Error extending lock instance: {Message}", ex.Message);
            throw;
        }
    }
    
    /// <summary>
    /// Tried to extend the lock by the specified duration
    /// Returns true if the lock was successfully extended, false otherwise 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="durationMs"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<bool> TryExtend(string resource, string owner, int durationMs, KahunaLockConsistency consistency = KahunaLockConsistency.Ephemeral)
    {
        try
        {
            return await communication.TryExtend(url, resource, owner, durationMs, consistency).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger?.LogInformation("Error extending lock instance: {Message}", ex.Message);
            throw;
        }
    }
    
    /// <summary>
    /// Unlocks a lock on a resource if the owner is the current lock owner 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="lockId"></param>
    /// <returns></returns>
    public async Task<bool> Unlock(string resource, string lockId)
    {
        try
        {
            return await communication.TryUnlock(url, resource, lockId).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger?.LogInformation("Error unlocking lock instance: {Message}", ex.Message);
            throw;
        }
    }
    
    /// <summary>
    /// Obtains information about an existing lock
    /// </summary>
    /// <param name="resource"></param>
    /// <returns></returns>
    public async Task<KahunaLockInfo?> GetLockInfo(string resource)
    {
        try
        {
            return await communication.Get(url, resource).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger?.LogError("Error getting lock instance: {Message}", ex.Message);
            throw;
        }
    }
}
