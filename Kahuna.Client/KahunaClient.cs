
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using Flurl.Http;
using Kahuna.Client.Communication;
using Kahuna.Shared.Locks;
using Microsoft.Extensions.Logging;

namespace Kahuna.Client;

/// <summary>
/// Client for the Kahuna service
/// </summary>
public class KahunaClient
{
    private readonly string[] urls;

    private readonly ILogger<KahunaClient>? logger;

    private readonly GrpcCommunication communication;

    private int currentServer;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="url"></param>
    /// <param name="logger"></param>
    public KahunaClient(string url, ILogger<KahunaClient>? logger)
    {
        this.urls = [url];
        this.logger = logger;
        this.communication = new(logger);
    }
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="urls"></param>
    /// <param name="logger"></param>
    public KahunaClient(string[] urls, ILogger<KahunaClient>? logger)
    {
        this.urls = urls;
        this.logger = logger;
        this.communication = new(logger);
    }
    
    private async Task<KahunaLockAcquireResult> TryAcquireLock(string resource, string lockId, TimeSpan expiryTime, LockConsistency consistency)
    {
        return await communication.TryAcquireLock(GetRoundRobinUrl(), resource, lockId, (int)expiryTime.TotalMilliseconds, consistency).ConfigureAwait(false);
    }
    
    private async Task<(KahunaLockAcquireResult, string?, LockConsistency)> PeriodicallyTryAcquireLock(
        string resource, 
        TimeSpan expiryTime, 
        TimeSpan wait, 
        TimeSpan retry,
        LockConsistency consistency
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

                return (result, owner, consistency);
            }

            return (result, null, consistency);
        }
        catch (Exception ex)
        {
            logger?.LogError("Error locking lock instance: {Message}", ex.Message);

            return (KahunaLockAcquireResult.Error, null, consistency);
        }
    }
    
    /// <summary>
    /// Tries to acquire a lock on a resource with a given expiry time
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="expiryTime"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    private async Task<(KahunaLockAcquireResult, string?, LockConsistency)> SingleTimeTryAcquireLock(string resource, TimeSpan expiryTime, LockConsistency consistency)
    {
        try
        {
            string owner = Guid.NewGuid().ToString("N");

            KahunaLockAcquireResult result = await TryAcquireLock(resource, owner, expiryTime, consistency).ConfigureAwait(false);

            return (result, owner, consistency);
        }
        catch (Exception ex)
        {
            logger?.LogError("Error locking lock instance: {Message}", ex.Message);

            return (KahunaLockAcquireResult.Error, null, consistency);
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
    public async Task<KahunaLock> GetOrCreateLock(string resource, int expiryTime = 30000, int waitTime = 0, int retryTime = 0, LockConsistency consistency = LockConsistency.Ephemeral)
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
    public async Task<KahunaLock> GetOrCreateLock(string resource, TimeSpan expiry, TimeSpan wait, TimeSpan retry, LockConsistency consistency = LockConsistency.Ephemeral)
    {        
        if (wait == TimeSpan.Zero)
            return new(this, resource, await SingleTimeTryAcquireLock(resource, expiry, consistency).ConfigureAwait(false));
        
        if (retry == TimeSpan.Zero)
            throw new KahunaException("Retry cannot be zero", LockResponseType.InvalidInput);
        
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
    public async Task<KahunaLock> GetOrCreateLock(string resource, TimeSpan expiry, LockConsistency consistency = LockConsistency.Ephemeral)
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
    public async Task<bool> TryExtend(string resource, string owner, TimeSpan duration, LockConsistency consistency = LockConsistency.Ephemeral)
    {
        try
        {
            return await communication.TryExtend(GetRoundRobinUrl(), resource, owner, (int)duration.TotalMilliseconds, consistency).ConfigureAwait(false);
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
    public async Task<bool> TryExtend(string resource, string owner, int durationMs, LockConsistency consistency = LockConsistency.Ephemeral)
    {
        try
        {
            return await communication.TryExtend(GetRoundRobinUrl(), resource, owner, durationMs, consistency).ConfigureAwait(false);
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
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<bool> Unlock(string resource, string lockId, LockConsistency consistency = LockConsistency.Ephemeral)
    {
        try
        {
            return await communication.TryUnlock(GetRoundRobinUrl(), resource, lockId, consistency).ConfigureAwait(false);
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
    /// <param name="consistency"></param>
    /// <returns></returns>
    public async Task<KahunaLockInfo?> GetLockInfo(string resource, LockConsistency consistency = LockConsistency.Ephemeral)
    {
        try
        {
            return await communication.Get(GetRoundRobinUrl(), resource, consistency).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger?.LogError("Error getting lock instance: {Message}", ex.Message);
            throw;
        }
    }
    
    /// <summary>
    /// Chooses the next server in the list of servers in a round-robin fashion
    /// </summary>
    /// <returns></returns>
    private string GetRoundRobinUrl()
    {
        int serverPointer = Interlocked.Increment(ref currentServer);
        return urls[serverPointer % urls.Length];
    }
}
