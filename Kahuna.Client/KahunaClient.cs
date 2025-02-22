
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using Flurl.Http;
using Kahuna.Client.Communication;

namespace Kahuna.Client;

/// <summary>
/// Client for the Kahuna service
/// </summary>
public class KahunaClient
{
    private readonly string url;

    private readonly HttpCommunication communication;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="url"></param>
    public KahunaClient(string url)
    {
        this.url = url;
        this.communication = new();
    }
    
    private async Task<KahunaLockAcquireResult> TryAcquireLock(string key, string lockId, TimeSpan expiryTime)
    {
        return await communication.TryAcquireLock(url, key, lockId, (int)expiryTime.TotalMilliseconds).ConfigureAwait(false);
    }
    
    private async Task<(KahunaLockAcquireResult, string?)> PeriodicallyTryAcquireLock(string key, TimeSpan expiryTime, TimeSpan wait, TimeSpan retry)
    {
        try
        {
            string lockId = Guid.NewGuid().ToString("N");
            
            Stopwatch stopWatch = Stopwatch.StartNew();
            KahunaLockAcquireResult result = KahunaLockAcquireResult.Error;

            while (stopWatch.Elapsed < wait)
            {
                result = await TryAcquireLock(key, lockId, expiryTime).ConfigureAwait(false);

                if (result != KahunaLockAcquireResult.Success)
                {
                    await Task.Delay(retry).ConfigureAwait(false);
                    continue;
                }

                return (result, lockId);
            }

            return (result, null);
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error locking lock instance: {0}", ex.Message);

            return (KahunaLockAcquireResult.Error, null);
        }
    }
    
    /// <summary>
    /// Tries to acquire a lock on a resource with a given expiry time
    /// </summary>
    /// <param name="key"></param>
    /// <param name="expiryTime"></param>
    /// <returns></returns>
    private async Task<(KahunaLockAcquireResult, string?)> SingleTimeTryAcquireLock(string key, TimeSpan expiryTime)
    {
        try
        {
            string lockId = Guid.NewGuid().ToString();

            KahunaLockAcquireResult result = await TryAcquireLock(key, lockId, expiryTime).ConfigureAwait(false);

            return (result, lockId);
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error locking lock instance: {0}", ex.Message);

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
    /// <returns></returns>
    public async Task<KahunaLock> GetOrCreateLock(string resource, int expiryTime = 30000, int waitTime = 10000, int retryTime = 100)
    {
        TimeSpan expiry = TimeSpan.FromMilliseconds(expiryTime);
        TimeSpan wait = TimeSpan.FromMilliseconds(waitTime);
        TimeSpan retry = TimeSpan.FromMilliseconds(retryTime);

        return await GetOrCreateLock(resource, expiry, wait, retry).ConfigureAwait(false);
    }

    /// <summary>
    /// Gets or creates a lock on a resource with a given expiry time.
    /// Gives up immediately if the lock is not available
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="expiryTime"></param>
    /// <returns></returns>
    public async Task<KahunaLock> GetOrCreateLock(string resource, int expiryTime = 30000)
    {
        TimeSpan expiry = TimeSpan.FromMilliseconds(expiryTime);
        
        return await GetOrCreateLock(resource, expiry).ConfigureAwait(false);
    }

    /// <summary>
    /// Gets or creates a lock on a resource with a given expiry time.
    /// If the lock can't be acquired immediately, it will try to acquire it periodically  
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="expiry"></param>
    /// <param name="wait"></param>
    /// <param name="retry"></param>
    /// <returns></returns>
    /// <exception cref="KahunaException"></exception>
    public async Task<KahunaLock> GetOrCreateLock(string resource, TimeSpan expiry, TimeSpan wait, TimeSpan retry)
    {
        if (retry == TimeSpan.Zero)
            throw new KahunaException("Retry cannot be zero");
        
        if (wait == TimeSpan.Zero)
            return new(this, resource, await SingleTimeTryAcquireLock(resource, expiry).ConfigureAwait(false));
        
        return new(this, resource, await PeriodicallyTryAcquireLock(resource, expiry, wait, retry).ConfigureAwait(false));
    }

    /// <summary>
    /// Gets or creates a lock on a resource with a given expiry time.
    /// Gives up immediately if the lock is not available
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="expiry"></param>
    /// <returns></returns>
    public async Task<KahunaLock> GetOrCreateLock(string resource, TimeSpan expiry)
    {
        return new(this, resource, await SingleTimeTryAcquireLock(resource, expiry).ConfigureAwait(false));
    }
    
    /// <summary>
    /// Unlocks a lock on a resource if the owner is the current lock owner
    /// </summary>
    /// <param name="key"></param>
    /// <param name="lockId"></param>
    /// <returns></returns>
    public async Task<bool> Unlock(string key, string lockId)
    {
        try
        {
            return await communication.TryUnlock(url, key, lockId).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error locking lock instance: {0}", ex.Message);
            return false;
        }
    }
}
