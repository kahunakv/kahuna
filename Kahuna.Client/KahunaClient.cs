
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Text;
using Kahuna.Client.Communication;
using Kahuna.Shared.KeyValue;
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

    private readonly IKahunaCommunication communication;

    private int currentServer;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="url"></param>
    /// <param name="logger"></param>
    /// <param name="communication"></param>
    public KahunaClient(string url, ILogger<KahunaClient>? logger = null, IKahunaCommunication? communication = null)
    {
        this.urls = [url];
        this.logger = logger;
        this.communication = (communication ?? new GrpcCommunication(logger));
    }
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="urls"></param>
    /// <param name="logger"></param>
    /// <param name="communication"></param>
    public KahunaClient(string[] urls, ILogger<KahunaClient>? logger = null, IKahunaCommunication? communication = null)
    {
        this.urls = urls;
        this.logger = logger;
        this.communication = (communication ?? new GrpcCommunication(logger));
    }
    
    private async Task<(KahunaLockAcquireResult, long)> TryAcquireLock(string resource, byte[] owner, TimeSpan expiryTime, LockDurability durability)
    {
        return await communication.TryAcquireLock(GetRoundRobinUrl(), resource, owner, (int)expiryTime.TotalMilliseconds, durability).ConfigureAwait(false);
    }
    
    private async Task<(KahunaLockAcquireResult, byte[]?, LockDurability, long)> PeriodicallyTryAcquireLock(
        string resource, 
        TimeSpan expiryTime, 
        TimeSpan wait, 
        TimeSpan retry,
        LockDurability durability
    )
    {
        byte[] owner = Guid.NewGuid().ToByteArray();
        
        Stopwatch stopWatch = Stopwatch.StartNew();
        
        long fencingToken = -1;
        KahunaLockAcquireResult result = KahunaLockAcquireResult.Error;

        while (stopWatch.Elapsed < wait)
        {
            (result, fencingToken) = await TryAcquireLock(resource, owner, expiryTime, durability).ConfigureAwait(false);

            if (result != KahunaLockAcquireResult.Success)
            {
                await Task.Delay((int)Math.Max(100, retry.TotalMilliseconds + Random.Shared.Next(-50, 50))).ConfigureAwait(false);
                continue;
            }

            return (result, owner, durability, fencingToken);
        }

        return (result, null, durability, fencingToken);
    }
    
    /// <summary>
    /// Tries to acquire a lock on a resource with a given expiry time
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="expiryTime"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    private async Task<(KahunaLockAcquireResult, byte[]?, LockDurability, long)> SingleTimeTryAcquireLock(string resource, TimeSpan expiryTime, LockDurability durability)
    {
        try
        {
            byte[] owner = Guid.NewGuid().ToByteArray();

            (KahunaLockAcquireResult result, long fencingToken) = await TryAcquireLock(resource, owner, expiryTime, durability).ConfigureAwait(false);

            return (result, owner, durability, fencingToken);
        }
        catch (Exception ex)
        {
            logger?.LogError("Error locking lock instance: {Message}", ex.Message);

            return (KahunaLockAcquireResult.Error, null, durability, -1);
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
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaLock> GetOrCreateLock(string resource, int expiryTime = 30000, int waitTime = 0, int retryTime = 0, LockDurability durability = LockDurability.Persistent)
    {
        TimeSpan expiry = TimeSpan.FromMilliseconds(expiryTime);
        TimeSpan wait = TimeSpan.FromMilliseconds(waitTime);
        TimeSpan retry = TimeSpan.FromMilliseconds(retryTime);

        return await GetOrCreateLock(resource, expiry, wait, retry, durability).ConfigureAwait(false);
    }

    /// <summary>
    /// Gets or creates a lock on a resource with a given expiry time.
    /// If the lock can't be acquired immediately, it will try to acquire it periodically 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="expiry"></param>
    /// <param name="wait"></param>
    /// <param name="retry"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    /// <exception cref="KahunaException"></exception>
    public async Task<KahunaLock> GetOrCreateLock(string resource, TimeSpan expiry, TimeSpan wait, TimeSpan retry, LockDurability durability = LockDurability.Persistent)
    {        
        if (wait == TimeSpan.Zero)
            return new(this, resource, await SingleTimeTryAcquireLock(resource, expiry, durability).ConfigureAwait(false));
        
        if (retry == TimeSpan.Zero)
            throw new KahunaException("Retry cannot be zero", LockResponseType.InvalidInput);
        
        return new(this, resource, await PeriodicallyTryAcquireLock(resource, expiry, wait, retry, durability).ConfigureAwait(false));
    }

    /// <summary>
    /// Gets or creates a lock on a resource with a given expiry time.
    /// Gives up immediately if the lock is not available 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="expiry"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaLock> GetOrCreateLock(string resource, TimeSpan expiry, LockDurability durability = LockDurability.Persistent)
    {
        return new(this, resource, await SingleTimeTryAcquireLock(resource, expiry, durability).ConfigureAwait(false));
    }
    
    /// <summary>
    /// Tried to extend the lock by the specified duration
    /// Returns true if the lock was successfully extended, false otherwise 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="duration"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(bool, long)> TryExtend(string resource, byte[] owner, TimeSpan duration, LockDurability durability = LockDurability.Persistent)
    {
        return await communication.TryExtend(GetRoundRobinUrl(), resource, owner, (int)duration.TotalMilliseconds, durability).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Tried to extend the lock by the specified duration
    /// Returns true if the lock was successfully extended, false otherwise 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="durationMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(bool, long)> TryExtend(string resource, byte[] owner, int durationMs, LockDurability durability = LockDurability.Persistent)
    {
        return await communication.TryExtend(GetRoundRobinUrl(), resource, owner, durationMs, durability).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Tried to extend the lock by the specified duration
    /// Returns true if the lock was successfully extended, false otherwise 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="durationMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(bool, long)> TryExtend(string resource, string owner, int durationMs, LockDurability durability = LockDurability.Persistent)
    {
        return await communication.TryExtend(GetRoundRobinUrl(), resource, Encoding.UTF8.GetBytes(owner), durationMs, durability).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Tried to extend the lock by the specified duration
    /// Returns true if the lock was successfully extended, false otherwise 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="durationMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(bool, long)> TryExtend(string resource, string owner, TimeSpan duration, LockDurability durability = LockDurability.Persistent)
    {
        return await communication.TryExtend(GetRoundRobinUrl(), resource, Encoding.UTF8.GetBytes(owner), (int)duration.TotalMilliseconds, durability).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Unlocks a lock on a resource if the owner is the current lock owner
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<bool> Unlock(string resource, byte[] owner, LockDurability durability = LockDurability.Persistent)
    {
        return await communication.TryUnlock(GetRoundRobinUrl(), resource, owner, durability).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Unlocks a lock on a resource if the owner is the current lock owner
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<bool> Unlock(string resource, string owner, LockDurability durability = LockDurability.Persistent)
    {
        return await communication.TryUnlock(GetRoundRobinUrl(), resource, Encoding.UTF8.GetBytes(owner), durability).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Obtains information about an existing lock
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaLockInfo?> GetLockInfo(string resource, LockDurability durability = LockDurability.Persistent)
    {
        return await communication.Get(GetRoundRobinUrl(), resource, durability).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Set key to hold the string value. If key already holds a value, it is overwritten
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="expiryTime"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> SetKeyValue(string key, byte[]? value, int expiryTime = 30000, KeyValueFlags flags = KeyValueFlags.Set, KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        (bool success, long revision) = await communication.TrySetKeyValue(GetRoundRobinUrl(), key, value, expiryTime, flags, durability).ConfigureAwait(false);
        
        return new(this, key, success, revision, durability);
    }
    
    /// <summary>
    /// Set key to hold the string value. If key already holds a value, it is overwritten
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="expiryTime"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> SetKeyValue(string key, string value, int expiryTime = 30000, KeyValueFlags flags = KeyValueFlags.Set, KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        (bool success, long revision) = await communication.TrySetKeyValue(GetRoundRobinUrl(), key, Encoding.UTF8.GetBytes(value), expiryTime, flags, durability).ConfigureAwait(false);
        
        return new(this, key, success, revision, durability);
    }
    
    /// <summary>
    /// Set key to hold the string value. If key already holds a value, it is overwritten
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="expiryTime"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> SetKeyValue(string key, string value, TimeSpan expiryTime, KeyValueFlags flags = KeyValueFlags.Set, KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        (bool success, long revision) = await communication.TrySetKeyValue(GetRoundRobinUrl(), key, Encoding.UTF8.GetBytes(value), (int)expiryTime.TotalMilliseconds, flags, durability).ConfigureAwait(false);
        
        return new(this, key, success, revision, durability);
    }
    
    /// <summary>
    /// Compare Value and Set (CVAS) operation. Sets the value of a key if the current value is equal to the expected value
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareValue"></param>
    /// <param name="expiryTime"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> TryCompareValueAndSetKeyValue(string key, byte[] value, byte[] compareValue, int expiryTime = 30000, KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        (bool success, long revision) = await communication.TryCompareValueAndSetKeyValue(GetRoundRobinUrl(), key, value, compareValue, expiryTime, durability).ConfigureAwait(false);
        
        return new(this, key, success, revision, durability);
    }
    
    /// <summary>
    /// Compare Value and Set (CVAS) operation. Sets the value of a key if the current value is equal to the expected value
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareValue"></param>
    /// <param name="expiryTime"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> TryCompareValueAndSetKeyValue(string key, string value, string compareValue, int expiryTime = 30000, KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        (bool success, long revision) = await communication.TryCompareValueAndSetKeyValue(
            GetRoundRobinUrl(), 
            key, 
            Encoding.UTF8.GetBytes(value), 
            Encoding.UTF8.GetBytes(compareValue), 
            expiryTime, 
            durability
        ).ConfigureAwait(false);
        
        return new(this, key, success, revision, durability);
    }
    
    /// <summary>
    /// Compare Revision and Set (CRAS) operation. Sets the value of a key if the current revision is equal to the expected value
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareRevision"></param>
    /// <param name="expiryTime"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> TryCompareRevisionAndSetKeyValue(string key, byte[]? value, long compareRevision, int expiryTime = 30000, KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        (bool success, long revision) = await communication.TryCompareRevisionAndSetKeyValue(
            GetRoundRobinUrl(), 
            key, 
            value, 
            compareRevision, 
            expiryTime, 
            durability
        ).ConfigureAwait(false);
        
        return new(this, key, success, revision, durability);
    }
    
    /// <summary>
    /// Compare Revision and Set (CRAS) operation. Sets the value of a key if the current revision is equal to the expected value
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareRevision"></param>
    /// <param name="expiryTime"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> TryCompareRevisionAndSetKeyValue(string key, string value, long compareRevision, int expiryTime = 30000, KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        (bool success, long revision) = await communication.TryCompareRevisionAndSetKeyValue(
            GetRoundRobinUrl(), 
            key, 
            Encoding.UTF8.GetBytes(value), 
            compareRevision, 
            expiryTime, 
            durability
        ).ConfigureAwait(false);
        
        return new(this, key, success, revision, durability);
    }
    
    /// <summary>
    /// Set key to hold the string value. If key already holds a value, it is overwritten.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="expiry"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> SetKeyValue(string key, byte[]? value, TimeSpan expiry, KeyValueFlags flags = KeyValueFlags.Set, KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        return await SetKeyValue(key, value, (int)expiry.TotalMilliseconds, flags, durability).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Get the value of a key. If the key does not exist success is false and null is returned
    /// </summary>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> GetKeyValue(string key, KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        (bool success, byte[]? value, long revision) = await communication.TryGetKeyValue(GetRoundRobinUrl(), key, -1, durability).ConfigureAwait(false);
        
        return new(this, key, success, value, revision, durability);
    }
    
    /// <summary>
    /// Get the value of a key at a specific revision. If the key's revision does not exist success is false and null is returned
    /// </summary>
    /// <param name="key"></param>
    /// <param name="revision"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> GetKeyValueRevision(string key, long revision, KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        (bool success, byte[]? value, long returnRevision) = await communication.TryGetKeyValue(GetRoundRobinUrl(), key, revision, durability).ConfigureAwait(false);
        
        return new(this, key, success, value, returnRevision, durability);
    }
    
    /// <summary>
    /// Removes the specified key. A key is ignored if it does not exist.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> DeleteKeyValue(string key, KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        (bool success, long revision) = await communication.TryDeleteKeyValue(GetRoundRobinUrl(), key, durability).ConfigureAwait(false);
        
        return new(this, key, success, revision, durability);
    }
    
    /// <summary>
    /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted
    /// </summary>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> ExtendKeyValue(string key, int expiresMs, KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        (bool success, long revision) = await communication.TryExtendKeyValue(GetRoundRobinUrl(), key, expiresMs, durability).ConfigureAwait(false);
        
        return new(this, key, success, revision, durability);
    }
    
    /// <summary>
    /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted
    /// </summary>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> ExtendKeyValue(string key, TimeSpan expiresMs, KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        (bool success, long revision) = await communication.TryExtendKeyValue(GetRoundRobinUrl(), key, (int)expiresMs.TotalMilliseconds, durability).ConfigureAwait(false);
        
        return new(this, key, success, revision, durability);
    }

    /// <summary>
    /// Executes a script on the key-value store
    /// Scripts are executed as all or nothing transactions
    /// if one command fails the entire transaction is aborted
    /// </summary>
    /// <param name="script"></param>
    /// <param name="hash"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValueTransactionResult> ExecuteKeyValueTransaction(string script, string? hash = null)
    {
        return await communication.TryExecuteKeyValueTransaction(GetRoundRobinUrl(), Encoding.UTF8.GetBytes(script), hash).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Executes a script on the key-value store
    /// Scripts are executed as all or nothing transactions
    /// if one command fails the entire transaction is aborted
    /// </summary>
    /// <param name="script"></param>
    /// <param name="hash"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValueTransactionResult> ExecuteKeyValueTransaction(byte[] script, string? hash = null)
    {
        return await communication.TryExecuteKeyValueTransaction(GetRoundRobinUrl(), script, hash).ConfigureAwait(false);
    }

    /// <summary>
    /// Loads a Script reference allowing to reuse server caches and plannings 
    /// </summary>
    /// <param name="script"></param>
    /// <returns></returns>
    public KahunaScript LoadScript(string script)
    {
        return new(this, script);
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

