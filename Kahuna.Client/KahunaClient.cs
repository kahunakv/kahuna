
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using Kahuna.Client.Communication;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander.Diagnostics;
using Microsoft.Extensions.Logging;

// ReSharper disable ConvertToAutoProperty
// ReSharper disable ConvertToAutoPropertyWhenPossible

namespace Kahuna.Client;

/// <summary>
/// Represents a client for interacting with the Kahuna locking and key-value system.
/// </summary>
public class KahunaClient
{
    private readonly string[] urls;

    private readonly ILogger<KahunaClient>? logger;

    private readonly IKahunaCommunication communication;

    private int currentServer;
    
    internal IKahunaCommunication Communication => communication;
    
    internal KahunaOptions Options { private set; get; }
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="url">Kahuna's server endpoint</param>
    /// <param name="logger">Logger</param>
    /// <param name="communication">An instance of one of the built-in communicators or a custom one</param>
    /// <param name="options">Client options</param>
    public KahunaClient(string url, ILogger<KahunaClient>? logger = null, IKahunaCommunication? communication = null, KahunaOptions? options = null)
    {
        this.urls = [url];
        this.logger = logger;
        this.Options = options ?? new();
        this.communication = communication ?? new GrpcCommunication(Options, logger);
    }
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="url">Kahuna's server endpoints</param>
    /// <param name="logger">Logger</param>
    /// <param name="communication">An instance of one of the built-in communicators or a custom one</param>
    /// <param name="options">Wheter the client must try to upgrade the ref objects to point directly to leaders</param>
    public KahunaClient(string[] urls, ILogger<KahunaClient>? logger = null, IKahunaCommunication? communication = null, KahunaOptions? options = null)
    {
        this.urls = urls;
        this.logger = logger;
        this.communication = (communication ?? new GrpcCommunication(Options, logger));
        this.Options = options ?? new();
    }

    /// <summary>
    /// Attempts to acquire a lock on a specified resource.
    /// </summary>
    /// <param name="resource">The name of the resource to lock.</param>
    /// <param name="owner">The identifier representing the entity attempting to acquire the lock.</param>
    /// <param name="expiryTime">The duration for which the lock is held before it expires.</param>
    /// <param name="durability">The durability level of the lock.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task representing the asynchronous operation. The result is a tuple containing the lock acquisition result, fencing token, and the server URL from which the lock was served.</returns>
    private async Task<(KahunaLockAcquireResult, long, string?)> TryAcquireLock(string resource, byte[] owner, TimeSpan expiryTime, LockDurability durability, CancellationToken cancellationToken = default)
    {
        return await communication.TryAcquireLock(
            GetRoundRobinUrl(), 
            resource, 
            owner, 
            (int)expiryTime.TotalMilliseconds, 
            durability, 
            cancellationToken
        ).ConfigureAwait(false);
    }

    /// <summary>
    /// Attempts to acquire a lock on a resource by periodically retrying within a specified wait time.
    /// </summary>
    /// <param name="resource">The resource identifier to acquire the lock for.</param>
    /// <param name="expiryTime">The time span after which the lock will expire if not manually released.</param>
    /// <param name="wait">The maximum duration to attempt acquiring the lock.</param>
    /// <param name="retry">The interval between each acquisition attempt within the wait duration.</param>
    /// <param name="durability">The durability level of the lock (e.g., Ephemeral, Persistent, or ReplicationConsistent).</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>Returns an instance of <see cref="KahunaLock"/> representing the acquired lock, or a failed lock instance if acquisition was unsuccessful within the specified wait time.</returns>
    private async Task<KahunaLock> PeriodicallyTryAcquireLock(
        string resource,
        TimeSpan expiryTime,
        TimeSpan wait,
        TimeSpan retry,
        LockDurability durability,
        CancellationToken cancellationToken = default
    )
    {
        byte[] owner = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N"));
        
        ValueStopwatch stopWatch = ValueStopwatch.StartNew();
        
        long fencingToken = -1;
        string? servedFrom = null;
        KahunaLockAcquireResult result = KahunaLockAcquireResult.Error;

        while (stopWatch.GetElapsedTime() < wait)
        {
            (result, fencingToken, servedFrom) = await TryAcquireLock(resource, owner, expiryTime, durability, cancellationToken).ConfigureAwait(false);

            if (result != KahunaLockAcquireResult.Success)
            {
                await Task.Delay((int)Math.Max(100, retry.TotalMilliseconds + Random.Shared.Next(-50, 50)), cancellationToken).ConfigureAwait(false);
                continue;
            }

            return new(this, resource, result, owner, durability, fencingToken, servedFrom);
        }

        return new(this, resource, result, null, durability, fencingToken, servedFrom);
    }

    /// <summary>
    /// Attempts to acquire a single-use lock for a specified resource with a defined expiry time and durability.
    /// </summary>
    /// <param name="resource">The name of the resource to lock.</param>
    /// <param name="expiryTime">The expiry time for the lock, in milliseconds.</param>
    /// <param name="durability">The durability level of the lock.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A <see cref="KahunaLock"/> instance representing the acquired lock.</returns>
    private async Task<KahunaLock> SingleTimeTryAcquireLock(string resource, int expiryTime, LockDurability durability, CancellationToken cancellationToken = default)
    {
        byte[] owner = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N"));

        (KahunaLockAcquireResult result, long fencingToken, string? servedFrom) = await TryAcquireLock(
            resource, 
            owner, 
            TimeSpan.FromMilliseconds(expiryTime), 
            durability, 
            cancellationToken
        ).ConfigureAwait(false);

        return new(this, resource, result, owner, durability, fencingToken, servedFrom);
    }

    /// <summary>
    /// Attempts to acquire a single-use lock on the specified resource.
    /// </summary>
    /// <param name="resource">The resource identifier to be locked.</param>
    /// <param name="expiryTime">The duration for which the lock is valid before it expires.</param>
    /// <param name="durability">The durability type of the lock (e.g., ephemeral, persistent).</param>
    /// <param name="cancellationToken">A token to observe while waiting for the lock acquisition to complete.</param>
    /// <returns>Returns a <see cref="KahunaLock"/> object representing the acquired lock, including lock details and metadata.</returns>
    private async Task<KahunaLock> SingleTimeTryAcquireLock(string resource, TimeSpan expiryTime, LockDurability durability, CancellationToken cancellationToken = default)
    {
        byte[] owner = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N"));

        (KahunaLockAcquireResult result, long fencingToken, string? servedFrom) = await TryAcquireLock(resource, owner, expiryTime, durability, cancellationToken).ConfigureAwait(false);

        return new(this, resource, result, owner, durability, fencingToken, servedFrom);
    }

    /// <summary>
    /// Acquires or creates a lock for the specified resource with the given parameters.    
    /// If the lock can't be acquired immediately, it will try to acquire it periodically.
    /// </summary>
    /// <param name="resource">The unique identifier for the resource the lock is being applied to.</param>
    /// <param name="expiryTime">The lock duration in milliseconds before it expires. Default is 30000 ms.</param>
    /// <param name="waitTime">The maximum time in milliseconds to wait to acquire the lock if unavailable. Default is 0 ms.</param>
    /// <param name="retryTime">The time in milliseconds to wait before retrying to acquire the lock. Default is 0 ms.</param>
    /// <param name="durability">Specifies whether the lock is ephemeral or persistent. Default is persistent.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task representing the asynchronous operation. The task result contains the acquired <see cref="KahunaLock"/>.</returns>
    public async Task<KahunaLock> GetOrCreateLock(
        string resource, 
        int expiryTime = 30000, 
        int waitTime = 0, 
        int retryTime = 0, 
        LockDurability durability = LockDurability.Persistent,
        CancellationToken cancellationToken = default
    )
    {
        TimeSpan expiry = TimeSpan.FromMilliseconds(expiryTime);
        TimeSpan wait = TimeSpan.FromMilliseconds(waitTime);
        TimeSpan retry = TimeSpan.FromMilliseconds(retryTime);

        return await GetOrCreateLock(resource, expiry, wait, retry, durability, cancellationToken).ConfigureAwait(false);
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
    public async Task<KahunaLock> GetOrCreateLock(string resource, TimeSpan expiry, TimeSpan wait, TimeSpan retry, LockDurability durability = LockDurability.Persistent, CancellationToken cancellationToken = default)
    {        
        if (wait == TimeSpan.Zero)
            return await SingleTimeTryAcquireLock(resource, expiry, durability, cancellationToken).ConfigureAwait(false);
        
        if (retry == TimeSpan.Zero)
            throw new KahunaException("Retry cannot be zero", LockResponseType.InvalidInput);
        
        return await PeriodicallyTryAcquireLock(resource, expiry, wait, retry, durability, cancellationToken).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Gets or creates a lock on a resource with a given expiry time.
    /// Gives up immediately if the lock is not available 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="expiry"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaLock> GetOrCreateLock(string resource, int expiry, LockDurability durability = LockDurability.Persistent, CancellationToken cancellationToken = default)
    {
        return await SingleTimeTryAcquireLock(resource, expiry, durability, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Gets or creates a lock on a resource with a given expiry time.
    /// Gives up immediately if the lock is not available 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="expiry"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaLock> GetOrCreateLock(string resource, TimeSpan expiry, LockDurability durability = LockDurability.Persistent, CancellationToken cancellationToken = default)
    {
        return await SingleTimeTryAcquireLock(resource, expiry, durability, cancellationToken).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Tried to extend the lock by the specified duration
    /// Returns true if the lock was successfully extended, false otherwise 
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="duration"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(bool, long)> TryExtendLock(string resource, byte[] owner, TimeSpan duration, LockDurability durability = LockDurability.Persistent, CancellationToken cancellationToken = default)
    {
        return await communication.TryExtendLock(GetRoundRobinUrl(), resource, owner, (int)duration.TotalMilliseconds, durability, cancellationToken).ConfigureAwait(false);
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
    public async Task<(bool, long)> TryExtendLock(string resource, byte[] owner, int durationMs, LockDurability durability = LockDurability.Persistent, CancellationToken cancellationToken = default)
    {
        return await communication.TryExtendLock(GetRoundRobinUrl(), resource, owner, durationMs, durability, cancellationToken).ConfigureAwait(false);
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
    public async Task<(bool, long)> TryExtendLock(string resource, string owner, int durationMs, LockDurability durability = LockDurability.Persistent, CancellationToken cancellationToken = default)
    {
        return await communication.TryExtendLock(GetRoundRobinUrl(), resource, Encoding.UTF8.GetBytes(owner), durationMs, durability, cancellationToken).ConfigureAwait(false);
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
    public async Task<(bool, long)> TryExtendLock(string resource, string owner, TimeSpan duration, LockDurability durability = LockDurability.Persistent, CancellationToken cancellationToken = default)
    {
        return await communication.TryExtendLock(GetRoundRobinUrl(), resource, Encoding.UTF8.GetBytes(owner), (int)duration.TotalMilliseconds, durability, cancellationToken).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Unlocks a lock on a resource if the owner is the current lock owner
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<bool> Unlock(string resource, byte[] owner, LockDurability durability = LockDurability.Persistent, CancellationToken cancellationToken = default)
    {
        return await communication.TryUnlock(GetRoundRobinUrl(), resource, owner, durability, cancellationToken).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Unlocks a lock on a resource if the owner is the current lock owner
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<bool> Unlock(string resource, string owner, LockDurability durability = LockDurability.Persistent, CancellationToken cancellationToken = default)
    {
        return await communication.TryUnlock(GetRoundRobinUrl(), resource, Encoding.UTF8.GetBytes(owner), durability, cancellationToken).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Obtains information about an existing lock
    /// </summary>
    /// <param name="resource"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaLockInfo?> GetLockInfo(string resource, LockDurability durability = LockDurability.Persistent, CancellationToken cancellationToken = default)
    {
        return await communication.Get(GetRoundRobinUrl(), resource, durability, cancellationToken).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Set key to hold the string value. If key already holds a value, it is overwritten
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="expiryTime"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> SetKeyValue(string key, byte[]? value, int expiryTime = 0, KeyValueFlags flags = KeyValueFlags.Set, KeyValueDurability durability = KeyValueDurability.Persistent, CancellationToken cancellationToken = default)
    {
        (bool success, long revision, int timeElapsedMs) = await communication.TrySetKeyValue(
            GetRoundRobinUrl(), 
            key, 
            value, 
            expiryTime, 
            flags, 
            durability, 
            cancellationToken
        ).ConfigureAwait(false);
        
        return new(this, key, success, value, revision, durability, timeElapsedMs);
    }
    
    /// <summary>
    /// Set key to hold the string value. If key already holds a value, it is overwritten
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="expiryTime"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> SetKeyValue(string key, string value, int expiryTime = 0, KeyValueFlags flags = KeyValueFlags.Set, KeyValueDurability durability = KeyValueDurability.Persistent, CancellationToken cancellationToken = default)
    {
        byte[] valueBytes = Encoding.UTF8.GetBytes(value);
        
        (bool success, long revision, int timeElapsedMs) = await communication.TrySetKeyValue(
            GetRoundRobinUrl(), 
            key, 
            valueBytes, 
            expiryTime, 
            flags, 
            durability, 
            cancellationToken
        ).ConfigureAwait(false);
        
        return new(this, key, success, valueBytes, revision, durability, timeElapsedMs);
    }

    /// <summary>
    /// Sets a key-value pair in the Kahuna key/value store with the specified parameters.
    /// If key already holds a value, it is overwritten.
    /// </summary>
    /// <param name="key">The key to set in the database.</param>
    /// <param name="value">The value to associate with the specified key.</param>
    /// <param name="expiryTime">The expiration time for the key-value pair.</param>
    /// <param name="flags">Optional flags for setting the key-value pair. Default is <c>KeyValueFlags.Set</c>.</param>
    /// <param name="durability">Specifies the durability level of the key-value pair. Default is <c>KeyValueDurability.Persistent</c>.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>Returns an instance of <c>KahunaKeyValue</c> containing details about the operation.</returns>
    public async Task<KahunaKeyValue> SetKeyValue(string key, string value, TimeSpan expiryTime, KeyValueFlags flags = KeyValueFlags.Set, KeyValueDurability durability = KeyValueDurability.Persistent,
        CancellationToken cancellationToken = default)
    {
        byte[] valueBytes = Encoding.UTF8.GetBytes(value);
        
        (bool success, long revision, int timeElapsedMs) = await communication.TrySetKeyValue(
            GetRoundRobinUrl(), 
            key, 
            valueBytes, 
            (int)expiryTime.TotalMilliseconds, 
            flags, 
            durability, 
            cancellationToken
        ).ConfigureAwait(false);
        
        return new(this, key, success, valueBytes, revision, durability, timeElapsedMs);
    }

    /// <summary>
    /// Compare-Value-And-Swap (CVAS) operation. Sets the value of a key if the current value is equal to the expected value.
    /// Attempts to compare a specified value with the current value associated with a key
    /// and, if they match, sets the key to a new value with an optional expiry time and durability setting.
    /// </summary>
    /// <param name="key">The key to be accessed or updated.</param>
    /// <param name="value">The new value to set if the comparison succeeds.</param>
    /// <param name="compareValue">The value to compare against the current value associated with the key.</param>
    /// <param name="expiryTime">The expiration time (in milliseconds) for the key-value pair. Default is 30000 milliseconds.</param>
    /// <param name="durability">Specifies whether the key-value pair is ephemeral or persistent. Default is persistent.</param>
    /// <param name="cancellationToken">Token used to propagate notification that operations should be canceled.</param>
    /// <returns>
    /// An instance of <see cref="KahunaKeyValue"/> that provides details about the operation,
    /// including whether it succeeded, the updated value, revision, and timing information.
    /// </returns>
    public async Task<KahunaKeyValue> TryCompareValueAndSetKeyValue(string key, byte[] value, byte[] compareValue, int expiryTime = 30000, KeyValueDurability durability = KeyValueDurability.Persistent,
        CancellationToken cancellationToken = default)
    {
        (bool success, long revision, int timeElapsedMs) = await communication.TryCompareValueAndSetKeyValue(
            GetRoundRobinUrl(), 
            key, 
            value, 
            compareValue, 
            expiryTime, 
            durability, 
            cancellationToken
        ).ConfigureAwait(false);
        
        return new(this, key, success, value, revision, durability, timeElapsedMs);
    }
    
    /// <summary>
    /// Compare-Value-and-Swap (CVAS) operation. Sets the value of a key if the current value is equal to the expected value
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareValue"></param>
    /// <param name="expiryTime"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> TryCompareValueAndSetKeyValue(string key, string value, string compareValue, int expiryTime = 0, KeyValueDurability durability = KeyValueDurability.Persistent, CancellationToken cancellationToken = default)
    {
        byte[] valueBytes = Encoding.UTF8.GetBytes(value);
        
        (bool success, long revision, int timeElapsedMs) = await communication.TryCompareValueAndSetKeyValue(
            GetRoundRobinUrl(), 
            key, 
            valueBytes, 
            Encoding.UTF8.GetBytes(compareValue), 
            expiryTime, 
            durability,
            cancellationToken
        ).ConfigureAwait(false);
        
        return new(this, key, success, valueBytes, revision, durability, timeElapsedMs);
    }
    
    /// <summary>
    /// Compare-Revision-And-Swap (CRAS) operation. Sets the value of a key if the current revision is equal to the expected value
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareRevision"></param>
    /// <param name="expiryTime"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> TryCompareRevisionAndSetKeyValue(string key, byte[]? value, long compareRevision, int expiryTime = 0, KeyValueDurability durability = KeyValueDurability.Persistent, CancellationToken cancellationToken = default)
    {
        (bool success, long revision, int timeElapsedMs) = await communication.TryCompareRevisionAndSetKeyValue(
            GetRoundRobinUrl(), 
            key, 
            value, 
            compareRevision, 
            expiryTime, 
            durability,
            cancellationToken
        ).ConfigureAwait(false);
        
        return new(this, key, success, value, revision, durability, timeElapsedMs);
    }
    
    /// <summary>
    /// Compare=Revision-and-Swap (CRAS) operation. Sets the value of a key if the current revision is equal to the expected value
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareRevision"></param>
    /// <param name="expiryTime"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> TryCompareRevisionAndSetKeyValue(string key, string value, long compareRevision, int expiryTime = 0, KeyValueDurability durability = KeyValueDurability.Persistent, CancellationToken cancellationToken = default)
    {
        byte[] valueBytes = Encoding.UTF8.GetBytes(value);
        
        (bool success, long revision, int timeElapsedMs) = await communication.TryCompareRevisionAndSetKeyValue(
            GetRoundRobinUrl(), 
            key, 
            valueBytes, 
            compareRevision, 
            expiryTime, 
            durability,
            cancellationToken
        ).ConfigureAwait(false);
        
        return new(this, key, success, valueBytes, revision, durability, timeElapsedMs);
    }
    
    /// <summary>
    /// Set key to hold the string value. If key already holds a value, it is overwritten.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="expiry"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> SetKeyValue(string key, byte[]? value, TimeSpan expiry, KeyValueFlags flags = KeyValueFlags.Set, KeyValueDurability durability = KeyValueDurability.Persistent, CancellationToken cancellationToken = default)
    {
        return await SetKeyValue(key, value, (int)expiry.TotalMilliseconds, flags, durability, cancellationToken).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Get the value of a key. If the key does not exist success is false and null is returned
    /// </summary>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> GetKeyValue(string key, KeyValueDurability durability = KeyValueDurability.Persistent, CancellationToken cancellationToken = default)
    {
        (bool success, byte[]? value, long revision, int timeElapsedMs) = await communication.TryGetKeyValue(
            GetRoundRobinUrl(), 
            key, 
            -1, 
            durability, 
            cancellationToken
        ).ConfigureAwait(false);
        
        return new(this, key, success, value, revision, durability, timeElapsedMs);
    }
    
    /// <summary>
    /// Checks if a key does exist
    /// </summary>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> ExistsKeyValue(string key, KeyValueDurability durability = KeyValueDurability.Persistent, CancellationToken cancellationToken = default)
    {
        (bool success, long revision, int timeElapsedMs) = await communication.TryExistsKeyValue(
            GetRoundRobinUrl(), 
            key, 
            -1, 
            durability, 
            cancellationToken
        ).ConfigureAwait(false);
        
        return new(this, key, success, revision, durability, timeElapsedMs);
    }
    
    /// <summary>
    /// Get the value of a key at a specific revision. If the key's revision does not exist success is false and null is returned
    /// </summary>
    /// <param name="key"></param>
    /// <param name="revision"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> GetKeyValueRevision(string key, long revision, KeyValueDurability durability = KeyValueDurability.Persistent, CancellationToken cancellationToken = default)
    {
        (bool success, byte[]? value, long returnRevision, int timeElapsedMs) = await communication.TryGetKeyValue(
            GetRoundRobinUrl(), 
            key, 
            revision, 
            durability, 
            cancellationToken
        ).ConfigureAwait(false);
        
        return new(this, key, success, value, returnRevision, durability, timeElapsedMs);
    }
    
    /// <summary>
    /// Removes the specified key. A key is ignored if it does not exist.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> DeleteKeyValue(string key, KeyValueDurability durability = KeyValueDurability.Persistent, CancellationToken cancellationToken = default)
    {
        (bool success, long revision, int timeElapsedMs) = await communication.TryDeleteKeyValue(
            GetRoundRobinUrl(), 
            key, 
            durability, 
            cancellationToken
        ).ConfigureAwait(false);
        
        return new(this, key, success, revision, durability, timeElapsedMs);
    }
    
    /// <summary>
    /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted
    /// </summary>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> ExtendKeyValue(
        string key, 
        int expiresMs, 
        KeyValueDurability durability = KeyValueDurability.Persistent, 
        CancellationToken cancellationToken = default
    )
    {
        (bool success, long revision, int timeElapsedMs) = await communication.TryExtendKeyValue(
            GetRoundRobinUrl(), 
            key, 
            expiresMs, 
            durability, 
            cancellationToken
        ).ConfigureAwait(false);
        
        return new(this, key, success, revision, durability, timeElapsedMs);
    }
    
    /// <summary>
    /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted
    /// </summary>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValue> ExtendKeyValue(
        string key, 
        TimeSpan expiresMs, 
        KeyValueDurability durability = KeyValueDurability.Persistent, 
        CancellationToken cancellationToken = default
    )
    {
        (bool success, long revision, int timeElapsedMs) = await communication.TryExtendKeyValue(
            GetRoundRobinUrl(), 
            key, 
            (int)expiresMs.TotalMilliseconds, 
            durability, 
            cancellationToken
        ).ConfigureAwait(false);
        
        return new(this, key, success, revision, durability, timeElapsedMs);
    }

    /// <summary>
    /// Executes a script on the key-value store
    /// Scripts are executed as all or nothing transactions
    /// if one command fails the entire transaction is aborted 
    /// </summary>
    /// <param name="script"></param>
    /// <param name="hash"></param>
    /// <param name="parameters"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValueTransactionResult> ExecuteKeyValueTransactionScript(
        string script,
        string? hash = null, 
        List<KeyValueParameter>? parameters = null, 
        CancellationToken cancellationToken = default
    )
    {
        return await communication.TryExecuteKeyValueTransactionScript(
            GetRoundRobinUrl(), 
            Encoding.UTF8.GetBytes(script), 
            hash, 
            parameters, 
            cancellationToken
        ).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Executes a script on the key-value store
    /// Scripts are executed as all or nothing transactions
    /// if one command fails the entire transaction is aborted 
    /// </summary>
    /// <param name="script"></param>
    /// <param name="hash"></param>
    /// <param name="parameters"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KahunaKeyValueTransactionResult> ExecuteKeyValueTransactionScript(
        byte[] script, 
        string? hash = null, 
        List<KeyValueParameter>? parameters = null, 
        CancellationToken cancellationToken = default
    )
    {
        return await communication.TryExecuteKeyValueTransactionScript(GetRoundRobinUrl(), script, hash, parameters, cancellationToken).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Get keys with a specific prefix
    /// </summary>
    /// <param name="prefixKey"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(bool, List<string>)> GetByPrefix(string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken = default)
    {
        return await communication.GetByPrefix(GetRoundRobinUrl(), prefixKey, durability, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Scan all nodes for keys with a specific prefix
    /// </summary>
    /// <param name="prefixKey"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(bool, List<string>)> ScanAllByPrefix(string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken = default)
    {
        return await communication.ScanAllByPrefix(GetRoundRobinUrl(), prefixKey, durability, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Loads a Script reference allowing to reuse server caches and plannings 
    /// </summary>
    /// <param name="script"></param>
    /// <returns></returns>
    public KahunaTransactionScript LoadTransactionScript(string script)
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
        return urls[Math.Abs(serverPointer) % urls.Length];
    }
}

