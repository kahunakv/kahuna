using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Time;

using Kahuna.Utils;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Executes point, prefix and range lock acquire/release against this node's actor rings.
///
/// Acquiring waits out a <i>transient</i> holder — an in-flight replication intent, or a write intent left
/// by a durably decided transaction whose resolution has not run yet — rather than failing the caller
/// outright. The wait is bounded by <see cref="AcquireLockWaitMs"/>: generous enough to absorb a loaded
/// resolution round-trip, and far below the write-intent lease so a genuinely stalled resolution still
/// surfaces as <c>MustRetry</c> instead of pinning the acquire until the lease lapses.
/// </summary>
internal sealed class LocalLockOperations
{
    private const int MaxRetries = 3;

    /// <summary>
    /// How long a point-lock acquire waits out a transient holder (an in-flight replication intent, or a write
    /// intent left by a durably decided transaction whose resolution has not run yet) before reporting MustRetry.
    /// Generous enough to absorb a loaded resolution round-trip, far below the write-intent lease so a genuinely
    /// stalled resolution still surfaces to the caller instead of pinning the acquire until the lease lapses.
    /// </summary>
    private const int AcquireLockWaitMs = 3_000;

    private readonly KeyValuesRuntime runtime;

    private readonly LocalKeyValueOperations localKeyValues;

    internal LocalLockOperations(KeyValuesRuntime runtime, LocalKeyValueOperations localKeyValues)
    {
        this.runtime = runtime;
        this.localKeyValues = localKeyValues;
    }

    // Aliases matching the field names the moved bodies use, so those bodies stay byte-for-byte as they were.
    private IRaft raft => runtime.Raft;

    private ILogger<IKahuna> logger => runtime.Logger;

    private KeyValueLocator locator => runtime.Locator;

    private PreparedIntentStore preparedIntentStore => runtime.PreparedIntentStore;

    private TransactionRecordStore transactionRecordStore => runtime.TransactionRecordStore;

    private KeyValueActorRing ephemeralKeyValuesRouter => runtime.Routers.Ephemeral;

    private KeyValueActorRing persistentKeyValuesRouter => runtime.Routers.Persistent;

    private Task<bool> TryRouteForeignDecision(KeyValueRequest request, string key, HLCTimestamp transactionId, KeyValueDurability durability, bool alreadyAttempted) =>
        localKeyValues.TryRouteForeignDecision(request, key, transactionId, durability, alreadyAttempted);

    private static ValueTask<KeyValueResponse?> AskKeyValueActor(
        KeyValueActorRing router,
        KeyValueRequest request) => KeyValueActorRouters.AskKeyValueActor(router, request);


    /// <summary>
    /// Passes a TryAcquireExclusiveLock request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability)
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryAcquireExclusiveLock, 
            transactionId, 
            HLCTimestamp.Zero,
            key, 
            null, 
            null,
            -1,
            KeyValueFlags.None,
            expiresMs, 
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            default
        );

        try
        {
            (KeyValueResponseType type, HLCTimestamp holder) = await AcquireExclusiveLockWithWait(
                request, key, transactionId, durability, Environment.TickCount64 + AcquireLockWaitMs);

            return (type, key, durability, holder);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Issues a point-lock acquire and waits out the transient conditions that hold the key without conflicting
    /// with the caller: an in-flight replication intent, and a write intent whose transaction is durably decided
    /// but not yet resolved (the deferred-settlement window). Both surface as
    /// <see cref="KeyValueResponseType.WaitingForReplication"/> and clear on their own, so the acquire is
    /// re-issued under exponential back-off instead of being reported as a conflict. A genuine live holder
    /// answers <c>AlreadyLocked</c> on the first attempt and never enters the wait. Exhausting
    /// <paramref name="deadline"/> yields <see cref="KeyValueResponseType.MustRetry"/> — retryable, never a decided
    /// outcome. The deadline is supplied by the caller so a multi-key acquire spends one budget across all its keys
    /// rather than one per key.
    /// </summary>
    private async Task<(KeyValueResponseType, HLCTimestamp)> AcquireExclusiveLockWithWait(
        KeyValueRequest request, string key, HLCTimestamp transactionId, KeyValueDurability durability, long deadline)
    {
        int backoffMs = 1;
        bool attemptedRoutedResolve = false;

        while (true)
        {
            KeyValueResponse? response;

            if (durability == KeyValueDurability.Ephemeral)
                response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
            else
                response = await AskKeyValueActor(persistentKeyValuesRouter, request);

            if (response is null)
                return (KeyValueResponseType.Errored, HLCTimestamp.Zero);

            // A holder whose canonical record lives on another partition cannot be classified by the actor, so it
            // reads as still-undecided and the acquire is denied. Route the lookup to the anchor leader once, off
            // the mailbox, and re-issue with the terminal decision so a settled holder is recognised as transient.
            if (response.Type is KeyValueResponseType.AlreadyLocked or KeyValueResponseType.WaitingForReplication
                && await TryRouteForeignDecision(request, key, transactionId, durability, attemptedRoutedResolve))
            {
                attemptedRoutedResolve = true;
                if (request.ForeignDecisionHint.TransactionId != HLCTimestamp.Zero)
                    continue;
            }

            if (response.Type != KeyValueResponseType.WaitingForReplication)
                return (response.Type, response.HolderTransactionId);

            if (Environment.TickCount64 >= deadline)
                return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);

            Transactions.DurableTransactionMetrics.AddKvRetryWait("AcquireExclusiveLockWithWait_4389");
            await Task.Delay(backoffMs);
            backoffMs = Math.Min(backoffMs * 2, 100);
        }
    }

    /// <summary>
    /// Passes a TryAcquireExclusivePrefixLock request to the key/value actor to lock a range of keys by the specified prefix
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> TryAcquireExclusivePrefixLock(
        HLCTimestamp transactionId, 
        string prefixKey, 
        int expiresMs, 
        KeyValueDurability durability
    )
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryAcquireExclusivePrefixLock, 
            transactionId,
            HLCTimestamp.Zero,
            prefixKey,
            null, 
            null,
            -1,
            KeyValueFlags.None,
            expiresMs, 
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            default
        );
        
        try
        {
            LazyRetryDelays retryDelays = new(TimeSpan.FromMilliseconds(1), MaxRetries);
            for (int retryAttempt = 0; retryAttempt < MaxRetries; retryAttempt++)
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return KeyValueResponseType.Errored;

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    Transactions.DurableTransactionMetrics.AddKvRetryWait("TryAcquireExclusivePrefixLock_4442");
                    if (retryDelays.TryNext(out TimeSpan delay)) await Task.Delay(delay);
                    continue;
                }

                return response.Type;
            }

            return KeyValueResponseType.MustRetry;
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Passes a TryAcquireExclusiveLock request to the key/value actor for the given keys.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)>> TryAcquireManyExclusiveLocks(
        HLCTimestamp transactionId,
        List<(string key, int expiresMs, KeyValueDurability durability)> keys
    )
    {
        List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> responses = new(keys.Count);

        // One wait budget for the whole batch: a key that has to wait out a resolution must not let a long key list
        // multiply the caller's worst case.
        long deadline = Environment.TickCount64 + AcquireLockWaitMs;

        foreach ((string key, int expiresMs, KeyValueDurability durability) key in keys)
        {
            KeyValueRequest request = KeyValueRequestPool.Rent(
                KeyValueRequestType.TryAcquireExclusiveLock,
                transactionId,
                HLCTimestamp.Zero,
                key.key,
                null,
                null,
                -1,
                KeyValueFlags.None,
                key.expiresMs,
                HLCTimestamp.Zero,
                key.durability,
                0,
                0,
                default
            );

            try
            {
                // Shares the single-key wait: a transient holder must not be reported as a failed acquire here
                // either, or a multi-key transaction aborts for merely overlapping a resolution window.
                (KeyValueResponseType type, HLCTimestamp holder) =
                    await AcquireExclusiveLockWithWait(request, key.key, transactionId, key.durability, deadline);

                responses.Add((type, key.key, key.durability, holder));

                if (type != KeyValueResponseType.Locked)
                    break;
            }
            finally
            {
                KeyValueRequestPool.Return(request);
            }
        }

        return responses;
    }
    
    /// <summary>
    /// Passes a TryAcquireExclusiveLock request to the key/value actor for the given keyValue name.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability)
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryReleaseExclusiveLock, 
            transactionId, 
            HLCTimestamp.Zero,
            key, 
            null, 
            null,
            -1,
            KeyValueFlags.None,
            0, 
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            default
        );

        try
        {
            LazyRetryDelays retryDelays = new(TimeSpan.FromMilliseconds(1), MaxRetries);
            for (int retryAttempt = 0; retryAttempt < MaxRetries; retryAttempt++)
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return (KeyValueResponseType.Errored, key);

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    Transactions.DurableTransactionMetrics.AddKvRetryWait("TryReleaseExclusiveLock_4556");
                    if (retryDelays.TryNext(out TimeSpan delay)) await Task.Delay(delay);
                    continue;
                }

                return (response.Type, key);
            }

            return (KeyValueResponseType.MustRetry, key);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>
    /// Passes a TryReleaseExclusivePrefixLock request to the key/value actor to lock a range of keys by the specified prefix
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> TryReleaseExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability)
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryReleaseExclusivePrefixLock, 
            transactionId, 
            HLCTimestamp.Zero,
            prefixKey, 
            null, 
            null,
            -1,
            KeyValueFlags.None,
            0, 
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            default
        );

        try
        {
            LazyRetryDelays retryDelays = new(TimeSpan.FromMilliseconds(1), MaxRetries);
            for (int retryAttempt = 0; retryAttempt < MaxRetries; retryAttempt++)
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return KeyValueResponseType.Errored;

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    Transactions.DurableTransactionMetrics.AddKvRetryWait("TryReleaseExclusivePrefixLock_4613");
                    if (retryDelays.TryNext(out TimeSpan delay)) await Task.Delay(delay);
                    continue;
                }

                return response.Type;
            }

            return KeyValueResponseType.MustRetry;
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability
    ) => TryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, RangeLockMode.Exclusive);

    public async Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        RangeLockMode mode
    )
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryAcquireExclusiveRangeLock,
            transactionId,
            HLCTimestamp.Zero,
            prefix,
            null,
            null,
            -1,
            KeyValueFlags.None,
            expiresMs,
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            default
        );

        request.StartKey       = startKey;
        request.StartInclusive = startInclusive;
        request.EndKey         = endKey;
        request.EndInclusive   = endInclusive;
        request.RangeLockMode  = mode;

        try
        {
            LazyRetryDelays retryDelays = new(TimeSpan.FromMilliseconds(1), MaxRetries);
            for (int retryAttempt = 0; retryAttempt < MaxRetries; retryAttempt++)
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return (KeyValueResponseType.Errored, HLCTimestamp.Zero);

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    Transactions.DurableTransactionMetrics.AddKvRetryWait("TryAcquireRangeLock_4686");
                    if (retryDelays.TryNext(out TimeSpan delay)) await Task.Delay(delay);
                    continue;
                }

                return (response.Type, response.HolderTransactionId);
            }

            return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    public async Task<KeyValueResponseType> TryReleaseExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        KeyValueDurability durability
    )
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryReleaseExclusiveRangeLock,
            transactionId,
            HLCTimestamp.Zero,
            prefix,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            default
        );

        request.StartKey       = startKey;
        request.StartInclusive = startInclusive;
        request.EndKey         = endKey;
        request.EndInclusive   = endInclusive;

        try
        {
            LazyRetryDelays retryDelays = new(TimeSpan.FromMilliseconds(1), MaxRetries);
            for (int retryAttempt = 0; retryAttempt < MaxRetries; retryAttempt++)
            {
                KeyValueResponse? response;

                if (durability == KeyValueDurability.Ephemeral)
                    response = await AskKeyValueActor(ephemeralKeyValuesRouter, request);
                else
                    response = await AskKeyValueActor(persistentKeyValuesRouter, request);

                if (response is null)
                    return KeyValueResponseType.Errored;

                if (response.Type == KeyValueResponseType.WaitingForReplication)
                {
                    Transactions.DurableTransactionMetrics.AddKvRetryWait("TryReleaseExclusiveRangeLock_4747");
                    if (retryDelays.TryNext(out TimeSpan delay)) await Task.Delay(delay);
                    continue;
                }

                return response.Type;
            }

            return KeyValueResponseType.MustRetry;
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }
}
