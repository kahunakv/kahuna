using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The routing façade for lock operations — point, prefix and range locks, acquire and release, single and
/// many-key.
///
/// A registered range-lock acquire records the lock into the transaction's working set through the completion
/// payload; losing that record loses the lock when the coordinator hands the transaction on. The
/// <c>WithHook</c> variants exist so tests can interleave a split or merge into the acquire window.
/// </summary>
internal sealed class RoutedLockOperations
{

    private readonly KeyValuesRuntime runtime;

    private readonly OperationRegistrar registrar;

    internal RoutedLockOperations(KeyValuesRuntime runtime, OperationRegistrar registrar)
    {
        this.runtime = runtime;
        this.registrar = registrar;
    }

    // Aliases matching the field names the moved bodies use, so those bodies stay byte-for-byte as they were.
    private IRaft raft => runtime.Raft;

    private ILogger<IKahuna> logger => runtime.Logger;

    private KeyValueLocator locator => runtime.Locator;

    private ParticipantOperationCache participantOperationCache => registrar.ParticipantOperationCache;

    private static RegistrationRouting ClassifyRegistration(HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId) =>
        OperationRegistrar.ClassifyRegistration(transactionId, coordinatorKey, operationId);

    private Task<bool> CompleteRegisteredOperation(
        string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId,
        object response, OperationCompletionPayload payload) =>
        registrar.CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, response, payload);

    private Task<object?> TryRecoverRegisteredOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId) =>
        registrar.TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId);

    private ValueTask<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey)> LocateAndBeginOperation(
        string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken) =>
        registrar.LocateAndBeginOperation(coordinatorKey, transactionId, operationId, kind, payloadDigest, cancellationToken);

    private ValueTask<(KeyValueResponseType outcome, string? anchor)> LocateAndCompleteOperation(
        string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, CancellationToken cancellationToken) =>
        registrar.LocateAndCompleteOperation(coordinatorKey, transactionId, operationId, payload, cancellationToken);

    /// <summary>
    /// Locates the leader node for the given key and executes the TryAcquireExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireExclusiveLock(
        HLCTimestamp transactionId,
        string key,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancelationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        if (ReservedKeys.IsReserved(key))
            return Task.FromResult((KeyValueResponseType.InvalidInput, key, durability, HLCTimestamp.Zero));

        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryAcquireExclusiveLock(transactionId, key, expiresMs, durability, cancelationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult((KeyValueResponseType.InvalidInput, key, durability, HLCTimestamp.Zero));

        return RegisterAndAcquireExclusiveLock(transactionId, coordinatorKey, operationId, key, expiresMs, durability, cancelationToken);
    }

    /// <summary>
    /// Register-remote wrapper for a transaction-scoped exclusive point-lock acquire: registers the
    /// operation and, on a confirmed lock, records the held point lock into the coordinator-owned lock
    /// set. A retry under the same operation id reports the lock as already held by this transaction.
    /// </summary>
    private async Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> RegisterAndAcquireExclusiveLock(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string key,
        int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.PointLock, OperationDigest.ForPointLockAcquire(key, expiresMs, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                // Replay the exact cached outcome: a first attempt that failed (e.g. AlreadyLocked/Aborted)
                // must not resurface as a successful acquire. The holder is only meaningful on a self-held
                // lock, so it is the transaction id on success and unknown (Zero) on a cached failure.
                participantOperationCache.Remove(transactionId, operationId);
                return (cachedType, key, durability, cachedType == KeyValueResponseType.Locked ? transactionId : HLCTimestamp.Zero);
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return ((KeyValueResponseType, string, KeyValueDurability, HLCTimestamp))recovered;
                return (KeyValueResponseType.MustRetry, key, durability, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, key, durability, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return (KeyValueResponseType.Aborted, key, durability, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, key, durability, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, key, durability, HLCTimestamp.Zero);
        }

        (KeyValueResponseType type, string resultKey, KeyValueDurability resultDurability, HLCTimestamp holder) =
            await locator.LocateAndTryAcquireExclusiveLock(transactionId, key, expiresMs, durability, cancellationToken);

        bool acquired = type == KeyValueResponseType.Locked;

        (KeyValueResponseType, string, KeyValueDurability, HLCTimestamp) response = (type, resultKey, resultDurability, holder);

        OperationCompletionPayload payload = OperationCompletionPayloadPool.Rent();
        payload.AcquiredPointLock = acquired ? key : null;
        payload.Durability = durability;
        payload.CachedType = type;

        if (!await CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, response, payload))
            return (KeyValueResponseType.MustRetry, key, durability, HLCTimestamp.Zero);

        return response;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="prefixKey"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<KeyValueResponseType> LocateAndTryAcquireExclusivePrefixLock(
        HLCTimestamp transactionId,
        string prefixKey,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult(KeyValueResponseType.InvalidInput);

        return RegisterAndAcquireExclusivePrefixLock(transactionId, coordinatorKey, operationId, prefixKey, expiresMs, durability, cancellationToken);
    }

    /// <summary>
    /// Register-remote wrapper for a transaction-scoped exclusive prefix-lock acquire: registers the
    /// operation and, on a confirmed lock, records the held prefix lock into the coordinator-owned lock
    /// set. A retry under the same operation id reports the lock as already held by this transaction.
    /// </summary>
    private async Task<KeyValueResponseType> RegisterAndAcquireExclusivePrefixLock(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string prefixKey,
        int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.PrefixLock, OperationDigest.ForPrefixLockAcquire(prefixKey, expiresMs, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                // Replay the exact cached outcome so a first-attempt failure does not resurface as success.
                participantOperationCache.Remove(transactionId, operationId);
                return cachedType;
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return (KeyValueResponseType)recovered;
                return KeyValueResponseType.MustRetry;
            case OperationRegistrationOutcome.RejectedCapacity:
                return KeyValueResponseType.MustRetry;
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return KeyValueResponseType.Aborted;
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return KeyValueResponseType.Aborted;
            case OperationRegistrationOutcome.RejectedDuplicate:
                return KeyValueResponseType.Errored;
        }

        KeyValueResponseType type =
            await locator.LocateAndTryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability, cancellationToken);

        bool acquired = type == KeyValueResponseType.Locked;

        OperationCompletionPayload payload = OperationCompletionPayloadPool.Rent();
        payload.AcquiredPrefixLock = acquired ? prefixKey : null;
        payload.Durability = durability;
        payload.CachedType = type;

        if (!await CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, type, payload))
            return KeyValueResponseType.MustRetry;

        return type;
    }

    /// <summary>
    /// Locates the leader node for the given keys and executes the TryAcquireManyExclusiveLocks request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)>> LocateAndTryAcquireManyExclusiveLocks(
        HLCTimestamp transactionId,
        List<(string key, int expiresMs, KeyValueDurability durability)> keys,
        CancellationToken cancelationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        // The whole batch registers as one coordinator operation so every acquired point lock folds into the
        // server-owned working set and is released on commit/rollback. Without this an interactive
        // transaction's batch locks are foreign to the coordinator, and a committed batch write reads back as
        // if never written. Fall back to the unregistered fan-out when no operation identity is supplied.
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryAcquireManyExclusiveLocks(transactionId, keys, cancelationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult(keys.Select(k => (KeyValueResponseType.InvalidInput, k.key, k.durability, HLCTimestamp.Zero)).ToList());

        return RegisterAndTryAcquireManyExclusiveLocks(transactionId, coordinatorKey, operationId, keys, cancelationToken);
    }

    private async Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)>> RegisterAndTryAcquireManyExclusiveLocks(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId,
        List<(string key, int expiresMs, KeyValueDurability durability)> keys, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, _, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.ManyPointLock, OperationDigest.ForManyPointLockAcquire(keys), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return (List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)>)recovered;
                return keys.Select(k => (KeyValueResponseType.MustRetry, k.key, k.durability, HLCTimestamp.Zero)).ToList();
            case OperationRegistrationOutcome.RejectedCapacity:
                return keys.Select(k => (KeyValueResponseType.MustRetry, k.key, k.durability, HLCTimestamp.Zero)).ToList();
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return keys.Select(k => (KeyValueResponseType.Aborted, k.key, k.durability, HLCTimestamp.Zero)).ToList();
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return keys.Select(k => (KeyValueResponseType.Aborted, k.key, k.durability, HLCTimestamp.Zero)).ToList();
            case OperationRegistrationOutcome.RejectedDuplicate:
                return keys.Select(k => (KeyValueResponseType.Errored, k.key, k.durability, HLCTimestamp.Zero)).ToList();
        }

        List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> responses =
            await locator.LocateAndTryAcquireManyExclusiveLocks(transactionId, keys, cancellationToken);

        // Fold every confirmed Locked key as a held point lock so commit/rollback release it. A transient
        // (MustRetry) key folds nothing; the caller resends only the transient subset as a fresh operation.
        List<(string, KeyValueDurability)> acquired = [];
        foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp _) in responses)
        {
            if (type == KeyValueResponseType.Locked)
                acquired.Add((key, durability));
        }

        OperationCompletionPayload payload = OperationCompletionPayloadPool.Rent();
        payload.AcquiredPointLocks = acquired.Count > 0 ? acquired : null;
        // A batch that acquired at least one lock completes terminally so its held locks fold. A
        // batch that acquired nothing must NOT be cached as a terminal success — that would let a
        // same-id retry replay the false success forever instead of re-registering. Mark it
        // transient so the completion cancels the registration and a same-id retry re-executes.
        payload.CachedType = acquired.Count > 0 ? KeyValueResponseType.Locked : KeyValueResponseType.MustRetry;

        if (!await CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, responses, payload))
            return keys.Select(k => (KeyValueResponseType.MustRetry, k.key, k.durability, HLCTimestamp.Zero)).ToList();

        return responses;
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryReleaseExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryReleaseExclusiveLock(transactionId, key, durability, cancelationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult((KeyValueResponseType.InvalidInput, key));

        return RegisterAndReleaseExclusiveLock(transactionId, coordinatorKey, operationId, key, durability, cancelationToken);
    }

    /// <summary>
    /// Register-remote wrapper for a transaction-scoped exclusive point-lock release: on a confirmed
    /// release, drops the held point lock from the coordinator-owned lock set so it is not released
    /// again at finalize.
    /// </summary>
    private async Task<(KeyValueResponseType, string)> RegisterAndReleaseExclusiveLock(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string key,
        KeyValueDurability durability, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.PointLock, OperationDigest.ForPointLockRelease(key, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                // Replay the exact cached outcome so a first-attempt failure does not resurface as success.
                participantOperationCache.Remove(transactionId, operationId);
                return (cachedType, key);
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return ((KeyValueResponseType, string))recovered;
                return (KeyValueResponseType.MustRetry, key);
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, key);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return (KeyValueResponseType.Aborted, key);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, key);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, key);
        }

        (KeyValueResponseType type, string resultKey) =
            await locator.LocateAndTryReleaseExclusiveLock(transactionId, key, durability, cancellationToken);

        bool released = type == KeyValueResponseType.Unlocked;

        (KeyValueResponseType, string) response = (type, resultKey);

        OperationCompletionPayload payload = OperationCompletionPayloadPool.Rent();
        payload.ReleasedPointLock = released ? key : null;
        payload.Durability = durability;
        payload.CachedType = type;

        if (!await CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, response, payload))
            return (KeyValueResponseType.MustRetry, key);

        return response;
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="prefixKey"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<KeyValueResponseType> LocateAndTryReleaseExclusivePrefixLock(
        HLCTimestamp transactionId,
        string prefixKey,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryReleaseExclusivePrefixLock(transactionId, prefixKey, durability, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult(KeyValueResponseType.InvalidInput);

        return RegisterAndReleaseExclusivePrefixLock(transactionId, coordinatorKey, operationId, prefixKey, durability, cancellationToken);
    }

    /// <summary>
    /// Register-remote wrapper for a transaction-scoped exclusive prefix-lock release: on a confirmed
    /// release, drops the held prefix lock from the coordinator-owned lock set so it is not released
    /// again at finalize.
    /// </summary>
    private async Task<KeyValueResponseType> RegisterAndReleaseExclusivePrefixLock(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string prefixKey,
        KeyValueDurability durability, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.PrefixLock, OperationDigest.ForPrefixLockRelease(prefixKey, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                // Replay the exact cached outcome so a first-attempt failure does not resurface as success.
                participantOperationCache.Remove(transactionId, operationId);
                return cachedType;
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return (KeyValueResponseType)recovered;
                return KeyValueResponseType.MustRetry;
            case OperationRegistrationOutcome.RejectedCapacity:
                return KeyValueResponseType.MustRetry;
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return KeyValueResponseType.Aborted;
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return KeyValueResponseType.Aborted;
            case OperationRegistrationOutcome.RejectedDuplicate:
                return KeyValueResponseType.Errored;
        }

        KeyValueResponseType type =
            await locator.LocateAndTryReleaseExclusivePrefixLock(transactionId, prefixKey, durability, cancellationToken);

        bool released = type == KeyValueResponseType.Unlocked;

        OperationCompletionPayload payload = OperationCompletionPayloadPool.Rent();
        payload.ReleasedPrefixLock = released ? prefixKey : null;
        payload.Durability = durability;
        payload.CachedType = type;

        if (!await CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, type, payload))
            return KeyValueResponseType.MustRetry;

        return type;
    }

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        RangeLockMode mode,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult((KeyValueResponseType.InvalidInput, HLCTimestamp.Zero));

        return RegisterAndAcquireRangeLock(transactionId, coordinatorKey, operationId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken);
    }

    /// <summary>
    /// Register-remote wrapper for a transaction-scoped range-lock acquire, upgrade, or renewal: registers
    /// the operation and, on a confirmed lock, records the range descriptor (bounds + mode) into the
    /// coordinator-owned lock set. A confirmed re-acquire at a different mode replaces the held mode, so a
    /// shared→exclusive upgrade or heartbeat renewal does not leave a duplicate descriptor.
    /// </summary>
    private async Task<(KeyValueResponseType, HLCTimestamp)> RegisterAndAcquireRangeLock(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string prefix,
        string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs,
        KeyValueDurability durability, RangeLockMode mode, CancellationToken cancellationToken,
        Func<Task>? afterSnapshot = null)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.RangeLock,
                OperationDigest.ForRangeLockAcquire(prefix, startKey, startInclusive, endKey, endInclusive, mode, expiresMs, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                // Replay the exact cached outcome: a first attempt that failed must not resurface as a
                // successful acquire. The holder is the transaction id on success, unknown (Zero) otherwise.
                participantOperationCache.Remove(transactionId, operationId);
                return (cachedType, cachedType == KeyValueResponseType.Locked ? transactionId : HLCTimestamp.Zero);
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return ((KeyValueResponseType, HLCTimestamp))recovered;
                return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return (KeyValueResponseType.Aborted, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, HLCTimestamp.Zero);
        }

        (KeyValueResponseType type, HLCTimestamp holder) =
            await locator.LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, afterSnapshot, cancellationToken);

        bool acquired = type == KeyValueResponseType.Locked;
        RangeLockKey range = new(prefix, startKey, startInclusive, endKey, endInclusive, durability);

        (KeyValueResponseType, HLCTimestamp) response = (type, holder);

        OperationCompletionPayload payload = OperationCompletionPayloadPool.Rent();
        payload.AcquiredRangeLock = acquired ? (range, mode) : null;
        payload.Durability = durability;
        payload.CachedType = type;

        if (!await CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, response, payload))
            return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);

        return response;
    }

    /// <summary>
    /// Test seam: runs the registered range-lock acquire with a hook fired after the range-map snapshot,
    /// so a split can be injected into the acquire window to exercise the generation fence and its
    /// effect on the coordinator-owned working set.
    /// </summary>
    internal Task<(KeyValueResponseType, HLCTimestamp)> RegisterAndAcquireRangeLockWithHook(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string prefix,
        string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs,
        KeyValueDurability durability, RangeLockMode mode, Func<Task> afterSnapshot, CancellationToken cancellationToken)
        => RegisterAndAcquireRangeLock(transactionId, coordinatorKey, operationId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken, afterSnapshot);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    ) => locator.LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, RangeLockMode.Exclusive, cancellationToken);

    internal Task<(KeyValueResponseType, HLCTimestamp)> LocateAndTryAcquireExclusiveRangeLockWithHook(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        Func<Task> afterSnapshot,
        CancellationToken cancellationToken
    ) => locator.LocateAndTryAcquireExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, afterSnapshot, cancellationToken);

    public Task<KeyValueResponseType> LocateAndTryReleaseExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult(KeyValueResponseType.InvalidInput);

        return RegisterAndReleaseRangeLock(transactionId, coordinatorKey, operationId, prefix, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);
    }

    /// <summary>
    /// Register-remote wrapper for a transaction-scoped range-lock release: on a confirmed release, drops
    /// the range descriptor from the coordinator-owned lock set so it is not released again at finalize.
    /// </summary>
    private async Task<KeyValueResponseType> RegisterAndReleaseRangeLock(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string prefix,
        string? startKey, bool startInclusive, string? endKey, bool endInclusive,
        KeyValueDurability durability, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.RangeLock,
                OperationDigest.ForRangeLockRelease(prefix, startKey, startInclusive, endKey, endInclusive, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                // Replay the exact cached outcome so a first-attempt failure does not resurface as success.
                participantOperationCache.Remove(transactionId, operationId);
                return cachedType;
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return (KeyValueResponseType)recovered;
                return KeyValueResponseType.MustRetry;
            case OperationRegistrationOutcome.RejectedCapacity:
                return KeyValueResponseType.MustRetry;
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return KeyValueResponseType.Aborted;
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return KeyValueResponseType.Aborted;
            case OperationRegistrationOutcome.RejectedDuplicate:
                return KeyValueResponseType.Errored;
        }

        KeyValueResponseType type =
            await locator.LocateAndTryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);

        bool released = type == KeyValueResponseType.Unlocked;
        RangeLockKey range = new(prefix, startKey, startInclusive, endKey, endInclusive, durability);

        OperationCompletionPayload payload = OperationCompletionPayloadPool.Rent();
        payload.ReleasedRangeLock = released ? range : null;
        payload.Durability = durability;
        payload.CachedType = type;

        if (!await CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, type, payload))
            return KeyValueResponseType.MustRetry;

        return type;
    }
}
