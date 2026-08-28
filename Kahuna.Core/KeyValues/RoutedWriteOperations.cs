using System.Runtime.CompilerServices;

using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The routing façade for key-value writes — set, delete, extend, and their many-key and system variants.
///
/// Each entry point does the same three things in the same order: reject a reserved key before any routing,
/// classify the request by the register-remote identity it carries, and then either take the legacy locator
/// path or register the operation on its coordinator, apply it on the key's leader, and record the confirmed
/// effect. A many-key call registers as a <b>single</b> coordinator operation so its confirmed persistent keys
/// fold into one working-set entry.
/// </summary>
internal sealed class RoutedWriteOperations
{

    private readonly KeyValuesRuntime runtime;

    private readonly OperationRegistrar registrar;

    internal RoutedWriteOperations(KeyValuesRuntime runtime, OperationRegistrar registrar)
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
    /// Locates the leader node for the given key and executes the TrySet request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="value"></param>
    /// <param name="compareValue"></param>
    /// <param name="compareRevision"></param>
    /// <param name="flags"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public ValueTask<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(
        HLCTimestamp transactionId,
        string key,
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        long routedGeneration = 0,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        if (ReservedKeys.IsReserved(key))
            return ValueTask.FromResult((KeyValueResponseType.InvalidInput, 0L, HLCTimestamp.Zero));

        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability, cancellationToken, routedGeneration);
        if (routing is RegistrationRouting.Malformed)
            return ValueTask.FromResult((KeyValueResponseType.InvalidInput, 0L, HLCTimestamp.Zero));

        byte[] digest = OperationDigest.ForSet(key, value, compareValue, compareRevision, flags, expiresMs, durability);

        return RegisterAndTrySetKeyValue(transactionId, coordinatorKey, operationId, key, value, compareValue, compareRevision, flags, expiresMs, durability, digest, routedGeneration, cancellationToken);
    }

    /// <summary>
    /// Register-remote wrapper for a transaction-scoped write: registers the operation on the
    /// coordinator (dedup guard), applies the write on the key's leader only when the registration is
    /// new, then records the confirmed effect and caches the response. A retry carrying the same
    /// operation id replays the cached response instead of applying the write twice.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<(KeyValueResponseType, long, HLCTimestamp)> RegisterAndTrySetKeyValue(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string key,
        byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs,
        KeyValueDurability durability, byte[] digest, long routedGeneration, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.Set, digest, cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                participantOperationCache.Remove(transactionId, operationId);
                return (cachedType, cachedRevision, cachedTimestamp);
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return ((KeyValueResponseType, long, HLCTimestamp))recovered;
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return (KeyValueResponseType.Aborted, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, 0, HLCTimestamp.Zero);
        }

        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) =
            await locator.LocateAndTrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability, cancellationToken, routedGeneration);

        // Only a confirmed set records a modified key + its implicit point lock into the working set.
        bool applied = type == KeyValueResponseType.Set;

        (KeyValueResponseType, long, HLCTimestamp) response = (type, revision, lastModified);

        // Drive the completion; if it is not acknowledged, surface MustRetry so a same-id retry
        // recovers the confirmed effect without applying the set a second time.
        OperationCompletionPayload payload = OperationCompletionPayloadPool.Rent();
        payload.ModifiedKey = applied ? key : null;
        payload.AcquiredPointLock = applied ? key : null;
        // Stage the confirmed value for a persistent write so the coordinator finalizes durably,
        // carrying the NoRevision flag so a registered SET NOREV materializes revision-free.
        payload.StagedMutations = applied && durability == KeyValueDurability.Persistent
            ? [new StagedMutationEffect(key, value, revision, expiresMs, (flags & KeyValueFlags.SetNoRevision) != 0)]
            : null;
        payload.Durability = durability;
        payload.CachedType = type;
        payload.CachedRevision = revision;
        payload.CachedTimestamp = lastModified;

        if (!await CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, response, payload))
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);

        return response;
    }

    /// <summary>
    /// Attempts to locate and set multiple key-value pairs in the system.
    /// </summary>
    /// <param name="setManyItems">A collection of key-value set requests to be processed.</param>
    /// <param name="cancellationToken">Token to signal cancellation of the operation.</param>
    /// <returns>A task that represents the asynchronous operation, containing a list of responses for the key-value set requests.</returns>
    public Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(
        List<KahunaSetKeyValueRequestItem> setManyItems,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        foreach (KahunaSetKeyValueRequestItem item in setManyItems)
            if (item.Key is not null && ReservedKeys.IsReserved(item.Key))
                return Task.FromResult(AllSetItemsResponse(setManyItems, KeyValueResponseType.InvalidInput));

        // The whole batch registers as a single coordinator operation so its confirmed persistent keys fold
        // in canonical request order and anchor the transaction record deterministically. Fall back to the
        // unregistered fan-out when the caller supplied no operation identity (non-transactional path).
        HLCTimestamp transactionId = setManyItems.Count > 0 ? setManyItems[0].TransactionId : HLCTimestamp.Zero;

        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTrySetManyKeyValue(setManyItems, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult(AllSetItemsResponse(setManyItems, KeyValueResponseType.InvalidInput));

        return RegisterAndTrySetManyKeyValue(transactionId, coordinatorKey, operationId, setManyItems, cancellationToken);
    }

    private async Task<List<KahunaSetKeyValueResponseItem>> RegisterAndTrySetManyKeyValue(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId,
        List<KahunaSetKeyValueRequestItem> setManyItems, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, _, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.SetMany, OperationDigest.ForSetMany(setManyItems), cancellationToken);


        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyPending:
                // A batch stuck pending from a lost completion cannot re-register as New, so re-driving is
                // impossible here; recover the confirmed effect from the participant cache instead.
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return (List<KahunaSetKeyValueResponseItem>)recovered;
                return AllSetItemsResponse(setManyItems, KeyValueResponseType.MustRetry);
            case OperationRegistrationOutcome.AlreadyCompleted:
                // A completed batch must NEVER be re-driven. The caller's same-id resend contract sends the
                // identical batch again only when its previous call answered every item transient; a batch
                // that confirmed nothing cancelled its registration (see the completion below), so a
                // legitimate resend re-registers as New. Reaching a COMPLETED record therefore means the
                // caller's transient view is stale — the first drive's detached completion folded its
                // confirmed effects after the caller stopped waiting. Re-executing the batch here would
                // mutate participants outside the operation registry: the freeze cannot see the re-drive
                // (nothing is pending), so it can land mid-commit or after rollback released the keys,
                // re-staging values and re-planting write intents for a transaction that is already
                // finalized. Answer transient instead: the caller's own retry budget bounds the loop, and
                // its statement fails conflict-retryable while the folded working set stays consistent.
                DurableTransactionMetrics.CompletedBatchRedriveRefusals.Add(1);
                return AllSetItemsResponse(setManyItems, KeyValueResponseType.MustRetry);
            case OperationRegistrationOutcome.RejectedCapacity:
                return AllSetItemsResponse(setManyItems, KeyValueResponseType.MustRetry);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return AllSetItemsResponse(setManyItems, KeyValueResponseType.Aborted);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return AllSetItemsResponse(setManyItems, KeyValueResponseType.Aborted);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return AllSetItemsResponse(setManyItems, KeyValueResponseType.Errored);
        }

        List<KahunaSetKeyValueResponseItem> responses =
            await locator.LocateAndTrySetManyKeyValue(setManyItems, cancellationToken);

        // Fold the confirmed writes in canonical request order (fan-out returns them unordered), so the first
        // persistent key deterministically anchors the transaction record. Only a genuine Set is a confirmed
        // write; a failed SetIfNotExists (NotSet) or a transient (MustRetry) item folds nothing.
        //
        // Unlike delete-many, this does NOT cancel the registration when some items come back transient. The
        // caller (a mass writer) resends only the transient keys on retry — never the already-written ones,
        // whose intents now exist — so a "cancel then re-drive the whole batch under the same id" contract
        // would re-issue a SetIfNotExists for an already-set unique key and get a false NotSet. Instead the
        // confirmed keys fold now, the operation completes, and the caller retries the transient keys as a
        // fresh operation. A confirmed write is idempotent under participant-cache replay of the same id.
        Dictionary<string, (KeyValueResponseType Type, long Revision)> byKey = new(responses.Count, StringComparer.Ordinal);
        foreach (KahunaSetKeyValueResponseItem response in responses)
            byKey[response.Key ?? ""] = (response.Type, response.Revision);

        List<(string, KeyValueDurability)> modifiedKeys = [];
        List<StagedMutationEffect> stagedMutations = [];
        foreach (KahunaSetKeyValueRequestItem item in setManyItems)
        {
            if (byKey.TryGetValue(item.Key ?? "", out (KeyValueResponseType Type, long Revision) result) && result.Type == KeyValueResponseType.Set)
            {
                modifiedKeys.Add((item.Key ?? "", item.Durability));

                // Stage the confirmed value + new revision for each persistent write so the coordinator finalizes
                // the batch through the durable-intent path instead of the manual ticket path.
                if (item.Durability == KeyValueDurability.Persistent)
                    stagedMutations.Add(new StagedMutationEffect(item.Key ?? "", item.Value, result.Revision, item.ExpiresMs, (item.Flags & KeyValueFlags.SetNoRevision) != 0));
            }
        }

        // Cache the batch result and drive the completion off the caller token; if it is not acknowledged,
        // surface MustRetry so a same-id retry recovers the confirmed effect without re-writing.

        OperationCompletionPayload payload = OperationCompletionPayloadPool.Rent();
        payload.ModifiedKeys = modifiedKeys.Count > 0 ? modifiedKeys : null;
        payload.StagedMutations = stagedMutations.Count > 0 ? stagedMutations : null;
        // A confirmed write folds its implicit point lock, exactly as the single-key path does. An
        // optimistic transaction takes no explicit locks, so this is its only source of the held-lock
        // set that PrepareMutations requires; for a pessimistic caller the explicit lock already
        // recorded these keys, so the HashSet fold is idempotent.
        payload.AcquiredPointLocks = modifiedKeys.Count > 0 ? modifiedKeys : null;
        // A batch that confirmed at least one write completes terminally so its effect folds. A
        // batch that confirmed nothing must NOT be cached as a terminal success — that would let a
        // same-id retry replay the false success forever instead of re-registering. Mark it
        // transient so the completion cancels the registration and a same-id retry re-executes.
        payload.CachedType = modifiedKeys.Count > 0 ? KeyValueResponseType.Set : KeyValueResponseType.MustRetry;

        if (!await CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, responses, payload))
            return AllSetItemsResponse(setManyItems, KeyValueResponseType.MustRetry);

        return responses;
    }

    private static List<KahunaSetKeyValueResponseItem> AllSetItemsResponse(
        List<KahunaSetKeyValueRequestItem> items, KeyValueResponseType type)
    {
        List<KahunaSetKeyValueResponseItem> responses = new(items.Count);
        foreach (KahunaSetKeyValueRequestItem item in items)
            responses.Add(new()
            {
                Key = item.Key ?? "",
                Durability = item.Durability,
                Type = type
            });
        return responses;
    }

    public Task<List<KahunaDeleteKeyValueResponseItem>> LocateAndTryDeleteManyKeyValue(
        List<KahunaDeleteKeyValueRequestItem> deleteManyItems,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        foreach (KahunaDeleteKeyValueRequestItem item in deleteManyItems)
            if (item.Key is not null && ReservedKeys.IsReserved(item.Key))
                return Task.FromResult(AllItemsResponse(deleteManyItems, KeyValueResponseType.InvalidInput));

        // The whole batch registers as a single coordinator operation so its confirmed persistent keys
        // fold in canonical request order and anchor the transaction record deterministically. Fall back to
        // the unregistered fan-out when the caller supplied no operation identity (non-transactional path).
        HLCTimestamp transactionId = deleteManyItems.Count > 0 ? deleteManyItems[0].TransactionId : HLCTimestamp.Zero;

        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryDeleteManyKeyValue(deleteManyItems, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult(AllItemsResponse(deleteManyItems, KeyValueResponseType.InvalidInput));

        return RegisterAndTryDeleteManyKeyValue(transactionId, coordinatorKey, operationId, deleteManyItems, cancellationToken);
    }

    private async Task<List<KahunaDeleteKeyValueResponseItem>> RegisterAndTryDeleteManyKeyValue(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId,
        List<KahunaDeleteKeyValueRequestItem> deleteManyItems, CancellationToken cancellationToken)
    {
        List<(string Key, KeyValueDurability Durability)> canonicalItems = new(deleteManyItems.Count);
        foreach (KahunaDeleteKeyValueRequestItem item in deleteManyItems)
            canonicalItems.Add((item.Key ?? "", item.Durability));

        (OperationRegistrationOutcome outcome, _, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.DeleteMany, OperationDigest.ForDeleteMany(canonicalItems), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyPending:
                // A batch stuck pending from a lost completion cannot re-register as New, so re-driving
                // is impossible here; recover the confirmed effect from the participant cache instead.
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return (List<KahunaDeleteKeyValueResponseItem>)recovered;
                return AllItemsResponse(deleteManyItems, KeyValueResponseType.MustRetry);
            case OperationRegistrationOutcome.AlreadyCompleted:
                // Same contract as the set-many path: a completed batch is never re-driven. A legitimate
                // same-id resend only exists for a batch that confirmed nothing, and that batch cancelled
                // its registration, so it re-registers as New. A COMPLETED record here is the stale-view
                // race (the first drive's detached completion won after the caller stopped waiting), and a
                // re-drive would mutate participants invisibly to the freeze — staging deletes and planting
                // write intents for a transaction that may already be finalizing. Answer transient; the
                // caller's bounded retry budget resolves it.
                DurableTransactionMetrics.CompletedBatchRedriveRefusals.Add(1);
                return AllItemsResponse(deleteManyItems, KeyValueResponseType.MustRetry);
            case OperationRegistrationOutcome.RejectedCapacity:
                return AllItemsResponse(deleteManyItems, KeyValueResponseType.MustRetry);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return AllItemsResponse(deleteManyItems, KeyValueResponseType.Aborted);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return AllItemsResponse(deleteManyItems, KeyValueResponseType.Aborted);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return AllItemsResponse(deleteManyItems, KeyValueResponseType.Errored);
        }

        List<KahunaDeleteKeyValueResponseItem> responses =
            await locator.LocateAndTryDeleteManyKeyValue(deleteManyItems, cancellationToken);

        // Fold the confirmed deletes in canonical request order (fan-out returns them unordered), so the
        // first persistent key deterministically anchors the transaction record. A transient (MustRetry) item
        // folds nothing. The registration is NOT cancelled on a partial-transient batch: the caller (a mass
        // deleter) resends only the transient keys on retry as a fresh operation, so the confirmed keys must
        // fold now rather than be discarded for a full re-drive that never comes.
        Dictionary<(string, KeyValueDurability), (KeyValueResponseType Type, long Revision)> byKey = new(responses.Count);
        foreach (KahunaDeleteKeyValueResponseItem response in responses)
            byKey[(response.Key ?? "", response.Durability)] = (response.Type, response.Revision);

        List<(string, KeyValueDurability)> modifiedKeys = [];
        List<StagedMutationEffect> stagedMutations = [];
        foreach ((string Key, KeyValueDurability Durability) item in canonicalItems)
        {
            if (byKey.TryGetValue(item, out (KeyValueResponseType Type, long Revision) result) && result.Type == KeyValueResponseType.Deleted)
            {
                modifiedKeys.Add(item);

                // Stage the tombstone (null value) + new revision for each persistent delete so the coordinator
                // finalizes the batch through the durable-intent path.
                if (item.Durability == KeyValueDurability.Persistent)
                    stagedMutations.Add(new StagedMutationEffect(item.Key, null, result.Revision, 0, NoRevision: false));
            }
        }

        // Cache the batch result and drive the completion off the caller token; if it is not acknowledged,
        // surface MustRetry so a same-id retry recovers the confirmed effect without re-deleting.
        OperationCompletionPayload payload = OperationCompletionPayloadPool.Rent();
        payload.ModifiedKeys = modifiedKeys.Count > 0 ? modifiedKeys : null;
        payload.StagedMutations = stagedMutations.Count > 0 ? stagedMutations : null;
        // A confirmed delete folds its implicit point lock, exactly as the single-key path does. An
        // optimistic transaction takes no explicit locks, so this is its only source of the held-lock
        // set that PrepareMutations requires; for a pessimistic caller the explicit lock already
        // recorded these keys, so the HashSet fold is idempotent.
        payload.AcquiredPointLocks = modifiedKeys.Count > 0 ? modifiedKeys : null;
        // A batch that confirmed at least one delete completes terminally so its effect folds. A
        // batch that confirmed nothing must NOT be cached as a terminal success — that would let a
        // same-id retry replay the false success forever instead of re-registering. Mark it
        // transient so the completion cancels the registration and a same-id retry re-executes.
        payload.CachedType = modifiedKeys.Count > 0 ? KeyValueResponseType.Deleted : KeyValueResponseType.MustRetry;

        if (!await CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, responses, payload))
            return AllItemsResponse(deleteManyItems, KeyValueResponseType.MustRetry);

        return responses;
    }

    // ── system-reserved keys ────────────────────────────────────────────────────────────────────
    // Accessors for records under the reserved namespace (see ReservedKeys). The public entry points
    // above reject those keys so clients cannot touch system state; the subsystems that own the
    // records reach them through these instead. Always persistent, never transaction-scoped.

    internal Task<(KeyValueResponseType, long, HLCTimestamp)> SystemSetKeyValue(
        string key,
        byte[]? value,
        long compareRevision,
        KeyValueFlags flags,
        CancellationToken cancellationToken
    )
    {
        return locator.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, value, null, compareRevision, flags, 0, KeyValueDurability.Persistent, cancellationToken).AsTask();
    }

    internal Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> SystemGetKeyValue(string key, CancellationToken cancellationToken)
    {
        return locator.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, cancellationToken);
    }

    internal Task<(KeyValueResponseType, long, HLCTimestamp)> SystemDeleteKeyValue(string key, CancellationToken cancellationToken)
    {
        return locator.LocateAndTryDeleteKeyValue(HLCTimestamp.Zero, key, KeyValueDurability.Persistent, cancellationToken);
    }

    private static List<KahunaDeleteKeyValueResponseItem> AllItemsResponse(
        List<KahunaDeleteKeyValueRequestItem> items, KeyValueResponseType type)
    {
        List<KahunaDeleteKeyValueResponseItem> responses = new(items.Count);
        foreach (KahunaDeleteKeyValueRequestItem item in items)
            responses.Add(new()
            {
                Key = item.Key ?? "",
                Type = type,
                Revision = -1,
                LastModified = HLCTimestamp.Zero,
                Durability = item.Durability
            });
        return responses;
    }
    /// <summary>
    /// Locates the leader node for the given key and executes the TryDelete request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        if (ReservedKeys.IsReserved(key))
            return Task.FromResult((KeyValueResponseType.InvalidInput, 0L, HLCTimestamp.Zero));

        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryDeleteKeyValue(transactionId, key, durability, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult((KeyValueResponseType.InvalidInput, 0L, HLCTimestamp.Zero));

        return RegisterAndTryDeleteKeyValue(transactionId, coordinatorKey, operationId, key, durability, OperationDigest.ForDelete(key, durability), cancellationToken);
    }

    private async Task<(KeyValueResponseType, long, HLCTimestamp)> RegisterAndTryDeleteKeyValue(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string key,
        KeyValueDurability durability, byte[] digest, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.Delete, digest, cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                participantOperationCache.Remove(transactionId, operationId);
                return (cachedType, cachedRevision, cachedTimestamp);
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return ((KeyValueResponseType, long, HLCTimestamp))recovered;
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return (KeyValueResponseType.Aborted, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, 0, HLCTimestamp.Zero);
        }

        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) =
            await locator.LocateAndTryDeleteKeyValue(transactionId, key, durability, cancellationToken);

        bool applied = type == KeyValueResponseType.Deleted;

        (KeyValueResponseType, long, HLCTimestamp) response = (type, revision, lastModified);

        OperationCompletionPayload payload = OperationCompletionPayloadPool.Rent();
        payload.ModifiedKey = applied ? key : null;
        payload.AcquiredPointLock = applied ? key : null;
        // Stage the tombstone (null value) for a persistent delete so the coordinator finalizes durably.
        payload.StagedMutations = applied && durability == KeyValueDurability.Persistent
            ? [new StagedMutationEffect(key, null, revision, 0, NoRevision: false)]
            : null;
        payload.Durability = durability;
        payload.CachedType = type;
        payload.CachedRevision = revision;
        payload.CachedTimestamp = lastModified;

        if (!await CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, response, payload))
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);

        return response;
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryExtend request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        if (ReservedKeys.IsReserved(key))
            return Task.FromResult((KeyValueResponseType.InvalidInput, 0L, HLCTimestamp.Zero));

        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryExtendKeyValue(transactionId, key, expiresMs, durability, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult((KeyValueResponseType.InvalidInput, 0L, HLCTimestamp.Zero));

        return RegisterAndTryExtendKeyValue(transactionId, coordinatorKey, operationId, key, expiresMs, durability, OperationDigest.ForExtend(key, expiresMs, durability), cancellationToken);
    }

    private async Task<(KeyValueResponseType, long, HLCTimestamp)> RegisterAndTryExtendKeyValue(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string key,
        int expiresMs, KeyValueDurability durability, byte[] digest, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.Extend, digest, cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                participantOperationCache.Remove(transactionId, operationId);
                return (cachedType, cachedRevision, cachedTimestamp);
            case OperationRegistrationOutcome.AlreadyPending:
                if (await TryRecoverRegisteredOperation(coordinatorKey, transactionId, operationId) is { } recovered)
                    return ((KeyValueResponseType, long, HLCTimestamp))recovered;
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return (KeyValueResponseType.Aborted, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, 0, HLCTimestamp.Zero);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, 0, HLCTimestamp.Zero);
        }

        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) =
            await locator.LocateAndTryExtendKeyValue(transactionId, key, expiresMs, durability, cancellationToken);

        bool applied = type == KeyValueResponseType.Extended;

        // Stage the extend for the durable-intent path so a registered transaction containing an extend stays on
        // the durable path instead of falling back to the ticket path. An extend changes only the expiry, so the
        // staged effect carries the key's current value and revision — read back within this transaction so it
        // sees the extend's own MVCC snapshot — plus the new relative TTL. If the value cannot be read back,
        // staging is skipped and the transaction falls back (correct, just not batched). Mirrors the script ExtendCommand.
        IReadOnlyList<StagedMutationEffect>? stagedMutations = null;
        if (applied && durability == KeyValueDurability.Persistent)
        {
            (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) =
                await locator.LocateAndTryGetValue(transactionId, key, -1, HLCTimestamp.Zero, durability, cancellationToken);
            if (readType == KeyValueResponseType.Get && entry is not null)
                stagedMutations = [new StagedMutationEffect(key, entry.Value, entry.Revision, expiresMs, NoRevision: false)];
        }

        (KeyValueResponseType, long, HLCTimestamp) response = (type, revision, lastModified);

        OperationCompletionPayload payload = OperationCompletionPayloadPool.Rent();
        payload.ModifiedKey = applied ? key : null;
        payload.AcquiredPointLock = applied ? key : null;
        payload.StagedMutations = stagedMutations;
        payload.Durability = durability;
        payload.CachedType = type;
        payload.CachedRevision = revision;
        payload.CachedTimestamp = lastModified;

        if (!await CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, response, payload))
            return (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);

        return response;
    }
}
