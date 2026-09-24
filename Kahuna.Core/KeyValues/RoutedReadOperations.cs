using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The routing façade for key-value reads — get, exists, their many-key variants, and the write-intent probes.
///
/// A read that is part of an interactive transaction still registers on the coordinator, because the read
/// observation it produces belongs in the transaction's working set and is what a later validation checks.
/// Its completion follows the same contract as a lock or a write: a completion that does not land answers
/// <c>MustRetry</c> instead of the value, keeps the observation in the participant retry cache, and a same-id
/// retry recovers it — never a value handed back over a record left pending, which would stall every finalize
/// of the transaction on the drain. The unconfirmed variant deliberately skips the leadership confirmation and
/// is only for callers that can tolerate a stale answer.
/// </summary>
internal sealed class RoutedReadOperations
{

    private readonly KeyValuesRuntime runtime;

    private readonly OperationRegistrar registrar;

    internal RoutedReadOperations(KeyValuesRuntime runtime, OperationRegistrar registrar)
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

    private Task<bool> CompleteRegisteredOperation<TResponse>(
        string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId,
        TResponse response, OperationCompletionPayload payload) where TResponse : notnull =>
        registrar.CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, response, payload);

    private Task<bool> TryRecoverRegisteredRead(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId) =>
        registrar.TryRecoverRegisteredRead(coordinatorKey, transactionId, operationId);

    private Task AbandonRegisteredOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId) =>
        registrar.AbandonRegisteredOperation(coordinatorKey, transactionId, operationId);

    private ValueTask<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey, TransactionConflictPolicy conflictPolicy)> LocateAndBeginOperation(
        string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken) =>
        registrar.LocateAndBeginOperation(coordinatorKey, transactionId, operationId, kind, payloadDigest, cancellationToken);

    /// <summary>
    /// Locates the leader node for the given key and executes the TryGetValue request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="revision"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        if (ReservedKeys.IsReserved(key))
            return Task.FromResult((KeyValueResponseType.InvalidInput, (ReadOnlyKeyValueEntry?)null));

        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryGetValue(transactionId, key, revision, readTimestamp, durability, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult((KeyValueResponseType.InvalidInput, (ReadOnlyKeyValueEntry?)null));

        return RegisterAndTryReadValue(OperationKind.Get, transactionId, coordinatorKey, operationId, key, revision, readTimestamp, durability, cancellationToken);
    }

    /// <summary>
    /// Register-remote wrapper for a transaction-scoped read: registers the read for finalize fencing
    /// and records its <c>{exists, revision}</c> observation into the coordinator-owned read set. On a
    /// duplicate operation id the read is re-executed without re-folding: the first-recorded observation
    /// is authoritative for commit validation even if this re-execution returns a newer value. A completion
    /// that does not land answers <c>MustRetry</c>, and the same-id retry recovers it before re-reading.
    /// </summary>
    private async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> RegisterAndTryReadValue(
        OperationKind kind, HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string key,
        long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        byte[] digest = OperationDigest.ForRead(kind, key, revision, readTimestamp, durability);

        (OperationRegistrationOutcome outcome, _, _, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, kind, digest, cancellationToken);

        // Pending means an earlier attempt read but its completion did not land: re-drive it from the retry
        // cache, then re-read below as under an already-completed id.
        if (outcome == OperationRegistrationOutcome.AlreadyPending)
        {
            if (!await TryRecoverRegisteredRead(coordinatorKey, transactionId, operationId))
                return (KeyValueResponseType.MustRetry, null);

            outcome = OperationRegistrationOutcome.AlreadyCompleted;
        }

        switch (outcome)
        {
            case OperationRegistrationOutcome.RejectedCapacity:
                return (KeyValueResponseType.MustRetry, null);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return (KeyValueResponseType.Aborted, null);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return (KeyValueResponseType.Aborted, null);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return (KeyValueResponseType.Errored, null);
        }

        KeyValueResponseType type;
        ReadOnlyKeyValueEntry? entry;

        // This frame is the registration's only completer, so a throw from the work must release it.
        try
        {
            (type, entry) = kind == OperationKind.Exists
                ? await locator.LocateAndTryExistsValue(transactionId, key, revision, readTimestamp, durability, cancellationToken)
                : await locator.LocateAndTryGetValue(transactionId, key, revision, readTimestamp, durability, cancellationToken);
        }
        catch
        {
            // A re-read under an already-completed id holds no pending registration; only a fresh one does.
            if (outcome == OperationRegistrationOutcome.New)
                await AbandonRegisteredOperation(coordinatorKey, transactionId, operationId);
            throw;
        }

        // A re-read under an already-completed id re-executes without re-folding:
        // the first-recorded observation is authoritative for commit validation.
        if (outcome != OperationRegistrationOutcome.New)
            return (type, entry);

        bool exists = entry is not null && type is KeyValueResponseType.Get or KeyValueResponseType.Exists;

        // A snapshot read is pinned to a past timestamp: it owns no live transactional MVCC entry and so
        // contributes no read dependency to validate at commit. It still completes for finalize fencing and
        // idempotent replay, but records no observation into the read set.
        bool snapshotRead = !readTimestamp.IsNull();

        OperationCompletionPayload payload = OperationCompletionPayloadPool.Rent();
        payload.Read = snapshotRead
            ? null
            : new KeyValueTransactionReadKey { Key = key, Durability = durability, Exists = exists, Revision = exists ? entry!.Revision : -1 };
        payload.Durability = durability;
        payload.CachedType = type;
        payload.CachedRevision = exists ? entry!.Revision : 0;
        payload.CachedTimestamp = exists ? entry!.LastModified : HLCTimestamp.Zero;

        // Owns the shell from here: recycled on an acknowledged fold, retained in the retry cache otherwise.
        // A completion that did not land must not hand the value back over a still-pending record.
        if (!await CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, type, payload))
            return (KeyValueResponseType.MustRetry, null);

        return (type, entry);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryExistsValue request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="revision"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryExistsValue(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        if (ReservedKeys.IsReserved(key))
            return Task.FromResult((KeyValueResponseType.InvalidInput, (ReadOnlyKeyValueEntry?)null));

        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryExistsValue(transactionId, key, revision, readTimestamp, durability, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult((KeyValueResponseType.InvalidInput, (ReadOnlyKeyValueEntry?)null));

        return RegisterAndTryReadValue(OperationKind.Exists, transactionId, coordinatorKey, operationId, key, revision, readTimestamp, durability, cancellationToken);
    }

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryExistsManyValues(transactionId, readTimestamp, keys, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult(BuildManyReadRejection(keys, KeyValueResponseType.InvalidInput));

        return RegisterAndTryReadManyValues(OperationKind.ExistsMany, transactionId, coordinatorKey, operationId, readTimestamp, keys, cancellationToken);
    }

    /// <summary>
    /// Staged-base variant of <see cref="LocateAndTryExistsManyValues"/> — see
    /// <see cref="KeyValueLocator.LocateAndTryExistsManyValuesUnconfirmed"/> for the leadership
    /// contract that makes the unconfirmed local read safe for the commit-time write-side
    /// compare-and-set and for no other caller. Never registers the reads: the probe is a commit
    /// guard, not part of any transaction's read set.
    /// </summary>
    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValuesUnconfirmed(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken
    )
    {
        return locator.LocateAndTryExistsManyValuesUnconfirmed(transactionId, readTimestamp, keys, cancellationToken);
    }

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryGetManyValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    )
    {
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryGetManyValues(transactionId, readTimestamp, keys, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult(BuildManyReadRejection(keys, KeyValueResponseType.InvalidInput));

        return RegisterAndTryReadManyValues(OperationKind.GetMany, transactionId, coordinatorKey, operationId, readTimestamp, keys, cancellationToken);
    }

    /// <summary>
    /// Builds a uniform per-key result list for a batch read that never reached the leaders (malformed
    /// registration, capacity/session rejection). One tuple per requested key carries the shared
    /// <paramref name="type"/> and the key's declared durability so callers see the same shape a real fan-out
    /// would return.
    /// </summary>
    private static List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> BuildManyReadRejection(
        List<(string key, long revision, KeyValueDurability durability)> keys, KeyValueResponseType type)
    {
        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> rejected = new(keys.Count);
        foreach ((string key, _, KeyValueDurability durability) in keys)
            rejected.Add((type, key, durability, null));
        return rejected;
    }

    /// <summary>
    /// Registers a batch point read (GetMany/ExistsMany) as a single coordinator operation so every key it
    /// observes becomes a read dependency of the transaction: an optimistic commit validates them and aborts if
    /// any changed after the read. Mirrors <see cref="RegisterAndTryReadValue"/> but folds one observation per
    /// returned key into <see cref="OperationCompletionPayload.ReadObservations"/>. A snapshot read (pinned read
    /// timestamp) owns no live transactional MVCC and so records no observations, but still registers for
    /// finalize fencing and idempotent replay. A completion that does not land answers <c>MustRetry</c> for
    /// every key, and the same-id retry recovers it before re-reading.
    /// </summary>
    private async Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> RegisterAndTryReadManyValues(
        OperationKind kind, HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId,
        HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken)
    {
        byte[] digest = kind == OperationKind.ExistsMany
            ? OperationDigest.ForExistsMany(keys, readTimestamp)
            : OperationDigest.ForGetMany(keys, readTimestamp);

        (OperationRegistrationOutcome outcome, _, _, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, kind, digest, cancellationToken);

        // Pending means an earlier attempt read but its completion did not land: re-drive it from the retry
        // cache, then re-read below as under an already-completed id.
        if (outcome == OperationRegistrationOutcome.AlreadyPending)
        {
            if (!await TryRecoverRegisteredRead(coordinatorKey, transactionId, operationId))
                return BuildManyReadRejection(keys, KeyValueResponseType.MustRetry);

            outcome = OperationRegistrationOutcome.AlreadyCompleted;
        }

        switch (outcome)
        {
            case OperationRegistrationOutcome.RejectedCapacity:
                return BuildManyReadRejection(keys, KeyValueResponseType.MustRetry);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return BuildManyReadRejection(keys, KeyValueResponseType.Aborted);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return BuildManyReadRejection(keys, KeyValueResponseType.Aborted);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return BuildManyReadRejection(keys, KeyValueResponseType.Errored);
        }

        List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> result;

        // This frame is the registration's only completer, so a throw from the work must release it.
        try
        {
            result = kind == OperationKind.ExistsMany
                ? await locator.LocateAndTryExistsManyValues(transactionId, readTimestamp, keys, cancellationToken)
                : await locator.LocateAndTryGetManyValues(transactionId, readTimestamp, keys, cancellationToken);
        }
        catch
        {
            // A re-read under an already-completed id holds no pending registration; only a fresh one does.
            if (outcome == OperationRegistrationOutcome.New)
                await AbandonRegisteredOperation(coordinatorKey, transactionId, operationId);
            throw;
        }

        // A re-read under an already-completed id re-executes without re-folding:
        // the first-recorded observations are authoritative for commit validation.
        if (outcome != OperationRegistrationOutcome.New)
            return result;

        // A snapshot read is pinned to a past timestamp: it owns no live transactional MVCC entry and so
        // contributes no read dependency to validate at commit. It still completes for finalize fencing and
        // idempotent replay, but records no observation into the read set.
        List<KeyValueTransactionReadKey>? observations = null;
        KeyValueResponseType batchCachedType = kind == OperationKind.ExistsMany ? KeyValueResponseType.Exists : KeyValueResponseType.Get;

        if (readTimestamp.IsNull())
        {
            // Fold only confirmed per-key responses: Get (present), Exists (present, exists-check), and
            // DoesNotExist (confirmed absent). Every other type — transients (MustRetry/WaitingForReplication),
            // Errored (dropped actor response), InvalidInput (malformed key) — means the key's state is
            // unknown, not "absent". Folding a non-confirmed result as absent manufactures a false "missing"
            // observation that conflicts with a retry confirming the key present, aborting an otherwise
            // conflict-free optimistic commit. An allowlist (not a transient denylist) stays correct if a
            // future read path introduces another non-confirmed response type.
            observations = new(result.Count);
            bool anyConfirmed = false;

            foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry) in result)
            {
                if (type is not (KeyValueResponseType.Get or KeyValueResponseType.Exists or KeyValueResponseType.DoesNotExist))
                    continue;  // state unknown (transient/errored/invalid), not "absent" — exclude from read set

                anyConfirmed = true;
                bool exists = entry is not null && type is KeyValueResponseType.Get or KeyValueResponseType.Exists;
                observations.Add(new KeyValueTransactionReadKey
                {
                    Key = key,
                    Durability = durability,
                    Exists = exists,
                    Revision = exists ? entry!.Revision : -1
                });
            }

            // An all-transient latest batch has no confirmed observations: cancel the registration so a
            // same-id retry re-registers as New (mirrors the point/write transient-cancel path).
            if (!anyConfirmed)
            {
                observations = null;
                batchCachedType = KeyValueResponseType.MustRetry;
            }
        }

        OperationCompletionPayload payload = OperationCompletionPayloadPool.Rent();
        payload.ReadObservations = observations;
        payload.Durability = keys.Count > 0 ? keys[0].durability : KeyValueDurability.Persistent;
        payload.CachedType = batchCachedType;

        // Owns the shell from here: recycled on an acknowledged fold, retained in the retry cache otherwise.
        // A completion that did not land must not hand the values back over a still-pending record.
        if (!await CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, batchCachedType, payload))
            return BuildManyReadRejection(keys, KeyValueResponseType.MustRetry);

        return result;
    }
    
    /// <summary>
    /// Probes a whole read set for concurrent write intents in one pass, grouped by the node owning each key.
    /// Returns one result per requested key; see <see cref="KeyValueLocator.LocateAndTryCheckManyWriteIntents"/>
    /// for the coverage contract callers rely on.
    /// </summary>
    public Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> LocateAndTryCheckManyWriteIntents(
        HLCTimestamp transactionId,
        List<KeyValueConflictProbe> keys,
        CancellationToken cancellationToken
    )
    {
        return locator.LocateAndTryCheckManyWriteIntents(transactionId, keys, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and checks whether a live write intent from another
    /// transaction exists. Used at commit time by optimistic transactions as a write-skew guard.
    /// </summary>
    public Task<KeyValueResponseType> LocateAndTryCheckWriteIntent(
        HLCTimestamp transactionId,
        string key,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        return locator.LocateAndTryCheckWriteIntent(transactionId, key, durability, cancellationToken);
    }
}
