using System.Runtime.CompilerServices;

using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The routing façade for scans and for the manual two-phase-commit ticket calls — prefix and range reads
/// that fan out across partitions, plus prepare/commit/rollback routed to each key's leader.
///
/// <see cref="LocateAndScanRange"/> streams: it yields each page as it arrives rather than accumulating the
/// range, so a large range does not have to fit in memory. The <c>WithHooks</c> variant is the seam tests use
/// to interleave a split into a scan.
/// </summary>
internal sealed class RoutedScanOperations
{

    private readonly KeyValuesRuntime runtime;

    private readonly OperationRegistrar registrar;

    internal RoutedScanOperations(KeyValuesRuntime runtime, OperationRegistrar registrar)
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

    private Task<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey)> LocateAndBeginOperation(
        string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken) =>
        registrar.LocateAndBeginOperation(coordinatorKey, transactionId, operationId, kind, payloadDigest, cancellationToken);

    private Task<(KeyValueResponseType outcome, string? anchor)> LocateAndCompleteOperation(
        string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, CancellationToken cancellationToken) =>
        registrar.LocateAndCompleteOperation(coordinatorKey, transactionId, operationId, payload, cancellationToken);

    /// <summary>
    /// Locates the leader node for the given keys and executes the TryReleaseManyExclusiveLocks requests
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryReleaseManyExclusiveLocks(HLCTimestamp transactionId, List<(string key, KeyValueDurability durability)> keys, CancellationToken cancelationToken)
    {
        return locator.LocateAndTryReleaseManyExclusiveLocks(transactionId, keys, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryPrepareMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> LocateAndTryPrepareMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key,
        KeyValueDurability durability,
        CancellationToken cancelationToken,
        long routedGeneration = 0,
        string? recordAnchorKey = null
    )
    {
        return locator.LocateAndTryPrepareMutations(transactionId, commitId, key, durability, cancelationToken, routedGeneration, recordAnchorKey);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes many TryPrepareMutations requests.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="commitId"></param> 
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> LocateAndTryPrepareManyMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        List<(string key, KeyValueDurability durability)> keys,
        CancellationToken cancelationToken,
        string? recordAnchorKey = null
    )
    {
        return locator.LocateAndTryPrepareManyMutations(transactionId, commitId, keys, cancelationToken, recordAnchorKey);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryCommitMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="ticketId"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        return locator.LocateAndTryCommitMutations(transactionId, key, ticketId, durability, cancelationToken);
    }

    /// <summary>
    /// Locates the leader node for the given keys and executes the TryCommitMutations request. 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryCommitManyMutations(HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, CancellationToken cancelationToken)
    {
        return locator.LocateAndTryCommitManyMutations(transactionId, keys, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryRollbackMutations request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="ticketId"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<(KeyValueResponseType, long)> LocateAndTryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        return locator.LocateAndTryRollbackMutations(transactionId, key, ticketId, durability, cancelationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes the TryRollbackMutations request. 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryRollbackManyMutations(
        HLCTimestamp transactionId, 
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, 
        CancellationToken cancellationToken
    )
    {
        return locator.LocateAndTryRollbackManyMutations(transactionId, keys, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given prefix and executes the GetByBucket request.
    /// </summary>
    /// <param name="prefixedKey"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task<KeyValueGetByBucketResult> LocateAndGetByBucket(HLCTimestamp transactionId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, cancellationToken);
        if (routing is RegistrationRouting.Malformed)
            return Task.FromResult(new KeyValueGetByBucketResult(KeyValueResponseType.InvalidInput, []));

        return RegisterAndGetByBucket(transactionId, coordinatorKey, operationId, prefixedKey, readTimestamp, durability, cancellationToken);
    }

    /// <summary>
    /// Register-remote wrapper for a transaction-scoped bucket scan: registers the operation so it is
    /// fenced against finalize, then records every returned item as a read observation with point-read-set
    /// semantics. The scan asserts only that these exact items were observed at these revisions — not that
    /// no other key matches the bucket predicate.
    /// </summary>
    private async Task<KeyValueGetByBucketResult> RegisterAndGetByBucket(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string prefixedKey,
        HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, _, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.Scan,
                OperationDigest.ForScan(prefixedKey, readTimestamp, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
            case OperationRegistrationOutcome.AlreadyPending:
            case OperationRegistrationOutcome.RejectedCapacity:
                // A pending or already-completed scan re-executes the read without re-folding observations:
                // the first-recorded observations are authoritative for commit validation even if this
                // re-execution returns a newer value.
                if (outcome == OperationRegistrationOutcome.AlreadyCompleted)
                    return await locator.LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, cancellationToken);
                return new KeyValueGetByBucketResult(KeyValueResponseType.MustRetry, []);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return new KeyValueGetByBucketResult(KeyValueResponseType.Aborted, []);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return new KeyValueGetByBucketResult(KeyValueResponseType.Aborted, []);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return new KeyValueGetByBucketResult(KeyValueResponseType.Errored, []);
        }

        KeyValueGetByBucketResult result =
            await locator.LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, cancellationToken);

        // A snapshot scan is pinned to a past timestamp and owns no live transactional MVCC, so its items
        // contribute no read dependencies; it still completes for finalize fencing and idempotent replay.
        List<KeyValueTransactionReadKey>? observations = null;
        if (readTimestamp.IsNull() && result.Type == KeyValueResponseType.Get && result.Items.Count > 0)
        {
            observations = new(result.Items.Count);
            foreach ((string itemKey, ReadOnlyKeyValueEntry entry) in result.Items)
                observations.Add(new KeyValueTransactionReadKey
                {
                    Key = itemKey,
                    Durability = durability,
                    Exists = entry.State == KeyValueState.Set,
                    Revision = entry.Revision
                });
        }

        await LocateAndCompleteOperation(
            coordinatorKey, transactionId, operationId,
            new OperationCompletionPayload
            {
                ReadObservations = observations,
                Durability = durability,
                CachedType = result.Type
            },
            cancellationToken);

        return result;
    }

    internal Task<KeyValueGetByBucketResult> LocateAndGetByBucketWithHooks(
        HLCTimestamp transactionId, string prefixedKey, KeyValueDurability durability,
        Func<int, Task>? beforeQuery, Func<int, Task>? afterDescriptor,
        CancellationToken cancellationToken) =>
        locator.LocateAndGetByBucket(transactionId, prefixedKey, HLCTimestamp.Zero, durability, beforeQuery, afterDescriptor, cancellationToken);

    public Task<KeyValueGetByRangeResult> LocateAndGetByRange(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        if (string.IsNullOrEmpty(coordinatorKey))
            return locator.LocateAndGetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken);

        // A single-shot registered scan folds observations exactly when it is a latest read.
        return RegisterAndGetByRange(transactionId, coordinatorKey, operationId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, readTimestamp.IsNull(), cancellationToken);
    }

    /// <summary>
    /// Registers a paged range scan as a coordinator operation so every key it observes becomes a read
    /// dependency of the transaction: an optimistic commit validates them and aborts if any changed after the
    /// scan. Mirrors <see cref="RegisterAndGetByBucket"/>; still registers for finalize fencing and idempotent
    /// replay even when it folds nothing. Paging issues one registered operation per page (distinct bounds →
    /// distinct digest).
    ///
    /// <paramref name="recordObservations"/> is decoupled from <paramref name="readTimestamp"/>: a streaming
    /// latest scan latches a consistent snapshot on its first page and reads pages 1+ <em>as of</em> that pinned
    /// timestamp, but still folds every page's rows as read dependencies. A genuine snapshot scan (the caller
    /// pinned the read timestamp) owns no live transactional MVCC and folds nothing.
    /// </summary>
    private async Task<KeyValueGetByRangeResult> RegisterAndGetByRange(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string prefix,
        string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit,
        HLCTimestamp readTimestamp, KeyValueDurability durability, bool recordObservations, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, _, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.Scan,
                OperationDigest.ForRangeScan(prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability), cancellationToken);

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
            case OperationRegistrationOutcome.AlreadyPending:
            case OperationRegistrationOutcome.RejectedCapacity:
                // A pending or already-completed scan re-executes the read without re-folding observations:
                // the first-recorded observations are authoritative for commit validation even if this
                // re-execution returns a newer value.
                if (outcome == OperationRegistrationOutcome.AlreadyCompleted)
                    return await locator.LocateAndGetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken);
                return new KeyValueGetByRangeResult(KeyValueResponseType.MustRetry, [], null, false);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return new KeyValueGetByRangeResult(KeyValueResponseType.Aborted, [], null, false);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return new KeyValueGetByRangeResult(KeyValueResponseType.Aborted, [], null, false);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return new KeyValueGetByRangeResult(KeyValueResponseType.Errored, [], null, false);
        }

        KeyValueGetByRangeResult result =
            await locator.LocateAndGetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken);

        List<KeyValueTransactionReadKey>? observations = null;
        if (recordObservations && result.Type == KeyValueResponseType.Get && result.Items.Count > 0)
        {
            observations = new(result.Items.Count);
            foreach ((string itemKey, ReadOnlyKeyValueEntry entry) in result.Items)
                observations.Add(new KeyValueTransactionReadKey
                {
                    Key = itemKey,
                    Durability = durability,
                    Exists = entry.State == KeyValueState.Set,
                    Revision = entry.Revision
                });
        }

        await LocateAndCompleteOperation(
            coordinatorKey, transactionId, operationId,
            new OperationCompletionPayload
            {
                ReadObservations = observations,
                Durability = durability,
                CachedType = result.Type
            },
            cancellationToken);

        return result;
    }

    /// <summary>
    /// Streams all key-value entries under <paramref name="prefix"/> as an <see cref="IAsyncEnumerable{T}"/>.
    /// Pages are fetched via <see cref="LocateAndGetByRange"/>; the snapshot timestamp captured on page 0
    /// is carried in every cursor and reused on each subsequent page for consistent reads.
    /// Transient <see cref="KeyValueResponseType.MustRetry"/> / <see cref="KeyValueResponseType.WaitingForReplication"/>
    /// responses cause the current page to be retried from the same cursor with exponential back-off.
    /// </summary>
    public async IAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> LocateAndScanRange(
        HLCTimestamp txId,
        string prefix,
        string? startKey,
        bool startInclusive,
        string? endKey,
        bool endInclusive,
        int pageSize,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        [EnumeratorCancellation] CancellationToken ct,
        string coordinatorKey = "",
        TransactionOperationId operationId = default)
    {
        string? cursorKey       = startKey;
        bool    cursorInclusive = startInclusive;
        // Seed from caller's T when supplied; Zero means "capture on first successful page".
        HLCTimestamp snapshotTs = readTimestamp;
        int backoffMs           = 1;
        // Each streamed page registers as its own coordinator operation (distinct bounds → distinct digest),
        // so it needs a distinct, deterministic operationId derived from the caller's base id and the page
        // number. An empty coordinatorKey means "legacy raw paging" and never registers.
        bool registered         = !string.IsNullOrEmpty(coordinatorKey);
        // Fold every page's rows as read dependencies exactly when the caller asked for a latest scan.
        // A latest scan latches a snapshot on page 0 for a consistent view, so pages 1+ read as of that
        // pinned timestamp — but they are still latest-scan observations and must fold. A genuine snapshot
        // scan (caller pinned readTimestamp) folds nothing.
        bool foldObservations   = readTimestamp.IsNull();
        int pageIndex           = 0;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            KeyValueGetByRangeResult page = registered
                ? await RegisterAndGetByRange(
                    txId, coordinatorKey, operationId.Derive(pageIndex), prefix,
                    cursorKey, cursorInclusive,
                    endKey, endInclusive,
                    pageSize, snapshotTs, durability, foldObservations, ct)
                : await locator.LocateAndGetByRange(
                    txId, prefix,
                    cursorKey, cursorInclusive,
                    endKey, endInclusive,
                    pageSize, snapshotTs, durability, ct);

            if (page.Type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
            {
                // On a transient failure at page 0, snapshotTs is still Zero, so the handler
                // will capture a fresh HLC tick on the next attempt.  This means the snapshot
                // instant is "first successful page 0" rather than "scan start."  That is
                // acceptable: MustRetry/WaitingForReplication means the leader wasn't ready yet,
                // so there is no meaningful earlier snapshot to preserve.  Pages 1+ always carry
                // the snapshotTs latched from the first successful page-0 cursor.
                Transactions.DurableTransactionMetrics.AddKvRetryWait("LocateAndScanRange_3201");
                await Task.Delay(backoffMs, ct);
                backoffMs = Math.Min(backoffMs * 2, 1000);
                continue;
            }

            backoffMs = 1;

            if (page.Type != KeyValueResponseType.Get)
                yield break;

            foreach ((string key, ReadOnlyKeyValueEntry entry) in page.Items)
                yield return (key, entry);

            if (!page.HasMore || page.NextCursor is null)
                yield break;

            // Decode cursor: advance past last key and latch the snapshot timestamp.
            if (!KeyValueRangeCursor.TryDecode(page.NextCursor, out string lastKey, out _, out _, out HLCTimestamp cursorTs))
                yield break;

            // If the caller supplied a readTimestamp it's already non-Null, so this
            // no-ops and preserves the caller's T across all pages.
            if (snapshotTs.IsNull())
                snapshotTs = cursorTs;

            cursorKey       = lastKey;
            cursorInclusive = false;
            pageIndex++;
        }
    }
}
