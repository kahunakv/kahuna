using System.Runtime.CompilerServices;

using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Logging;
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
///
/// A registered scan completes under the same contract as a lock or a write: a completion that does not land
/// answers <c>MustRetry</c> instead of the page, keeps the page's observations in the participant retry cache,
/// and a same-id retry recovers it — never a page handed back over a record left pending, which would stall
/// every finalize of the transaction on the drain.
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

    // How long one scan page may keep answering MustRetry/WaitingForReplication before the scan fails
    // loudly instead of retrying in silence. Finite so an unresolvable page — an orphaned foreign write
    // intent with no commit timestamp — cannot hang aggregate reads and reconciliation forever, and low
    // enough to fire BELOW the smallest client command deadline in front of the scan (10 s in the shipped
    // client stack): a budget above that deadline raises the named error into a call the client has
    // already cancelled, so nobody ever sees it. Genuine settlement lag resolves in milliseconds, and the
    // budget resets on every page that makes progress, so exhausting it means the page cannot serve; the
    // thrown error is retryable, so a false positive under extreme lag costs one retried scan.
    // Test-overridable so the failure path is testable without a multi-second wait.
    private const int DefaultScanPageRetryBudgetMs = 5_000;

    private int ScanPageRetryBudgetMs =>
        TestScanPageRetryBudgetMs > 0 ? TestScanPageRetryBudgetMs
        : runtime.Configuration.ScanPageRetryBudgetMs > 0 ? runtime.Configuration.ScanPageRetryBudgetMs
        : DefaultScanPageRetryBudgetMs;

    /// <summary>Overrides the per-page scan retry budget for tests. Zero or negative restores the default.</summary>
    internal int TestScanPageRetryBudgetMs { private get; set; }

    /// <summary>
    /// Intercepts every fetched scan page for tests, keyed by page index; the returned page replaces the
    /// fetched one. This is the seam that lets a test drive the scan loop through a failed page — a state
    /// a healthy embedded node cannot be asked to produce on demand. Null (the default) is a no-op.
    /// </summary>
    internal Func<int, KeyValueGetByRangeResult, KeyValueGetByRangeResult>? TestScanPageInterceptor { private get; set; }

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
    
    /// <summary>Whether the hash-routed key is led by this node, with leadership confirmed.</summary>
    public Task<bool> IsLocallyLedHashKey(string key, CancellationToken cancellationToken) =>
        locator.IsLocallyLedHashKey(key, cancellationToken);

    /// <summary>
    /// Finalizes one ephemeral mutation in a single actor turn when the key is led by this node; null otherwise.
    /// </summary>
    public Task<(KeyValueResponseType, Handlers.KeyValueFinalizeStage)?> TryFinalizeMutationIfLocal(
        HLCTimestamp transactionId, HLCTimestamp commitId, string key, KeyValueDurability durability,
        string? recordAnchorKey, CancellationToken cancellationToken)
    {
        return locator.TryFinalizeMutationIfLocal(transactionId, commitId, key, durability, recordAnchorKey, cancellationToken);
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
    /// no other key matches the bucket predicate. A completion that does not land answers <c>MustRetry</c>,
    /// and the same-id retry recovers it before re-scanning.
    /// </summary>
    private async Task<KeyValueGetByBucketResult> RegisterAndGetByBucket(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string prefixedKey,
        HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        (OperationRegistrationOutcome outcome, _, _, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.Scan,
                OperationDigest.ForScan(prefixedKey, readTimestamp, durability), cancellationToken);

        // Pending means an earlier attempt scanned but its completion did not land: re-drive it from the retry
        // cache, then re-scan below as under an already-completed id.
        if (outcome == OperationRegistrationOutcome.AlreadyPending)
        {
            if (!await TryRecoverRegisteredRead(coordinatorKey, transactionId, operationId))
                return new KeyValueGetByBucketResult(KeyValueResponseType.MustRetry, []);

            outcome = OperationRegistrationOutcome.AlreadyCompleted;
        }

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                // An already-completed scan re-executes the read without re-folding observations: the
                // first-recorded observations are authoritative for commit validation even if this
                // re-execution returns a newer value.
                return await locator.LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, cancellationToken);
            case OperationRegistrationOutcome.RejectedCapacity:
                return new KeyValueGetByBucketResult(KeyValueResponseType.MustRetry, []);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return new KeyValueGetByBucketResult(KeyValueResponseType.Aborted, []);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return new KeyValueGetByBucketResult(KeyValueResponseType.Aborted, []);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return new KeyValueGetByBucketResult(KeyValueResponseType.Errored, []);
        }

        KeyValueGetByBucketResult result;

        // This frame is the registration's only completer, so a throw from the work must release it.
        try
        {
            result = await locator.LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, cancellationToken);
        }
        catch
        {
            await AbandonRegisteredOperation(coordinatorKey, transactionId, operationId);
            throw;
        }

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

        OperationCompletionPayload payload = OperationCompletionPayloadPool.Rent();
        payload.ReadObservations = observations;
        payload.Durability = durability;
        payload.CachedType = result.Type;

        // Owns the shell from here: recycled on an acknowledged fold, retained in the retry cache otherwise.
        // A completion that did not land must not hand the items back over a still-pending record.
        if (!await CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, result.Type, payload))
            return new KeyValueGetByBucketResult(KeyValueResponseType.MustRetry, []);

        return result;
    }

    internal Task<KeyValueGetByBucketResult> LocateAndGetByBucketWithHooks(
        HLCTimestamp transactionId, string prefixedKey, KeyValueDurability durability,
        Func<int, Task>? beforeQuery, Func<int, Task>? afterDescriptor,
        CancellationToken cancellationToken) =>
        locator.LocateAndGetByBucket(transactionId, prefixedKey, HLCTimestamp.Zero, durability, beforeQuery, afterDescriptor, cancellationToken);

    public Task<KeyValueGetByRangeResult> LocateAndGetByRange(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default, bool snapshotAtLeader = false, KeyValueReadMode readMode = KeyValueReadMode.Default)
    {
        // A statement read without a timestamp has no snapshot to read at; refused before it registers or routes.
        // A leader-minted snapshot supplies the timestamp on the serving node, so it is well formed here.
        if (!snapshotAtLeader && !KeyValueReadModeChecks.IsWellFormed(readMode, readTimestamp))
            return Task.FromResult(new KeyValueGetByRangeResult(KeyValueResponseType.InvalidInput, [], null, false));

        if (string.IsNullOrEmpty(coordinatorKey))
            return locator.LocateAndGetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken, snapshotAtLeader, readMode);

        // A single-shot registered scan folds observations when it is a latest read or a statement read: both are
        // the transaction's own reads, and the rows they serve are what commit validation compares against.
        return RegisterAndGetByRange(transactionId, coordinatorKey, operationId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability,
            RoutedReadOperations.RecordsReadObservation(readTimestamp, readMode), cancellationToken, snapshotAtLeader, readMode);
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
    /// pinned the read timestamp) owns no live transactional MVCC and folds nothing. A completion that does not
    /// land answers <c>MustRetry</c>, and the same-id retry recovers it before re-scanning.
    /// </summary>
    private async Task<KeyValueGetByRangeResult> RegisterAndGetByRange(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string prefix,
        string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit,
        HLCTimestamp readTimestamp, KeyValueDurability durability, bool recordObservations, CancellationToken cancellationToken,
        bool snapshotAtLeader = false, KeyValueReadMode readMode = KeyValueReadMode.Default)
    {
        (OperationRegistrationOutcome outcome, _, _, _, _, _) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.Scan,
                OperationDigest.ForRangeScan(prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, readMode), cancellationToken);

        // Pending means an earlier attempt scanned but its completion did not land: re-drive it from the retry
        // cache, then re-scan below as under an already-completed id.
        if (outcome == OperationRegistrationOutcome.AlreadyPending)
        {
            if (!await TryRecoverRegisteredRead(coordinatorKey, transactionId, operationId))
                return new KeyValueGetByRangeResult(KeyValueResponseType.MustRetry, [], null, false);

            outcome = OperationRegistrationOutcome.AlreadyCompleted;
        }

        switch (outcome)
        {
            case OperationRegistrationOutcome.AlreadyCompleted:
                // An already-completed scan re-executes the read without re-folding observations: the
                // first-recorded observations are authoritative for commit validation even if this
                // re-execution returns a newer value.
                return await locator.LocateAndGetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken, snapshotAtLeader, readMode);
            case OperationRegistrationOutcome.RejectedCapacity:
                return new KeyValueGetByRangeResult(KeyValueResponseType.MustRetry, [], null, false);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return new KeyValueGetByRangeResult(KeyValueResponseType.Aborted, [], null, false);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return new KeyValueGetByRangeResult(KeyValueResponseType.Aborted, [], null, false);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return new KeyValueGetByRangeResult(KeyValueResponseType.Errored, [], null, false);
        }

        KeyValueGetByRangeResult result;

        // This frame is the registration's only completer, so a throw from the work must release it.
        try
        {
            result = await locator.LocateAndGetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken, snapshotAtLeader, readMode);
        }
        catch
        {
            await AbandonRegisteredOperation(coordinatorKey, transactionId, operationId);
            throw;
        }

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

        OperationCompletionPayload payload = OperationCompletionPayloadPool.Rent();
        payload.ReadObservations = observations;
        payload.Durability = durability;
        payload.CachedType = result.Type;

        // Owns the shell from here: recycled on an acknowledged fold, retained in the retry cache otherwise.
        // A completion that did not land must not hand the page back over a still-pending record. The refusal
        // keeps what the scan cost, so the retried page reports the sum over its attempts.
        if (!await CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, result.Type, payload))
            return new KeyValueGetByRangeResult(KeyValueResponseType.MustRetry, [], null, false, result.Observation);

        return result;
    }

    /// <summary>
    /// Locates and runs a batched range probe: many bounded seeks under one prefix, each answered as a single range
    /// page over its bounds and limit is, and with <see cref="KeyValueProbeLock.Shared"/> each under a Shared range
    /// lock over its bounds taken in the same actor turn as its read (see
    /// <see cref="Handlers.TryProbeManyRangesHandler"/>). Returns one result per seek, in input order. A
    /// transaction-scoped probe registers as one coordinator operation; see <see cref="RegisterAndTryProbeManyRanges"/>.
    /// </summary>
    public Task<KeyValueProbeRangeResult[]> LocateAndTryProbeManyRanges(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        string prefix,
        IReadOnlyList<KeyValueProbeRange> seeks,
        KeyValueProbeLock lockMode,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default,
        KeyValueReadMode readMode = KeyValueReadMode.Default
    )
    {
        if (!KeyValueReadModeChecks.IsWellFormed(readMode, readTimestamp))
            return Task.FromResult(BuildProbeRangesRejection(seeks.Count, KeyValueResponseType.InvalidInput));

        RegistrationRouting routing = ClassifyRegistration(transactionId, coordinatorKey, operationId);
        if (routing is RegistrationRouting.Legacy)
            return locator.LocateAndTryProbeManyRanges(transactionId, readTimestamp, prefix, seeks, lockMode, expiresMs, durability, cancellationToken, readMode);

        // The re-entry-only mode is the replay a registered probe derives for itself; a caller never asks for it.
        if (routing is RegistrationRouting.Malformed || lockMode == KeyValueProbeLock.SharedReentryOnly)
            return Task.FromResult(BuildProbeRangesRejection(seeks.Count, KeyValueResponseType.InvalidInput));

        return RegisterAndTryProbeManyRanges(transactionId, coordinatorKey, operationId, readTimestamp, prefix, seeks, lockMode, expiresMs, durability, readMode, cancellationToken);
    }

    private static KeyValueProbeRangeResult[] BuildProbeRangesRejection(int count, KeyValueResponseType type)
    {
        KeyValueProbeRangeResult[] rejected = new KeyValueProbeRangeResult[count];
        for (int i = 0; i < count; i++)
            rejected[i] = KeyValueProbeRangeResult.MustRetry with { Type = type };
        return rejected;
    }

    /// <summary>
    /// Registers a batched range probe as one coordinator operation. On completion it folds every entry a latest seek
    /// returned into the read set, exactly as a registered range page does, and every Shared range lock it took into
    /// the held range-lock set with the leaderships they were granted under, so commit validates the reads and
    /// finalize releases the locks.
    ///
    /// <para>A same-id replay re-reads without folding again, as every read replay does. A replay that locks never
    /// takes a lock: the completed operation recorded the locks it took, so the replay only renews the ones this
    /// transaction still holds over the same bounds and reads those seeks, and answers <c>MustRetry</c> for the others
    /// (see <see cref="KeyValueProbeLock.SharedReentryOnly"/>).</para>
    ///
    /// <para>A completion that does not land answers <c>MustRetry</c> for every seek, keeping each seek's lock flag so
    /// the caller can still release what it holds, and the same-id retry recovers the completion before it re-reads.
    /// A latest probe that served no seek and took no lock cancels the registration, so a same-id retry registers
    /// anew.</para>
    /// </summary>
    private async Task<KeyValueProbeRangeResult[]> RegisterAndTryProbeManyRanges(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId,
        HLCTimestamp readTimestamp, string prefix, IReadOnlyList<KeyValueProbeRange> seeks, KeyValueProbeLock lockMode,
        int expiresMs, KeyValueDurability durability, KeyValueReadMode readMode, CancellationToken cancellationToken)
    {
        byte[] digest = OperationDigest.ForProbeManyRanges(prefix, seeks, readTimestamp, lockMode, expiresMs, durability, readMode);

        (OperationRegistrationOutcome outcome, _, _, _, _, TransactionConflictPolicy sessionPolicy) =
            await LocateAndBeginOperation(coordinatorKey, transactionId, operationId, OperationKind.ProbeManyRanges, digest, cancellationToken);

        if (outcome == OperationRegistrationOutcome.AlreadyPending)
        {
            if (!await TryRecoverRegisteredRead(coordinatorKey, transactionId, operationId))
                return BuildProbeRangesRejection(seeks.Count, KeyValueResponseType.MustRetry);

            outcome = OperationRegistrationOutcome.AlreadyCompleted;
        }

        switch (outcome)
        {
            case OperationRegistrationOutcome.RejectedCapacity:
                return BuildProbeRangesRejection(seeks.Count, KeyValueResponseType.MustRetry);
            case OperationRegistrationOutcome.RejectedSessionBudget:
                return BuildProbeRangesRejection(seeks.Count, KeyValueResponseType.Aborted);
            case OperationRegistrationOutcome.RejectedSessionClosed:
                return BuildProbeRangesRejection(seeks.Count, KeyValueResponseType.Aborted);
            case OperationRegistrationOutcome.RejectedDuplicate:
                return BuildProbeRangesRejection(seeks.Count, KeyValueResponseType.Errored);
        }

        bool locking = lockMode == KeyValueProbeLock.Shared;

        // A yielding transaction may not hold a range lock: the rule a single range-lock acquire applies.
        if (locking && outcome == OperationRegistrationOutcome.New && sessionPolicy == TransactionConflictPolicy.Yield)
        {
            OperationCompletionPayload rejectPayload = OperationCompletionPayloadPool.Rent();
            rejectPayload.CachedType = KeyValueResponseType.Errored;
            await CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, KeyValueResponseType.Errored, rejectPayload);
            logger.LogYieldingPredicateLockRejected("range", prefix, transactionId);
            return BuildProbeRangesRejection(seeks.Count, KeyValueResponseType.Errored);
        }

        KeyValueProbeLock effectiveLock = locking && outcome != OperationRegistrationOutcome.New
            ? KeyValueProbeLock.SharedReentryOnly
            : lockMode;

        KeyValueProbeRangeResult[] results;
        LockGrantCapture grants;

        // This frame is the registration's only completer, so a throw from the work must release it.
        try
        {
            using (LockGrantScope.Begin(out grants))
                results = await locator.LocateAndTryProbeManyRanges(transactionId, readTimestamp, prefix, seeks, effectiveLock, expiresMs, durability, cancellationToken, readMode);
        }
        catch
        {
            if (outcome == OperationRegistrationOutcome.New)
                await AbandonRegisteredOperation(coordinatorKey, transactionId, operationId);
            throw;
        }

        // A replay re-executes without re-folding: the first-recorded observations and locks are authoritative.
        if (outcome != OperationRegistrationOutcome.New)
            return results;

        List<KeyValueTransactionReadKey>? observations = null;
        List<(RangeLockKey Range, RangeLockMode Mode)>? acquired = null;
        bool served = false;
        bool foldsReads = RoutedReadOperations.RecordsReadObservation(readTimestamp, readMode);

        for (int i = 0; i < results.Length; i++)
        {
            KeyValueProbeRangeResult result = results[i];

            if (result.Locked)
            {
                KeyValueProbeRange seek = seeks[i];
                (acquired ??= []).Add((new RangeLockKey(prefix, seek.StartKey, seek.StartInclusive, seek.EndKey, seek.EndInclusive, durability), RangeLockMode.Shared));
            }

            if (result.Type != KeyValueResponseType.Get)
                continue;

            served = true;

            // A plain as-of probe owns no live transactional MVCC, so its entries contribute no read dependencies;
            // a latest probe and a statement probe fold theirs (see RoutedReadOperations.RecordsReadObservation).
            if (!foldsReads)
                continue;

            foreach ((string itemKey, ReadOnlyKeyValueEntry entry) in result.Items)
            {
                (observations ??= new(result.Items.Count)).Add(new KeyValueTransactionReadKey
                {
                    Key = itemKey,
                    Durability = durability,
                    Exists = entry.State == KeyValueState.Set,
                    Revision = entry.Revision
                });
            }
        }

        // A folding probe that served no seek and took no lock has no effect: cancel the registration so a same-id
        // retry registers as new and can still fold. A lock is an effect even when every read under it waited, so it
        // always completes.
        KeyValueResponseType cachedType = foldsReads && !served && acquired is null
            ? KeyValueResponseType.MustRetry
            : KeyValueResponseType.Get;

        OperationCompletionPayload payload = OperationCompletionPayloadPool.Rent();
        payload.ReadObservations = observations;
        payload.AcquiredRangeLocks = acquired;
        payload.LockExpiresMs = acquired is null ? 0 : expiresMs;
        payload.LockGrantTerms = acquired is null ? null : grants.Take();
        payload.Durability = durability;
        payload.CachedType = cachedType;

        if (!await CompleteRegisteredOperation(coordinatorKey, transactionId, operationId, cachedType, payload))
        {
            for (int i = 0; i < results.Length; i++)
                results[i] = KeyValueProbeRangeResult.MustRetry with { Locked = results[i].Locked };
        }

        return results;
    }

    /// <summary>
    /// Streams all key-value entries under <paramref name="prefix"/> as an <see cref="IAsyncEnumerable{T}"/>.
    /// Pages are fetched via <see cref="LocateAndGetByRange"/>; the snapshot timestamp captured on page 0
    /// is carried in every cursor and reused on each subsequent page for consistent reads.
    /// Transient <see cref="KeyValueResponseType.MustRetry"/> / <see cref="KeyValueResponseType.WaitingForReplication"/>
    /// responses cause the current page to be retried from the same cursor with the short poll of
    /// <see cref="ReadWaitBackoff"/>, which never sleeps past the page retry budget.
    /// Any other non-<see cref="KeyValueResponseType.Get"/> page fails the scan with a
    /// <see cref="KahunaServerException"/> carrying the response type: the stream never ends early on a
    /// failure, so an exhausted enumeration always means the range was scanned completely.
    /// <para>
    /// <paramref name="observations"/>, when given, receives each served page's observation summed over every
    /// attempt of that page, with the retries this loop took for it. Observations never change paging,
    /// retries or results.
    /// </para>
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
        TransactionOperationId operationId = default,
        KeyValueScanObservationSink? observations = null,
        KeyValueReadMode readMode = KeyValueReadMode.Default)
    {
        // A statement scan without a timestamp has no snapshot to read at: the first page would latch a fresh
        // snapshot of its own and the scan would silently become a pinning latest scan. Refused up front.
        if (!KeyValueReadModeChecks.IsWellFormed(readMode, readTimestamp))
            throw new KahunaServerException(
                $"Range scan over '{prefix}' asked for a statement read without a read timestamp.",
                KeyValueResponseType.InvalidInput);

        string? cursorKey       = startKey;
        bool    cursorInclusive = startInclusive;
        // Seed from caller's T when supplied; Zero means "capture on first successful page".
        HLCTimestamp snapshotTs = readTimestamp;
        int backoffMs           = 1;
        // Consecutive-transient budget for one page. Without it a page that never stops answering
        // MustRetry/WaitingForReplication — an orphaned foreign write intent with no commit timestamp
        // is one durable producer of that state — spins this loop forever, and the caller observes a
        // scan that simply never returns: reconciliation and aggregate reads then hang in silence.
        // The budget turns that hang into a loud, retryable failure. It resets on every page that
        // makes progress, so a long scan over a healthy range never trips it.
        long pageRetryDeadline  = Environment.TickCount64 + ScanPageRetryBudgetMs;
        // Each streamed page registers as its own coordinator operation (distinct bounds → distinct digest),
        // so it needs a distinct, deterministic operationId derived from the caller's base id and the page
        // number. An empty coordinatorKey means "legacy raw paging" and never registers.
        bool registered         = !string.IsNullOrEmpty(coordinatorKey);
        // Fold every page's rows as read dependencies when the caller asked for a latest scan or a statement
        // scan. A latest scan latches a snapshot on page 0 for a consistent view, so pages 1+ read as of that
        // pinned timestamp — but they are still latest-scan observations and must fold. A statement scan reads
        // every page at the statement's timestamp and folds the rows it served. A plain as-of scan (caller
        // pinned readTimestamp) folds nothing.
        bool foldObservations   = RoutedReadOperations.RecordsReadObservation(readTimestamp, readMode);
        int pageIndex           = 0;
        // What the current page's refused attempts cost, and how many this loop retried. Reset with the
        // back-off when a page is served.
        KeyValueScanObservation retriedAttempts = default;
        long pageRetries        = 0;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            KeyValueGetByRangeResult page = registered
                ? await RegisterAndGetByRange(
                    txId, coordinatorKey, operationId.Derive(pageIndex), prefix,
                    cursorKey, cursorInclusive,
                    endKey, endInclusive,
                    pageSize, snapshotTs, durability, foldObservations, ct, readMode: readMode)
                : await locator.LocateAndGetByRange(
                    txId, prefix,
                    cursorKey, cursorInclusive,
                    endKey, endInclusive,
                    pageSize, snapshotTs, durability, ct, readMode: readMode);

            if (TestScanPageInterceptor is not null)
                page = TestScanPageInterceptor(pageIndex, page);

            if (page.Type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
            {
                // On a transient failure at page 0, snapshotTs is still Zero, so the handler
                // will capture a fresh HLC tick on the next attempt.  This means the snapshot
                // instant is "first successful page 0" rather than "scan start."  That is
                // acceptable: MustRetry/WaitingForReplication means the leader wasn't ready yet,
                // so there is no meaningful earlier snapshot to preserve.  Pages 1+ always carry
                // the snapshotTs latched from the first successful page-0 cursor.
                if (Environment.TickCount64 >= pageRetryDeadline)
                {
                    // The page has answered transient for the whole budget: this is no longer
                    // settlement lag but a page that cannot serve — most often a key inside it
                    // holding a foreign write intent whose commit timestamp never resolves.
                    // Failing loudly is mandatory here: a silent break would hand the caller a
                    // TRUNCATED result as if the range ended, and a silent continue is the
                    // permanent invisible hang this budget exists to end.
                    Transactions.DurableTransactionMetrics.ScanPageRetryBudgetExhausted.Add(1);
                    logger.LogError(
                        "Range scan page over {Prefix} [{Cursor},{EndKey}) still answers {Type} after {BudgetMs} ms of retries; failing the scan",
                        prefix, cursorKey ?? "-inf", endKey ?? "+inf", page.Type, ScanPageRetryBudgetMs);
                    throw new KahunaServerException(
                        $"Range scan page over '{prefix}' did not settle within {ScanPageRetryBudgetMs} ms " +
                        $"(last response: {page.Type}); a key in the page may hold an unresolved write intent. Retry the scan.",
                        page.Type);
                }

                if (observations is not null)
                {
                    if (page.Observation is { } refused)
                        retriedAttempts += refused;
                    pageRetries++;
                }

                Transactions.DurableTransactionMetrics.AddKvRetryWait("LocateAndScanRange_3201");
                await Task.Delay(ReadWaitBackoff.Next(ref backoffMs, pageRetryDeadline), ct);
                continue;
            }

            backoffMs = 1;
            pageRetryDeadline = Environment.TickCount64 + ScanPageRetryBudgetMs;

            if (page.Type != KeyValueResponseType.Get)
            {
                // A non-Get page is a failed page, never an empty range: an empty range still answers
                // Get with zero items. Failing loudly is mandatory here for the same reason the retry
                // budget above throws — ending the stream would hand the caller a TRUNCATED result
                // indistinguishable from a completed scan, and every consumer that treats an empty
                // enumerable as "this range holds nothing" would act on it and report success.
                Transactions.DurableTransactionMetrics.ScanPageFailed.Add(1);
                logger.LogError(
                    "Range scan page {PageIndex} over {Prefix} [{Cursor},{EndKey}) failed with {Type}; failing the scan",
                    pageIndex, prefix, cursorKey ?? "-inf", endKey ?? "+inf", page.Type);
                throw new KahunaServerException(
                    $"Range scan page {pageIndex} over '{prefix}' [{cursorKey ?? "-inf"},{endKey ?? "+inf"}) " +
                    $"failed with {page.Type}; the scan was aborted instead of returning a truncated result.",
                    page.Type);
            }

            if (observations is not null)
            {
                // A served page without an observation stays unknown; the sink still counts its retries.
                observations.Add(page.Observation is { } served ? served + retriedAttempts : null, pageRetries);
                retriedAttempts = default;
                pageRetries = 0;
            }

            foreach ((string key, ReadOnlyKeyValueEntry entry) in page.Items)
                yield return (key, entry);

            if (!page.HasMore || page.NextCursor is null)
                yield break;

            // Decode cursor: advance past last key and latch the snapshot timestamp.
            if (!KeyValueRangeCursor.TryDecode(page.NextCursor, out string lastKey, out _, out _, out HLCTimestamp cursorTs))
            {
                // The page says more items remain but its continuation cursor cannot be decoded, so the
                // scan cannot advance. The cursor is produced by this server, so this is a broken
                // invariant, not caller input; ending the stream here would silently truncate the range.
                Transactions.DurableTransactionMetrics.ScanPageFailed.Add(1);
                logger.LogError(
                    "Range scan page {PageIndex} over {Prefix} [{Cursor},{EndKey}) returned an undecodable continuation cursor; failing the scan",
                    pageIndex, prefix, cursorKey ?? "-inf", endKey ?? "+inf");
                throw new KahunaServerException(
                    $"Range scan page {pageIndex} over '{prefix}' [{cursorKey ?? "-inf"},{endKey ?? "+inf"}) " +
                    "returned an undecodable continuation cursor with more items pending; the scan was aborted instead of returning a truncated result.");
            }

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
