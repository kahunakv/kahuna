
using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Generic transaction context holding identity, policy, lifecycle state, and confirmed working-set
/// entries. Contains no parser, AST, variable, or script-execution references.
/// </summary>
internal class TransactionContext
{
    /// <summary>
    /// HLC timestamp that uniquely identifies this transaction.
    /// </summary>
    public HLCTimestamp TransactionId { get; init; }

    /// <summary>
    /// The routing key that pins this session to the partition whose leader owns the coordinator.
    /// Set once at StartTransaction and carried on every subsequent request.
    /// </summary>
    public string CoordinatorKey { get; init; } = string.Empty;

    /// <summary>
    /// Maximum duration in milliseconds before the transaction times out.
    /// </summary>
    public int Timeout { get; init; }

    /// <summary>
    /// Pessimistic or optimistic locking strategy for this transaction.
    /// </summary>
    public KeyValueTransactionLocking Locking { get; init; }

    /// <summary>
    /// Transaction-wide snapshot timestamp for reads. Zero means "latest".
    /// </summary>
    public HLCTimestamp ReadTimestamp { get; init; }

    /// <summary>
    /// Controls whether reads are tracked and validated for write-skew at commit time.
    /// </summary>
    public ReadValidation ReadValidation { get; init; }

    /// <summary>
    /// Controls how durable the coordinator decision record must be before the client receives the outcome.
    /// </summary>
    public DecisionDurability DecisionDurability { get; init; }

    /// <summary>
    /// Last result of the current key-value execution.
    /// </summary>
    public KeyValueTransactionResult? Result { get; set; }

    /// <summary>
    /// Last result of a key-value write operation.
    /// </summary>
    public KeyValueTransactionResult? ModifiedResult { get; set; }

    /// <summary>
    /// Whether the transaction should commit or abort.
    /// </summary>
    public KeyValueTransactionAction Action { get; set; }

    /// <summary>
    /// Whether transaction resources should be released asynchronously upon completion.
    /// </summary>
    public bool AsyncRelease { get; set; }

    /// <summary>
    /// Point locks acquired during execution.
    /// </summary>
    public HashSet<(string, KeyValueDurability)>? LocksAcquired { get; set; }

    /// <summary>
    /// Prefix locks acquired during execution.
    /// </summary>
    public HashSet<(string, KeyValueDurability)>? PrefixLocksAcquired { get; set; }

    /// <summary>
    /// Range locks held during execution, keyed by their logical bounds and valued by their current mode
    /// so an upgrade or renewal replaces the mode of the matching descriptor rather than adding a second.
    /// </summary>
    public Dictionary<RangeLockKey, RangeLockMode>? RangeLocksAcquired { get; set; }

    /// <summary>
    /// Keys modified during the transaction along with their durability.
    /// </summary>
    public HashSet<(string, KeyValueDurability)>? ModifiedKeys { get; set; }

    /// <summary>
    /// The immutable record anchor: the first confirmed persistent modified key. Assigned exactly once,
    /// at the coordinator, under <see cref="registryLock"/> the first time a persistent
    /// <c>ModifiedKey</c> effect is folded in; ephemeral-only modifications never assign it. Names the
    /// data partition that will own a Durable transaction record. Null until (and unless) a persistent
    /// write is confirmed — a transaction with no persistent modification has no anchor and cannot be
    /// promoted to Durable.
    /// </summary>
    public string? RecordAnchorKey { get; private set; }

    /// <summary>
    /// Keys read during the transaction and their observed revisions.
    /// </summary>
    public Dictionary<(string, KeyValueDurability), KeyValueTransactionReadKey>? ReadKeys { get; set; }

    /// <summary>
    /// Internal 2PC state field; advanced atomically via <see cref="SetState"/>.
    /// </summary>
    private KeyValueTransactionState state = KeyValueTransactionState.Pending;

    /// <summary>
    /// Current 2PC state of the transaction.
    /// </summary>
    public KeyValueTransactionState State => state;

    /// <summary>
    /// Atomically advances the transaction state from <paramref name="expectedState"/> to
    /// <paramref name="newState"/>. Returns true when the CAS succeeds.
    /// </summary>
    public bool SetState(KeyValueTransactionState newState, KeyValueTransactionState expectedState)
    {
        return expectedState == Interlocked.CompareExchange(ref state, newState, expectedState);
    }

    // ---- session lifecycle + operation registry ----
    //
    // The registry is reached concurrently: several transaction-scoped operations register and
    // complete in parallel while a finalize path may be closing the session. All lifecycle reads,
    // lifecycle transitions, the operation map, the pending count, and the drain signal are guarded
    // by a single monitor so that the "accepting?" check and the map insert are one atomic step —
    // otherwise an operation could slip in after finalize begins, or two operations could tear the
    // map. The 2PC <see cref="SetState"/> chain is independent and stays lock-free.

    /// <summary>Upper bound on operations that may be pending before new registrations are rejected.</summary>
    private const int MaxPendingOperations = 4096;

    private readonly object registryLock = new();
    private SessionLifecycle lifecycle = SessionLifecycle.AcceptingOperations;
    private bool readObservationConflict;
    private int pendingOperationCount;
    private Dictionary<TransactionOperationId, OperationRecord>? operations;
    private WorkingSetSnapshot? finalizeSnapshot;

    /// <summary>Signal completed when the pending count reaches zero during a finalize drain.</summary>
    private TaskCompletionSource? pendingDrainSignal;

    /// <summary>The coarse lifecycle phase of this session.</summary>
    public SessionLifecycle Lifecycle
    {
        get { lock (registryLock) return lifecycle; }
    }

    /// <summary>
    /// True once two operations recorded conflicting base observations (differing existence or revision) for
    /// the same key. The transaction read an unstable snapshot of that key, so it must fail read validation
    /// at commit rather than commit on a self-inconsistent read set.
    /// </summary>
    internal bool ReadObservationConflict
    {
        get { lock (registryLock) return readObservationConflict; }
    }

    /// <summary>
    /// Registers an operation under this session for idempotent tracking. Atomic against finalize:
    /// once the session leaves <see cref="SessionLifecycle.AcceptingOperations"/> no new operation
    /// can register. A repeat of a known ID must carry the identical declaration.
    /// </summary>
    internal OperationRegistrationResult BeginOperation(
        TransactionOperationId operationId,
        OperationKind kind,
        byte[]? payloadDigest)
    {
        lock (registryLock)
        {
            // Every outcome carries the transaction's current anchor so a retry recovers the same
            // canonical handle even when it lands on an already-completed operation.
            if (lifecycle != SessionLifecycle.AcceptingOperations)
                return new(OperationRegistrationOutcome.RejectedSessionClosed, recordAnchorKey: RecordAnchorKey);

            operations ??= new();

            if (operations.TryGetValue(operationId, out OperationRecord? existing))
            {
                // Reusing an ID with a different kind or payload is a caller error, not a retry.
                if (existing.Kind != kind || !DigestsEqual(existing.PayloadDigest, payloadDigest))
                    return new(OperationRegistrationOutcome.RejectedDuplicate, recordAnchorKey: RecordAnchorKey);

                return existing.Status switch
                {
                    OperationStatus.Completed => new(OperationRegistrationOutcome.AlreadyCompleted, existing.CachedResponse, RecordAnchorKey),
                    _                         => new(OperationRegistrationOutcome.AlreadyPending, recordAnchorKey: RecordAnchorKey)
                };
            }

            if (pendingOperationCount >= MaxPendingOperations)
                return new(OperationRegistrationOutcome.RejectedCapacity, recordAnchorKey: RecordAnchorKey);

            operations[operationId] = new() { Kind = kind, PayloadDigest = payloadDigest };
            pendingOperationCount++;
            return new(OperationRegistrationOutcome.New, recordAnchorKey: RecordAnchorKey);
        }
    }

    /// <summary>
    /// Marks the operation as completed, records its confirmed working-set effect, and stores the
    /// response for future duplicate requests. Idempotent: completing an already-terminal operation is
    /// a no-op, so a replayed completion never double-records an effect.
    /// </summary>
    /// <summary>Returns the transaction's record anchor after this effect is folded in (null if none yet).</summary>
    internal string? CompleteOperation(TransactionOperationId operationId, OperationEffect? effect, object? response)
    {
        lock (registryLock)
        {
            if (operations is null || !operations.TryGetValue(operationId, out OperationRecord? record))
                return RecordAnchorKey;

            if (record.Status != OperationStatus.Pending)
                return RecordAnchorKey;

            record.Status = OperationStatus.Completed;
            record.CachedResponse = response;
            ApplyEffectLocked(effect);
            DecrementPending();
            return RecordAnchorKey;
        }
    }

    /// <summary>
    /// Caller must hold <see cref="registryLock"/>. Records one confirmed modified key and, if it is the
    /// first persistent one, assigns the immutable record anchor. Because the lock serializes concurrent
    /// completions, exactly one modification wins the anchor. Ephemeral modifications never become the
    /// anchor: a Durable record cannot live on an ephemeral key.
    /// </summary>
    private void FoldModifiedKeyLocked((string Key, KeyValueDurability Durability) modified)
    {
        ModifiedKeys ??= [];
        ModifiedKeys.Add(modified);

        if (RecordAnchorKey is null && modified.Durability == KeyValueDurability.Persistent)
            RecordAnchorKey = modified.Key;
    }

    /// <summary>Caller must hold <see cref="registryLock"/>. Folds a confirmed effect into the working set.</summary>
    private void ApplyEffectLocked(OperationEffect? effect)
    {
        if (effect is null)
            return;

        if (effect.ModifiedKey is { } modified)
            FoldModifiedKeyLocked(modified);

        // A batch folds its confirmed keys in canonical request order, so the first persistent one wins the
        // anchor deterministically regardless of the order per-partition fan-out completed.
        if (effect.ModifiedKeys is { } modifiedKeys)
        {
            foreach ((string, KeyValueDurability) batchModified in modifiedKeys)
                FoldModifiedKeyLocked(batchModified);
        }

        if (effect.PointLock is { } pointLock)
        {
            LocksAcquired ??= [];
            LocksAcquired.Add(pointLock);
        }

        if (effect.RemovePointLock is { } removedLock)
            LocksAcquired?.Remove(removedLock);

        if (effect.PrefixLock is { } prefixLock)
        {
            PrefixLocksAcquired ??= [];
            PrefixLocksAcquired.Add(prefixLock);
        }

        if (effect.RemovePrefixLock is { } removedPrefixLock)
            PrefixLocksAcquired?.Remove(removedPrefixLock);

        if (effect.RangeLock is { } rangeLock)
        {
            RangeLocksAcquired ??= new();
            // Add on first acquire; replace the mode on a confirmed upgrade or renewal of the same bounds.
            RangeLocksAcquired[rangeLock.Range] = rangeLock.Mode;
        }

        if (effect.RemoveRangeLock is { } removedRangeLock)
            RangeLocksAcquired?.Remove(removedRangeLock);

        if (effect.ReadObservation is { } read)
            FoldReadObservationLocked(read);

        if (effect.ReadObservations is { } reads)
        {
            foreach (KeyValueTransactionReadKey observed in reads)
                FoldReadObservationLocked(observed);
        }
    }

    /// <summary>
    /// Caller must hold <see cref="registryLock"/>. Records the first base observation for a key and keeps
    /// it stable: a later observation for the same key with a different existence or base revision means the
    /// transaction saw two inconsistent snapshots of that key, which cannot both be valid — flag the read set
    /// as conflicted so commit-time validation aborts. The first observation is retained (never overwritten).
    /// </summary>
    private void FoldReadObservationLocked(KeyValueTransactionReadKey observed)
    {
        if (string.IsNullOrEmpty(observed.Key))
            return;

        ReadKeys ??= [];
        (string, KeyValueDurability) key = (observed.Key, observed.Durability);

        if (ReadKeys.TryGetValue(key, out KeyValueTransactionReadKey? existing))
        {
            if (existing.Exists != observed.Exists || existing.Revision != observed.Revision)
                readObservationConflict = true;

            return;
        }

        ReadKeys[key] = observed;
    }

    /// <summary>
    /// Cancels the operation if it is still pending. Returns true when the cancel succeeded.
    /// </summary>
    internal bool TryCancelOperation(TransactionOperationId operationId)
    {
        lock (registryLock)
        {
            if (operations is null || !operations.TryGetValue(operationId, out OperationRecord? record))
                return false;

            if (record.Status != OperationStatus.Pending)
                return false;

            // Remove the record entirely rather than leaving a terminal marker. BeginOperation maps every
            // non-completed record to AlreadyPending, so a lingering cancelled entry would wedge every
            // same-id retry on MustRetry forever. Cancellation only happens after a transient/no-effect
            // result — nothing was folded into the working set — so the id is safe to release for a fresh
            // registration that re-drives the operation.
            operations.Remove(operationId);
            DecrementPending();
            return true;
        }
    }

    /// <summary>Caller must hold <see cref="registryLock"/>. Drops the pending count and wakes a drain waiter.</summary>
    private void DecrementPending()
    {
        pendingOperationCount--;
        if (pendingOperationCount == 0)
            pendingDrainSignal?.TrySetResult();
    }

    // ---- finalize fence ----

    /// <summary>
    /// Atomically moves the session from <see cref="SessionLifecycle.AcceptingOperations"/> to
    /// <see cref="SessionLifecycle.Finalizing"/>. Returns false if another finalize already won or
    /// the session is terminal.
    /// </summary>
    internal bool TryBeginFinalizing()
    {
        lock (registryLock)
        {
            if (lifecycle != SessionLifecycle.AcceptingOperations)
                return false;

            lifecycle = SessionLifecycle.Finalizing;
            return true;
        }
    }

    /// <summary>
    /// Atomically closes the session to new operations as part of reaping, returning true only when the
    /// session was still <see cref="SessionLifecycle.AcceptingOperations"/>. A session already finalizing or
    /// terminal belongs to an in-flight commit/rollback and is left alone. Once this returns true, every
    /// subsequent <see cref="BeginOperation"/> is rejected — so a caller that captured this context just
    /// before the reaper removed it from the session map cannot register a new operation on a session that
    /// is about to vanish (which would apply a mutation with no coordinator record).
    /// </summary>
    internal bool TryCloseForReaping()
    {
        lock (registryLock)
        {
            if (lifecycle != SessionLifecycle.AcceptingOperations)
                return false;

            lifecycle = SessionLifecycle.Reaping;
            return true;
        }
    }

    /// <summary>
    /// Publishes the terminal working-set snapshot and moves the session to
    /// <see cref="SessionLifecycle.Terminal"/> as one atomic step, so a racing finalize either sees
    /// the published snapshot or a still-finalizing session — never a terminal session with no snapshot.
    /// </summary>
    internal void PublishTerminal(WorkingSetSnapshot snapshot)
    {
        lock (registryLock)
        {
            finalizeSnapshot = snapshot;
            lifecycle = SessionLifecycle.Terminal;
        }
    }

    /// <summary>The published finalize snapshot, or null if finalize has not completed yet.</summary>
    internal WorkingSetSnapshot? PublishedSnapshot
    {
        get { lock (registryLock) return finalizeSnapshot; }
    }

    /// <summary>
    /// Completes when every operation registered before the caller began finalizing has reached a
    /// terminal state. Resolves immediately when nothing is pending. Honors cancellation so a close
    /// deadline surfaces as a cancellation the caller can turn into a retry.
    /// </summary>
    internal Task WaitForPendingOperations(CancellationToken cancellationToken)
    {
        TaskCompletionSource signal;

        lock (registryLock)
        {
            if (pendingOperationCount == 0)
                return Task.CompletedTask;

            pendingDrainSignal ??= new(TaskCreationOptions.RunContinuationsAsynchronously);
            signal = pendingDrainSignal;
        }

        return signal.Task.WaitAsync(cancellationToken);
    }

    // ---- working set snapshot ----

    /// <summary>
    /// Captures an immutable snapshot of the current working set for use by the finalization path.
    /// The finalize fence guarantees no operation mutates the working set concurrently with this
    /// call, so the copies below are a consistent point-in-time view.
    /// </summary>
    internal WorkingSetSnapshot GetWorkingSetSnapshot()
    {
        lock (registryLock)
        {
            return new()
            {
                LocksAcquired        = LocksAcquired        != null ? new HashSet<(string, KeyValueDurability)>(LocksAcquired)        : null,
                PrefixLocksAcquired  = PrefixLocksAcquired  != null ? new HashSet<(string, KeyValueDurability)>(PrefixLocksAcquired)  : null,
                RangeLocksAcquired   = RangeLocksAcquired   != null ? new Dictionary<RangeLockKey, RangeLockMode>(RangeLocksAcquired) : null,
                ModifiedKeys         = ModifiedKeys          != null ? new HashSet<(string, KeyValueDurability)>(ModifiedKeys)         : null,
                ReadKeys             = ReadKeys              != null ? new Dictionary<(string, KeyValueDurability), KeyValueTransactionReadKey>(ReadKeys) : null,
                RecordAnchorKey      = RecordAnchorKey,
                PendingOperationCount = pendingOperationCount,
                Lifecycle            = lifecycle
            };
        }
    }

    private static bool DigestsEqual(byte[]? a, byte[]? b)
    {
        if (ReferenceEquals(a, b))
            return true;
        if (a is null || b is null)
            return false;
        return a.AsSpan().SequenceEqual(b);
    }
}
