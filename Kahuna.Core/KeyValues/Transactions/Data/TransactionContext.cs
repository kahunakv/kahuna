
using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>The committed value staged for one modified key on the durable-intent finalize path: the mutation's
/// value (null = delete tombstone), its revision, and its <b>relative</b> TTL in milliseconds (0 = no expiry). The
/// relative TTL is resolved to an absolute expiry HLC of <c>commitTimestamp + ExpiresMs</c> at freeze, so a TTL
/// write's expiry is anchored to the one canonical commit timestamp rather than an actor-local wall clock.</summary>
public readonly record struct StagedValue(byte[]? Value, long Revision, long ExpiresMs);

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
    /// Per-key staged committed value for the durable-intent finalize path, accumulated across every mutation
    /// command (unlike <see cref="ModifiedResult"/>, which only reflects the last command's result). A key present
    /// in <see cref="ModifiedKeys"/> but absent here has no losslessly-stageable value (e.g. an extend), so the
    /// transaction falls back to the ticket path. A null <see cref="StagedValue.Value"/> is a delete tombstone.
    /// </summary>
    public Dictionary<string, StagedValue>? StagedMutations { get; private set; }

    /// <summary>Records the staged committed value of one modified key for the durable-intent path. The expiry is
    /// the write's <b>relative</b> TTL in milliseconds (0 = none); it is resolved to an absolute HLC at freeze.</summary>
    public void StageMutation(string key, byte[]? value, long revision, long expiresMs)
    {
        lock (registryLock)
        {
            StagedMutations ??= [];
            StagedMutations[key] = new StagedValue(value, revision, expiresMs);
        }
    }

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

    /// <summary>
    /// Upper bound on total retained operation records per session (pending + completed). Completed records
    /// are never evicted — they remain until session teardown for duplicate-response replay — so this counter
    /// is incremented on every new registration and never decremented. A non-positive value disables the bound.
    /// Must be strictly greater than <see cref="MaxPendingOperations"/> so the two caps do not interfere.
    /// </summary>
    private const int MaxOperationsPerSession = 65536;

    private readonly object registryLock = new();
    private SessionLifecycle lifecycle = SessionLifecycle.AcceptingOperations;
    private bool renewalExcluded;
    private bool readObservationConflict;
    private int pendingOperationCount;
    private int retainedOperationCount;

    /// <summary>
    /// Overrides <see cref="MaxOperationsPerSession"/> for test scenarios. Zero or negative restores
    /// the production default. Only set from test assemblies via <c>InternalsVisibleTo</c>.
    /// </summary>
    internal int TestOperationBudgetOverride { private get; set; }
    private Dictionary<TransactionOperationId, OperationRecord>? operations;
    private WorkingSetSnapshot? finalizeSnapshot;
    private FinalizeAttempt? activeFinalize;

    /// <summary>Signal completed when the pending count reaches zero during a finalize drain.</summary>
    private TaskCompletionSource? pendingDrainSignal;

    /// <summary>The coarse lifecycle phase of this session.</summary>
    public SessionLifecycle Lifecycle
    {
        get { lock (registryLock) return lifecycle; }
    }

    /// <summary>
    /// True while at least one operation the coordinator dispatched has not yet reported completion. The
    /// reaper never cancels such an operation: it waits out the extended deadline and, if any remain, expires
    /// the session without reporting <c>RolledBack</c> so a late effect receives no success acknowledgement.
    /// </summary>
    internal bool HasPendingOperations
    {
        get { lock (registryLock) return pendingOperationCount > 0; }
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

            int effectiveBudget = TestOperationBudgetOverride > 0 ? TestOperationBudgetOverride : MaxOperationsPerSession;
            if (effectiveBudget > 0 && retainedOperationCount >= effectiveBudget)
                return new(OperationRegistrationOutcome.RejectedSessionBudget, recordAnchorKey: RecordAnchorKey);

            if (pendingOperationCount >= MaxPendingOperations)
                return new(OperationRegistrationOutcome.RejectedCapacity, recordAnchorKey: RecordAnchorKey);

            operations[operationId] = new() { Kind = kind, PayloadDigest = payloadDigest };
            pendingOperationCount++;
            retainedOperationCount++;
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

        // A write supersedes any earlier read observation of the same key: it is validated as a write, not as a
        // read dependency on external committed state. Dropping it also prevents a later read of the key's own
        // staged value from tripping the two-inconsistent-snapshots conflict against that write's revision.
        ReadKeys?.Remove(modified);

        if (RecordAnchorKey is null && modified.Durability == KeyValueDurability.Persistent)
            RecordAnchorKey = modified.Key;
    }

    /// <summary>
    /// Records a confirmed modified key from a path that does not flow through the operation registry — the script
    /// executor's direct mutation commands — assigning the record anchor on the first persistent key exactly as the
    /// registry fold does. Without this, a script transaction would never acquire an anchor and could not take the
    /// durable-intent finalize path. Takes <see cref="registryLock"/> to stay consistent with concurrent registry folds.
    /// </summary>
    public void RecordModifiedKey((string Key, KeyValueDurability Durability) modified)
    {
        lock (registryLock)
            FoldModifiedKeyLocked(modified);
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

        if (effect.AcquiredPointLocks is { } acquiredPointLocks)
        {
            LocksAcquired ??= [];
            foreach ((string, KeyValueDurability) batchLock in acquiredPointLocks)
                LocksAcquired.Add(batchLock);
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

        (string, KeyValueDurability) key = (observed.Key, observed.Durability);

        // A key this transaction has written is validated as a write, not a read: reading back its own
        // uncommitted value is not a dependency on external committed state, so it never enters the read set
        // (and must not trip the two-inconsistent-snapshots conflict against the staged write's revision).
        if (ModifiedKeys is not null && ModifiedKeys.Contains(key))
            return;

        ReadKeys ??= [];

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
    /// Enters the single finalize slot for a commit or rollback. Exactly one caller owns an active
    /// <see cref="FinalizeAttempt"/> at a time: the owner runs the finalize and publishes its outcome via
    /// <see cref="CompleteFinalize"/>; concurrent commits/rollbacks observe the same attempt and mirror its
    /// outcome. Installing the first attempt also closes the session to new operations
    /// (<see cref="SessionLifecycle.AcceptingOperations"/> → <see cref="SessionLifecycle.Finalizing"/>) so
    /// no operation can join once finalization has begun. A session already claimed by the reaper (or made
    /// terminal) is rejected — there is nothing left to finalize.
    /// </summary>
    internal FinalizeAdmission EnterFinalize(out FinalizeAttempt? attempt)
    {
        lock (registryLock)
        {
            if (lifecycle is SessionLifecycle.Reaping or SessionLifecycle.Terminal)
            {
                attempt = null;
                return FinalizeAdmission.Rejected;
            }

            if (activeFinalize is not null)
            {
                attempt = activeFinalize;
                return FinalizeAdmission.Mirror;
            }

            // Install a fresh attempt. The session may already be Finalizing — a prior attempt that timed
            // out during its drain cleared the slot but left the session closed to new operations; a new
            // owner rejoins that same drain rather than reopening the session.
            attempt = activeFinalize = new FinalizeAttempt();
            if (lifecycle == SessionLifecycle.AcceptingOperations)
                lifecycle = SessionLifecycle.Finalizing;
            return FinalizeAdmission.Owner;
        }
    }

    /// <summary>
    /// Atomically claims the finalize slot for the reaper. Succeeds only when <b>no</b> finalize is active —
    /// i.e. no commit/rollback (or in-flight reap) owns the slot — and the session is either still accepting
    /// operations or is a finalization that was <b>abandoned</b>: closed to new operations
    /// (<see cref="SessionLifecycle.Finalizing"/>) but with its slot released because the last commit,
    /// rollback, or Close returned a non-terminal <see cref="KeyValueResponseType.MustRetry"/> (or stored a
    /// Close snapshot) and the caller then disappeared. Claiming transitions the session to
    /// <see cref="SessionLifecycle.Reaping"/> and installs an attempt the reaper publishes so a commit/rollback
    /// racing the reaper mirrors the reap outcome. Returns null when a finalize already owns the slot (leave it
    /// to that finalize) or the session is already Reaping/Terminal.
    /// </summary>
    internal FinalizeAttempt? TryEnterReap()
    {
        lock (registryLock)
        {
            if (activeFinalize is not null)
                return null;

            if (lifecycle is not (SessionLifecycle.AcceptingOperations or SessionLifecycle.Finalizing))
                return null;

            lifecycle = SessionLifecycle.Reaping;
            return activeFinalize = new FinalizeAttempt();
        }
    }

    /// <summary>
    /// Re-arms the finalize slot for a session already claimed by the reaper whose prior cleanup could not
    /// fully release (it published a non-terminal <see cref="KeyValueResponseType.MustRetry"/>, freeing the
    /// slot but leaving the session <see cref="SessionLifecycle.Reaping"/>). A later reaper tick calls this
    /// to retry the release. Returns null when the session is not Reaping or the slot is still held (a reap
    /// is mid-flight), so a resume never races the in-flight attempt.
    /// </summary>
    internal FinalizeAttempt? TryResumeReap()
    {
        lock (registryLock)
        {
            if (lifecycle != SessionLifecycle.Reaping || activeFinalize is not null)
                return null;

            return activeFinalize = new FinalizeAttempt();
        }
    }

    /// <summary>
    /// Publishes the outcome of a finalize to every waiter mirroring the attempt. A terminal outcome is
    /// retained (the slot stays installed) so a later duplicate finalize mirrors the same answer; a
    /// non-terminal <see cref="KeyValueResponseType.MustRetry"/> releases the slot so a subsequent call can
    /// run a fresh attempt. Called exactly once by the owner, always (even on failure), so no mirroring
    /// waiter can hang.
    /// </summary>
    internal void CompleteFinalize(FinalizeAttempt attempt, FinalizeOutcome outcome)
    {
        lock (registryLock)
        {
            if (!outcome.IsTerminal && ReferenceEquals(activeFinalize, attempt))
                activeFinalize = null;
        }

        attempt.Publish(outcome);
    }

    /// <summary>
    /// Stores the immutable working-set snapshot captured by a <c>CloseTransaction</c> once the session is
    /// frozen (closed to new operations and drained). The session stays <see cref="SessionLifecycle.Finalizing"/>
    /// — <b>not</b> terminal — so a later commit or rollback can still finalize the frozen transaction and reuse
    /// this exact snapshot. Stored once; a repeat Close returns the same snapshot rather than re-capturing.
    /// </summary>
    internal void StoreCloseSnapshot(WorkingSetSnapshot snapshot)
    {
        lock (registryLock)
            finalizeSnapshot ??= snapshot;
    }

    /// <summary>The frozen Close snapshot, or null if the session has not been closed and snapshotted yet.</summary>
    internal WorkingSetSnapshot? CloseSnapshot
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

    /// <summary>
    /// Excludes this session from all future renewal snapshots. Called under <see cref="registryLock"/>
    /// at the top of <c>ReleaseWorkingSet</c> — the single hand-off point where cleanup takes ownership of
    /// the range locks. After this call <see cref="SnapshotRenewableRangeLocks"/> returns an empty list,
    /// so no renewal tick can re-extend a lock that cleanup is in the process of releasing.
    /// </summary>
    internal void MarkRenewalExcluded()
    {
        lock (registryLock)
            renewalExcluded = true;
    }

    /// <summary>
    /// Returns a point-in-time copy of the range locks this session holds for renewal, gated on the
    /// <see cref="renewalExcluded"/> flag rather than the session lifecycle. A <see cref="SessionLifecycle.Finalizing"/>
    /// session whose drain is still in progress keeps renewing so the lease never lapses while
    /// <c>WaitForPendingOperations</c> is blocking — renewal stops only when <see cref="MarkRenewalExcluded"/>
    /// is called at the top of <c>ReleaseWorkingSet</c>, ensuring cleanup owns the lock from that instant.
    /// The reap-deadline guard in the renewal sweep independently stops renewing sessions past their timeout,
    /// so a stuck transaction can never hold locks indefinitely.
    /// </summary>
    internal List<(RangeLockKey Range, RangeLockMode Mode)> SnapshotRenewableRangeLocks()
    {
        lock (registryLock)
        {
            if (renewalExcluded || RangeLocksAcquired is null || RangeLocksAcquired.Count == 0)
                return [];

            List<(RangeLockKey, RangeLockMode)> snapshot = new(RangeLocksAcquired.Count);
            foreach ((RangeLockKey range, RangeLockMode mode) in RangeLocksAcquired)
                snapshot.Add((range, mode));

            return snapshot;
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
