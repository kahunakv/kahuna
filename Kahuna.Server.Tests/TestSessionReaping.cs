using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit coverage for the session-reaping fence on <see cref="TransactionContext"/>. The reaper claims the
/// single finalize slot atomically before it removes the session from the map, so a BeginOperation that
/// already captured the context reference is rejected instead of registering a new operation on a session
/// that is about to vanish. Claiming must not steal a session that is already finalizing or owned by an
/// in-flight commit/rollback.
/// </summary>
public sealed class TestSessionReaping
{
    private static TransactionContext NewSession() =>
        new() { CoordinatorKey = "c", TransactionId = new HLCTimestamp(1, 100, 0) };

    [Fact]
    public void Reap_ClaimsSlotAndRejectsSubsequentRegistrations()
    {
        TransactionContext ctx = NewSession();

        Assert.Equal(OperationRegistrationOutcome.New,
            ctx.BeginOperation(TransactionOperationId.NewRandom(), OperationKind.Set, [1]).Outcome);

        Assert.NotNull(ctx.TryEnterReap());
        Assert.Equal(SessionLifecycle.Reaping, ctx.Lifecycle);

        // The race the reaper closes: a BeginOperation that captured this context just before removal now
        // observes the closed lifecycle and is rejected, rather than registering a New op on a dead session.
        Assert.Equal(OperationRegistrationOutcome.RejectedSessionClosed,
            ctx.BeginOperation(TransactionOperationId.NewRandom(), OperationKind.Set, [2]).Outcome);
    }

    [Fact]
    public void Reap_IsRejectedForAFinalizingSession()
    {
        TransactionContext ctx = NewSession();

        Assert.Equal(FinalizeAdmission.Owner, ctx.EnterFinalize(out _));   // a commit/rollback owns finalize
        Assert.Null(ctx.TryEnterReap());                                    // must not steal it
        Assert.Equal(SessionLifecycle.Finalizing, ctx.Lifecycle);
    }

    [Fact]
    public void Reap_IsIdempotentlyNullOnceClaimed()
    {
        TransactionContext ctx = NewSession();

        Assert.NotNull(ctx.TryEnterReap());
        Assert.Null(ctx.TryEnterReap());
        Assert.Equal(SessionLifecycle.Reaping, ctx.Lifecycle);
    }

    [Fact]
    public async Task Reap_AConcurrentCommitMirrorsTheReapOutcome()
    {
        TransactionContext ctx = NewSession();

        FinalizeAttempt? reap = ctx.TryEnterReap();
        Assert.NotNull(reap);

        // A commit racing the reaper mirrors the reap's attempt rather than being rejected: fabricating a
        // definite Aborted while the reap is still deciding could contradict the outcome the reap publishes
        // (for a fenced durable transaction, even Committed).
        Assert.Equal(FinalizeAdmission.Mirror, ctx.EnterFinalize(out FinalizeAttempt? commit));
        Assert.Same(reap, commit);

        ctx.CompleteFinalize(reap!, new FinalizeOutcome(KeyValueResponseType.RolledBack, null));
        Assert.Equal(KeyValueResponseType.RolledBack, (await commit!.Completion).Type);
    }

    [Fact]
    public void Reap_BetweenAttempts_RejectsAConcurrentCommit()
    {
        TransactionContext ctx = NewSession();

        FinalizeAttempt? reap = ctx.TryEnterReap();
        Assert.NotNull(reap);

        // The reap's cleanup could not fully release: it publishes a non-terminal MustRetry, freeing the slot
        // while the session stays Reaping until a later sweep. Only in this window — nothing to mirror, the
        // reaper still owns the session — is a commit rejected (the coordinator answers MustRetry for it).
        ctx.CompleteFinalize(reap!, new FinalizeOutcome(KeyValueResponseType.MustRetry, null));

        Assert.Equal(FinalizeAdmission.Rejected, ctx.EnterFinalize(out FinalizeAttempt? commit));
        Assert.Null(commit);
    }

    [Fact]
    public void Reap_ClaimsAbandonedFinalizingSession_ButNotOneWithAnActiveOwner()
    {
        TransactionContext ctx = NewSession();

        // A commit/rollback owns the finalize slot — the reaper must not steal it.
        Assert.Equal(FinalizeAdmission.Owner, ctx.EnterFinalize(out FinalizeAttempt? owner));
        Assert.Null(ctx.TryEnterReap());
        Assert.Equal(SessionLifecycle.Finalizing, ctx.Lifecycle);

        // The owner published a non-terminal MustRetry (a drain timeout, say) and then disappeared: the slot
        // is free but the session stays Finalizing, closed to new operations, with no owner to decide it.
        ctx.CompleteFinalize(owner!, new FinalizeOutcome(KeyValueResponseType.MustRetry, null));
        Assert.Equal(SessionLifecycle.Finalizing, ctx.Lifecycle);

        // The reaper reclaims that abandoned finalization (previously it was stranded forever — TryEnterReap
        // required an accepting session).
        Assert.NotNull(ctx.TryEnterReap());
        Assert.Equal(SessionLifecycle.Reaping, ctx.Lifecycle);
    }

    /// <summary>
    /// A range lock held by a live session is renewable while the session is accepting operations. Entering the
    /// finalize fence (<see cref="SessionLifecycle.Finalizing"/>) does NOT stop renewal — the lease must stay
    /// effective through the finalize drain. Renewal stops only when cleanup calls
    /// <see cref="TransactionContext.ExcludeAndDrainRangeRenewalsAsync"/> at the top of <c>ReleaseWorkingSet</c>,
    /// the single hand-off point where cleanup takes ownership.
    /// </summary>
    [Fact]
    public void SnapshotRenewableRangeLocks_ReturnsHeldLockWhileAccepting_AndWhileFinalizing_EmptyOnlyAfterCleanupExcludesRenewal()
    {
        TransactionContext ctx = NewSession();

        TransactionOperationId opId = TransactionOperationId.NewRandom();
        Assert.Equal(OperationRegistrationOutcome.New, ctx.BeginOperation(opId, OperationKind.RangeLock, [1]).Outcome);
        RangeLockKey range = new("pfx", "a", true, "z", false, KeyValueDurability.Persistent);
        ctx.CompleteOperation(opId, new OperationCompletionPayload { AcquiredRangeLock = (range, RangeLockMode.Exclusive) }, response: null);

        List<(RangeLockKey Range, RangeLockMode Mode)> whileAccepting = ctx.SnapshotRenewableRangeLocks();
        Assert.Single(whileAccepting);
        Assert.Equal(range, whileAccepting[0].Range);
        Assert.Equal(RangeLockMode.Exclusive, whileAccepting[0].Mode);

        // Entering the finalize fence closes the session to new operations but must NOT stop renewal:
        // the finalize drain (WaitForPendingOperations) can outlast the renewal TTL, and the predicate
        // lock must stay effective throughout.
        Assert.Equal(FinalizeAdmission.Owner, ctx.EnterFinalize(out _));
        Assert.Equal(SessionLifecycle.Finalizing, ctx.Lifecycle);
        Assert.Single(ctx.SnapshotRenewableRangeLocks());

        // Cleanup closes renewal admission before releasing: from this instant no renewal snapshot
        // includes the session, and cleanup is the sole owner of the range lock.
        Assert.True(ctx.ExcludeAndDrainRangeRenewalsAsync().IsCompletedSuccessfully);
        Assert.Empty(ctx.SnapshotRenewableRangeLocks());
    }

    /// <summary>
    /// A session claimed by the reaper contributes renewable range locks until cleanup calls
    /// <see cref="TransactionContext.ExcludeAndDrainRangeRenewalsAsync"/>. In practice the reaper only claims
    /// sessions past their reap deadline, so the renewal sweep's deadline guard already skips them;
    /// the flag hand-off is the authoritative stop.
    /// </summary>
    [Fact]
    public void SnapshotRenewableRangeLocks_ReturnsHeldLockWhileReaping_EmptyAfterCleanupExcludesRenewal()
    {
        TransactionContext ctx = NewSession();

        TransactionOperationId opId = TransactionOperationId.NewRandom();
        Assert.Equal(OperationRegistrationOutcome.New, ctx.BeginOperation(opId, OperationKind.RangeLock, [1]).Outcome);
        RangeLockKey range = new("pfx", null, true, null, false, KeyValueDurability.Persistent);
        ctx.CompleteOperation(opId, new OperationCompletionPayload { AcquiredRangeLock = (range, RangeLockMode.Shared) }, response: null);
        Assert.Single(ctx.SnapshotRenewableRangeLocks());

        // The reaper claims the session, but the lock is still held until ReleaseWorkingSet runs.
        Assert.NotNull(ctx.TryEnterReap());
        Assert.Single(ctx.SnapshotRenewableRangeLocks());

        // The hand-off at the top of ReleaseWorkingSet is authoritative: it excludes the session from renewal.
        Assert.True(ctx.ExcludeAndDrainRangeRenewalsAsync().IsCompletedSuccessfully);
        Assert.Empty(ctx.SnapshotRenewableRangeLocks());
    }

    /// <summary>
    /// A renewal admitted before cleanup closes admission keeps cleanup waiting: the drain task stays pending
    /// until every admitted renewal ends, so an acquire already sent lands before cleanup releases the lock.
    /// From the instant admission closes, no further renewal may enter and the snapshot is empty.
    /// </summary>
    [Fact]
    public void ExcludeAndDrainRangeRenewals_WaitsForAdmittedRenewals_ThenRefusesNewAdmission()
    {
        TransactionContext ctx = NewSession();

        TransactionOperationId opId = TransactionOperationId.NewRandom();
        Assert.Equal(OperationRegistrationOutcome.New, ctx.BeginOperation(opId, OperationKind.RangeLock, [1]).Outcome);
        RangeLockKey range = new("pfx", null, true, null, false, KeyValueDurability.Persistent);
        ctx.CompleteOperation(opId, new OperationCompletionPayload { AcquiredRangeLock = (range, RangeLockMode.Exclusive) }, response: null);
        Assert.Single(ctx.SnapshotRenewableRangeLocks());

        // Two renewals obtained their concurrency slot before cleanup started.
        Assert.True(ctx.TryBeginRangeRenewal());
        Assert.True(ctx.TryBeginRangeRenewal());

        Task drain = ctx.ExcludeAndDrainRangeRenewalsAsync();
        Assert.False(drain.IsCompleted);

        // Admission is closed: a renewal that obtains its slot now sends nothing, and no snapshot includes the session.
        Assert.False(ctx.TryBeginRangeRenewal());
        Assert.Empty(ctx.SnapshotRenewableRangeLocks());

        // The drain completes only when the last admitted renewal ends.
        ctx.EndRangeRenewal();
        Assert.False(drain.IsCompleted);
        ctx.EndRangeRenewal();
        Assert.True(drain.IsCompletedSuccessfully);

        // A second cleanup pass (a rollback retry) finds nothing to wait for.
        Assert.True(ctx.ExcludeAndDrainRangeRenewalsAsync().IsCompletedSuccessfully);
        Assert.False(ctx.TryBeginRangeRenewal());
    }

    /// <summary>
    /// With no admitted renewal, cleanup takes ownership at once: the drain is already complete, the
    /// snapshot is empty, and a renewal that arrives afterwards is refused.
    /// </summary>
    [Fact]
    public void ExcludeAndDrainRangeRenewals_CompletesAtOnceWhenNoRenewalIsActive()
    {
        TransactionContext ctx = NewSession();

        TransactionOperationId opId = TransactionOperationId.NewRandom();
        Assert.Equal(OperationRegistrationOutcome.New, ctx.BeginOperation(opId, OperationKind.RangeLock, [1]).Outcome);
        RangeLockKey range = new("pfx", "a", true, "z", false, KeyValueDurability.Persistent);
        ctx.CompleteOperation(opId, new OperationCompletionPayload { AcquiredRangeLock = (range, RangeLockMode.Shared) }, response: null);
        Assert.Single(ctx.SnapshotRenewableRangeLocks());

        // A renewal that entered and ended before cleanup leaves nothing behind.
        Assert.True(ctx.TryBeginRangeRenewal());
        ctx.EndRangeRenewal();

        Task drain = ctx.ExcludeAndDrainRangeRenewalsAsync();
        Assert.True(drain.IsCompletedSuccessfully);
        Assert.Empty(ctx.SnapshotRenewableRangeLocks());
        Assert.False(ctx.TryBeginRangeRenewal());
    }

    /// <summary>A session that holds no range locks has nothing to renew.</summary>
    [Fact]
    public void SnapshotRenewableRangeLocks_EmptyWhenNoRangeLocksHeld()
    {
        TransactionContext ctx = NewSession();
        Assert.Empty(ctx.SnapshotRenewableRangeLocks());
    }

    [Fact]
    public void HasPendingOperations_ReflectsRegisteredButUncompletedWork()
    {
        TransactionContext ctx = NewSession();
        Assert.False(ctx.HasPendingOperations);

        TransactionOperationId opId = TransactionOperationId.NewRandom();
        Assert.Equal(OperationRegistrationOutcome.New, ctx.BeginOperation(opId, OperationKind.Set, [1]).Outcome);
        Assert.True(ctx.HasPendingOperations);

        // Completing the operation drains the pending count.
        ctx.CompleteOperation(opId, payload: null, response: null);
        Assert.False(ctx.HasPendingOperations);
    }

    [Fact]
    public void TryResumeReap_ReArmsSlotForCleanupRetry_OnlyAfterAReleasingPublish()
    {
        TransactionContext ctx = NewSession();

        // Not reaping yet → nothing to resume.
        Assert.Null(ctx.TryResumeReap());

        FinalizeAttempt? first = ctx.TryEnterReap();
        Assert.NotNull(first);
        Assert.Equal(SessionLifecycle.Reaping, ctx.Lifecycle);

        // Slot still held by the in-flight reap → resume must not race it.
        Assert.Null(ctx.TryResumeReap());

        // A cleanup that could not fully release publishes a non-terminal MustRetry, which frees the slot
        // but leaves the session Reaping.
        ctx.CompleteFinalize(first!, new FinalizeOutcome(KeyValueResponseType.MustRetry, null));
        Assert.Equal(SessionLifecycle.Reaping, ctx.Lifecycle);

        // A later sweep re-arms the slot to retry the cleanup.
        FinalizeAttempt? second = ctx.TryResumeReap();
        Assert.NotNull(second);
        Assert.NotSame(first, second);

        // A terminal publish retains the slot (idempotent duplicate mirrors it), so no further resume fires.
        ctx.CompleteFinalize(second!, new FinalizeOutcome(KeyValueResponseType.RolledBack, null));
        Assert.Null(ctx.TryResumeReap());
    }
}
