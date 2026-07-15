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
    public void Reap_RejectsAConcurrentCommitWhichMirrorsTheReapOutcome()
    {
        TransactionContext ctx = NewSession();

        FinalizeAttempt? reap = ctx.TryEnterReap();
        Assert.NotNull(reap);

        // A commit racing the reaper is rejected: there is nothing left to finalize on a reaped session.
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
    /// A range lock a live session holds is renewable while the session is still accepting operations, but the
    /// snapshot goes empty the instant the session enters the finalize fence — so a renewal tick can never
    /// re-extend a lock a concurrent commit/rollback is releasing.
    /// </summary>
    [Fact]
    public void SnapshotRenewableRangeLocks_ReturnsHeldLockWhileAccepting_EmptyOnceFinalizing()
    {
        TransactionContext ctx = NewSession();

        TransactionOperationId opId = TransactionOperationId.NewRandom();
        Assert.Equal(OperationRegistrationOutcome.New, ctx.BeginOperation(opId, OperationKind.RangeLock, [1]).Outcome);
        RangeLockKey range = new("pfx", "a", true, "z", false, KeyValueDurability.Persistent);
        ctx.CompleteOperation(opId, new OperationEffect { RangeLock = (range, RangeLockMode.Exclusive) }, response: null);

        List<(RangeLockKey Range, RangeLockMode Mode)> whileAccepting = ctx.SnapshotRenewableRangeLocks();
        Assert.Single(whileAccepting);
        Assert.Equal(range, whileAccepting[0].Range);
        Assert.Equal(RangeLockMode.Exclusive, whileAccepting[0].Mode);

        // A commit/rollback owns the finalize slot: the lock is now the finalize path's to release, not the
        // renewal tick's to extend.
        Assert.Equal(FinalizeAdmission.Owner, ctx.EnterFinalize(out _));
        Assert.Empty(ctx.SnapshotRenewableRangeLocks());
    }

    /// <summary>
    /// A session claimed by the reaper likewise contributes no renewable range locks: the reaper is releasing
    /// them, so renewal must not fight it.
    /// </summary>
    [Fact]
    public void SnapshotRenewableRangeLocks_EmptyOnceReaping()
    {
        TransactionContext ctx = NewSession();

        TransactionOperationId opId = TransactionOperationId.NewRandom();
        Assert.Equal(OperationRegistrationOutcome.New, ctx.BeginOperation(opId, OperationKind.RangeLock, [1]).Outcome);
        RangeLockKey range = new("pfx", null, true, null, false, KeyValueDurability.Persistent);
        ctx.CompleteOperation(opId, new OperationEffect { RangeLock = (range, RangeLockMode.Shared) }, response: null);
        Assert.Single(ctx.SnapshotRenewableRangeLocks());

        Assert.NotNull(ctx.TryEnterReap());
        Assert.Empty(ctx.SnapshotRenewableRangeLocks());
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
        ctx.CompleteOperation(opId, effect: null, response: null);
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
