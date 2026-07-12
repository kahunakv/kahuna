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
}
