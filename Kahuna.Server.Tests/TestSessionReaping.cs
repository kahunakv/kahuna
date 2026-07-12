using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit coverage for the session-reaping fence on <see cref="TransactionContext"/>. The reaper must be able
/// to close a session to new operations atomically before it removes the session from the map, so a
/// BeginOperation that already captured the context reference is rejected instead of registering a new
/// operation on a session that is about to vanish. Closing must not steal a session that is already
/// finalizing (an in-flight commit/rollback owns that one).
/// </summary>
public sealed class TestSessionReaping
{
    private static TransactionContext NewSession() =>
        new() { CoordinatorKey = "c", TransactionId = new HLCTimestamp(1, 100, 0) };

    [Fact]
    public void CloseForReaping_RejectsSubsequentRegistrations()
    {
        TransactionContext ctx = NewSession();

        Assert.Equal(OperationRegistrationOutcome.New,
            ctx.BeginOperation(TransactionOperationId.NewRandom(), OperationKind.Set, [1]).Outcome);

        Assert.True(ctx.TryCloseForReaping());

        // The race the reaper closes: a BeginOperation that captured this context just before removal now
        // observes the closed lifecycle and is rejected, rather than registering a New op on a dead session.
        Assert.Equal(OperationRegistrationOutcome.RejectedSessionClosed,
            ctx.BeginOperation(TransactionOperationId.NewRandom(), OperationKind.Set, [2]).Outcome);
    }

    [Fact]
    public void CloseForReaping_IsRejectedForAFinalizingSession()
    {
        TransactionContext ctx = NewSession();

        Assert.True(ctx.TryBeginFinalizing());
        Assert.False(ctx.TryCloseForReaping());   // must not steal an in-flight finalize
        Assert.Equal(SessionLifecycle.Finalizing, ctx.Lifecycle);
    }

    [Fact]
    public void CloseForReaping_IsIdempotentlyFalseOnceClosed()
    {
        TransactionContext ctx = NewSession();

        Assert.True(ctx.TryCloseForReaping());
        Assert.False(ctx.TryCloseForReaping());
        Assert.Equal(SessionLifecycle.Reaping, ctx.Lifecycle);
    }
}
