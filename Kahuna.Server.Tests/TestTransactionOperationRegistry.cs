using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit tests for the per-session operation registry and finalize fence on <see cref="TransactionContext"/>.
/// The registry is reached concurrently by in-flight transaction operations and by the finalize path, so
/// these assert atomic registration, idempotent duplicates, digest-conflict rejection, the accepting →
/// finalizing fence, the pending-operation drain, capacity bounding, and race-free close publication.
/// </summary>
public sealed class TestTransactionOperationRegistry
{
    private static TransactionContext NewContext(int timeoutMs = 5000) => new()
    {
        TransactionId = new HLCTimestamp(1, 100, 0),
        CoordinatorKey = "coord",
        Timeout = timeoutMs
    };

    private static TransactionOperationId Op(int n) => new((ulong)n, 0);

    [Fact]
    public void BeginOperation_FreshId_IsNew()
    {
        TransactionContext ctx = NewContext();
        Assert.Equal(OperationRegistrationOutcome.New, ctx.BeginOperation(Op(1), OperationKind.Set, [1, 2, 3]).Outcome);
    }

    [Fact]
    public void BeginOperation_SameIdWhilePending_IsAlreadyPending()
    {
        TransactionContext ctx = NewContext();
        ctx.BeginOperation(Op(1), OperationKind.Set, [1]);
        Assert.Equal(OperationRegistrationOutcome.AlreadyPending, ctx.BeginOperation(Op(1), OperationKind.Set, [1]).Outcome);
    }

    [Fact]
    public void BeginOperation_SameIdDifferentDigest_IsRejectedDuplicate()
    {
        TransactionContext ctx = NewContext();
        ctx.BeginOperation(Op(1), OperationKind.Set, [1, 2, 3]);
        Assert.Equal(OperationRegistrationOutcome.RejectedDuplicate, ctx.BeginOperation(Op(1), OperationKind.Set, [9, 9, 9]).Outcome);
    }

    [Fact]
    public void BeginOperation_SameIdDifferentKind_IsRejectedDuplicate()
    {
        TransactionContext ctx = NewContext();
        ctx.BeginOperation(Op(1), OperationKind.Set, [1]);
        Assert.Equal(OperationRegistrationOutcome.RejectedDuplicate, ctx.BeginOperation(Op(1), OperationKind.Delete, [1]).Outcome);
    }

    [Fact]
    public void BeginOperation_CompletedThenReplayed_ReturnsCachedResponse()
    {
        TransactionContext ctx = NewContext();
        ctx.BeginOperation(Op(1), OperationKind.Set, [1]);
        ctx.CompleteOperation(Op(1), null, "cached-answer");

        OperationRegistrationResult replay = ctx.BeginOperation(Op(1), OperationKind.Set, [1]);
        Assert.Equal(OperationRegistrationOutcome.AlreadyCompleted, replay.Outcome);
        Assert.Equal("cached-answer", replay.CachedResponse);
    }

    [Fact]
    public void BeginOperation_AfterFinalizing_IsRejectedSessionClosed()
    {
        TransactionContext ctx = NewContext();
        Assert.True(ctx.TryBeginFinalizing());
        Assert.Equal(OperationRegistrationOutcome.RejectedSessionClosed, ctx.BeginOperation(Op(1), OperationKind.Set, [1]).Outcome);
    }

    [Fact]
    public void BeginOperation_BeyondCapacity_IsRejectedCapacity()
    {
        TransactionContext ctx = NewContext();
        // 4096 is the pending cap; fill it, then the next distinct id must be rejected.
        for (int i = 0; i < 4096; i++)
            Assert.Equal(OperationRegistrationOutcome.New, ctx.BeginOperation(Op(i), OperationKind.Set, null).Outcome);

        Assert.Equal(OperationRegistrationOutcome.RejectedCapacity, ctx.BeginOperation(Op(4096), OperationKind.Set, null).Outcome);

        // Completing one frees a slot.
        ctx.CompleteOperation(Op(0), null, null);
        Assert.Equal(OperationRegistrationOutcome.New, ctx.BeginOperation(Op(4096), OperationKind.Set, null).Outcome);
    }

    [Fact]
    public async Task BeginOperation_ConcurrentDistinctIds_EachRegisteredExactlyOnce()
    {
        TransactionContext ctx = NewContext();
        const int count = 2000;

        int newCount = 0;
        await Parallel.ForEachAsync(Enumerable.Range(0, count), async (i, _) =>
        {
            await Task.Yield();
            if (ctx.BeginOperation(Op(i), OperationKind.Set, null).Outcome == OperationRegistrationOutcome.New)
                Interlocked.Increment(ref newCount);
        });

        // No torn dictionary, no lost registrations: exactly `count` distinct ids registered as New,
        // and the snapshot's pending count agrees.
        Assert.Equal(count, newCount);
        Assert.Equal(count, ctx.GetWorkingSetSnapshot().PendingOperationCount);
    }

    [Fact]
    public async Task BeginOperation_ConcurrentSameId_RegistersOnce()
    {
        TransactionContext ctx = NewContext();
        int newCount = 0;

        await Parallel.ForEachAsync(Enumerable.Range(0, 500), async (_, _) =>
        {
            await Task.Yield();
            if (ctx.BeginOperation(Op(7), OperationKind.Set, [1]).Outcome == OperationRegistrationOutcome.New)
                Interlocked.Increment(ref newCount);
        });

        Assert.Equal(1, newCount);
        Assert.Equal(1, ctx.GetWorkingSetSnapshot().PendingOperationCount);
    }

    [Fact]
    public async Task WaitForPendingOperations_ResolvesWhenLastCompletes()
    {
        TransactionContext ctx = NewContext();
        ctx.BeginOperation(Op(1), OperationKind.Set, null);
        ctx.BeginOperation(Op(2), OperationKind.Set, null);
        Assert.True(ctx.TryBeginFinalizing());

        Task drain = ctx.WaitForPendingOperations(TestContext.Current.CancellationToken);
        Assert.False(drain.IsCompleted);

        ctx.CompleteOperation(Op(1), null, null);
        Assert.False(drain.IsCompleted);

        ctx.CompleteOperation(Op(2), null, null);
        await drain.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.True(drain.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task WaitForPendingOperations_CancelledOnDeadline()
    {
        TransactionContext ctx = NewContext();
        ctx.BeginOperation(Op(1), OperationKind.Set, null); // never completes
        Assert.True(ctx.TryBeginFinalizing());

        using CancellationTokenSource cts = new(TimeSpan.FromMilliseconds(100));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ctx.WaitForPendingOperations(cts.Token));
    }

    [Fact]
    public void Finalize_SecondCallerBeforePublish_SeesNoSnapshot_ThenSeesItAfterPublish()
    {
        TransactionContext ctx = NewContext();

        Assert.True(ctx.TryBeginFinalizing());
        // A racing finalize loses the CAS and, before the winner publishes, must not observe a snapshot.
        Assert.False(ctx.TryBeginFinalizing());
        Assert.Null(ctx.PublishedSnapshot);

        WorkingSetSnapshot snap = ctx.GetWorkingSetSnapshot();
        ctx.PublishTerminal(snap);

        Assert.NotNull(ctx.PublishedSnapshot);
        Assert.Equal(SessionLifecycle.Terminal, ctx.Lifecycle);
    }

    [Fact]
    public void RevertFinalizing_ReopensForOperations()
    {
        TransactionContext ctx = NewContext();
        Assert.True(ctx.TryBeginFinalizing());
        ctx.RevertFinalizing();

        Assert.Equal(SessionLifecycle.AcceptingOperations, ctx.Lifecycle);
        Assert.Equal(OperationRegistrationOutcome.New, ctx.BeginOperation(Op(1), OperationKind.Set, null).Outcome);
    }

    [Fact]
    public void CompleteOperation_RecordsEffectIntoWorkingSet()
    {
        TransactionContext ctx = NewContext();
        ctx.BeginOperation(Op(1), OperationKind.Set, null);
        ctx.CompleteOperation(
            Op(1),
            new OperationEffect
            {
                ModifiedKey = ("k", KeyValueDurability.Persistent),
                PointLock = ("k", KeyValueDurability.Persistent)
            },
            new CachedOperationResponse(KeyValueResponseType.Set, 3, HLCTimestamp.Zero));

        WorkingSetSnapshot snap = ctx.GetWorkingSetSnapshot();
        Assert.Contains(("k", KeyValueDurability.Persistent), snap.ModifiedKeys!);
        Assert.Contains(("k", KeyValueDurability.Persistent), snap.LocksAcquired!);
    }

    [Fact]
    public void CompleteOperation_ReplayedDoesNotDoubleRecordEffect()
    {
        TransactionContext ctx = NewContext();
        ctx.BeginOperation(Op(1), OperationKind.Delete, null);
        OperationEffect effect = new() { ModifiedKey = ("k", KeyValueDurability.Persistent) };
        ctx.CompleteOperation(Op(1), effect, new CachedOperationResponse(KeyValueResponseType.Deleted, 1, HLCTimestamp.Zero));
        // A replayed completion (already terminal) must be a no-op — the set stays a single entry.
        ctx.CompleteOperation(Op(1), effect, new CachedOperationResponse(KeyValueResponseType.Deleted, 1, HLCTimestamp.Zero));

        Assert.Single(ctx.GetWorkingSetSnapshot().ModifiedKeys!);
    }

    [Fact]
    public void WorkingSetSnapshot_IsIndependentCopy()
    {
        TransactionContext ctx = NewContext();
        ctx.ModifiedKeys = [("k", KeyValueDurability.Persistent)];

        WorkingSetSnapshot snap = ctx.GetWorkingSetSnapshot();
        ctx.ModifiedKeys.Add(("k2", KeyValueDurability.Persistent));

        // Mutating the live session does not change the captured snapshot.
        Assert.Single(snap.ModifiedKeys!);
    }
}
