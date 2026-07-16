using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit coverage for the single finalize slot on <see cref="TransactionContext"/> that serializes
/// commit / rollback / reaper. Exactly one caller owns an active <see cref="FinalizeAttempt"/>; concurrent
/// finalizers mirror its published outcome. A terminal outcome is retained so duplicate finalizes replay
/// the same answer; a non-terminal <see cref="KeyValueResponseType.MustRetry"/> releases the slot so a
/// later call runs a fresh attempt. Every owner publishes on exit, so no mirroring waiter can hang.
/// </summary>
public sealed class TestFinalizeSlot
{
    private static TransactionContext NewSession() =>
        new() { CoordinatorKey = "c", TransactionId = new HLCTimestamp(1, 100, 0) };

    [Fact]
    public void FirstCallerOwns_SecondMirrorsSameAttempt()
    {
        TransactionContext ctx = NewSession();

        Assert.Equal(FinalizeAdmission.Owner, ctx.EnterFinalize(out FinalizeAttempt? owner));
        Assert.Equal(SessionLifecycle.Finalizing, ctx.Lifecycle);

        Assert.Equal(FinalizeAdmission.Mirror, ctx.EnterFinalize(out FinalizeAttempt? mirror));
        Assert.Same(owner, mirror);
    }

    [Fact]
    public async Task TerminalOutcome_IsPublishedToAllWaiters_AndRetainedForLateComers()
    {
        TransactionContext ctx = NewSession();

        Assert.Equal(FinalizeAdmission.Owner, ctx.EnterFinalize(out FinalizeAttempt? owner));
        Assert.Equal(FinalizeAdmission.Mirror, ctx.EnterFinalize(out FinalizeAttempt? mirror));

        ctx.CompleteFinalize(owner!, new FinalizeOutcome(KeyValueResponseType.Committed, "anchor-key"));

        FinalizeOutcome ownerResult = await owner!.Completion;
        FinalizeOutcome mirrorResult = await mirror!.Completion;
        Assert.Equal(KeyValueResponseType.Committed, ownerResult.Type);
        Assert.Equal("anchor-key", ownerResult.RecordAnchorKey);
        Assert.Equal(KeyValueResponseType.Committed, mirrorResult.Type);

        // A finalize that arrives after the terminal answer still mirrors the retained outcome — the slot
        // is not released for a terminal result, so a duplicate commit replays "Committed" rather than
        // running a second two-phase commit.
        Assert.Equal(FinalizeAdmission.Mirror, ctx.EnterFinalize(out FinalizeAttempt? late));
        Assert.Same(owner, late);
        FinalizeOutcome lateResult = await late!.Completion;
        Assert.Equal(KeyValueResponseType.Committed, lateResult.Type);
    }

    [Fact]
    public async Task MustRetry_ReleasesSlot_SoALaterCallRunsAFreshAttempt()
    {
        TransactionContext ctx = NewSession();

        Assert.Equal(FinalizeAdmission.Owner, ctx.EnterFinalize(out FinalizeAttempt? first));
        ctx.CompleteFinalize(first!, new FinalizeOutcome(KeyValueResponseType.MustRetry, null));

        FinalizeOutcome firstResult = await first!.Completion;
        Assert.Equal(KeyValueResponseType.MustRetry, firstResult.Type);

        // The non-terminal outcome released the slot: the retry becomes a brand-new owner, not a mirror.
        Assert.Equal(FinalizeAdmission.Owner, ctx.EnterFinalize(out FinalizeAttempt? retry));
        Assert.NotSame(first, retry);

        ctx.CompleteFinalize(retry!, new FinalizeOutcome(KeyValueResponseType.Committed, "anchor"));
        Assert.Equal(KeyValueResponseType.Committed, (await retry!.Completion).Type);
    }

    [Fact]
    public async Task CommitAndRollback_ContendForTheSameSlot()
    {
        TransactionContext ctx = NewSession();

        // Commit wins the slot; a concurrent rollback mirrors the commit's outcome rather than running its
        // own finalize — first finalizer to enter decides the session.
        Assert.Equal(FinalizeAdmission.Owner, ctx.EnterFinalize(out FinalizeAttempt? commit));
        Assert.Equal(FinalizeAdmission.Mirror, ctx.EnterFinalize(out FinalizeAttempt? rollback));
        Assert.Same(commit, rollback);

        ctx.CompleteFinalize(commit!, new FinalizeOutcome(KeyValueResponseType.Committed, null));
        Assert.Equal(KeyValueResponseType.Committed, (await rollback!.Completion).Type);
    }

    [Fact]
    public async Task ManyConcurrentFinalizers_AllObserveExactlyOneOwnersOutcome()
    {
        TransactionContext ctx = NewSession();

        int owners = 0;
        FinalizeAttempt? ownerAttempt = null;
        List<Task<FinalizeOutcome>> waiters = [];

        // Race a crowd of finalizers at the same slot. Exactly one must own; every one resolves the owner's
        // published outcome.
        await Task.WhenAll(Enumerable.Range(0, 64).Select(_ => Task.Run(() =>
        {
            FinalizeAdmission admission = ctx.EnterFinalize(out FinalizeAttempt? attempt);
            lock (waiters)
            {
                if (admission == FinalizeAdmission.Owner)
                {
                    Interlocked.Increment(ref owners);
                    ownerAttempt = attempt;
                }
                waiters.Add(attempt!.Completion);
            }
        })));

        Assert.Equal(1, owners);
        Assert.NotNull(ownerAttempt);

        ctx.CompleteFinalize(ownerAttempt!, new FinalizeOutcome(KeyValueResponseType.Committed, "a"));

        FinalizeOutcome[] results = await Task.WhenAll(waiters);
        Assert.All(results, r => Assert.Equal(KeyValueResponseType.Committed, r.Type));
    }

    [Fact]
    public async Task OwnerPublishingOnFailurePath_NeverLeavesAWaiterHanging()
    {
        TransactionContext ctx = NewSession();

        Assert.Equal(FinalizeAdmission.Owner, ctx.EnterFinalize(out FinalizeAttempt? owner));
        Assert.Equal(FinalizeAdmission.Mirror, ctx.EnterFinalize(out FinalizeAttempt? mirror));

        // Model the coordinator's finally-block publish after an unexpected owner failure: the owner still
        // publishes an outcome, so the mirror's await completes instead of hanging forever.
        ctx.CompleteFinalize(owner!, new FinalizeOutcome(KeyValueResponseType.Aborted, null));

        FinalizeOutcome mirrored = await mirror!.Completion.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Aborted, mirrored.Type);
    }
}
