using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The ledger is the rendezvous between the ordered consumer apply of a durable entry and the write scheduler's
/// completion for the same log entry: the consumer records what its apply produced, the completion waits for it and
/// never applies the delta itself. These pin the claim semantics (single-shot, faithful, bounded residue) and the
/// wait semantics (answered at once when recorded or already applied, parked until recorded otherwise, released
/// by a leadership loss, and never thrown at the completion — a timeout or cancellation answers NotApplied).
/// </summary>
public sealed class TestDurableApplyResultLedger
{
    private const int PartitionId = 7;

    private static readonly TimeSpan Long = TimeSpan.FromSeconds(10);

    [Fact]
    public void RecordedResult_IsConsumedExactlyOnce()
    {
        DurableApplyResultLedger ledger = new();
        ledger.RecordApplied(PartitionId, 42, result: true);

        Assert.True(ledger.TryConsume(PartitionId, 42, out bool applied));
        Assert.True(applied);

        // Consumed: a second claim must not silently reuse a stale outcome.
        Assert.False(ledger.TryConsume(PartitionId, 42, out _));
    }

    [Fact]
    public void RejectedPrepare_IsCarriedFaithfully()
    {
        DurableApplyResultLedger ledger = new();

        // A rejected prepare must reach the producer as a rejection; silently reporting success here would let a
        // transaction commit a mutation whose recoverable intent it never owned.
        ledger.RecordApplied(PartitionId, 42, result: false);

        Assert.True(ledger.TryConsume(PartitionId, 42, out bool acknowledged));
        Assert.False(acknowledged);
    }

    [Fact]
    public void UnrecordedEntry_IsNotClaimable()
    {
        DurableApplyResultLedger ledger = new();
        ledger.RecordApplied(PartitionId, 42, result: true);

        Assert.False(ledger.TryConsume(PartitionId, 41, out _));
        Assert.False(ledger.TryConsume(PartitionId, 43, out _));

        // A different partition's entry at the same index is a different entry.
        Assert.False(ledger.TryConsume(PartitionId + 1, 42, out _));
    }

    [Fact]
    public void UnindexedEntry_IsNeverMatched()
    {
        DurableApplyResultLedger ledger = new();

        // A non-positive index carries no entry identity; recording it must not create a result that some unrelated
        // entry could consume in place of its own apply, nor advance the applied cursor.
        ledger.RecordApplied(PartitionId, 0, result: true);
        ledger.RecordApplied(PartitionId, -1, result: true);

        Assert.False(ledger.TryConsume(PartitionId, 0, out _));
        Assert.False(ledger.TryConsume(PartitionId, -1, out _));
        Assert.Equal(0, ledger.AppliedThrough(PartitionId));
    }

    [Fact]
    public void UnclaimedResults_DoNotAccumulate()
    {
        DurableApplyResultLedger ledger = new();

        // Follower applies never have a completion to claim them. Those must not pile up for the process's lifetime.
        for (long index = 1; index <= 20_000; index++)
            ledger.RecordApplied(PartitionId, index, result: true);

        Assert.False(ledger.TryConsume(PartitionId, 1, out _));

        // Recent results are still claimable, and the cursor remembers how far the consumer got.
        Assert.True(ledger.TryConsume(PartitionId, 20_000, out bool recent));
        Assert.True(recent);
        Assert.Equal(20_000, ledger.AppliedThrough(PartitionId));
    }

    [Fact]
    public async Task WaitApplied_AnswersARecordedResultAtOnce()
    {
        DurableApplyResultLedger ledger = new();
        ledger.RecordApplied(PartitionId, 42, result: false);

        DurableApplyWaitOutcome outcome = await ledger.WaitAppliedAsync(PartitionId, 42, Long, CancellationToken.None);

        Assert.Equal(DurableApplyWaitStatus.Recorded, outcome.Status);
        Assert.False(outcome.Result);

        // Claimed by the wait: nothing is left for a second claimant.
        Assert.False(ledger.TryConsume(PartitionId, 42, out _));
    }

    [Fact]
    public async Task WaitApplied_ParksUntilTheConsumerRecordsTheEntry()
    {
        DurableApplyResultLedger ledger = new();

        ValueTask<DurableApplyWaitOutcome> wait = ledger.WaitAppliedAsync(PartitionId, 42, Long, CancellationToken.None);
        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.False(wait.IsCompleted);

        ledger.RecordApplied(PartitionId, 42, result: true);

        DurableApplyWaitOutcome outcome = await wait;
        Assert.Equal(DurableApplyWaitStatus.Recorded, outcome.Status);
        Assert.True(outcome.Result);
    }

    [Fact]
    public async Task WaitApplied_ReportsDisplaced_WhenTheCursorPassedTheEntry()
    {
        DurableApplyResultLedger ledger = new();

        // The consumer applied 42 and then more than a window's worth of entries: the result is gone, the apply is
        // provably done (the consumer applies in log order), and the caller must read the store rather than apply.
        for (long index = 42; index <= 42 + 2_000; index++)
            ledger.RecordApplied(PartitionId, index, result: true);

        DurableApplyWaitOutcome outcome = await ledger.WaitAppliedAsync(PartitionId, 42, Long, CancellationToken.None);

        Assert.Equal(DurableApplyWaitStatus.AppliedResultDisplaced, outcome.Status);
    }

    [Fact]
    public async Task WaitApplied_TimesOut_AsNotApplied()
    {
        DurableApplyResultLedger ledger = new();

        DurableApplyWaitOutcome outcome = await ledger.WaitAppliedAsync(PartitionId, 42, TimeSpan.FromMilliseconds(50), CancellationToken.None);

        Assert.Equal(DurableApplyWaitStatus.NotApplied, outcome.Status);

        // The waiter is gone: a later record of the entry finds nobody to wake and leaves its result claimable.
        ledger.RecordApplied(PartitionId, 42, result: true);
        Assert.True(ledger.TryConsume(PartitionId, 42, out bool late));
        Assert.True(late);
    }

    [Fact]
    public async Task WaitApplied_Cancelled_AsNotApplied()
    {
        DurableApplyResultLedger ledger = new();
        using CancellationTokenSource cts = new();

        ValueTask<DurableApplyWaitOutcome> wait = ledger.WaitAppliedAsync(PartitionId, 42, Long, cts.Token);
        cts.Cancel();

        DurableApplyWaitOutcome outcome = await wait;
        Assert.Equal(DurableApplyWaitStatus.NotApplied, outcome.Status);
    }

    [Fact]
    public async Task LeadershipLoss_ReleasesEveryParkedWait_AndAnswersLaterOnesAtOnce()
    {
        DurableApplyResultLedger ledger = new();

        ValueTask<DurableApplyWaitOutcome> first = ledger.WaitAppliedAsync(PartitionId, 42, Long, CancellationToken.None);
        ValueTask<DurableApplyWaitOutcome> second = ledger.WaitAppliedAsync(PartitionId, 43, Long, CancellationToken.None);
        ValueTask<DurableApplyWaitOutcome> other = ledger.WaitAppliedAsync(PartitionId + 1, 42, Long, CancellationToken.None);
        await Task.Delay(20, TestContext.Current.CancellationToken);

        Assert.Equal(2, ledger.NoteLeadershipLost(PartitionId));
        Assert.Equal(DurableApplyWaitStatus.LeadershipLost, (await first).Status);
        Assert.Equal(DurableApplyWaitStatus.LeadershipLost, (await second).Status);
        Assert.True(ledger.HasLostLeadership(PartitionId));

        // Another partition's wait is untouched.
        Assert.False(other.IsCompleted);

        // Asked after the loss: answered without parking. Idempotent: nothing left to release.
        Assert.Equal(DurableApplyWaitStatus.LeadershipLost, (await ledger.WaitAppliedAsync(PartitionId, 44, Long, CancellationToken.None)).Status);
        Assert.Equal(0, ledger.NoteLeadershipLost(PartitionId));

        // A result the consumer records is still preferred over the loss, before and after.
        ledger.RecordApplied(PartitionId, 45, result: false);
        DurableApplyWaitOutcome recorded = await ledger.WaitAppliedAsync(PartitionId, 45, Long, CancellationToken.None);
        Assert.Equal(DurableApplyWaitStatus.Recorded, recorded.Status);
        Assert.False(recorded.Result);

        // Leading again: waits park until recorded.
        ledger.NoteLeadershipRegained(PartitionId);
        Assert.False(ledger.HasLostLeadership(PartitionId));
        ValueTask<DurableApplyWaitOutcome> parked = ledger.WaitAppliedAsync(PartitionId, 46, Long, CancellationToken.None);
        await Task.Delay(20, TestContext.Current.CancellationToken);
        Assert.False(parked.IsCompleted);
        ledger.RecordApplied(PartitionId, 46, result: true);
        Assert.Equal(DurableApplyWaitStatus.Recorded, (await parked).Status);

        ledger.RecordApplied(PartitionId + 1, 42, result: true);
        Assert.Equal(DurableApplyWaitStatus.Recorded, (await other).Status);
    }

    [Fact]
    public async Task WaitApplied_ForAnUnindexedEntry_IsNotApplied()
    {
        DurableApplyResultLedger ledger = new();

        DurableApplyWaitOutcome outcome = await ledger.WaitAppliedAsync(PartitionId, 0, Long, CancellationToken.None);

        Assert.Equal(DurableApplyWaitStatus.NotApplied, outcome.Status);
    }
}

/// <summary>
/// End-to-end proof that the rendezvous fires on the real durable path — that the log index the write scheduler
/// reports per entry and the index Raft stamps on the committed entry it delivers to the consumer are the same
/// identity. If they ever diverge, every completion would wait out its bound and answer its producer unacknowledged,
/// so this asserts the behavior rather than the wiring: a batch of transactions commits, and the completions took
/// their results from the ordered apply.
/// </summary>
public sealed class TestDurableApplyResultLedgerEndToEnd
{
    private readonly ILoggerFactory loggerFactory;

    public TestDurableApplyResultLedgerEndToEnd(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    [Fact]
    public async Task DurableCommit_TakesTheOrderedApplysResult_AndStillCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        EmbeddedKahunaOptions options = new()
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        };

        await using EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("ledger/row-1", ct);

        long before = DurableTransactionMetrics.RedundantAppliesSkippedCount;
        long timeoutsBefore = DurableTransactionMetrics.OrderedApplyWaitTimeoutsCount;

        const int transactions = 25;

        for (int i = 0; i < transactions; i++)
        {
            KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes($"BEGIN SET `ledger/row-{i}a` 'v{i}a' SET `ledger/row-{i}b` 'v{i}b' COMMIT END"), null, null);
            Assert.Equal(KeyValueResponseType.Set, result.Type);
        }

        // Every committed value is correct, and no completion had to apply or give up.
        for (int i = 0; i < transactions; i++)
        {
            (KeyValueResponseType ta, ReadOnlyKeyValueEntry? ea) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, $"ledger/row-{i}a", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, ta);
            Assert.Equal(Encoding.UTF8.GetBytes($"v{i}a"), ea!.Value);

            (KeyValueResponseType tb, ReadOnlyKeyValueEntry? eb) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, $"ledger/row-{i}b", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, tb);
            Assert.Equal(Encoding.UTF8.GetBytes($"v{i}b"), eb!.Value);
        }

        long taken = DurableTransactionMetrics.RedundantAppliesSkippedCount - before;
        Assert.True(taken > 0, $"expected the durable completions to take the ordered apply's results across {transactions} transactions, saw {taken}");
        Assert.Equal(timeoutsBefore, DurableTransactionMetrics.OrderedApplyWaitTimeoutsCount);
    }
}
