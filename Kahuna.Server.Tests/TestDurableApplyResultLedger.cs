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
/// On the leader a durable record/intent entry would otherwise be applied twice: once when Raft delivers the
/// committed entry to the consumer, and again by the write scheduler's completion, which needs that apply's prepare
/// acknowledgement to resolve its producer. The consumer apply runs first and leaves its result against the log
/// entry so the completion reuses it instead of deserializing the same delta again.
///
/// <para>The safety property is that a miss always means "apply it yourself": no state of this ledger can turn into
/// a missed apply, only into the original double apply.</para>
/// </summary>
public sealed class TestDurableApplyResultLedger
{
    private const int PartitionId = 7;

    [Fact]
    public void RecordedResult_IsConsumedExactlyOnce()
    {
        DurableApplyResultLedger ledger = new();
        ledger.RecordApplied(PartitionId, 42, result: true);

        Assert.True(ledger.TryConsume(PartitionId, 42, out bool applied));
        Assert.True(applied);

        // Consumed: a second claim must fall through to applying rather than silently reusing a stale outcome.
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
    public void UnrecordedEntry_FallsThroughToApplying()
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
        // entry could consume in place of its own apply.
        ledger.RecordApplied(PartitionId, 0, result: true);
        ledger.RecordApplied(PartitionId, -1, result: true);

        Assert.False(ledger.TryConsume(PartitionId, 0, out _));
        Assert.False(ledger.TryConsume(PartitionId, -1, out _));
    }

    [Fact]
    public void UnclaimedResults_DoNotAccumulate()
    {
        DurableApplyResultLedger ledger = new();

        // Completions that already applied for themselves never claim their result. Those must not pile up for the
        // process's lifetime; dropping them only costs the double apply they already paid.
        for (long index = 1; index <= 20_000; index++)
            ledger.RecordApplied(PartitionId, index, result: true);

        Assert.False(ledger.TryConsume(PartitionId, 1, out _));

        // Recent results are still claimable — pruning must not defeat the optimization it exists to bound.
        Assert.True(ledger.TryConsume(PartitionId, 20_000, out bool recent));
        Assert.True(recent);
    }
}

/// <summary>
/// End-to-end proof that the skip actually fires on the real durable path — that the log index the write scheduler
/// reports per entry and the index Raft stamps on the committed entry it delivers to the consumer are the same
/// identity. If they ever diverge the skip silently stops happening and the redundant parse returns, so this asserts
/// the behavior rather than the wiring.
///
/// <para>The saving is opportunistic, not guaranteed: the commit path releases the proposal ticket just before it
/// applies to the consumer, so a completion that gets scheduled promptly can overtake the consumer apply and do the
/// apply itself. That costs the original double apply and nothing else, which is why this drives a batch of
/// transactions and asserts that skipping happens rather than that it happens every time.</para>
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDurableApplyResultLedgerEndToEnd
{
    private readonly ILoggerFactory loggerFactory;

    public TestDurableApplyResultLedgerEndToEnd(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    [Fact]
    public async Task DurableCommit_SkipsTheRedundantApply_AndStillCommits()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        EmbeddedKahunaOptions options = new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        };

        await using EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("ledger/row-1", ct);

        long before = DurableTransactionMetrics.RedundantAppliesSkippedCount;

        const int transactions = 25;

        for (int i = 0; i < transactions; i++)
        {
            KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes($"BEGIN SET `ledger/row-{i}a` 'v{i}a' SET `ledger/row-{i}b` 'v{i}b' COMMIT END"), null, null);
            Assert.Equal(KeyValueResponseType.Set, result.Type);
        }

        // Every committed value is correct even though most of the applies behind them were skipped.
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

        long skipped = DurableTransactionMetrics.RedundantAppliesSkippedCount - before;
        Assert.True(skipped > 0, $"expected redundant durable applies to be skipped across {transactions} transactions, saw {skipped}");
    }
}
