
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for <see cref="TerminalOutcomeWindow"/> — the bounded idempotency window replayed to duplicate
/// commit/rollback requests. The contract under test: the size cap is a strict upper bound with FIFO
/// eviction on retention order, a re-retained id moves to the back (newest again), TTL pruning removes
/// exactly the entries past the window, and the structure stays consistent under concurrent
/// retain/lookup/prune (the previous implementation's per-insert eviction scan was the server's largest
/// commit-path contention point, which is why this structure exists at all).
/// </summary>
public sealed class TestTerminalOutcomeWindow
{
    private static HLCTimestamp Tx(long l) => new(0, l, 0);

    private static FinalizeOutcome Committed => new(KeyValueResponseType.Committed, "anchor");

    [Fact]
    public void SizeCap_IsStrict_AndEvictionIsFifo()
    {
        TerminalOutcomeWindow window = new();

        for (long i = 1; i <= 5; i++)
            window.Retain(Tx(i), Committed, Tx(i), max: 3);

        Assert.Equal(3, window.Count);

        // Oldest two (1, 2) evicted; newest three (3, 4, 5) retained.
        Assert.False(window.TryGet(Tx(1), out _));
        Assert.False(window.TryGet(Tx(2), out _));
        Assert.True(window.TryGet(Tx(3), out _));
        Assert.True(window.TryGet(Tx(4), out _));
        Assert.True(window.TryGet(Tx(5), out _));
    }

    [Fact]
    public void ReRetainedId_MovesToBack_AndSurvivesEvictionOfItsOldSlot()
    {
        TerminalOutcomeWindow window = new();

        window.Retain(Tx(1), Committed, Tx(1), max: 3);
        window.Retain(Tx(2), Committed, Tx(2), max: 3);
        window.Retain(Tx(3), Committed, Tx(3), max: 3);

        // Duplicate finalize of the oldest id: it becomes the newest entry, not a stale front slot.
        window.Retain(Tx(1), Committed, Tx(4), max: 3);
        Assert.Equal(3, window.Count);

        // Two more inserts evict 2 then 3 — never the re-retained 1.
        window.Retain(Tx(5), Committed, Tx(5), max: 3);
        window.Retain(Tx(6), Committed, Tx(6), max: 3);

        Assert.Equal(3, window.Count);
        Assert.True(window.TryGet(Tx(1), out TerminalOutcomeWindow.RetainedOutcome retained));
        Assert.Equal(Tx(4), retained.RetainedAt);
        Assert.False(window.TryGet(Tx(2), out _));
        Assert.False(window.TryGet(Tx(3), out _));
        Assert.True(window.TryGet(Tx(5), out _));
        Assert.True(window.TryGet(Tx(6), out _));
    }

    [Fact]
    public void RetainedOutcome_IsReplayedVerbatim()
    {
        TerminalOutcomeWindow window = new();
        FinalizeOutcome rolledBack = new(KeyValueResponseType.RolledBack, null);

        window.Retain(Tx(1), Committed, Tx(10), max: 10);
        window.Retain(Tx(2), rolledBack, Tx(11), max: 10);

        Assert.True(window.TryGet(Tx(1), out TerminalOutcomeWindow.RetainedOutcome first));
        Assert.Equal(KeyValueResponseType.Committed, first.Outcome.Type);
        Assert.Equal("anchor", first.Outcome.RecordAnchorKey);

        Assert.True(window.TryGet(Tx(2), out TerminalOutcomeWindow.RetainedOutcome second));
        Assert.Equal(KeyValueResponseType.RolledBack, second.Outcome.Type);
        Assert.Null(second.Outcome.RecordAnchorKey);
    }

    [Fact]
    public void PruneExpired_RemovesOnlyEntriesPastTheTtl()
    {
        TerminalOutcomeWindow window = new();

        window.Retain(Tx(1), Committed, new HLCTimestamp(0, 1_000, 0), max: 10);
        window.Retain(Tx(2), Committed, new HLCTimestamp(0, 9_500, 0), max: 10);

        // At 10_000 the first entry is 9s old, the second 0.5s: a 5s TTL prunes exactly the first.
        window.PruneExpired(new HLCTimestamp(0, 10_000, 0), TimeSpan.FromSeconds(5));

        Assert.Equal(1, window.Count);
        Assert.False(window.TryGet(Tx(1), out _));
        Assert.True(window.TryGet(Tx(2), out _));

        // A later insert still evicts correctly after the prune emptied the front of the order list.
        window.Retain(Tx(3), Committed, new HLCTimestamp(0, 10_500, 0), max: 1);
        Assert.Equal(1, window.Count);
        Assert.False(window.TryGet(Tx(2), out _));
        Assert.True(window.TryGet(Tx(3), out _));
    }

    [Fact]
    public void PruneExpired_ScansPastAFreshEntry_ToReachAnOlderOne()
    {
        TerminalOutcomeWindow window = new();

        // Retention HLCs slightly out of insertion order, as racing finalizers can produce: the
        // insertion-newer entry carries the OLDER timestamp. The prune must not stop at the first
        // fresh entry it meets.
        window.Retain(Tx(1), Committed, new HLCTimestamp(0, 9_500, 0), max: 10);
        window.Retain(Tx(2), Committed, new HLCTimestamp(0, 1_000, 0), max: 10);

        window.PruneExpired(new HLCTimestamp(0, 10_000, 0), TimeSpan.FromSeconds(5));

        Assert.Equal(1, window.Count);
        Assert.True(window.TryGet(Tx(1), out _));
        Assert.False(window.TryGet(Tx(2), out _));
    }

    [Fact]
    public async Task ConcurrentRetains_KeepTheCapStrict_AndTheStructureConsistent()
    {
        TerminalOutcomeWindow window = new();
        const int max = 100;
        const int writers = 8;
        const int perWriter = 5_000;

        await Task.WhenAll(Enumerable.Range(0, writers).Select(w => Task.Run(() =>
        {
            for (int i = 0; i < perWriter; i++)
            {
                long id = (long)w * perWriter + i;
                window.Retain(Tx(id), Committed, Tx(id), max);

                // Interleave lock-free lookups with the writes, as commit/rollback replay does.
                window.TryGet(Tx(id), out _);
            }
        })));

        Assert.Equal(max, window.Count);

        // The window still behaves: a fresh retain is present, and eviction still runs.
        window.Retain(Tx(long.MaxValue), Committed, Tx(long.MaxValue), max);
        Assert.Equal(max, window.Count);
        Assert.True(window.TryGet(Tx(long.MaxValue), out _));
    }

    [Fact]
    public async Task ConcurrentRetainAndPrune_DoNotCorruptTheWindow()
    {
        TerminalOutcomeWindow window = new();
        const int max = 50;
        using CancellationTokenSource cts = new();

        Task pruner = Task.Run(() =>
        {
            while (!cts.IsCancellationRequested)
                window.PruneExpired(new HLCTimestamp(0, 100_000_000, 0), TimeSpan.FromMilliseconds(1));
        });

        for (long i = 0; i < 20_000; i++)
            window.Retain(Tx(i), Committed, Tx(i), max);

        cts.Cancel();
        await pruner;

        // Everything the aggressive pruner left is still internally consistent: within cap, and a
        // final retain both lands and evicts correctly.
        Assert.InRange(window.Count, 0, max);
        window.Retain(Tx(long.MaxValue), Committed, Tx(long.MaxValue), max);
        Assert.True(window.TryGet(Tx(long.MaxValue), out _));
        Assert.InRange(window.Count, 1, max);
    }
}
