using Kahuna.Server.KeyValues.Transactions;

namespace Kahuna.Server.Tests;

/// <summary>The proposed-delta cache hands out a registered batch once per budgeted take, so every
/// co-hosted node's single local apply can reuse the producer's decoded commands; the entry is released
/// once the budget is spent. An entry that no apply takes is released when a later registration in its
/// stripe drops its generation. These tests drive the budget explicitly, because the default budget reads
/// the live-node census, which other tests in the process move, and they drive the generations with an
/// explicit clock and one stripe.</summary>
public sealed class TestProposedDeltaCache
{
    [Fact]
    public void TakesExactlyTheBudget_ThenMisses()
    {
        ProposedDeltaCache<string[]> cache = new();
        byte[] bytes = [1, 2, 3];
        string[] commands = ["a", "b"];

        cache.Register(bytes, commands, takeBudget: 2);

        Assert.True(cache.TryTake(bytes, out string[]? first));
        Assert.Same(commands, first);
        Assert.True(cache.TryTake(bytes, out string[]? second));
        Assert.Same(commands, second);
        Assert.False(cache.TryTake(bytes, out _));
    }

    [Fact]
    public void EqualContentInADifferentArray_DoesNotHit()
    {
        ProposedDeltaCache<string[]> cache = new();
        byte[] registered = [1, 2, 3];
        byte[] lookalike = [1, 2, 3];

        cache.Register(registered, ["a"], takeBudget: 5);

        // The cache keys on array identity — a fresh materialization of the same bytes (WAL replay,
        // network receive) must decode, never inherit another array's commands.
        Assert.False(cache.TryTake(lookalike, out _));
        Assert.True(cache.TryTake(registered, out _));
    }

    [Fact]
    public void AClaim_TakesThePayload_AndRemovesTheEntryWhateverItsBudget()
    {
        ProposedDeltaCache<string[]> cache = new();
        byte[] bytes = [1, 2, 3];
        string[] commands = ["a"];

        cache.Register(bytes, commands, takeBudget: 3);

        Assert.True(cache.TryClaim(bytes, out string[]? claimed));
        Assert.Same(commands, claimed);
        Assert.False(cache.TryPeek(bytes, out _));
        Assert.False(cache.TryTake(bytes, out _));
        Assert.False(cache.TryClaim(bytes, out _));
    }

    [Fact]
    public void AForget_RemovesTheEntryWhateverItsBudget_AndIgnoresBytesWithNoEntry()
    {
        ProposedDeltaCache<string[]> cache = new();
        byte[] bytes = [1, 2, 3];
        byte[] other = [4, 5];

        cache.Register(bytes, ["a"], takeBudget: 3);
        cache.Register(other, ["b"], takeBudget: 1);

        cache.Forget(bytes);
        cache.Forget([9]);

        Assert.False(cache.TryTake(bytes, out _));
        Assert.True(cache.TryTake(other, out string[]? kept));
        Assert.Equal(["b"], kept);
    }

    private sealed class ManualClock
    {
        internal long Now;

        internal long Read() => Now;
    }

    private static ProposedDeltaCache<string[]> OneStripe(ManualClock clock, int capacity = 100, long bytes = 1_000_000, long ageMs = 1_000, int maxBytes = 10_000) =>
        new(stripeCount: 1, generationCapacity: capacity, generationBytes: bytes, generationAgeMs: ageMs, maxRegisteredBytes: maxBytes, clock: clock.Read);

    [Fact]
    public void AnEntryThatNoApplyTakes_IsDroppedAtTheSecondGenerationChange_AndKeptAtLeastTheAge()
    {
        ManualClock clock = new();
        ProposedDeltaCache<string[]> cache = OneStripe(clock);
        byte[] untaken = [1];
        byte[] second = [2];
        byte[] third = [3];
        byte[] fourth = [4];

        cache.Register(untaken, ["a"], takeBudget: 1);

        clock.Now = 999;
        cache.Register(second, ["b"], takeBudget: 1);

        // The current generation is 1,000 ms old at this registration, so it becomes the previous one.
        clock.Now = 1_000;
        cache.Register(third, ["c"], takeBudget: 1);
        Assert.True(cache.TryPeek(untaken, out _));
        Assert.True(cache.TryPeek(second, out _));

        // The next change drops the generation that holds the first two entries.
        clock.Now = 2_000;
        cache.Register(fourth, ["d"], takeBudget: 1);

        Assert.False(cache.TryTake(untaken, out _));
        Assert.False(cache.TryTake(second, out _));
        Assert.True(cache.TryTake(third, out string[]? kept));
        Assert.Equal(["c"], kept);
        Assert.True(cache.TryTake(fourth, out _));
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public void WithNoLaterRegistration_AnOldEntryStays()
    {
        ManualClock clock = new();
        ProposedDeltaCache<string[]> cache = OneStripe(clock);
        byte[] bytes = [1];

        cache.Register(bytes, ["a"], takeBudget: 1);
        clock.Now = 60_000;

        // Only a registration changes the generations; a take of an old entry still hits.
        Assert.True(cache.TryTake(bytes, out _));
    }

    [Fact]
    public void AFullGeneration_ChangesAtTheNextRegistration_WhateverItsAge()
    {
        ManualClock clock = new();
        ProposedDeltaCache<string[]> cache = OneStripe(clock, capacity: 2);
        byte[][] bytes = [[1], [2], [3], [4], [5]];

        for (int i = 0; i < bytes.Length; i++)
            cache.Register(bytes[i], [i.ToString()], takeBudget: 1);

        // Generations: {1, 2}, then {3, 4} drops nothing, then {5} drops {1, 2}.
        Assert.False(cache.TryPeek(bytes[0], out _));
        Assert.False(cache.TryPeek(bytes[1], out _));
        Assert.True(cache.TryPeek(bytes[2], out _));
        Assert.True(cache.TryPeek(bytes[3], out _));
        Assert.True(cache.TryPeek(bytes[4], out _));
        Assert.Equal(3, cache.Count);
    }

    [Fact]
    public void TakenEntries_DoNotCountTowardTheBoundsOfTheirGeneration()
    {
        ManualClock clock = new();
        ProposedDeltaCache<string[]> cache = OneStripe(clock, capacity: 2, bytes: 2);
        byte[] untaken = [9];

        cache.Register(untaken, ["kept"], takeBudget: 1);

        for (byte i = 0; i < 50; i++)
        {
            byte[] bytes = [i];
            cache.Register(bytes, ["x"], takeBudget: 1);
            Assert.True(cache.TryTake(bytes, out _));
        }

        // Each registration was taken before the next one, so the generation never held more than two entries or two
        // bytes.
        Assert.True(cache.TryTake(untaken, out string[]? kept));
        Assert.Equal(["kept"], kept);
    }

    [Fact]
    public void TheKeyBytesOfAGeneration_AreBounded()
    {
        ManualClock clock = new();
        ProposedDeltaCache<string[]> cache = OneStripe(clock, bytes: 10);
        byte[] first = new byte[6];
        byte[] second = new byte[4];
        byte[] third = new byte[1];
        byte[] fourth = new byte[10];

        cache.Register(first, ["a"], takeBudget: 1);
        cache.Register(second, ["b"], takeBudget: 1);

        // 10 bytes are in the current generation, so one more byte starts a new generation.
        cache.Register(third, ["c"], takeBudget: 1);
        Assert.True(cache.TryPeek(first, out _));

        // 1 + 10 bytes do not fit, so the generation of the first two entries is dropped.
        cache.Register(fourth, ["d"], takeBudget: 1);
        Assert.False(cache.TryPeek(first, out _));
        Assert.False(cache.TryPeek(second, out _));
        Assert.True(cache.TryPeek(third, out _));
        Assert.True(cache.TryPeek(fourth, out _));
    }

    [Fact]
    public void BytesLongerThanTheLimit_AreNotRegistered()
    {
        ManualClock clock = new();
        ProposedDeltaCache<string[]> cache = OneStripe(clock, maxBytes: 4);
        byte[] atLimit = new byte[4];
        byte[] overLimit = new byte[5];

        cache.Register(atLimit, ["a"], takeBudget: 1);
        cache.Register(overLimit, ["b"], takeBudget: 1);

        Assert.True(cache.Accepts(4));
        Assert.False(cache.Accepts(5));
        Assert.True(cache.TryTake(atLimit, out _));
        Assert.False(cache.TryPeek(overLimit, out _));
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public void AnEntryInThePreviousGeneration_IsTakenClaimedAndForgotten()
    {
        ManualClock clock = new();
        ProposedDeltaCache<string[]> cache = OneStripe(clock);
        byte[] taken = [1];
        byte[] claimed = [2];
        byte[] forgotten = [3];

        cache.Register(taken, ["a"], takeBudget: 2);
        cache.Register(claimed, ["b"], takeBudget: 2);
        cache.Register(forgotten, ["c"], takeBudget: 2);

        clock.Now = 1_000;
        cache.Register([4], ["d"], takeBudget: 1);

        Assert.True(cache.TryTake(taken, out _));
        Assert.True(cache.TryTake(taken, out _));
        Assert.False(cache.TryTake(taken, out _));
        Assert.True(cache.TryClaim(claimed, out string[]? claimedPayload));
        Assert.Equal(["b"], claimedPayload);
        Assert.False(cache.TryPeek(claimed, out _));
        cache.Forget(forgotten);
        Assert.False(cache.TryPeek(forgotten, out _));
        Assert.Equal(1, cache.Count);
    }

    [Fact]
    public void ARegistrationOfTheSameBytes_ReplacesTheEntry_AndMovesItToTheCurrentGeneration()
    {
        ManualClock clock = new();
        ProposedDeltaCache<string[]> cache = OneStripe(clock);
        byte[] bytes = [1];

        cache.Register(bytes, ["old"], takeBudget: 1);

        // The first registration goes to the previous generation.
        clock.Now = 1_000;
        cache.Register([2], ["other"], takeBudget: 1);

        clock.Now = 1_500;
        cache.Register(bytes, ["new"], takeBudget: 1);
        Assert.Equal(2, cache.Count);

        // A change of generation drops the generation of the first registration, not the entry.
        clock.Now = 2_000;
        cache.Register([3], ["third"], takeBudget: 1);

        Assert.True(cache.TryTake(bytes, out string[]? payload));
        Assert.Equal(["new"], payload);
        Assert.False(cache.TryTake(bytes, out _));
    }

    [Fact]
    public async Task ConcurrentTakes_SpendEachBudgetExactly()
    {
        const int arrays = 2_000;
        const int takers = 4;
        ProposedDeltaCache<string[]> cache = new();
        byte[][] keys = new byte[arrays][];
        string[] payload = ["p"];

        for (int i = 0; i < arrays; i++)
        {
            keys[i] = BitConverter.GetBytes(i);
            cache.Register(keys[i], payload, takeBudget: takers);
        }

        int hits = 0;
        using Barrier start = new(takers);
        Task[] tasks = new Task[takers + 1];

        for (int t = 0; t < takers; t++)
        {
            int seed = t;
            tasks[t] = Task.Factory.StartNew(() =>
            {
                start.SignalAndWait();

                for (int round = 0; round < 2; round++)
                {
                    for (int i = 0; i < arrays; i++)
                    {
                        // Each taker visits every array once in a round, from its own start.
                        int index = (i + seed * 499) % arrays;
                        if (cache.TryTake(keys[index], out _))
                            Interlocked.Increment(ref hits);
                    }
                }
            }, TaskCreationOptions.LongRunning);
        }

        // Registrations and claims of other bytes in the same stripes beside the takes.
        tasks[takers] = Task.Factory.StartNew(() =>
        {
            for (int i = 0; i < 500; i++)
            {
                byte[] other = BitConverter.GetBytes(-1 - i);
                cache.Register(other, payload, takeBudget: 1);
                Assert.True(cache.TryClaim(other, out _));
            }
        }, TaskCreationOptions.LongRunning);

        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

        // Each array was taken exactly its budget, whatever the interleaving, and every entry is released.
        Assert.Equal(arrays * takers, hits);
        Assert.Equal(0, cache.Count);
    }
}
