using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions;

using Identity = Kahuna.Server.KeyValues.Transactions.PreparedIntentStore.SettledIdentity;
using Memory = Kahuna.Server.KeyValues.Transactions.PreparedIntentStore.SettledIdentityMemory;
using Entry = Kahuna.Server.KeyValues.Transactions.PreparedIntentStore.SettledIdentityEntry;

namespace Kahuna.Server.Tests;

/// <summary>
/// The store of the settled identities of one ledger slice. It keeps the identities in many small maps, so no
/// array grows with the number of identities, and it lets a capture and the physical sweep stop after each
/// small map and resume later. These tests check the structure alone; the verdicts it feeds are covered by
/// <see cref="TestSettledPrepareReplay"/>.
/// </summary>
public sealed class TestSettledIdentityMemory
{
    // Every record in these tests is inside the window unless a test says otherwise.
    private const long NoBound = long.MinValue;

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static Identity Id(int i) => new(Ts(1_000 + i), 1, $"ranged1/accounts/k{i:D12}");

    private static Memory Filled(int count, long settledAtL = 5_000)
    {
        Memory memory = new();
        for (int i = 0; i < count; i++)
            Assert.True(memory.Record(Id(i), Ts(settledAtL), replace: false, NoBound));
        return memory;
    }

    /// <summary>Walks the whole memory through <see cref="Memory.CopyNext"/> with the given step capacity and
    /// runs <paramref name="betweenSteps"/> after each step, as a capture that releases the gate does.</summary>
    private static List<Entry> Walk(Memory memory, long retainedFromL = NoBound, int capacity = 1_024, Action<int>? betweenSteps = null)
    {
        List<Entry> all = [];
        List<Entry> chunk = new(capacity);
        long position = 0;
        bool more = true;
        int step = 0;

        while (more)
        {
            more = memory.CopyNext(ref position, chunk, retainedFromL);
            all.AddRange(chunk);
            chunk.Clear();
            betweenSteps?.Invoke(step++);
        }

        return all;
    }

    [Fact]
    public void AnIdentity_EqualsOnlyTheSameTransactionEpochAndKey_AndKeepsItsSize()
    {
        Identity identity = new(new HLCTimestamp(2, 1_000, 7), 3, "ranged1/k");

        Assert.Equal(identity, new Identity(new HLCTimestamp(2, 1_000, 7), 3, new string("ranged1/k".AsSpan())));
        Assert.Equal(identity.GetHashCode(), new Identity(new HLCTimestamp(2, 1_000, 7), 3, new string("ranged1/k".AsSpan())).GetHashCode());

        Assert.NotEqual(identity, new Identity(new HLCTimestamp(3, 1_000, 7), 3, "ranged1/k"));
        Assert.NotEqual(identity, new Identity(new HLCTimestamp(2, 1_001, 7), 3, "ranged1/k"));
        Assert.NotEqual(identity, new Identity(new HLCTimestamp(2, 1_000, 8), 3, "ranged1/k"));
        Assert.NotEqual(identity, new Identity(new HLCTimestamp(2, 1_000, 7), 4, "ranged1/k"));
        Assert.NotEqual(identity, new Identity(new HLCTimestamp(2, 1_000, 7), 3, "ranged1/K"));

        Assert.Equal(new HLCTimestamp(2, 1_000, 7), identity.TransactionId);
        Assert.Equal(3, identity.Epoch);
        Assert.Equal("ranged1/k", identity.Key);

        // The stored hash must not grow the entry of the map: a million identities are held for each
        // partition under load.
        Assert.Equal(40, System.Runtime.CompilerServices.Unsafe.SizeOf<Identity>());
    }

    [Fact]
    public void EveryIdentityIsFound_AfterTheShardsSplit()
    {
        const int count = 100_000;
        Memory memory = Filled(count);

        Assert.Equal(count, memory.Count);

        // A shard holds at most its split size, so this many identities need at least this many shards.
        Assert.True(memory.ShardCount >= count / (Memory.MaxShardEntries + 1), $"{memory.ShardCount} shards");

        for (int i = 0; i < count; i++)
        {
            Assert.True(memory.TryGetValue(Id(i), out HLCTimestamp settledAt), $"identity {i} is missing");
            Assert.Equal(Ts(5_000), settledAt);
        }

        Assert.False(memory.TryGetValue(Id(count), out _));
        Assert.False(memory.TryGetValue(new Identity(Ts(1_000), 2, "ranged1/accounts/k000000000000"), out _));
    }

    [Fact]
    public void TheLegsOfOneLargeTransaction_SpreadOverTheShards()
    {
        // One transaction that wrote 50,000 keys: every identity has the same transaction id and epoch. The
        // shards must still split, or one map would hold them all.
        const int legs = 50_000;
        Memory memory = new();

        for (int i = 0; i < legs; i++)
            memory.Record(new Identity(Ts(9_000), 3, $"bulk/row{i:D9}"), Ts(9_001), replace: false, NoBound);

        Assert.Equal(legs, memory.Count);
        Assert.True(memory.ShardCount >= legs / (Memory.MaxShardEntries + 1), $"{memory.ShardCount} shards");

        for (int i = 0; i < legs; i++)
            Assert.True(memory.TryGetValue(new Identity(Ts(9_000), 3, $"bulk/row{i:D9}"), out _));
    }

    [Fact]
    public void Record_KeepsTheTimeBaseOfAnIdentityInsideTheWindow_UnlessToldToReplaceIt()
    {
        Memory memory = new();
        Identity identity = Id(1);

        Assert.True(memory.Record(identity, Ts(5_000), replace: false, retainedFromL: 1_000));

        // Held and inside the window: the first time base stays.
        Assert.False(memory.Record(identity, Ts(7_000), replace: false, retainedFromL: 1_000));
        Assert.True(memory.TryGetValue(identity, out HLCTimestamp kept));
        Assert.Equal(Ts(5_000), kept);

        Assert.False(memory.Record(identity, Ts(8_000), replace: true, retainedFromL: 1_000));
        Assert.True(memory.TryGetValue(identity, out HLCTimestamp replaced));
        Assert.Equal(Ts(8_000), replaced);

        Assert.Equal(1, memory.Count);
    }

    [Fact]
    public void Record_OverAnIdentityOutsideTheWindow_StoresTheNewTimeBase_AsItDoesAfterTheSweep()
    {
        // An identity before the bound is absent to every verdict. A record over it must give the result it
        // gives when the sweep already removed the entry, or two replicas that swept at different moments
        // would remember different time bases.
        Identity identity = Id(1);

        Memory notSwept = new();
        notSwept.Record(identity, Ts(5_000), replace: false, NoBound);
        Assert.False(notSwept.Record(identity, Ts(90_000), replace: false, retainedFromL: 30_000));

        Memory swept = new();
        swept.Record(identity, Ts(5_000), replace: false, NoBound);
        swept.BeginSweep();
        swept.SweepStep(retainedFromL: 30_000, out int removed, out _);
        Assert.Equal(1, removed);
        Assert.True(swept.Record(identity, Ts(90_000), replace: false, retainedFromL: 30_000));

        Assert.True(notSwept.TryGetValue(identity, out HLCTimestamp a));
        Assert.True(swept.TryGetValue(identity, out HLCTimestamp b));
        Assert.Equal(Ts(90_000), a);
        Assert.Equal(a, b);
    }

    [Fact]
    public void AWalk_CopiesEveryIdentityOnce_InStepsOfTheChunkCapacity()
    {
        const int count = 60_000;
        Memory memory = Filled(count);

        int largestStep = 0;
        List<Entry> chunk = new(1_024);
        HashSet<string> seen = new(StringComparer.Ordinal);
        long position = 0;
        bool more = true;
        int steps = 0;

        while (more)
        {
            more = memory.CopyNext(ref position, chunk, NoBound);
            largestStep = Math.Max(largestStep, chunk.Count);

            foreach (Entry entry in chunk)
                Assert.True(seen.Add(entry.Key), $"{entry.Key} was copied twice");

            chunk.Clear();
            steps++;
        }

        Assert.Equal(count, seen.Count);

        // No step passes the capacity of the list, so the list does not grow and the gate is held for a
        // bounded number of entries.
        Assert.True(largestStep <= 1_024, $"a step copied {largestStep} entries");
        Assert.True(steps >= count / 1_024, $"{steps} steps");
    }

    [Fact]
    public void AWalk_ThatMeetsSplitsBetweenItsSteps_StillCopiesEveryEarlierIdentityOnce()
    {
        // The capture releases the gate between steps, and the records applied meanwhile split shards on both
        // sides of the walk's position. An identity held from before the walk must be copied once; one
        // recorded during the walk at most once.
        const int before = 40_000;
        const int perStep = 2_000;
        Memory memory = Filled(before);

        int added = 0;
        List<Entry> copied = Walk(memory, betweenSteps: _ =>
        {
            for (int i = 0; i < perStep; i++)
                memory.Record(Id(before + added++), Ts(5_000), replace: false, NoBound);
        });

        Assert.True(added > before, "the memory must at least double during the walk, so that its shards split");

        Dictionary<string, int> copies = new(StringComparer.Ordinal);
        foreach (Entry entry in copied)
            copies[entry.Key] = copies.GetValueOrDefault(entry.Key) + 1;

        for (int i = 0; i < before; i++)
            Assert.Equal(1, copies.GetValueOrDefault(Id(i).Key));

        Assert.All(copies.Values, n => Assert.Equal(1, n));
    }

    [Fact]
    public void AWalk_LeavesOutTheIdentitiesBeforeItsBound()
    {
        Memory memory = new();
        for (int i = 0; i < 5_000; i++)
            memory.Record(Id(i), Ts(i < 2_000 ? 10_000 : 80_000), replace: false, NoBound);

        List<Entry> retained = Walk(memory, retainedFromL: 20_000);

        Assert.Equal(3_000, retained.Count);
        Assert.All(retained, entry => Assert.Equal(Ts(80_000), entry.SettledAt));
    }

    [Fact]
    public void TheSweep_VisitsOneShardAtAStep_AndRemovesOnlyTheIdentitiesBeforeItsBound()
    {
        const int old = 30_000;
        const int recent = 10_000;

        Memory memory = new();
        long oldKeyChars = 0;

        for (int i = 0; i < old + recent; i++)
        {
            Identity identity = Id(i);
            bool isOld = i < old;
            memory.Record(identity, Ts(isOld ? 10_000 : 80_000), replace: false, NoBound);

            if (isOld)
                oldKeyChars += identity.Key.Length;
        }

        int shards = memory.ShardCount;
        Assert.False(memory.SweepInProgress);

        // No sweep was started: a step does nothing.
        memory.SweepStep(retainedFromL: 20_000, out int idle, out _);
        Assert.Equal(0, idle);

        memory.BeginSweep();

        int steps = 0;
        int removedTotal = 0;
        long removedChars = 0;
        int largestStep = 0;

        while (memory.SweepInProgress)
        {
            memory.SweepStep(retainedFromL: 20_000, out int removed, out long chars);
            removedTotal += removed;
            removedChars += chars;
            largestStep = Math.Max(largestStep, removed);
            steps++;
        }

        // One step for each shard: the work under the gate is that of one small map.
        Assert.Equal(shards, steps);
        Assert.True(largestStep <= Memory.MaxShardEntries + 1, $"one step removed {largestStep} entries");

        Assert.Equal(old, removedTotal);
        Assert.Equal(oldKeyChars, removedChars);
        Assert.Equal(recent, memory.Count);

        for (int i = 0; i < old; i++)
            Assert.False(memory.TryGetValue(Id(i), out _));

        for (int i = old; i < old + recent; i++)
            Assert.True(memory.TryGetValue(Id(i), out _));

        // A walk after the sweep copies the survivors, each once.
        Assert.Equal(recent, Walk(memory).Count);
    }

    [Fact]
    public void TheSweep_ThatLeavesNothing_DropsTheDirectoryOfTheBurst()
    {
        Memory memory = Filled(50_000, settledAtL: 10_000);
        Assert.True(memory.ShardCount > 32);

        memory.BeginSweep();
        while (memory.SweepInProgress)
            memory.SweepStep(retainedFromL: 20_000, out _, out _);

        Assert.Equal(0, memory.Count);
        Assert.Equal(1, memory.ShardCount);

        // The memory serves the next load as a new one does.
        for (int i = 0; i < 5_000; i++)
            Assert.True(memory.Record(Id(i), Ts(90_000), replace: false, NoBound));

        Assert.Equal(5_000, Walk(memory).Count);
    }

    [Fact]
    public void BeginSweep_DuringASweep_LetsTheSweepInProgressContinue()
    {
        Memory memory = Filled(20_000, settledAtL: 10_000);
        int shards = memory.ShardCount;

        memory.BeginSweep();
        memory.SweepStep(retainedFromL: 20_000, out _, out _);

        // A second start must not send the sweep back to the first shard: at a low record rate the sweep
        // would then visit the first shards again and again and never reach the last ones.
        memory.BeginSweep();

        int steps = 1;
        while (memory.SweepInProgress)
        {
            memory.SweepStep(retainedFromL: 20_000, out _, out _);
            steps++;
        }

        Assert.Equal(shards, steps);
        Assert.Equal(0, memory.Count);
    }
}
