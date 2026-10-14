
using Kahuna.Server.Persistence;
using Kahuna.Server.KeyValues;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Recording a committed write into the unflushed overlay runs twice per direct write on the leader, so it
/// must not allocate beyond the dictionary's own node. The row is an 80-byte struct the dictionary cannot
/// write atomically, so it replaces the node on every update (about 120 bytes); the merge rule must not add a
/// closure or a delegate on top of that.
/// </summary>
public sealed class TestUnflushedKeyValueWritesIndexAllocation(ITestOutputHelper output)
{
    [Fact]
    public void Record_OfAnExistingKey_AllocatesNoMoreThanTheDictionaryNode()
    {
        UnflushedKeyValueWritesIndex overlay = new();
        byte[] value = [1, 2, 3];

        // Warm up: the first record inserts the key; later ones take the update path under test.
        for (int i = 0; i < 1_000; i++)
            overlay.Record("k", value, i, HLCTimestamp.Zero, HLCTimestamp.Zero, new HLCTimestamp(0, 1 + i, 0), KeyValueState.Set, false);

        const int Iterations = 10_000;

        long before = GC.GetAllocatedBytesForCurrentThread();

        for (int i = 0; i < Iterations; i++)
            overlay.Record("k", value, 1_000 + i, HLCTimestamp.Zero, HLCTimestamp.Zero, new HLCTimestamp(0, 2_000 + i, 0), KeyValueState.Set, false);

        long perCall = (GC.GetAllocatedBytesForCurrentThread() - before) / Iterations;

        output.WriteLine($"{perCall} bytes allocated per Record of an existing key");

        // The replaced node alone is about 120 bytes; a captured update lambda added a display class and a
        // delegate (about 160 bytes more) per call.
        Assert.InRange(perCall, 0, 160);

        Assert.True(overlay.TryGet("k", out UnflushedKeyValueWrite head));
        Assert.Equal(1_000 + Iterations - 1, head.Revision);
        Assert.Equal(0, head.OldestRevision);
    }

    [Fact]
    public void Record_KeepsTheNewestHeadAndTheOldestQueuedRevision()
    {
        UnflushedKeyValueWritesIndex overlay = new();

        overlay.Record("k", [1], 5, HLCTimestamp.Zero, HLCTimestamp.Zero, new HLCTimestamp(0, 50, 0), KeyValueState.Set, false);
        overlay.Record("k", [2], 7, HLCTimestamp.Zero, HLCTimestamp.Zero, new HLCTimestamp(0, 70, 0), KeyValueState.Set, false);

        // An older revision recorded late (a replay) must neither replace the head nor raise the floor.
        overlay.Record("k", [3], 3, HLCTimestamp.Zero, HLCTimestamp.Zero, new HLCTimestamp(0, 30, 0), KeyValueState.Set, false);

        Assert.True(overlay.TryGet("k", out UnflushedKeyValueWrite head));
        Assert.Equal(7, head.Revision);
        Assert.Equal([2], head.Value);
        Assert.Equal(3, head.OldestRevision);

        // Same revision: the later commit timestamp wins.
        overlay.Record("k", [4], 7, HLCTimestamp.Zero, HLCTimestamp.Zero, new HLCTimestamp(0, 71, 0), KeyValueState.Deleted, false);

        Assert.True(overlay.TryGet("k", out head));
        Assert.Equal(7, head.Revision);
        Assert.Equal(KeyValueState.Deleted, head.State);
        Assert.Equal(new HLCTimestamp(0, 71, 0), head.LastModified);
        Assert.Equal(3, head.OldestRevision);
    }
}
