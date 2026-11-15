
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

    [Fact]
    public void IsEmpty_FollowsRecordsAndConfirmedFlushes()
    {
        UnflushedKeyValueWritesIndex overlay = new();
        Assert.True(overlay.IsEmpty);

        overlay.Record("a", [1], 1, HLCTimestamp.Zero, HLCTimestamp.Zero, new HLCTimestamp(0, 10, 0), KeyValueState.Set, false);
        Assert.False(overlay.IsEmpty);

        // A second head of the same key and a second key: two keys, three records.
        overlay.Record("a", [2], 2, HLCTimestamp.Zero, HLCTimestamp.Zero, new HLCTimestamp(0, 20, 0), KeyValueState.Set, false);
        overlay.Record("b", [3], 1, HLCTimestamp.Zero, HLCTimestamp.Zero, new HLCTimestamp(0, 30, 0), KeyValueState.Set, false);

        // A flush of an older revision of "a" advances its floor and keeps the key.
        overlay.RemoveFlushed("a", 1, new HLCTimestamp(0, 10, 0));
        Assert.True(overlay.TryGet("a", out _));
        Assert.False(overlay.IsEmpty);

        overlay.RemoveFlushed("a", 2, new HLCTimestamp(0, 20, 0));
        Assert.False(overlay.TryGet("a", out _));
        Assert.False(overlay.IsEmpty);

        // A repeated confirmation of a key that is already gone must not count twice.
        overlay.RemoveFlushed("a", 2, new HLCTimestamp(0, 20, 0));
        Assert.False(overlay.IsEmpty);

        overlay.RemoveFlushed("b", 1, new HLCTimestamp(0, 30, 0));
        Assert.True(overlay.IsEmpty);

        overlay.Record("b", [4], 2, HLCTimestamp.Zero, HLCTimestamp.Zero, new HLCTimestamp(0, 40, 0), KeyValueState.Set, false);
        Assert.False(overlay.IsEmpty);
    }

    [Fact]
    public async Task IsEmpty_IsExactAfterConcurrentRecordsAndFlushes()
    {
        UnflushedKeyValueWritesIndex overlay = new();
        const int Keys = 64;
        const int Rounds = 2_000;

        // Several writers record and confirm the same keys, so adds race updates and removals race both.
        Task[] workers = new Task[8];
        for (int w = 0; w < workers.Length; w++)
        {
            int worker = w;
            workers[w] = Task.Run(() =>
            {
                for (int i = 0; i < Rounds; i++)
                {
                    string key = "k" + ((i + worker) % Keys);
                    HLCTimestamp stamp = new(0, 1 + i, (uint)worker);
                    overlay.Record(key, [1], i, HLCTimestamp.Zero, HLCTimestamp.Zero, stamp, KeyValueState.Set, false);
                    overlay.RemoveFlushed(key, i, stamp);
                }
            }, TestContext.Current.CancellationToken);
        }

        await Task.WhenAll(workers);

        // Drain whatever a lost race left behind, then the count must agree with the contents.
        for (int k = 0; k < Keys; k++)
        {
            while (overlay.TryGet("k" + k, out UnflushedKeyValueWrite left))
                overlay.RemoveFlushed("k" + k, left.Revision, left.LastModified);
        }

        Assert.True(overlay.IsEmpty);
    }

    [Fact]
    public void LockOverlay_IsEmpty_FollowsRecordsAndConfirmedFlushes()
    {
        UnflushedLockWritesIndex overlay = new();
        Assert.True(overlay.IsEmpty);

        overlay.Record("r", [1], 1, HLCTimestamp.Zero, HLCTimestamp.Zero, new HLCTimestamp(0, 10, 0), Kahuna.Server.Locks.Data.LockState.Locked);
        overlay.Record("r", [1], 2, HLCTimestamp.Zero, HLCTimestamp.Zero, new HLCTimestamp(0, 20, 0), Kahuna.Server.Locks.Data.LockState.Locked);
        Assert.False(overlay.IsEmpty);

        // A flush of the older head leaves the newer one covered.
        overlay.RemoveFlushed("r", 1, new HLCTimestamp(0, 10, 0));
        Assert.False(overlay.IsEmpty);

        overlay.RemoveFlushed("r", 2, new HLCTimestamp(0, 20, 0));
        Assert.True(overlay.IsEmpty);

        overlay.RemoveFlushed("r", 2, new HLCTimestamp(0, 20, 0));
        Assert.True(overlay.IsEmpty);
    }
}
