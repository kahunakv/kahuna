using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The backend-wide revision sweep used to re-walk every key of an idle store on every cleanup
/// interval, and on a keyspace larger than the prune-memo bound it never got a memo hit, so an idle
/// node spent a steady share of a core pruning nothing. The RocksDB backend now proves a store clean
/// with one complete cycle and skips later cycles until something could make a key prunable again:
/// a caller write or delete, a moved snapshot floor, other retention settings, or age retention
/// reaching the oldest row it may delete. These tests pin that contract and the memo admission rule
/// that keeps the sweep from emptying the memo table.
/// </summary>
public sealed class TestRevisionPruneSweepQuiescence : IDisposable
{
    private readonly string path = Path.Combine(Path.GetTempPath(), "kahuna-sweep-quiescence-" + Guid.NewGuid().ToString("N"));

    private readonly RocksDbPersistenceBackend backend;

    public TestRevisionPruneSweepQuiescence()
    {
        Directory.CreateDirectory(path);
        backend = new RocksDbPersistenceBackend(path, "v1");
    }

    public void Dispose()
    {
        backend.Dispose();
        try { Directory.Delete(path, recursive: true); } catch { /* best effort */ }
    }

    private static long NowMs => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static PersistenceRequestItem Item(string key, long revision, long physical) =>
        new(key, [1, 2, 3], revision, 0, 0, 0, 0, 0, 0, 0, physical, 0, (int)KeyValueState.Set);

    private RevisionPruneResult Sweep(int count = 0, TimeSpan age = default, int batchSize = 1000, HLCTimestamp floor = default)
    {
        Assert.True(backend.PruneKeyValueRevisions(null, count, age, batchSize, floor, Timeout.InfiniteTimeSpan, out RevisionPruneResult result));
        return result;
    }

    /// <summary>Runs sweep passes until the cursor wraps (a pass that does not pause) and sums them.</summary>
    private (int Visited, int Deleted, int Skipped) SweepCycle(int count = 0, TimeSpan age = default, int batchSize = 1000, HLCTimestamp floor = default)
    {
        int visited = 0, deleted = 0, skipped = 0;

        for (int pass = 0; pass < 1000; pass++)
        {
            RevisionPruneResult r = Sweep(count, age, batchSize, floor);
            Assert.False(r.SweepIdle);
            visited += r.KeysVisited;
            deleted += r.RevisionsDeleted;
            skipped += r.KeysSkipped;

            if (!r.BatchLimitReached)
                return (visited, deleted, skipped);
        }

        Assert.Fail("The sweep never wrapped around.");
        return default;
    }

    private void StoreKeys(string prefix, int keys, long physical)
    {
        List<PersistenceRequestItem> items = [];
        for (int k = 0; k < keys; k++)
        {
            items.Add(Item($"{prefix}/{k}", 1, physical));
            items.Add(Item($"{prefix}/{k}", 2, physical + 1));
        }

        Assert.True(backend.StoreKeyValues(items));
    }

    [Fact]
    public void CleanCycle_MakesTheNextCycleIdle()
    {
        TimeSpan age = TimeSpan.FromMinutes(10);
        StoreKeys("idle", 20, NowMs);

        (int visited, int deleted, _) = SweepCycle(age: age);
        Assert.Equal(20, visited);
        Assert.Equal(0, deleted);
        Assert.True(backend.SweepIsQuiescent);

        for (int i = 0; i < 3; i++)
        {
            RevisionPruneResult idle = Sweep(age: age);
            Assert.True(idle.SweepIdle);
            Assert.Equal(0, idle.KeysVisited);
            Assert.False(idle.BatchLimitReached);
        }
    }

    [Fact]
    public void CleanCycleAcrossPausedPasses_MakesTheNextCycleIdle()
    {
        TimeSpan age = TimeSpan.FromMinutes(10);
        StoreKeys("paused", 9, NowMs);

        // A key budget of 2 splits the cycle into five passes; the proof must span all of them.
        (int visited, _, _) = SweepCycle(age: age, batchSize: 2);
        Assert.Equal(9, visited);

        Assert.True(Sweep(age: age, batchSize: 2).SweepIdle);
    }

    [Fact]
    public void WriteDuringACycle_KeepsTheCycleFromProvingTheStoreClean()
    {
        TimeSpan age = TimeSpan.FromMinutes(10);
        StoreKeys("midcycle", 6, NowMs);

        RevisionPruneResult first = Sweep(age: age, batchSize: 2);
        Assert.True(first.BatchLimitReached);

        // Lands behind the cursor: this cycle cannot see it, so it must not call the store clean.
        Assert.True(backend.StoreKeyValues([Item("midcycle/0", 3, NowMs)]));

        SweepCycle(age: age, batchSize: 2);
        Assert.False(backend.SweepIsQuiescent);

        // The next cycle walks again, sees no further writes, and proves the store clean.
        Assert.False(Sweep(age: age, batchSize: 1000).SweepIdle);
        Assert.True(Sweep(age: age, batchSize: 1000).SweepIdle);
    }

    [Fact]
    public void Write_ReopensTheSweep_AndOldHistoryIsPruned()
    {
        TimeSpan age = TimeSpan.FromSeconds(30);
        StoreKeys("write", 5, NowMs);
        SweepCycle(age: age);
        Assert.True(Sweep(age: age).SweepIdle);

        // Old non-current history arrives: the next cycle must run and delete it.
        long old = NowMs - 120_000;
        Assert.True(backend.StoreKeyValues([Item("write/new", 1, old), Item("write/new", 2, NowMs)]));

        RevisionPruneResult reopened = Sweep(age: age);
        Assert.False(reopened.SweepIdle);
        Assert.Equal(1, reopened.RevisionsDeleted);
        Assert.Null(backend.GetKeyValueRevision("write/new", 1));

        Assert.True(Sweep(age: age).SweepIdle);
    }

    [Fact]
    public void Delete_ReopensTheSweep()
    {
        TimeSpan age = TimeSpan.FromMinutes(10);
        StoreKeys("delete", 3, NowMs);
        SweepCycle(age: age);
        Assert.True(Sweep(age: age).SweepIdle);

        Assert.True(backend.DeleteKeyValues(["delete/0"]));

        RevisionPruneResult reopened = Sweep(age: age);
        Assert.False(reopened.SweepIdle);
        Assert.Equal(2, reopened.KeysVisited);
    }

    [Fact]
    public void MovedFloorOrOtherRetention_ReopensTheSweep()
    {
        long old = NowMs - 120_000;
        Assert.True(backend.StoreKeyValues([Item("floor/a", 1, old), Item("floor/a", 2, old + 1), Item("floor/a", 3, NowMs)]));

        // A hold at the oldest revision protects revisions 1 and up.
        HLCTimestamp floor = new(0, old, 0);
        TimeSpan age = TimeSpan.FromSeconds(30);

        (_, int deleted, _) = SweepCycle(age: age, floor: floor);
        Assert.Equal(0, deleted);
        Assert.True(Sweep(age: age, floor: floor).SweepIdle);

        // The hold is released: the floor moves and the protected rows become deletable.
        RevisionPruneResult released = Sweep(age: age, floor: HLCTimestamp.Zero);
        Assert.False(released.SweepIdle);
        Assert.Equal(2, released.RevisionsDeleted);
        Assert.True(Sweep(age: age).SweepIdle);

        // Other retention settings are other inputs: no idle answer carries over.
        Assert.False(Sweep(count: 1).SweepIdle);
        Assert.True(Sweep(count: 1).SweepIdle);
    }

    [Fact]
    public async Task AgeRetention_ReopensTheSweepWhenTheOldestRowCrossesTheCutoff()
    {
        TimeSpan age = TimeSpan.FromSeconds(3);
        long written = NowMs;
        Assert.True(backend.StoreKeyValues([Item("aging/a", 1, written), Item("aging/a", 2, written + 1)]));

        (_, int deleted, _) = SweepCycle(age: age);
        Assert.Equal(0, deleted);

        // Revision 1 is younger than the window: idle until it ages past the cutoff.
        if (NowMs - written < 2_000)
            Assert.True(Sweep(age: age).SweepIdle);

        await Task.Delay(TimeSpan.FromMilliseconds(Math.Max(0, written + 3_500 - NowMs)), TestContext.Current.CancellationToken);

        RevisionPruneResult aged = Sweep(age: age);
        Assert.False(aged.SweepIdle);
        Assert.Equal(1, aged.RevisionsDeleted);
        Assert.Null(backend.GetKeyValueRevision("aging/a", 1));
        Assert.NotNull(backend.GetKeyValue("aging/a"));

        // Only the head's own row is left, so nothing can age into eligibility.
        Assert.True(Sweep(age: age).SweepIdle);
    }

    [Fact]
    public void SweepIntoAFullMemoTable_KeepsTheMemosItHas()
    {
        backend.PruneMemoCapacity = 3;
        TimeSpan age = TimeSpan.FromMinutes(10);
        StoreKeys("full", 5, NowMs);

        SweepCycle(age: age);
        Assert.Equal(3, backend.PruneMemoCount);

        // An unrelated write reopens the sweep. The three memos survived the first cycle, so their
        // keys are answered without a walk; a clearing table would have none left by now.
        Assert.True(backend.StoreKeyValues([Item("other", 1, NowMs)]));

        (int visited, _, int skipped) = SweepCycle(age: age);
        Assert.Equal(6, visited);
        Assert.Equal(3, skipped);
        Assert.Equal(3, backend.PruneMemoCount);
    }

    [Fact]
    public void TargetedWalkIntoAFullMemoTable_StillClearsIt()
    {
        backend.PruneMemoCapacity = 2;
        TimeSpan age = TimeSpan.FromMinutes(10);
        StoreKeys("targeted", 3, NowMs);

        Assert.True(backend.PruneKeyValueRevisions(["targeted/0", "targeted/1", "targeted/2"], 0, age, 1000, HLCTimestamp.Zero, out _));

        // The hot path keeps priority: the third key cleared the table and took a slot.
        Assert.Equal(1, backend.PruneMemoCount);
    }

    [Fact]
    public void SharedWalkIterator_KeepsEachKeysCurrentRowApart()
    {
        // "a" and "a~b" share the raw prefix "a~": the walk of "a" passes over "a~b"'s rows, and its
        // own current row must be told apart from the longer "a~b~CURRENT".
        long old = NowMs - 120_000;
        Assert.True(backend.StoreKeyValues([
            Item("a", 1, old), Item("a", 2, old + 1), Item("a", 3, NowMs),
            Item("a~b", 1, old), Item("a~b", 2, NowMs),
        ]));

        Assert.True(backend.PruneKeyValueRevisions(["a", "a~b"], 0, TimeSpan.FromSeconds(30), 1000, HLCTimestamp.Zero, out RevisionPruneResult result));
        Assert.Equal(3, result.RevisionsDeleted);

        Assert.Null(backend.GetKeyValueRevision("a", 1));
        Assert.Null(backend.GetKeyValueRevision("a", 2));
        Assert.NotNull(backend.GetKeyValueRevision("a", 3));
        Assert.Null(backend.GetKeyValueRevision("a~b", 1));
        Assert.NotNull(backend.GetKeyValueRevision("a~b", 2));
        Assert.Equal(3, backend.GetKeyValue("a")!.Revision);
        Assert.Equal(2, backend.GetKeyValue("a~b")!.Revision);
    }
}
