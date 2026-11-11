using System.Diagnostics;
using System.Security.Cryptography;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// A deleted key used to keep rows on disk for ever: the revision prune never touched the rows of a
/// key's current revision, and the current revision of a deleted key is its delete record. A key
/// space deleted as a whole therefore gave back almost nothing. The prune also wrote one synced batch
/// per key, so what it could delete it deleted at tens of rows per pass, and the store had no
/// compaction that starts from deletes, so even the deleted rows kept their space.
/// <para>
/// The RocksDB backend now releases a deleted key once age retention has passed its delete record
/// and no snapshot hold is older than it: every row of the key except the current row goes. The
/// current row stays, because it carries the revision a later write must continue from. These tests
/// pin that rule, the batched writes, and the return of the space.
/// </para>
/// </summary>
public sealed class TestDeletedKeyRevisionPrune : IDisposable
{
    private static readonly TimeSpan Age = TimeSpan.FromHours(1);

    private readonly string path = Path.Combine(Path.GetTempPath(), "kahuna-deleted-key-prune-" + Guid.NewGuid().ToString("N"));

    private readonly RocksDbPersistenceBackend backend;

    public TestDeletedKeyRevisionPrune()
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

    private static long HoursAgo(double hours) => NowMs - (long)TimeSpan.FromHours(hours).TotalMilliseconds;

    private static HLCTimestamp At(long physical) => new(0, physical, 0);

    private static PersistenceRequestItem Value(string key, long revision, long physical, byte[]? value = null, bool noRevision = false) =>
        new(key, value ?? [1, 2, 3], revision, 0, 0, 0, 0, 0, 0, 0, physical, 0, (int)KeyValueState.Set, noRevision);

    private static PersistenceRequestItem DeleteRecord(string key, long revision, long physical) =>
        new(key, null, revision, 0, 0, 0, 0, 0, 0, 0, physical, 0, (int)KeyValueState.Deleted);

    private RevisionPruneResult Prune(IReadOnlyCollection<string>? keys, TimeSpan age, int count = 0, int batchSize = 10_000, HLCTimestamp floor = default)
    {
        Assert.True(backend.PruneKeyValueRevisions(keys, count, age, batchSize, floor, Timeout.InfiniteTimeSpan, out RevisionPruneResult result));
        return result;
    }

    private void AssertOnlyTheDeleteRecordIsLeft(string key, long deleteRevision)
    {
        ReadOnlyKeyValueEntry? current = backend.GetKeyValue(key);
        Assert.NotNull(current);
        Assert.Equal(KeyValueState.Deleted, current.State);
        Assert.Equal(deleteRevision, current.Revision);

        for (long revision = 0; revision <= deleteRevision; revision++)
            Assert.Null(backend.GetKeyValueRevision(key, revision));
    }

    [Fact]
    public void DeletedKeyPastTheRetentionAge_KeepsOnlyItsCurrentRow()
    {
        const string key = "dropped/a";
        long deletedAt = HoursAgo(2);

        Assert.True(backend.StoreKeyValues([
            Value(key, 0, HoursAgo(4)),
            Value(key, 1, HoursAgo(3)),
            DeleteRecord(key, 2, deletedAt)
        ]));

        RevisionPruneResult result = Prune([key], Age);

        // Both older values and the delete record's own history row.
        Assert.Equal(3, result.RevisionsDeleted);
        Assert.Equal(1, result.DeleteRecordsReleased);
        Assert.False(result.BatchLimitReached);
        AssertOnlyTheDeleteRecordIsLeft(key, deleteRevision: 2);

        // The boundary of every cut below the delete is gone, and the floor says so.
        Assert.Equal(At(deletedAt), backend.GetPrunedHistoryFloor());

        // Nothing is left to do for the key: the next visit is answered without a walk.
        RevisionPruneResult again = Prune([key], Age);
        Assert.Equal(0, again.RevisionsDeleted);
        Assert.Equal(1, again.KeysSkipped);
    }

    [Fact]
    public void WriteAfterTheRelease_ContinuesAboveTheDeleteRecord()
    {
        const string key = "dropped/recreated";

        Assert.True(backend.StoreKeyValues([Value(key, 0, HoursAgo(3)), DeleteRecord(key, 1, HoursAgo(2))]));
        Assert.Equal(1, Prune([key], Age).DeleteRecordsReleased);

        // The current row is what tells every later write where the key's revisions stand: the next
        // revision wins over it.
        long recreatedAt = NowMs;
        Assert.True(backend.StoreKeyValues([Value(key, 2, recreatedAt, [9])]));

        ReadOnlyKeyValueEntry? current = backend.GetKeyValue(key);
        Assert.NotNull(current);
        Assert.Equal(KeyValueState.Set, current.State);
        Assert.Equal(2, current.Revision);

        // A read between the delete and the new value finds no version: the key did not exist then.
        Assert.Null(backend.GetKeyValueRevisionAtOrBefore(key, 1, At(recreatedAt - 1)));
    }

    [Fact]
    public void StaleRecordAfterTheRelease_DoesNotReplaceTheDeleteRecord()
    {
        const string key = "dropped/stale";

        Assert.True(backend.StoreKeyValues([Value(key, 0, HoursAgo(3)), DeleteRecord(key, 1, HoursAgo(2))]));
        Assert.Equal(1, Prune([key], Age).DeleteRecordsReleased);

        // A delayed copy of the value from before the delete: with the current row removed too, it
        // would have found no head to lose against and brought the deleted value back.
        Assert.True(backend.StoreKeyValues([Value(key, 0, HoursAgo(3))]));

        ReadOnlyKeyValueEntry? current = backend.GetKeyValue(key);
        Assert.NotNull(current);
        Assert.Equal(KeyValueState.Deleted, current.State);
        Assert.Equal(1, current.Revision);
    }

    [Fact]
    public void DeleteRecordInsideTheRetentionAge_KeepsItsHistoryRow_UntilTheAgePassesIt()
    {
        const string key = "dropped/recent";

        Assert.True(backend.StoreKeyValues([Value(key, 0, HoursAgo(3)), DeleteRecord(key, 1, NowMs)]));

        // The old value was what a read saw until the delete, a moment ago: reads inside the
        // retention age still need it. The delete record is new and keeps its row too.
        RevisionPruneResult first = Prune([key], Age);
        Assert.Equal(0, first.RevisionsDeleted);
        Assert.Equal(0, first.DeleteRecordsReleased);
        Assert.NotNull(backend.GetKeyValueRevision(key, 0));
        Assert.NotNull(backend.GetKeyValueRevision(key, 1));

        // Until the age reaches the delete record, the key costs no walk.
        Assert.Equal(1, Prune([key], Age).KeysSkipped);

        // A shorter age stands in for the passing of time: the memo must wake the key by itself.
        Thread.Sleep(20);
        RevisionPruneResult later = Prune([key], TimeSpan.FromMilliseconds(5));
        Assert.Equal(0, later.KeysSkipped);
        Assert.Equal(2, later.RevisionsDeleted);
        Assert.Equal(1, later.DeleteRecordsReleased);
        AssertOnlyTheDeleteRecordIsLeft(key, deleteRevision: 1);
    }

    [Fact]
    public void SweepWakesForADeleteRecord_WhenTheAgeReachesIt()
    {
        // Long enough that both sweeps below run inside it, short enough to wait out.
        TimeSpan age = TimeSpan.FromSeconds(2);
        long deletedAt = NowMs;

        Assert.True(backend.StoreKeyValues([DeleteRecord("dropped/sweep-wake", 0, deletedAt), Value("live/sweep-wake", 0, deletedAt)]));

        // One clean cycle proves the store clean; the next cycle is skipped.
        Assert.False(Prune(null, age).SweepIdle);
        Assert.True(Prune(null, age).SweepIdle);

        // The delete record is the row whose age ends that proof: same settings, no write, only time.
        while (NowMs - deletedAt <= (long)age.TotalMilliseconds + 50)
            Thread.Sleep(50);

        RevisionPruneResult woken = Prune(null, age);
        Assert.False(woken.SweepIdle);
        Assert.Equal(1, woken.DeleteRecordsReleased);
        AssertOnlyTheDeleteRecordIsLeft("dropped/sweep-wake", deleteRevision: 0);
        Assert.NotNull(backend.GetKeyValueRevision("live/sweep-wake", 0));
    }

    [Fact]
    public void HoldOlderThanTheDelete_KeepsTheRowsItReads_UntilTheFloorMoves()
    {
        const string key = "dropped/held";
        long valueAt = HoursAgo(4);
        long deletedAt = HoursAgo(2);

        Assert.True(backend.StoreKeyValues([Value(key, 0, valueAt), DeleteRecord(key, 1, deletedAt)]));

        // A hold between the value and the delete reads the value: nothing of the key may go.
        HLCTimestamp holdBeforeTheDelete = At(HoursAgo(3));
        RevisionPruneResult held = Prune([key], Age, floor: holdBeforeTheDelete);
        Assert.Equal(0, held.RevisionsDeleted);
        Assert.Equal(0, held.DeleteRecordsReleased);
        Assert.Equal(0, held.FloorViolations);
        Assert.NotNull(backend.GetKeyValueRevision(key, 0));
        Assert.NotNull(backend.GetKeyValueRevision(key, 1));

        // The same floor again: the memo knows only a moved floor can change the answer.
        RevisionPruneResult skipped = Prune([key], Age, floor: holdBeforeTheDelete);
        Assert.Equal(1, skipped.KeysSkipped);
        Assert.Equal(1, skipped.KeysFloorBlocked);

        // A hold at or after the delete reads the delete record, which is the current row.
        RevisionPruneResult released = Prune([key], Age, floor: At(deletedAt));
        Assert.Equal(2, released.RevisionsDeleted);
        Assert.Equal(1, released.DeleteRecordsReleased);
        Assert.Equal(0, released.FloorViolations);
        AssertOnlyTheDeleteRecordIsLeft(key, deleteRevision: 1);
    }

    [Fact]
    public void DeleteBudget_NeverLeavesTheKeyWithoutItsDeleteRecordRow_WhileOlderRowsRemain()
    {
        const string key = "dropped/budget";
        const long deleteRevision = 5;

        List<PersistenceRequestItem> rows = [];
        for (long revision = 0; revision < deleteRevision; revision++)
            rows.Add(Value(key, revision, HoursAgo(9 - revision)));
        rows.Add(DeleteRecord(key, deleteRevision, HoursAgo(2)));
        Assert.True(backend.StoreKeyValues(rows));

        int passes = 0;
        int deleted = 0;
        RevisionPruneResult result;

        do
        {
            result = Prune([key], Age, batchSize: 2);
            deleted += result.RevisionsDeleted;
            passes++;

            // The delete record's own row is the last to go: while any older row is still there, an
            // as-of read that walks the history must still meet the delete record above it.
            bool anyOlderRow = false;
            for (long revision = 0; revision < deleteRevision; revision++)
                anyOlderRow |= backend.GetKeyValueRevision(key, revision) is not null;

            if (anyOlderRow)
                Assert.NotNull(backend.GetKeyValueRevision(key, deleteRevision));

            Assert.True(passes < 20, "the prune never finished the key");
        }
        while (result.BatchLimitReached);

        Assert.Equal(6, deleted);
        Assert.True(passes > 1, "a delete budget of two rows must take more than one pass for six rows");
        Assert.Equal(1, result.DeleteRecordsReleased);
        AssertOnlyTheDeleteRecordIsLeft(key, deleteRevision);
    }

    [Fact]
    public void DeleteBudgetOfOneRow_StillReleasesTheKey()
    {
        const string key = "dropped/budget-one";

        Assert.True(backend.StoreKeyValues([
            Value(key, 0, HoursAgo(3), noRevision: true),
            DeleteRecord(key, 1, HoursAgo(2))
        ]));

        // The delete record's history row and the no-revision marker leave as one unit of two rows.
        int released = 0;
        for (int pass = 0; pass < 10; pass++)
        {
            RevisionPruneResult result = Prune([key], Age, batchSize: 1);
            released += result.DeleteRecordsReleased;
            if (!result.BatchLimitReached)
                break;
        }

        Assert.Equal(1, released);
        AssertOnlyTheDeleteRecordIsLeft(key, deleteRevision: 1);
    }

    [Fact]
    public void CountOnlyRetention_NeverReleasesADeletedKey()
    {
        const string key = "dropped/count-only";

        Assert.True(backend.StoreKeyValues([
            Value(key, 0, HoursAgo(5)),
            Value(key, 1, HoursAgo(4)),
            DeleteRecord(key, 2, HoursAgo(3))
        ]));

        // Without an age, nothing says how far back the store still answers.
        RevisionPruneResult result = Prune([key], age: TimeSpan.Zero, count: 2);
        Assert.Equal(1, result.RevisionsDeleted);
        Assert.Equal(0, result.DeleteRecordsReleased);
        Assert.Null(backend.GetKeyValueRevision(key, 0));
        Assert.NotNull(backend.GetKeyValueRevision(key, 1));
        Assert.NotNull(backend.GetKeyValueRevision(key, 2));
    }

    [Fact]
    public void LiveKeys_AndKeysThatShareTheDeletedKeysPrefix_AreNotTouched()
    {
        // "dropped/x~1" is a key of its own whose rows sort inside the row range of "dropped/x".
        Assert.True(backend.StoreKeyValues([
            Value("dropped/x", 0, HoursAgo(3)),
            DeleteRecord("dropped/x", 1, HoursAgo(2)),
            Value("dropped/x~1", 0, HoursAgo(3)),
            Value("dropped/x~1", 1, HoursAgo(2)),
            Value("dropped/y", 0, NowMs)
        ]));

        RevisionPruneResult result = Prune(null, Age);

        // dropped/x loses both rows; dropped/x~1 loses the row replaced before the cutoff, as any live key does.
        Assert.Equal(3, result.RevisionsDeleted);
        Assert.Equal(1, result.DeleteRecordsReleased);
        AssertOnlyTheDeleteRecordIsLeft("dropped/x", deleteRevision: 1);

        Assert.Equal(1, backend.GetKeyValue("dropped/x~1")!.Revision);
        Assert.NotNull(backend.GetKeyValueRevision("dropped/x~1", 1));
        Assert.NotNull(backend.GetKeyValueRevision("dropped/y", 0));
    }

    /// <summary>
    /// The no-revision marker of a released key must go with its rows. Left behind, it made an as-of
    /// image fail closed once the key was written again: the image saw a no-revision write at or
    /// before the cut and no surviving revision to prove it was not the value at the cut.
    /// </summary>
    [Fact]
    public void ReleasedKeyWithANoRevisionPast_DoesNotFailALaterAsOfImage()
    {
        const string key = "dropped/norev";
        long deletedAt = HoursAgo(3);

        Assert.True(backend.StoreKeyValues([
            Value(key, 0, HoursAgo(5), noRevision: true),
            DeleteRecord(key, 1, deletedAt)
        ]));

        RevisionPruneResult result = Prune([key], Age);
        Assert.Equal(2, result.RevisionsDeleted);
        Assert.Equal(1, result.DeleteRecordsReleased);
        Assert.Equal(At(deletedAt), backend.GetPrunedHistoryFloor());

        // The key is written again, then an image is cut between the delete and the new value.
        Assert.True(backend.StoreKeyValues([Value(key, 2, NowMs, [7])]));

        HLCTimestamp cut = At(HoursAgo(2));
        Assert.True(cut.CompareTo(backend.GetPrunedHistoryFloor()) >= 0);

        backend.CreateCheckpointAsOf(Path.Combine(path, "asof"), appliedIndex: 3, cut, TestContext.Current.CancellationToken);

        using RocksDbPersistenceBackend image = new(path, "asof");
        Assert.Null(image.GetKeyValue(key));
    }

    [Fact]
    public void Sweep_ReleasesAWholeDeletedKeySpace_InAFewWriteBatches()
    {
        const int deletedKeys = 5_000;
        const int liveKeys = 50;

        long valueAt = HoursAgo(4);
        long deletedAt = HoursAgo(2);

        List<PersistenceRequestItem> rows = new(1024);
        for (int k = 0; k < deletedKeys; k++)
        {
            rows.Add(Value($"db-7/{k:D6}", 0, valueAt));
            rows.Add(DeleteRecord($"db-7/{k:D6}", 1, deletedAt));

            if (rows.Count >= 1024)
            {
                Assert.True(backend.StoreKeyValues(rows));
                rows.Clear();
            }
        }

        for (int k = 0; k < liveKeys; k++)
            rows.Add(Value($"db-8/{k:D6}", 0, valueAt));

        Assert.True(backend.StoreKeyValues(rows));

        long batchesBefore = backend.PruneWriteBatches;

        int passes = 0;
        int deleted = 0;
        int released = 0;
        RevisionPruneResult result;

        do
        {
            result = Prune(null, Age, batchSize: 10_000);
            deleted += result.RevisionsDeleted;
            released += result.DeleteRecordsReleased;
            Assert.True(++passes < 10, "the sweep took too many passes");
        }
        while (result.BatchLimitReached);

        Assert.Equal(2 * deletedKeys, deleted);
        Assert.Equal(deletedKeys, released);

        // One synced batch per key was the old cost: 5,000 batches for this key space.
        long batches = backend.PruneWriteBatches - batchesBefore;
        Assert.InRange(batches, 1, 16);

        AssertOnlyTheDeleteRecordIsLeft("db-7/000000", deleteRevision: 1);
        AssertOnlyTheDeleteRecordIsLeft($"db-7/{deletedKeys - 1:D6}", deleteRevision: 1);
        Assert.NotNull(backend.GetKeyValueRevision("db-8/000000", 0));

        // The cycle left nothing behind, so the next one is skipped.
        Assert.True(Prune(null, Age).SweepIdle);
    }

    /// <summary>
    /// The end of the path: the bytes come back. The values sit in the bottom level, the key space is
    /// deleted, and no other write arrives. The sweep's deletes must leave the memtable by themselves,
    /// and the table file they land in must start a compaction by itself.
    /// <para>
    /// In the second case the delete records are already in a table file of their own when the prune
    /// runs, as after a large drop: that file holds no deletes and nothing marks it, so the file with
    /// the prune's deletes must take it along.
    /// </para>
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DeletedKeySpace_GivesItsDiskSpaceBack_WithNoOtherWrites(bool deleteRecordsInATableFile)
    {
        const int keys = 2_000;
        const int valueBytes = 4 * 1024;

        long valueAt = HoursAgo(4);
        long deletedAt = HoursAgo(2);

        List<PersistenceRequestItem> rows = new(256);
        for (int k = 0; k < keys; k++)
        {
            // Random bytes, so the table compression cannot hide the size.
            rows.Add(Value($"db-9/{k:D6}", 0, valueAt, RandomNumberGenerator.GetBytes(valueBytes)));

            if (rows.Count >= 256)
            {
                Assert.True(backend.StoreKeyValues(rows));
                rows.Clear();
            }
        }

        Assert.True(backend.StoreKeyValues(rows));
        rows.Clear();

        // Every value is stored twice (the current row and its history row), in the bottom level.
        backend.FlushMemTablesForTesting();
        backend.CompactKeyValuesForTesting();

        long before = backend.GetKeyValueTableBytesForTesting();
        Assert.True(before > 2L * keys * valueBytes * 9 / 10, $"the store holds only {before} bytes of table files");

        for (int k = 0; k < keys; k++)
            rows.Add(DeleteRecord($"db-9/{k:D6}", 1, deletedAt));

        Assert.True(backend.StoreKeyValues(rows));

        if (deleteRecordsInATableFile)
            backend.FlushMemTablesForTesting();

        RevisionPruneResult result;
        int deleted = 0;
        do
        {
            result = Prune(null, Age);
            deleted += result.RevisionsDeleted;
        }
        while (result.BatchLimitReached);

        Assert.Equal(2 * keys, deleted);

        // Flush and compaction run on the engine's own threads.
        Stopwatch waited = Stopwatch.StartNew();
        long after = backend.GetKeyValueTableBytesForTesting();

        while (after > before / 10 && waited.Elapsed < TimeSpan.FromSeconds(60))
        {
            await Task.Delay(100, TestContext.Current.CancellationToken);
            after = backend.GetKeyValueTableBytesForTesting();
        }

        Assert.True(after <= before / 10,
            $"the deleted key space still holds {after} of {before} table-file bytes after {waited.Elapsed.TotalSeconds:F0} s");

        AssertOnlyTheDeleteRecordIsLeft("db-9/000000", deleteRevision: 1);
    }
}
