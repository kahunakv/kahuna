using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Age retention says how far back the persisted history stays readable. A revision is what a read
/// sees from its own commit until the commit of the revision that replaces it, so its age counts from
/// the replacement. The prune used to count from the revision's own commit: a key written two hours
/// ago and overwritten a minute ago lost the old value at once, and a snapshot read five minutes back
/// found no revision and reported the key as absent.
/// <para>
/// The rule these tests pin, for both durable backends: the newest revision committed before the
/// cutoff, and every newer one, is kept; only revisions below it go by age.
/// </para>
/// </summary>
public sealed class TestRevisionAgeRetentionBoundary : IDisposable
{
    private static readonly TimeSpan Age = TimeSpan.FromHours(1);

    private readonly string path = Path.Combine(Path.GetTempPath(), "kahuna-age-boundary-" + Guid.NewGuid().ToString("N"));

    private readonly List<IDisposable> opened = [];

    public void Dispose()
    {
        foreach (IDisposable backend in opened)
            backend.Dispose();

        try { Directory.Delete(path, recursive: true); } catch { /* best effort */ }
    }

    private IPersistenceBackend Open(string kind)
    {
        Directory.CreateDirectory(path);

        IPersistenceBackend backend = kind switch
        {
            "rocksdb" => new RocksDbPersistenceBackend(path, "v1"),
            "sqlite" => new SqlitePersistenceBackend(path, "v1"),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "unknown backend kind")
        };

        opened.Add((IDisposable)backend);
        return backend;
    }

    private static long NowMs => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static long MinutesAgo(double minutes) => NowMs - (long)TimeSpan.FromMinutes(minutes).TotalMilliseconds;

    private static HLCTimestamp At(long physical) => new(0, physical, 0);

    private static PersistenceRequestItem Value(string key, long revision, long physical) =>
        new(key, [(byte)revision], revision, 0, 0, 0, 0, 0, 0, 0, physical, 0, (int)KeyValueState.Set);

    private static RevisionPruneResult Prune(IPersistenceBackend backend, IReadOnlyCollection<string>? keys, TimeSpan age, int count = 0, int batchSize = 10_000, HLCTimestamp floor = default)
    {
        Assert.True(backend.PruneKeyValueRevisions(keys, count, age, batchSize, floor, out RevisionPruneResult result));
        return result;
    }

    private static void AssertReadSees(IPersistenceBackend backend, string key, long readPhysical, long expectedRevision)
    {
        ReadOnlyKeyValueEntry? entry = backend.GetKeyValueRevisionAtOrBefore(key, long.MaxValue, At(readPhysical));
        Assert.NotNull(entry);
        Assert.Equal(expectedRevision, entry.Revision);
        Assert.NotNull(entry.Value);
        Assert.Equal((byte)expectedRevision, Assert.Single(entry.Value));
    }

    [Theory]
    [InlineData("rocksdb", false)]
    [InlineData("rocksdb", true)]
    [InlineData("sqlite", false)]
    [InlineData("sqlite", true)]
    public void OldValueOverwrittenInsideTheWindow_StaysReadableBelowTheOverwrite(string kind, bool sweep)
    {
        const string key = "age/overwritten";
        IPersistenceBackend backend = Open(kind);

        long writtenLongAgo = MinutesAgo(120);
        long overwritten = MinutesAgo(1);

        Assert.True(backend.StoreKeyValues([Value(key, 1, writtenLongAgo)]));
        Assert.True(backend.StoreKeyValues([Value(key, 2, overwritten)]));

        RevisionPruneResult result = Prune(backend, sweep ? null : [key], Age);

        Assert.Equal(0, result.RevisionsDeleted);
        Assert.Equal(0, result.FloorViolations);

        // Five minutes ago the key still held its first value.
        AssertReadSees(backend, key, MinutesAgo(5), expectedRevision: 1);
        AssertReadSees(backend, key, NowMs, expectedRevision: 2);

        // The scan form of the same read resolves the same revision.
        List<(string Key, ReadOnlyKeyValueEntry Current, ReadOnlyKeyValueEntry? Snapshot)> rows =
            backend.GetKeyValueByPrefixAtOrBefore("age", At(MinutesAgo(5)));

        (string scannedKey, _, ReadOnlyKeyValueEntry? snapshot) = Assert.Single(rows);
        Assert.Equal(key, scannedKey);
        Assert.NotNull(snapshot);
        Assert.Equal(1, snapshot.Revision);
    }

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    public void RevisionsReplacedBeforeTheCutoff_Go_AndTheOneVisibleAtTheCutoffStays(string kind)
    {
        const string key = "age/chain";
        IPersistenceBackend backend = Open(kind);

        // 1 and 2 were each replaced more than an hour ago. 3 was committed before the cutoff and
        // replaced inside the window, so it is what a read at the cutoff sees. 4 and 5 are inside it.
        Assert.True(backend.StoreKeyValues([Value(key, 1, MinutesAgo(240))]));
        Assert.True(backend.StoreKeyValues([Value(key, 2, MinutesAgo(180))]));
        Assert.True(backend.StoreKeyValues([Value(key, 3, MinutesAgo(120))]));
        Assert.True(backend.StoreKeyValues([Value(key, 4, MinutesAgo(30))]));
        Assert.True(backend.StoreKeyValues([Value(key, 5, MinutesAgo(10))]));

        RevisionPruneResult result = Prune(backend, [key], Age);

        Assert.Equal(2, result.RevisionsDeleted);
        Assert.Null(backend.GetKeyValueRevision(key, 1));
        Assert.Null(backend.GetKeyValueRevision(key, 2));

        AssertReadSees(backend, key, MinutesAgo(59), expectedRevision: 3);
        AssertReadSees(backend, key, MinutesAgo(31), expectedRevision: 3);
        AssertReadSees(backend, key, MinutesAgo(20), expectedRevision: 4);
        AssertReadSees(backend, key, MinutesAgo(5), expectedRevision: 5);

        // Nothing changed, so a second pass has nothing more to take.
        Assert.Equal(0, Prune(backend, [key], Age).RevisionsDeleted);
        Assert.NotNull(backend.GetKeyValueRevision(key, 3));
    }

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    public void EveryRevisionOlderThanTheCutoff_LeavesOnlyTheCurrentRevision(string kind)
    {
        const string key = "age/all-old";
        IPersistenceBackend backend = Open(kind);

        for (long revision = 1; revision <= 4; revision++)
            Assert.True(backend.StoreKeyValues([Value(key, revision, MinutesAgo(300 - revision))]));

        RevisionPruneResult result = Prune(backend, [key], Age);

        // The current revision is itself older than the cutoff: it answers every read in the window.
        Assert.Equal(3, result.RevisionsDeleted);
        for (long revision = 1; revision <= 3; revision++)
            Assert.Null(backend.GetKeyValueRevision(key, revision));

        Assert.NotNull(backend.GetKeyValueRevision(key, 4));
        AssertReadSees(backend, key, MinutesAgo(59), expectedRevision: 4);
    }

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    public void HoldOlderThanTheCutoff_KeepsItsOwnBoundary_AndAgeTakesWhatLiesBelow(string kind)
    {
        const string key = "age/hold-older";
        IPersistenceBackend backend = Open(kind);

        Assert.True(backend.StoreKeyValues([Value(key, 1, MinutesAgo(300))]));
        Assert.True(backend.StoreKeyValues([Value(key, 2, MinutesAgo(240))]));
        Assert.True(backend.StoreKeyValues([Value(key, 3, MinutesAgo(120))]));
        Assert.True(backend.StoreKeyValues([Value(key, 4, MinutesAgo(10))]));

        // The hold reads at three hours ago and sees revision 2; the age cutoff alone would keep 3.
        long hold = MinutesAgo(180);
        RevisionPruneResult result = Prune(backend, [key], Age, floor: At(hold));

        Assert.Equal(1, result.RevisionsDeleted);
        Assert.Equal(0, result.FloorViolations);
        Assert.Null(backend.GetKeyValueRevision(key, 1));

        AssertReadSees(backend, key, hold, expectedRevision: 2);
        AssertReadSees(backend, key, MinutesAgo(30), expectedRevision: 3);
    }

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    public void HoldInsideTheWindow_DoesNotLetAgeTakeTheRevisionVisibleAtTheCutoff(string kind)
    {
        const string key = "age/hold-newer";
        IPersistenceBackend backend = Open(kind);

        Assert.True(backend.StoreKeyValues([Value(key, 1, MinutesAgo(240))]));
        Assert.True(backend.StoreKeyValues([Value(key, 2, MinutesAgo(120))]));
        Assert.True(backend.StoreKeyValues([Value(key, 3, MinutesAgo(40))]));
        Assert.True(backend.StoreKeyValues([Value(key, 4, MinutesAgo(10))]));

        // The hold sees revision 3. The cutoff, an hour ago, sees revision 2: the older boundary wins.
        RevisionPruneResult result = Prune(backend, [key], Age, floor: At(MinutesAgo(20)));

        Assert.Equal(1, result.RevisionsDeleted);
        Assert.Null(backend.GetKeyValueRevision(key, 1));

        AssertReadSees(backend, key, MinutesAgo(50), expectedRevision: 2);
        AssertReadSees(backend, key, MinutesAgo(20), expectedRevision: 3);
    }

    [Fact]
    public void RocksDbMemo_SkipsTheKeptBoundary_UntilItsSuccessorPassesTheCutoff()
    {
        const string key = "age/memo";
        RocksDbPersistenceBackend backend = (RocksDbPersistenceBackend)Open("rocksdb");

        TimeSpan age = TimeSpan.FromSeconds(2);

        Assert.True(backend.StoreKeyValues([Value(key, 1, MinutesAgo(120))]));
        Assert.True(backend.StoreKeyValues([Value(key, 2, NowMs)]));

        // The first visit walks and keeps the boundary. The row it kept is far older than the
        // cutoff, and that alone must not send the key back to a walk on every pass.
        RevisionPruneResult first = Prune(backend, [key], age);
        Assert.Equal(0, first.KeysSkipped);
        Assert.Equal(0, first.RevisionsDeleted);

        RevisionPruneResult second = Prune(backend, [key], age);
        Assert.Equal(1, second.KeysSkipped);
        Assert.Equal(0, second.RevisionsDeleted);

        // Once the overwrite is older than the cutoff the old value is out of the window.
        Thread.Sleep(age + TimeSpan.FromMilliseconds(250));

        RevisionPruneResult third = Prune(backend, [key], age);
        Assert.Equal(0, third.KeysSkipped);
        Assert.Equal(1, third.RevisionsDeleted);
        Assert.Null(backend.GetKeyValueRevision(key, 1));
        Assert.NotNull(backend.GetKeyValueRevision(key, 2));
    }

    [Fact]
    public void RocksDbSweep_GoesIdleOverAKeptBoundary_AndWakesWhenItsSuccessorPassesTheCutoff()
    {
        const string key = "age/sweep-idle";
        RocksDbPersistenceBackend backend = (RocksDbPersistenceBackend)Open("rocksdb");

        TimeSpan age = TimeSpan.FromSeconds(2);

        Assert.True(backend.StoreKeyValues([Value(key, 1, MinutesAgo(120))]));
        Assert.True(backend.StoreKeyValues([Value(key, 2, NowMs)]));

        Assert.False(Prune(backend, null, age).SweepIdle);
        Assert.True(Prune(backend, null, age).SweepIdle);
        Assert.NotNull(backend.GetKeyValueRevision(key, 1));

        Thread.Sleep(age + TimeSpan.FromMilliseconds(250));

        RevisionPruneResult woken = Prune(backend, null, age);
        Assert.False(woken.SweepIdle);
        Assert.Equal(1, woken.RevisionsDeleted);
        Assert.Null(backend.GetKeyValueRevision(key, 1));
    }
}
