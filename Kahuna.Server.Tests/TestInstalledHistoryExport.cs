using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// What a whole-partition snapshot carries of a partition's revision history, and what the receiver can answer from
/// it: the revision at each registered hold, the complete run above complete-from, nothing else, and a durable record
/// that refuses every other timestamp. Runs on each backend the server ships, because the export reads history
/// through the backend contract and the import relies on each backend landing an older record as retained history.
/// </summary>
public sealed class TestInstalledHistoryExport : IDisposable
{
    private const int HashPoolSize = 3;

    private const int Partition = 2;

    /// <summary>The nodes' clock: complete-from lies <see cref="CommitAdmission.MaxClockLeadMs"/> below it.</summary>
    private const long NowPhysical = 100_000;

    private static readonly long CompleteFromPhysical = NowPhysical - CommitAdmission.MaxClockLeadMs;

    private readonly string dir = Path.Combine(Path.GetTempPath(), "kahuna-installed-history-" + Guid.NewGuid().ToString("N"));

    private readonly List<IDisposable> opened = [];

    public TestInstalledHistoryExport() => Directory.CreateDirectory(dir);

    public void Dispose()
    {
        foreach (IDisposable disposable in opened)
            disposable.Dispose();

        try { Directory.Delete(dir, recursive: true); } catch { /* best-effort temp cleanup */ }
    }

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private sealed class Node
    {
        public required IPersistenceBackend Backend { get; init; }
        public required PartitionStateTransfer Transfer { get; init; }
        public required InstalledHistoryFence Fence { get; init; }
        public required List<HLCTimestamp> Holds { get; init; }
    }

    private IPersistenceBackend Backend(string kind)
    {
        string path = Path.Combine(dir, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);

        IPersistenceBackend backend = kind switch
        {
            "memory" => new MemoryPersistenceBackend(),
            "sqlite" => new SqlitePersistenceBackend(path, "v1"),
            "rocksdb" => new RocksDbPersistenceBackend(path, "v1"),
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        };

        if (backend is IDisposable disposable)
            opened.Add(disposable);

        return backend;
    }

    private Node MakeNode(string kind, bool attachHistory = true)
    {
        IPersistenceBackend backend = Backend(kind);
        RangeMap map = new([new RangeDescriptor { KeySpace = "ranged", PartitionId = Partition, Generation = 1 }]);

        PartitionStateTransfer transfer = new(
            new PartitionDataEnumerator(backend, () => map, new DataPartitionRouter(HashPoolSize)),
            backend, new CompletionReceiptStore(), new TransactionRecordStore(), new PreparedIntentStore(),
            () => map, new DataPartitionRouter(HashPoolSize),
            () => Task.CompletedTask,
            null, "rev", NullLogger<IKahuna>.Instance);

        InstalledHistoryFence fence = new(null, null, NullLogger<IKahuna>.Instance);
        List<HLCTimestamp> holds = [];

        if (attachHistory)
            transfer.AttachInstalledHistory(fence, () => holds, () => Ts(NowPhysical));

        return new Node { Backend = backend, Transfer = transfer, Fence = fence, Holds = holds };
    }

    private static PersistenceRequestItem Row(string key, long revision, long lastModified, KeyValueState state = KeyValueState.Set) => new(
        key, state == KeyValueState.Deleted ? null : [(byte)revision], revision,
        expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
        lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
        lastModifiedNode: 0, lastModifiedPhysical: lastModified, lastModifiedCounter: 0,
        state: (int)state);

    private static void Seed(Node node, string key, params long[] lastModifiedByRevision)
    {
        List<PersistenceRequestItem> rows = [];
        for (int i = 0; i < lastModifiedByRevision.Length; i++)
            rows.Add(Row(key, i + 1, lastModifiedByRevision[i]));

        Assert.True(node.Backend.StoreKeyValues(rows));
    }

    private static async Task Install(Node from, Node to)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using Stream exported = await from.Transfer.ExportPartitionState(Partition, 42, ct);
        using MemoryStream snapshot = new();
        await exported.CopyToAsync(snapshot, ct);
        snapshot.Position = 0;

        await to.Transfer.ImportPartitionState(Partition, snapshot, ct);
    }

    private static long? RevisionAt(Node node, string key, long at) =>
        node.Backend.GetKeyValueRevisionAtOrBefore(key, long.MaxValue / 2, Ts(at))?.Revision;

    private static bool HasRevision(Node node, string key, long revision) =>
        node.Backend.GetKeyValueRevision(key, revision) is not null;

    [Theory]
    [InlineData("memory")]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task CarriesTheRevisionAtEachHold_AndNothingBetween(string kind)
    {
        Node source = MakeNode(kind);
        Node target = MakeNode(kind);

        Seed(source, "ranged/moved", 100, 200, 300, 400, 500, 600, 700, 800, 900, 1_000);
        Seed(source, "ranged/quiet", 100);
        Seed(source, "ranged/late", 800);
        Assert.True(source.Backend.StoreKeyValues(
        [
            Row("ranged/deleted", 1, 100),
            Row("ranged/deleted", 2, 800, KeyValueState.Deleted)
        ]));

        source.Holds.AddRange([Ts(350), Ts(720)]);

        await Install(source, target);

        // Current state is what it was.
        Assert.Equal(10, target.Backend.GetKeyValue("ranged/moved")!.Revision);
        Assert.Equal(1, target.Backend.GetKeyValue("ranged/quiet")!.Revision);
        Assert.Equal(KeyValueState.Deleted, target.Backend.GetKeyValue("ranged/deleted")!.State);

        // Each hold reads the revision the source reads.
        Assert.Equal(3, RevisionAt(target, "ranged/moved", 350));
        Assert.Equal(7, RevisionAt(target, "ranged/moved", 720));
        Assert.Equal(1, RevisionAt(target, "ranged/quiet", 350));
        Assert.Equal(1, RevisionAt(target, "ranged/deleted", 350));
        Assert.Equal(KeyValueState.Set, target.Backend.GetKeyValueRevisionAtOrBefore("ranged/deleted", long.MaxValue / 2, Ts(350))!.State);

        // A key that did not exist at the hold still does not.
        Assert.Null(RevisionAt(target, "ranged/late", 350));

        // Only the pinned revisions and the head were carried.
        foreach (long absent in new long[] { 1, 2, 4, 5, 6, 8, 9 })
            Assert.False(HasRevision(target, "ranged/moved", absent), $"revision {absent} was not needed");

        // And the receiver says so: the holds and everything from complete-from up, and no timestamp between.
        Assert.True(target.Fence.IsServable(Partition, Ts(350)));
        Assert.True(target.Fence.IsServable(Partition, Ts(720)));
        Assert.True(target.Fence.IsServable(Partition, Ts(CompleteFromPhysical)));
        Assert.False(target.Fence.IsServable(Partition, Ts(351)));
        Assert.False(target.Fence.IsServable(Partition, Ts(719)));
        Assert.False(target.Fence.IsServable(Partition, Ts(CompleteFromPhysical - 1)));
        Assert.True(target.Fence.MayRefuse(Ts(351)));
        Assert.False(target.Fence.MayRefuse(Ts(CompleteFromPhysical)));

        // The partition's fence is its own.
        Assert.True(target.Fence.IsServable(Partition + 1, Ts(351)));
    }

    [Theory]
    [InlineData("memory")]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task WithNoHold_OldKeysCarryOnlyTheirHead(string kind)
    {
        Node source = MakeNode(kind);
        Node target = MakeNode(kind);

        Seed(source, "ranged/old", 100, 200, 300);

        await Install(source, target);

        Assert.Equal(3, target.Backend.GetKeyValue("ranged/old")!.Revision);
        Assert.False(HasRevision(target, "ranged/old", 1));
        Assert.False(HasRevision(target, "ranged/old", 2));
    }

    [Theory]
    [InlineData("memory")]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task AboveCompleteFrom_EveryRevisionAndTheBoundaryAreCarried(string kind)
    {
        Node source = MakeNode(kind);
        Node target = MakeNode(kind);

        long from = CompleteFromPhysical;
        Seed(source, "ranged/hot", 100, from - 10, from + 10, from + 20, from + 30);

        await Install(source, target);

        Assert.False(HasRevision(target, "ranged/hot", 1));

        Assert.Equal(2, RevisionAt(target, "ranged/hot", from));
        Assert.Equal(3, RevisionAt(target, "ranged/hot", from + 15));
        Assert.Equal(4, RevisionAt(target, "ranged/hot", from + 25));
        Assert.Equal(5, RevisionAt(target, "ranged/hot", from + 35));
    }

    [Theory]
    [InlineData("memory")]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task AnInstalledExporterPassesOnOnlyWhatItCanAnswer(string kind)
    {
        Node first = MakeNode(kind);
        Node second = MakeNode(kind);
        Node third = MakeNode(kind);

        Seed(first, "ranged/moved", 100, 200, 300, 400, 500, 600, 700, 800);
        first.Holds.Add(Ts(350));

        await Install(first, second);

        // A hold the second node's install did not carry: it must refuse to vouch for it, not export a wrong answer.
        second.Holds.AddRange([Ts(350), Ts(650)]);

        await Install(second, third);

        Assert.True(third.Fence.IsServable(Partition, Ts(350)));
        Assert.Equal(3, RevisionAt(third, "ranged/moved", 350));

        Assert.False(third.Fence.IsServable(Partition, Ts(650)));
    }

    [Theory]
    [InlineData("memory")]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task AnotherInstallReplacesTheBound(string kind)
    {
        Node source = MakeNode(kind);
        Node target = MakeNode(kind);

        Seed(source, "ranged/moved", 100, 200, 300, 400, 500);
        source.Holds.Add(Ts(150));

        await Install(source, target);
        Assert.True(target.Fence.IsServable(Partition, Ts(150)));

        source.Holds.Clear();
        source.Holds.Add(Ts(350));

        await Install(source, target);

        Assert.True(target.Fence.IsServable(Partition, Ts(350)));
        Assert.False(target.Fence.IsServable(Partition, Ts(150)));
        Assert.False(HasRevision(target, "ranged/moved", 1));
        Assert.Equal(3, RevisionAt(target, "ranged/moved", 350));
    }

    [Theory]
    [InlineData("memory")]
    [InlineData("sqlite")]
    [InlineData("rocksdb")]
    public async Task ASnapshotThatNamesNoHistoryFencesAtTheImportersClock(string kind)
    {
        Node oldExporter = MakeNode(kind, attachHistory: false);
        Node target = MakeNode(kind);

        Seed(oldExporter, "ranged/moved", 100, 200, 300);

        await Install(oldExporter, target);

        Assert.Equal(3, target.Backend.GetKeyValue("ranged/moved")!.Revision);
        Assert.False(target.Fence.IsServable(Partition, Ts(NowPhysical - 1)));
        Assert.True(target.Fence.IsServable(Partition, Ts(NowPhysical)));
    }

    [Fact]
    public void TheBoundSurvivesARestart()
    {
        InstalledHistoryFence fence = new(dir, "v1", NullLogger<IKahuna>.Instance);
        fence.Record(Partition, Ts(1_000), [Ts(200), Ts(100), Ts(200), Ts(5_000)]);

        InstalledHistoryFence restarted = new(dir, "v1", NullLogger<IKahuna>.Instance);

        Assert.True(restarted.MayRefuse(Ts(999)));
        Assert.True(restarted.IsServable(Partition, Ts(100)));
        Assert.True(restarted.IsServable(Partition, Ts(200)));
        Assert.True(restarted.IsServable(Partition, Ts(1_000)));
        Assert.False(restarted.IsServable(Partition, Ts(150)));
        Assert.False(restarted.IsServable(Partition, Ts(999)));
        Assert.Equal([Ts(100), Ts(200)], restarted.BoundOf(Partition)!.Value.Pinned);
    }

    [Fact]
    public void AnInstallFromAnOlderBuildIsFencedAtStartup_OnceAndForAll()
    {
        InstalledHistoryFence fence = new(dir, "v1", NullLogger<IKahuna>.Instance);
        fence.Record(1, Ts(50), []);

        fence.FenceAtStartup([1, Partition], Ts(1_000));

        // A recorded bound stands; an install with none is fenced at the startup's clock.
        Assert.True(fence.IsServable(1, Ts(50)));
        Assert.False(fence.IsServable(Partition, Ts(999)));
        Assert.True(fence.IsServable(Partition, Ts(1_000)));

        // A later restart does not move the bound.
        InstalledHistoryFence restarted = new(dir, "v1", NullLogger<IKahuna>.Instance);
        restarted.FenceAtStartup([1, Partition], Ts(2_000));
        Assert.True(restarted.IsServable(Partition, Ts(1_000)));
    }

    [Fact]
    public void AnUnreadableFileFencesEveryPartition_AndKeepsTheFence()
    {
        File.WriteAllText(Path.Combine(dir, "installedhistory_v1.txt"), "not a bound\n");

        InstalledHistoryFence fence = new(dir, "v1", NullLogger<IKahuna>.Instance);
        fence.FenceAtStartup([], Ts(1_000));

        Assert.False(fence.IsServable(Partition, Ts(999)));
        Assert.True(fence.IsServable(Partition, Ts(1_000)));
        Assert.Equal(Ts(1_000), fence.CompleteFromOf(Partition));

        InstalledHistoryFence restarted = new(dir, "v1", NullLogger<IKahuna>.Instance);
        restarted.FenceAtStartup([], Ts(2_000));
        Assert.False(restarted.IsServable(Partition, Ts(999)));
        Assert.True(restarted.IsServable(Partition, Ts(1_000)));
    }
}
