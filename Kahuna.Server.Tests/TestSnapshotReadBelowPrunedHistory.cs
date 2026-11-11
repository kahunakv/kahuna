using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// A snapshot read finds a key's value at its timestamp in the persisted revision history. Once revision
/// retention has removed that history, the lookup finds nothing, and "nothing" used to be reported as
/// "the key did not exist": a scan dropped the row and a point read answered DoesNotExist, with no error.
/// <para>
/// Every prune records the timestamp below which some key lost revisions. A snapshot read below it is now
/// refused as too old, whatever its shape; a read at or above it, and a read a snapshot hold protected, is
/// answered as before. These tests drive the key-value actor over a real store that was pruned.
/// </para>
/// </summary>
public sealed class TestSnapshotReadBelowPrunedHistory : RaftTrackingTest
{
    private static readonly TimeSpan Age = TimeSpan.FromHours(1);

    private readonly string path = Path.Combine(Path.GetTempPath(), "kahuna-snapshot-too-old-" + Guid.NewGuid().ToString("N"));

    // The SQLite backend picks the shard of a scan from the prefix as the bucket name, with no trailing slash.
    private const string ScanPrefix = "doc";

    private static long MinutesAgo(double minutes) =>
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - (long)TimeSpan.FromMinutes(minutes).TotalMilliseconds;

    private static HLCTimestamp At(long physical) => new(0, physical, 0);

    private static PersistenceRequestItem Value(string key, long revision, long physical) =>
        new(key, [(byte)revision], revision, 0, 0, 0, 0, 0, 0, 0, physical, 0, (int)KeyValueState.Set);

    private IPersistenceBackend Open(string kind)
    {
        Directory.CreateDirectory(path);

        return kind switch
        {
            "rocksdb" => new RocksDbPersistenceBackend(path, "v1"),
            "sqlite" => new SqlitePersistenceBackend(path, "v1"),
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "unknown backend kind")
        };
    }

    /// <summary>
    /// "doc/a" was written three hours ago, overwritten two hours ago and again ten minutes ago; "doc/b" was
    /// written three hours ago and never changed.
    /// </summary>
    private static void Seed(IPersistenceBackend backend)
    {
        Assert.True(backend.StoreKeyValues([Value("doc/a", 0, MinutesAgo(180))]));
        Assert.True(backend.StoreKeyValues([Value("doc/a", 1, MinutesAgo(120))]));
        Assert.True(backend.StoreKeyValues([Value("doc/a", 2, MinutesAgo(10))]));
        Assert.True(backend.StoreKeyValues([Value("doc/b", 0, MinutesAgo(180))]));
    }

    private static KeyValueRequest Snapshot(KeyValueRequestType type, string key, HLCTimestamp readTimestamp)
    {
        KeyValueRequest request = new(type,
            HLCTimestamp.Zero, HLCTimestamp.Zero,
            key, null, null, -1,
            KeyValueFlags.None, 0,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent, 0, 1, default);

        request.ReadTimestamp = readTimestamp;

        if (type == KeyValueRequestType.GetByRange)
        {
            request.Limit = 100;
            request.StartInclusive = true;
        }

        return request;
    }

    private async Task WithActor(
        IPersistenceBackend backend,
        Func<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>, Task> body)
    {
        KahunaConfiguration config = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            CacheEntryTtl = TimeSpan.FromMinutes(5),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = 50_000,
            MaxBytesPerActor = 256L * 1024 * 1024,
            CollectBatchMax = 1000,
            RevisionRetention = 16
        });

        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = Track(new(
            new RaftConfiguration
            {
                NodeName = "snapshot-too-old",
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false,
                PartitionExecutorPoolSize = 1
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger));

        FairReadScheduler scheduler = (FairReadScheduler)raft.ReadScheduler;
        scheduler.Start();

        try
        {
            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);

            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "snapshot-too-old-actor", null!, null!, backend, raft,
                    raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            await body(actorRef);
        }
        finally
        {
            scheduler.Stop();
            (backend as IDisposable)?.Dispose();
            try { Directory.Delete(path, recursive: true); } catch { /* best effort */ }
        }
    }

    private static async Task<KeyValueResponse> Ask(
        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef, KeyValueRequest request)
    {
        KeyValueResponse? response = await actorRef.Ask(request, TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.NotNull(response);
        return response;
    }

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    public async Task ReadBelowThePrunedHistory_IsRefusedAsTooOld_InEveryShape(string kind)
    {
        IPersistenceBackend backend = Open(kind);
        Seed(backend);

        // Age retention keeps what a read in the last hour sees: "doc/a" loses its first revision.
        Assert.True(backend.PruneKeyValueRevisions(null, 0, Age, 10_000, HLCTimestamp.Zero, out RevisionPruneResult pruned));
        Assert.Equal(1, pruned.RevisionsDeleted);

        // Two and a half hours ago "doc/a" held the revision that is gone.
        HLCTimestamp tooOld = At(MinutesAgo(150));

        await WithActor(backend, async actorRef =>
        {
            KeyValueResponse scan = await Ask(actorRef, Snapshot(KeyValueRequestType.GetByRange, ScanPrefix, tooOld));
            Assert.Equal(KeyValueResponseType.SnapshotTooOld, scan.Type);

            KeyValueResponse get = await Ask(actorRef, Snapshot(KeyValueRequestType.TryGet, "doc/a", tooOld));
            Assert.Equal(KeyValueResponseType.SnapshotTooOld, get.Type);

            KeyValueResponse exists = await Ask(actorRef, Snapshot(KeyValueRequestType.TryExists, "doc/a", tooOld));
            Assert.Equal(KeyValueResponseType.SnapshotTooOld, exists.Type);
        });
    }

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    public async Task ReadInsideTheRetentionWindow_IsAnsweredAfterThePrune(string kind)
    {
        IPersistenceBackend backend = Open(kind);
        Seed(backend);

        Assert.True(backend.PruneKeyValueRevisions(null, 0, Age, 10_000, HLCTimestamp.Zero, out RevisionPruneResult pruned));
        Assert.Equal(1, pruned.RevisionsDeleted);

        // Half an hour ago "doc/a" held its second revision, written two hours ago.
        HLCTimestamp inWindow = At(MinutesAgo(30));

        await WithActor(backend, async actorRef =>
        {
            KeyValueResponse scan = await Ask(actorRef, Snapshot(KeyValueRequestType.GetByRange, ScanPrefix, inWindow));
            Assert.NotNull(scan.RangeResult);
            Assert.Equal(2, scan.RangeResult.Items.Count);
            Assert.Equal("doc/a", scan.RangeResult.Items[0].Item1);
            Assert.Equal(1, scan.RangeResult.Items[0].Item2.Revision);
            Assert.Equal("doc/b", scan.RangeResult.Items[1].Item1);
            Assert.Equal(0, scan.RangeResult.Items[1].Item2.Revision);

            KeyValueResponse get = await Ask(actorRef, Snapshot(KeyValueRequestType.TryGet, "doc/a", inWindow));
            Assert.Equal(KeyValueResponseType.Get, get.Type);
            Assert.NotNull(get.Entry);
            Assert.Equal(1, get.Entry.Revision);
        });
    }

    [Theory]
    [InlineData("rocksdb")]
    [InlineData("sqlite")]
    public async Task ReadAtASnapshotHold_IsAnsweredAfterThePrune(string kind)
    {
        IPersistenceBackend backend = Open(kind);
        Seed(backend);

        // The same timestamp the first test is refused at, now protected by a hold when the prune runs.
        HLCTimestamp held = At(MinutesAgo(150));

        Assert.True(backend.PruneKeyValueRevisions(null, 0, Age, 10_000, held, out RevisionPruneResult pruned));
        Assert.Equal(0, pruned.RevisionsDeleted);
        Assert.Equal(0, pruned.FloorViolations);

        await WithActor(backend, async actorRef =>
        {
            KeyValueResponse scan = await Ask(actorRef, Snapshot(KeyValueRequestType.GetByRange, ScanPrefix, held));
            Assert.NotNull(scan.RangeResult);
            Assert.Equal(2, scan.RangeResult.Items.Count);
            Assert.Equal("doc/a", scan.RangeResult.Items[0].Item1);
            Assert.Equal(0, scan.RangeResult.Items[0].Item2.Revision);
            Assert.Equal("doc/b", scan.RangeResult.Items[1].Item1);

            KeyValueResponse get = await Ask(actorRef, Snapshot(KeyValueRequestType.TryGet, "doc/a", held));
            Assert.Equal(KeyValueResponseType.Get, get.Type);
            Assert.NotNull(get.Entry);
            Assert.Equal(0, get.Entry.Revision);
        });
    }

    [Fact]
    public async Task PruneThatStopsOnItsDeleteBudget_LeavesNoHoleBelowTheFloorItRecords()
    {
        IPersistenceBackend backend = Open("rocksdb");

        // Six revisions replaced long before the cutoff, then a head inside the window.
        for (long revision = 0; revision <= 5; revision++)
            Assert.True(backend.StoreKeyValues([Value("doc/a", revision, MinutesAgo(300 - revision * 10))]));
        Assert.True(backend.StoreKeyValues([Value("doc/a", 6, MinutesAgo(5))]));

        // The budget stops the pass after two of the five deletable rows: the two oldest.
        Assert.True(backend.PruneKeyValueRevisions(["doc/a"], 0, Age, 2, HLCTimestamp.Zero, out RevisionPruneResult partial));
        Assert.Equal(2, partial.RevisionsDeleted);
        Assert.True(partial.BatchLimitReached);
        Assert.Null(backend.GetKeyValueRevision("doc/a", 0));
        Assert.Null(backend.GetKeyValueRevision("doc/a", 1));
        Assert.NotNull(backend.GetKeyValueRevision("doc/a", 2));

        // The floor names the oldest survivor, so every read it admits finds the revision it sees.
        HLCTimestamp floor = backend.GetPrunedHistoryFloor();
        Assert.Equal(At(MinutesAgo(280)).L / 1000, floor.L / 1000);

        await WithActor(backend, async actorRef =>
        {
            KeyValueResponse below = await Ask(actorRef, Snapshot(KeyValueRequestType.TryGet, "doc/a", At(floor.L - 1)));
            Assert.Equal(KeyValueResponseType.SnapshotTooOld, below.Type);

            // Between revisions 4 and 5: revision 4, which a newest-first delete would have removed.
            KeyValueResponse between = await Ask(actorRef, Snapshot(KeyValueRequestType.TryGet, "doc/a", At(MinutesAgo(255))));
            Assert.Equal(KeyValueResponseType.Get, between.Type);
            Assert.NotNull(between.Entry);
            Assert.Equal(4, between.Entry.Revision);
        });
    }
}
