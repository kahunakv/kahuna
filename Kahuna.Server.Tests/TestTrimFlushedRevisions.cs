
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Flushed-revision trimming: with the option on, a persistent key's archive keeps only the revisions the
/// background writer has not confirmed, plus the floor-boundary revision a snapshot hold pins; the persisted
/// history answers for everything flushed. Ephemeral keys and the option-off path keep the count-based window.
/// The archive-time trim and the flush-time trim share one routine; the end-to-end case drives a real node and
/// checks that an as-of read and a by-revision read of a trimmed revision still return the right bytes.
/// </summary>
[Collection("SnapshotFloorMetrics")]
public sealed class TestTrimFlushedRevisions : RaftTrackingTest
{
    private readonly ILoggerFactory loggerFactory;

    public TestTrimFlushedRevisions(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private sealed class TrimHandler(KeyValueContext context) : BaseHandler(context)
    {
        public void Trim(KeyValueEntry entry, long refRevision, KeyValueDurability durability) =>
            RemoveExpiredRevisions(entry, refRevision, durability);
    }

    // ── archive-time trim ────────────────────────────────────────────────────────────────

    [Fact]
    public void OptionOff_KeepsTheCountWindow()
    {
        (KeyValueContext context, _) = BuildContext(retention: 3, trimFlushed: false);
        KeyValueEntry entry = BuildEntry(count: 6, flushedRevision: 6);

        new TrimHandler(context).Trim(entry, refRevision: 7, KeyValueDurability.Persistent);

        AssertRevisions(entry, 5, 6);
    }

    [Fact]
    public void OptionOn_Persistent_EveryFlushedRevisionLeavesTheArchive()
    {
        (KeyValueContext context, _) = BuildContext(retention: 3, trimFlushed: true);
        KeyValueEntry entry = BuildEntry(count: 6, flushedRevision: 6);
        long before = entry.CachedBytes;

        new TrimHandler(context).Trim(entry, refRevision: 7, KeyValueDurability.Persistent);

        AssertRevisions(entry);
        // Six slots and the history container were reclaimed from the entry's byte estimate.
        long expectedFreed = 0;
        for (int i = 1; i <= 6; i++)
            expectedFreed += KeyValueStoreAccounting.EstimateRevisionRemovedBytes(i == 6, [(byte)i]);
        Assert.Equal(before - expectedFreed, entry.CachedBytes);
    }

    [Fact]
    public void OptionOn_Persistent_UnflushedRevisionsStay()
    {
        (KeyValueContext context, _) = BuildContext(retention: 3, trimFlushed: true);
        KeyValueEntry entry = BuildEntry(count: 6, flushedRevision: 4);

        new TrimHandler(context).Trim(entry, refRevision: 7, KeyValueDurability.Persistent);

        AssertRevisions(entry, 5, 6);
    }

    [Fact]
    public void OptionOn_Ephemeral_KeepsTheCountWindow()
    {
        (KeyValueContext context, _) = BuildContext(retention: 3, trimFlushed: true);
        KeyValueEntry entry = BuildEntry(count: 6, flushedRevision: 6);

        new TrimHandler(context).Trim(entry, refRevision: 7, KeyValueDurability.Ephemeral);

        AssertRevisions(entry, 5, 6);
    }

    [Fact]
    public void OptionOn_Persistent_TheFloorBoundaryIsPinned()
    {
        (KeyValueContext context, RaftManager raft) = BuildContext(retention: 3, trimFlushed: true, withFloorStore: true);
        const long boundaryRevision = 2;
        HLCTimestamp holdStamp = new(1, boundaryRevision * 1000L, 0);
        HLCTimestamp farFuture = new(1, long.MaxValue / 2, 0);
        Assert.True(InjectHold(context.SnapshotFloorStore!, new SnapshotHold("h1", "client", holdStamp, farFuture)));
        _ = raft;

        KeyValueEntry entry = BuildEntry(count: 6, flushedRevision: 6);

        new TrimHandler(context).Trim(entry, refRevision: 7, KeyValueDurability.Persistent);

        AssertRevisions(entry, boundaryRevision);
        Assert.Equal(boundaryRevision, entry.FloorBoundaryRevision);
        // Revisions above the boundary were trimmed, so a boundary hit is authoritative only below revision 3's stamp.
        Assert.Equal(new HLCTimestamp(1, 3000L, 0), entry.FloorBoundaryCoverageEnd);
    }

    // ── flush-time trim ──────────────────────────────────────────────────────────────────

    [Fact]
    public void FlushTimeTrim_RemovesEverythingAtOrBelowTheAcknowledgedRevision()
    {
        (KeyValueContext context, _) = BuildContext(retention: 3, trimFlushed: true);
        KeyValueEntry entry = BuildEntry(count: 6, flushedRevision: 4);
        HashSet<long> scratch = [];

        RevisionArchiveTrim.Trim(context, entry, cutoff: 4 + 1, scratch);

        AssertRevisions(entry, 5, 6);
        Assert.Empty(scratch);

        RevisionArchiveTrim.Trim(context, entry, cutoff: 6 + 1, scratch);
        AssertRevisions(entry);
    }

    // ── end to end ───────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task EndToEnd_TheArchiveEmptiesAfterTheFlush_AndHistoryReadsComeFromDisk()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        EmbeddedKahunaOptions options = new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            TrimFlushedRevisions = true
        };

        await using EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);
        string key = "trim/e2e/" + Guid.NewGuid().ToString("N")[..8];
        await node.WaitForLeaderForKeyAsync(key, ct);

        byte[] first = "first"u8.ToArray();
        byte[] second = "second"u8.ToArray();
        byte[] third = "third"u8.ToArray();

        await node.Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, first, null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        (KeyValueResponseType firstType, ReadOnlyKeyValueEntry? firstEntry) =
            await node.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, firstType);
        HLCTimestamp firstStamp = firstEntry!.LastModified;
        long firstRevision = firstEntry.Revision;

        await node.Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, second, null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        await node.Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, third, null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

        await node.Kahuna.FlushPersistenceAsync();

        KeyValueEntry resident = await WaitForResident((KahunaManager)node.Kahuna, key);

        // Every revision is confirmed on disk, so the flush acknowledgements emptied the archive.
        await WaitUntil(() => resident.FlushedRevision == resident.Revision && (resident.Revisions?.Count ?? 0) == 0);
        Assert.Equal(firstRevision + 2, resident.Revision);

        (KeyValueResponseType latestType, ReadOnlyKeyValueEntry? latest) =
            await node.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, latestType);
        Assert.Equal(third, latest!.Value);

        // The as-of read and the by-revision read of the trimmed first revision are answered from the persisted history.
        (KeyValueResponseType asOfType, ReadOnlyKeyValueEntry? asOf) =
            await node.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, firstStamp, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, asOfType);
        Assert.Equal(first, asOf!.Value);
        Assert.Equal(firstRevision, asOf.Revision);

        (KeyValueResponseType byRevisionType, ReadOnlyKeyValueEntry? byRevision) =
            await node.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, firstRevision, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, byRevisionType);
        Assert.Equal(first, byRevision!.Value);
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private static async Task<KeyValueEntry> WaitForResident(KahunaManager manager, string key)
    {
        KeyValueEntry? entry = null;
        await WaitUntil(() => (entry = FindEntry(manager, key)) is not null);
        return entry!;
    }

    private static KeyValueEntry? FindEntry(KahunaManager manager, string key)
    {
        foreach (IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef in manager.KeyValues.PersistentInstances)
        {
            if (((ActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>)actorRef).Runner.Actor is KeyValueActor actor
                && actor.GetContext().Store.TryGetValue(key, out KeyValueEntry? entry))
                return entry;
        }

        return null;
    }

    private static async Task WaitUntil(Func<bool> predicate, int timeoutMs = 10_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate()) return;
            await Task.Delay(10);
        }
        Assert.True(predicate(), "condition not met in time");
    }

    private static void AssertRevisions(KeyValueEntry entry, params long[] expected)
    {
        Assert.NotNull(entry.Revisions);
        List<long> actual = [];
        foreach ((long revision, _) in entry.Revisions!)
            actual.Add(revision);
        Assert.Equal(expected, actual);
    }

    /// <summary>Revisions 1..count, revision i stamped at physical i*1000, each with a one-byte value.</summary>
    private static KeyValueEntry BuildEntry(int count, long flushedRevision)
    {
        KeyValueRevisionHistory revisions = new();
        for (int i = 1; i <= count; i++)
            revisions[i] = new KeyValueRevisionEntry([(byte)i], new HLCTimestamp(1, i * 1000L, 0), HLCTimestamp.Zero, KeyValueState.Set);

        return new KeyValueEntry
        {
            Revisions = revisions,
            FlushedRevision = flushedRevision,
            CachedBytes = 100_000
        };
    }

    private static bool InjectHold(SnapshotFloorStore store, SnapshotHold hold)
    {
        SnapshotFloorDeltaMessage delta = new();
        delta.Entries.Add(new SnapshotFloorDeltaEntry
        {
            Hold = new SnapshotHoldMessage
            {
                HoldId = hold.HoldId,
                HolderId = hold.HolderId,
                TimestampNode = hold.Timestamp.N,
                TimestampPhysical = hold.Timestamp.L,
                TimestampCounter = hold.Timestamp.C,
                LeaseExpiryNode = hold.LeaseExpiry.N,
                LeaseExpiryPhysical = hold.LeaseExpiry.L,
                LeaseExpiryCounter = hold.LeaseExpiry.C,
            },
        });
        byte[] data = ReplicationSerializer.Serialize(delta);
        RaftLog log = new() { LogType = ReplicationTypes.SnapshotFloor, LogData = data };
        return store.Restore(RangeMapStore.MetaPartitionId, log);
    }

    private (KeyValueContext, RaftManager) BuildContext(int retention, bool trimFlushed, bool withFloorStore = false)
    {
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;

        RaftManager raft = Track(new RaftManager(
            new RaftConfiguration
            {
                NodeName = "trim-flushed-revisions",
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

        KahunaConfiguration config = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            RevisionRetention = retention,
            TrimFlushedRevisions = trimFlushed
        });

        SnapshotFloorStore? floorStore = withFloorStore ? new SnapshotFloorStore(raft, null, null, logger) : null;

        KeyValueContext context = new(
            actorContext: null!,
            store: new KeyValueStore(),
            locksByPrefix: null!,
            locksByRange: null!,
            proposals: null!,
            backgroundWriter: null!,
            writeAggregator: null!,
            persistenceBackend: null!,
            raft: raft,
            backendReadScheduler: raft.ReadScheduler,
            keySpaceRegistry: null!,
            rangeMapStore: null!,
            configuration: config,
            logger: logger,
            snapshotFloorStore: floorStore);

        return (context, raft);
    }
}
