using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A snapshot read at a fixed timestamp must return the same revision every time it is asked, whatever commits
/// or flushes after the first ask. Each test here drives one path that used to answer, and then answer
/// differently: the durable prepared-intent overlay answering past a third writer's in-flight commit, and the
/// persisted revision history answering for revisions the background writer has not flushed yet.
/// </summary>
public sealed class TestSnapshotReadRepeatability
{
    private readonly ILoggerFactory loggerFactory;

    public TestSnapshotReadRepeatability(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    private async Task<EmbeddedKahunaNode> StartNode(CancellationToken ct, Action<EmbeddedKahunaOptions>? configure = null)
    {
        EmbeddedKahunaOptions options = new()
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            DurableDeferredSettlement = true
        };
        configure?.Invoke(options);

        EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("snaprep/seed", ct);
        return node;
    }

    private static HLCTimestamp Tick(EmbeddedKahunaNode node) =>
        node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

    private static async Task<ReadOnlyKeyValueEntry> Seed(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, type);

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, readType);
        return entry!;
    }

    /// <summary>
    /// The durable prepared-intent overlay holds a committed, not yet settled transaction T1 on the key, and a
    /// second transaction T2 — begun before the snapshot — has already staged its own write there. T2's commit
    /// timestamp can still land at or below the snapshot, so a snapshot read must wait for it exactly as it does
    /// when no overlay intent is present. Answering T1's value at once is the defect: once T2 commits inside the
    /// snapshot, the same read answers T2's value.
    /// </summary>
    [Fact]
    public async Task OverlayRead_WaitsForAThirdWritersInFlightCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        const string key = "snaprep/overlay";
        ReadOnlyKeyValueEntry seeded = await Seed(kahuna, key, "v0", ct);

        // T1: committed but unsettled — its intent stays in the store with a commit decision.
        HLCTimestamp t1 = Tick(node);
        HLCTimestamp t1Commit = Tick(node);
        PreparedIntentStore store = kahuna.DurablePreparedIntentStore;
        store.Apply(new PrepareIntentCommand(new PreparedIntent(
            TransactionId: t1, Epoch: 1, Key: key, ManifestHash: 0, RecordAnchorKey: key,
            CommitTimestamp: t1Commit, State: KeyValueState.Set, Value: "v1"u8.ToArray(), Bucket: "snaprep",
            Revision: seeded.Revision + 1, Expires: HLCTimestamp.Zero, NoRevision: false,
            BaseRevision: seeded.Revision, BaseState: KeyValueState.Set,
            RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0), Resolution: PreparedIntentResolution.Pending)));
        store.Apply(new ResolveIntentCommand(t1, 1, key, Commit: true));

        // T2 stages a write on the same key: its write intent is live and it has no commit timestamp yet.
        (KeyValueResponseType startType, TransactionHandle t2) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = key + "/t2",
                Locking = KeyValueTransactionLocking.Optimistic,
                Timeout = 60_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        (KeyValueResponseType setType, _, _) = await kahuna.LocateAndTrySetKeyValue(
            t2.TransactionId, key, "v2"u8.ToArray(), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct,
            coordinatorKey: t2.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, setType);

        // The overlay intent is still the one answering: T2's staging materialized T1 at T1's own revision.
        Assert.NotNull(store.Get(key));

        // A snapshot above T1's commit and above T2's transaction id.
        HLCTimestamp snapshot = Tick(node);
        Assert.True(snapshot > t2.TransactionId);

        Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> read = kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, snapshot, KeyValueDurability.Persistent, ct);

        Task finished = await Task.WhenAny(read, Task.Delay(500, ct));
        if (finished == read)
        {
            (KeyValueResponseType earlyType, ReadOnlyKeyValueEntry? earlyEntry) = await read;
            Assert.Fail($"the snapshot read answered while T2 could still commit inside it: {earlyType} " +
                        $"{(earlyEntry?.Value is null ? "<null>" : Encoding.UTF8.GetString(earlyEntry.Value))}");
        }

        // T2 rolls back: nothing else can commit inside the snapshot, so the read answers T1's committed value.
        Assert.Equal(KeyValueResponseType.RolledBack, await kahuna.LocateAndRollbackTransaction(t2, ct));

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await read;
        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.Equal("v1", Encoding.UTF8.GetString(entry!.Value!));

        (KeyValueResponseType againType, ReadOnlyKeyValueEntry? again) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, snapshot, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, againType);
        Assert.Equal("v1", Encoding.UTF8.GetString(again!.Value!));
    }

    /// <summary>
    /// The background writer never flushes during the test, and the key takes more writes after the snapshot than
    /// the in-memory archive retains by count. The archive must keep every unflushed revision: trimming the as-of
    /// revision sent the read to a persisted history that did not hold it yet, which answered DoesNotExist (or an
    /// older row) now and the right revision after the flush.
    /// </summary>
    [Fact]
    public async Task SnapshotRead_OfUnflushedHotKey_ReturnsTheSameRevisionAfterManyWrites()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int retention = 2;

        await using EmbeddedKahunaNode node = await StartNode(ct, options =>
        {
            options.RevisionRetention = retention;
            options.RevisionsToKeepCached = retention;
            options.DirtyObjectsWriterDelay = 600_000;
        });
        IKahuna kahuna = node.Kahuna;

        const string key = "snaprep/hot";
        ReadOnlyKeyValueEntry first = await Seed(kahuna, key, "r0", ct);
        HLCTimestamp snapshot = first.LastModified;

        (KeyValueResponseType before, ReadOnlyKeyValueEntry? beforeEntry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, snapshot, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, before);
        Assert.Equal(first.Revision, beforeEntry!.Revision);

        for (int i = 1; i <= retention * 3; i++)
            await Seed(kahuna, key, "r" + i, ct);

        (KeyValueResponseType after, ReadOnlyKeyValueEntry? afterEntry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, snapshot, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, after);
        Assert.Equal(first.Revision, afterEntry!.Revision);
        Assert.Equal("r0", Encoding.UTF8.GetString(afterEntry.Value!));

        (KeyValueResponseType exists, ReadOnlyKeyValueEntry? existsEntry) = await kahuna.LocateAndTryExistsValue(
            HLCTimestamp.Zero, key, -1, snapshot, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Exists, exists);
        Assert.Equal(first.Revision, existsEntry!.Revision);

        // After the flush the persisted history holds every revision and the answer is still the same.
        await node.FlushAsync();

        (KeyValueResponseType flushed, ReadOnlyKeyValueEntry? flushedEntry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, snapshot, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, flushed);
        Assert.Equal(first.Revision, flushedEntry!.Revision);
    }

    /// <summary>
    /// The unflushed-writes index keeps the oldest revision of a key still queued behind its newest head, and a
    /// confirmed flush advances it past the flushed revision without dropping a newer queued head.
    /// </summary>
    [Fact]
    public void UnflushedIndex_TracksTheOldestQueuedRevision()
    {
        UnflushedKeyValueWritesIndex index = new();
        HLCTimestamp Ts(long l) => new(1, l, 0);

        index.Record("k", "a"u8.ToArray(), 5, HLCTimestamp.Zero, Ts(5), Ts(5), KeyValueState.Set, false);
        index.Record("k", "b"u8.ToArray(), 6, HLCTimestamp.Zero, Ts(6), Ts(6), KeyValueState.Set, false);
        index.Record("k", "c"u8.ToArray(), 7, HLCTimestamp.Zero, Ts(7), Ts(7), KeyValueState.Set, false);

        Assert.True(index.TryGet("k", out UnflushedKeyValueWrite queued));
        Assert.Equal(7, queued.Revision);
        Assert.Equal(5, queued.OldestRevision);

        // A stale record for an older revision replaces nothing but keeps the oldest bound honest.
        index.Record("k", "z"u8.ToArray(), 4, HLCTimestamp.Zero, Ts(4), Ts(4), KeyValueState.Set, false);
        Assert.True(index.TryGet("k", out queued));
        Assert.Equal(7, queued.Revision);
        Assert.Equal(4, queued.OldestRevision);

        index.RemoveFlushed("k", 6, Ts(6));
        Assert.True(index.TryGet("k", out queued));
        Assert.Equal(7, queued.Revision);
        Assert.Equal(7, queued.OldestRevision);

        index.RemoveFlushed("k", 7, Ts(7));
        Assert.False(index.TryGet("k", out _));
    }

    /// <summary>
    /// The history fence refuses a persisted-history answer exactly when a queued revision at or below the
    /// ceiling is missing from the archive, and allows it when the archive covers every queued revision.
    /// </summary>
    [Fact]
    public void HistoryFence_RefusesOnlyWhileAQueuedRevisionIsMissingFromTheArchive()
    {
        UnflushedKeyValueWritesIndex index = new();
        HLCTimestamp Ts(long l) => new(1, l, 0);

        for (long revision = 3; revision <= 6; revision++)
            index.Record("k", [], revision, HLCTimestamp.Zero, Ts(revision), Ts(revision), KeyValueState.Set, false);

        KeyValueRevisionHistory archive = new();
        archive[4] = new KeyValueRevisionEntry([], Ts(4), HLCTimestamp.Zero, KeyValueState.Set);
        archive[5] = new KeyValueRevisionEntry([], Ts(5), HLCTimestamp.Zero, KeyValueState.Set);

        // Revision 3 is queued and not archived: a read below the head (ceiling 5) cannot trust the disk.
        Assert.True(UnflushedHistoryFence.HistoryMayLag(index, "k", archive, 5));
        Assert.True(UnflushedHistoryFence.HistoryMayLag(index, "k", archive: null, 5));

        // Everything at or below the ceiling is below the oldest queued revision: the disk is complete there.
        Assert.False(UnflushedHistoryFence.HistoryMayLag(index, "k", archive, 2));

        // Revision 3 flushed: the queued run 4..5 is fully archived, so the disk answers for anything older.
        index.RemoveFlushed("k", 3, Ts(3));
        Assert.False(UnflushedHistoryFence.HistoryMayLag(index, "k", archive, 5));

        // No queued writes for the key at all.
        Assert.False(UnflushedHistoryFence.HistoryMayLag(index, "other", archive, 5));
    }
}
