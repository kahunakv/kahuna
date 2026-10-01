using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A non-transactional write proceeds over a committed-but-unsettled durable intent: it materializes the intent and
/// writes the next revision, while the intent lingers in the prepared-intent store until its settlement removes it.
/// Until then every read — latest or snapshot, point or scan, resident or a cache miss — must serve the newer head,
/// never the intent's older value; otherwise a client writes a key and reads the previous value back.
///
/// <para>The lingering intent is built directly in the durable stores (a pending intent plus its committed canonical
/// record, committed before the head was written) so the window stays open for the whole test.</para>
/// </summary>
public sealed class TestSupersededIntentReads
{
    private const int Partitions = 4;

    private readonly ILoggerFactory loggerFactory;

    public TestSupersededIntentReads(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = Partitions,
            DurableDeferredSettlement = true
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("superseded/x", ct);
        return node;
    }

    /// <summary>The key, its bucket, the intent's revision (the older committed value) and the head's revision.</summary>
    private readonly record struct SupersededState(string Key, string Bucket, long IntentRevision, long HeadRevision, HLCTimestamp BetweenTimestamp);

    /// <summary>
    /// Builds the window: transaction T1 committed <c>'old'</c> (its intent still pending settlement), then a
    /// non-transactional write moved the head past it — a SET of <c>'new'</c>, or a DELETE.
    /// </summary>
    private static async Task<SupersededState> BuildSupersededIntent(KahunaManager kahuna, bool deleteHead, CancellationToken ct)
    {
        string bucket = "superseded/" + Guid.NewGuid().ToString("N")[..8];
        string key = bucket + "/k";
        string anchor = bucket + "/committer";

        // T1's identity doubles as its commit timestamp, so it is minted before the head is written.
        (KeyValueResponseType started, TransactionHandle committer) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = anchor, Timeout = 10_000, Locking = KeyValueTransactionLocking.Optimistic }, ct);
        Assert.Equal(KeyValueResponseType.Set, started);
        HLCTimestamp txId = committer.TransactionId;

        (KeyValueResponseType set, long intentRevision, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, "old"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, set);

        ReadOnlyKeyValueEntry old = await Latest(kahuna, key, ct);
        HLCTimestamp between = old.LastModified;

        long headRevision;
        if (deleteHead)
        {
            (KeyValueResponseType deleted, long revision, _) = await kahuna.LocateAndTryDeleteKeyValue(
                HLCTimestamp.Zero, key, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Deleted, deleted);
            headRevision = revision;
        }
        else
        {
            (KeyValueResponseType setNew, long revision, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, "new"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, setNew);
            headRevision = revision;
        }

        Assert.True(headRevision > intentRevision);

        kahuna.DurablePreparedIntentStore.ImportIntents([new PreparedIntent(
            txId, Epoch: 0, key, ManifestHash: 0, RecordAnchorKey: anchor,
            CommitTimestamp: txId, State: KeyValueState.Set, Value: "old"u8.ToArray(), Bucket: bucket,
            Revision: intentRevision, Expires: HLCTimestamp.Zero, NoRevision: false, BaseRevision: intentRevision - 1,
            BaseState: KeyValueState.Undefined, RecoveryDeadline: HLCTimestamp.Zero,
            Resolution: PreparedIntentResolution.Pending)]);

        kahuna.DurableTransactionRecordStore.ImportRecords([new TransactionRecord(
            txId, Epoch: 0, CoordinatorKey: anchor, RecordAnchorKey: anchor,
            CommitTimestamp: txId, DecisionDeadline: HLCTimestamp.Zero, ManifestHash: 0,
            Participants: [new TransactionParticipantRef(key, KeyValueDurability.Persistent)], ManifestPresent: true,
            Decision: TransactionDecision.Commit, AbortClass: TransactionAbortClass.None, WinningOpId: txId,
            CreatedAt: txId, DecidedAt: txId)]);

        return new(key, bucket, intentRevision, headRevision, between);
    }

    private static async Task<ReadOnlyKeyValueEntry> Latest(IKahuna kahuna, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, type);
        return entry!;
    }

    /// <summary>Drops every resident entry of the key's partition, so the next read of the key is a cache miss.</summary>
    private static async Task EvictResident(EmbeddedKahunaNode node, KahunaManager kahuna, string key)
    {
        await node.FlushAsync();
        await kahuna.KeyValues.EvictPartitionEntriesAsync(
            PartitionDataEnumerator.HashPartitionOfKeySpace(KeyValueKeySpace.OfKey(key)!, Partitions));
    }

    private static async Task AssertPointReadsServeHead(IKahuna kahuna, SupersededState state, bool deleteHead, HLCTimestamp readTimestamp, CancellationToken ct)
    {
        (KeyValueResponseType get, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, state.Key, -1, readTimestamp, KeyValueDurability.Persistent, ct);

        (KeyValueResponseType exists, ReadOnlyKeyValueEntry? existsEntry) = await kahuna.LocateAndTryExistsValue(
            HLCTimestamp.Zero, state.Key, -1, readTimestamp, KeyValueDurability.Persistent, ct);

        if (deleteHead)
        {
            Assert.Equal(KeyValueResponseType.DoesNotExist, get);
            Assert.Equal(KeyValueResponseType.DoesNotExist, exists);
            return;
        }

        Assert.Equal(KeyValueResponseType.Get, get);
        Assert.Equal(state.HeadRevision, entry!.Revision);
        Assert.Equal("new"u8.ToArray(), entry.Value);

        Assert.Equal(KeyValueResponseType.Exists, exists);
        Assert.Equal(state.HeadRevision, existsEntry!.Revision);
    }

    private static async Task AssertScansServeHead(IKahuna kahuna, SupersededState state, bool deleteHead, CancellationToken ct)
    {
        KeyValueGetByRangeResult range = await kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, state.Bucket, null, true, null, false, 100, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, range.Type);

        KeyValueGetByBucketResult bucket = await kahuna.LocateAndGetByBucket(
            HLCTimestamp.Zero, state.Bucket, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, bucket.Type);

        foreach (List<(string Key, ReadOnlyKeyValueEntry Entry)> items in new[] { range.Items, bucket.Items })
        {
            List<(string Key, ReadOnlyKeyValueEntry Entry)> rows = items.Where(i => i.Key == state.Key).ToList();
            if (deleteHead)
            {
                Assert.Empty(rows);
                continue;
            }

            (string _, ReadOnlyKeyValueEntry row) = Assert.Single(rows);
            Assert.Equal(state.HeadRevision, row.Revision);
            Assert.Equal("new"u8.ToArray(), row.Value);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LatestPointReads_ServeTheNewerHead(bool deleteHead)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        SupersededState state = await BuildSupersededIntent(kahuna, deleteHead, ct);

        await AssertPointReadsServeHead(kahuna, state, deleteHead, HLCTimestamp.Zero, ct);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LatestPointReads_OnACacheMiss_ServeTheNewerHead(bool deleteHead)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        SupersededState state = await BuildSupersededIntent(kahuna, deleteHead, ct);
        await EvictResident(node, kahuna, state.Key);

        await AssertPointReadsServeHead(kahuna, state, deleteHead, HLCTimestamp.Zero, ct);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Scans_ServeTheNewerHead(bool deleteHead)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        SupersededState state = await BuildSupersededIntent(kahuna, deleteHead, ct);

        await AssertScansServeHead(kahuna, state, deleteHead, ct);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Scans_OnACacheMiss_ServeTheNewerHead(bool deleteHead)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        SupersededState state = await BuildSupersededIntent(kahuna, deleteHead, ct);
        await EvictResident(node, kahuna, state.Key);

        await AssertScansServeHead(kahuna, state, deleteHead, ct);
    }

    /// <summary>A snapshot at or after the newer head serves it; one taken between the two commits still serves the
    /// older committed value.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SnapshotPointReads_ServeTheRevisionAsOfTheSnapshot(bool deleteHead)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        SupersededState state = await BuildSupersededIntent(kahuna, deleteHead, ct);

        (_, TransactionHandle now) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions { CoordinatorKey = state.Bucket + "/clock", Timeout = 10_000 }, ct);
        await AssertPointReadsServeHead(kahuna, state, deleteHead, now.TransactionId, ct);

        (KeyValueResponseType before, ReadOnlyKeyValueEntry? older) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, state.Key, -1, state.BetweenTimestamp, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, before);
        Assert.Equal(state.IntentRevision, older!.Revision);
        Assert.Equal("old"u8.ToArray(), older.Value);
    }
}
