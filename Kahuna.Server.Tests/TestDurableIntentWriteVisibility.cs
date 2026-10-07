using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Deferred-settlement writer visibility: a non-transactional write that meets a foreign durable prepared
/// intent must resolve its canonical outcome before replacing it — a committed intent materializes (so the write's
/// existence checks and next revision are based on the committed value), an undecided intent forces a retry, and an
/// aborted intent is ignored. Drives the real LocateAndTrySetKeyValue entry point with injected intents.
/// </summary>
public sealed class TestDurableIntentWriteVisibility
{
    private readonly ILoggerFactory loggerFactory;

    public TestDurableIntentWriteVisibility(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static PreparedIntent Intent(string key, byte[] value, long revision, PreparedIntentResolution resolution, KeyValueState state = KeyValueState.Set) => new(
        TransactionId: new HLCTimestamp(0, 100, 0), Epoch: 1, Key: key, ManifestHash: 0, RecordAnchorKey: key,
        CommitTimestamp: new HLCTimestamp(0, 200, 0),
        State: state, Value: value, Bucket: null, Revision: revision, Expires: HLCTimestamp.Zero,
        NoRevision: false, BaseRevision: revision - 1, BaseState: KeyValueState.Set,
        RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0), Resolution: resolution);

    private static void Inject(PreparedIntentStore store, PreparedIntent intent, PreparedIntentResolution finalState)
    {
        store.Apply(new PrepareIntentCommand(intent with { Resolution = PreparedIntentResolution.Pending }));
        if (finalState == PreparedIntentResolution.Committed)
            store.Apply(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true));
        else if (finalState == PreparedIntentResolution.Aborted)
            store.Apply(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: false));
        // Pending: leave unresolved.
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
            InitialPartitions = 1,
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("wtest/a", ct);
        return node;
    }

    [Fact]
    public async Task Write_MeetingCommittedIntent_SeesItExists_SetIfNotExistsFails()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        Inject(store, Intent("wtest/a", Encoding.UTF8.GetBytes("V1"), 7, PreparedIntentResolution.Committed), PreparedIntentResolution.Committed);

        // The committed intent makes the key logically exist even though nothing is materialized yet:
        // SetIfNotExists must not create it.
        (KeyValueResponseType t, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, "wtest/a", Encoding.UTF8.GetBytes("V2"), null, -1,
            KeyValueFlags.SetIfNotExists, 0, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.NotSet, t);
    }

    [Fact]
    public async Task Write_MeetingCommittedIntent_BasesNextRevisionOnIt()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        Inject(store, Intent("wtest/c", Encoding.UTF8.GetBytes("V1"), 7, PreparedIntentResolution.Committed), PreparedIntentResolution.Committed);

        (KeyValueResponseType t, long revision, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, "wtest/c", Encoding.UTF8.GetBytes("V2"), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Set, t);
        Assert.Equal(8, revision); // committed intent revision 7 → new write revision 8
    }

    [Fact]
    public async Task Write_MeetingUndecidedIntent_MustRetry()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        Inject(store, Intent("wtest/b", Encoding.UTF8.GetBytes("V1"), 3, PreparedIntentResolution.Pending), PreparedIntentResolution.Pending);

        (KeyValueResponseType t, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, "wtest/b", Encoding.UTF8.GetBytes("V2"), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.MustRetry, t);
    }

    [Fact]
    public async Task Delete_MeetingCommittedIntentOnlyKey_DeletesIt()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        Inject(store, Intent("wtest/e", Encoding.UTF8.GetBytes("V1"), 4, PreparedIntentResolution.Committed), PreparedIntentResolution.Committed);

        // The key exists only as a committed intent; a delete must tombstone it rather than report DoesNotExist.
        (KeyValueResponseType t, _, _) = await node.Kahuna.LocateAndTryDeleteKeyValue(
            HLCTimestamp.Zero, "wtest/e", KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Deleted, t);
    }

    [Fact]
    public async Task Delete_MeetingUndecidedIntent_MustRetry()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        Inject(store, Intent("wtest/f", Encoding.UTF8.GetBytes("V1"), 2, PreparedIntentResolution.Pending), PreparedIntentResolution.Pending);

        (KeyValueResponseType t, _, _) = await node.Kahuna.LocateAndTryDeleteKeyValue(
            HLCTimestamp.Zero, "wtest/f", KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.MustRetry, t);
    }

    [Fact]
    public async Task Extend_MeetingCommittedIntentOnlyKey_ExtendsIt()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        Inject(store, Intent("wtest/g", Encoding.UTF8.GetBytes("V1"), 6, PreparedIntentResolution.Committed), PreparedIntentResolution.Committed);

        (KeyValueResponseType t, _, _) = await node.Kahuna.LocateAndTryExtendKeyValue(
            HLCTimestamp.Zero, "wtest/g", 30000, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Extended, t);
    }

    [Fact]
    public async Task Extend_MeetingUndecidedIntent_MustRetry()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        Inject(store, Intent("wtest/h", Encoding.UTF8.GetBytes("V1"), 1, PreparedIntentResolution.Pending), PreparedIntentResolution.Pending);

        (KeyValueResponseType t, _, _) = await node.Kahuna.LocateAndTryExtendKeyValue(
            HLCTimestamp.Zero, "wtest/h", 30000, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.MustRetry, t);
    }

    [Fact]
    public async Task Write_MeetingAbortedIntent_ProceedsAsIfAbsent()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        Inject(store, Intent("wtest/d", Encoding.UTF8.GetBytes("V1"), 5, PreparedIntentResolution.Aborted), PreparedIntentResolution.Aborted);

        // Aborted intent is invisible: SetIfNotExists succeeds (the key does not exist) at revision 0.
        (KeyValueResponseType t, long revision, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, "wtest/d", Encoding.UTF8.GetBytes("V2"), null, -1,
            KeyValueFlags.SetIfNotExists, 0, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Set, t);
        Assert.Equal(0, revision);
    }

    /// <summary>
    /// A committed intent's commit timestamp is minted on the coordinator's clock, which can run ahead of this
    /// leader's clock. A write that bases its next revision on that intent must still stamp a later
    /// LastModified than the revision it replaces: MVCC reads by timestamp and the revision archive assume
    /// LastModified grows with the revision of a key.
    /// </summary>
    [Fact]
    public async Task Write_MeetingCommittedIntentFromFasterClock_StampsAfterIt()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        HLCTimestamp aheadCommit = new(0, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 10_000, 0);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        Inject(store, Intent("wtest/i", Encoding.UTF8.GetBytes("V1"), 7, PreparedIntentResolution.Committed) with { CommitTimestamp = aheadCommit },
            PreparedIntentResolution.Committed);

        (KeyValueResponseType t, long revision, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, "wtest/i", Encoding.UTF8.GetBytes("V2"), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Set, t);
        Assert.Equal(8, revision);

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "wtest/i", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Get, readType);
        Assert.NotNull(entry);
        Assert.Equal(8, entry.Revision);
        Assert.True(entry.LastModified > aheadCommit,
            $"revision 8 stamped {entry.LastModified}, not after revision 7's commit timestamp {aheadCommit}");
    }

    /// <summary>
    /// The read-side consequence: a snapshot read just before the committed intent's commit timestamp must not
    /// observe the write that was based on that intent. The write happened after the commit, so it cannot be
    /// visible at a snapshot where the commit itself is not.
    /// </summary>
    [Fact]
    public async Task SnapshotRead_BeforeCommittedIntent_DoesNotSeeTheWriteBasedOnIt()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        HLCTimestamp aheadCommit = new(0, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 10_000, 0);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        Inject(store, Intent("wtest/j", Encoding.UTF8.GetBytes("V1"), 7, PreparedIntentResolution.Committed) with { CommitTimestamp = aheadCommit },
            PreparedIntentResolution.Committed);

        (KeyValueResponseType t, long revision, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, "wtest/j", Encoding.UTF8.GetBytes("V2"), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Set, t);
        Assert.Equal(8, revision);

        HLCTimestamp beforeCommit = new(0, aheadCommit.L - 1, 0);

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "wtest/j", -1, beforeCommit, KeyValueDurability.Persistent, ct);

        Assert.False(readType == KeyValueResponseType.Get && entry is { Revision: 8 },
            $"a snapshot read at {beforeCommit}, before revision 7's commit at {aheadCommit}, returned revision 8 stamped {entry?.LastModified}");
    }

    /// <summary>
    /// A delete over a committed intent from a faster clock takes a new revision, so its tombstone must stamp
    /// after the commit it replaces. A snapshot read at the commit timestamp then still sees the committed value,
    /// not a tombstone that was written later.
    /// </summary>
    [Fact]
    public async Task Delete_MeetingCommittedIntentFromFasterClock_StampsAfterIt()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        HLCTimestamp aheadCommit = new(0, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 10_000, 0);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        Inject(store, Intent("wtest/k", Encoding.UTF8.GetBytes("V1"), 7, PreparedIntentResolution.Committed) with { CommitTimestamp = aheadCommit },
            PreparedIntentResolution.Committed);

        (KeyValueResponseType t, _, _) = await node.Kahuna.LocateAndTryDeleteKeyValue(
            HLCTimestamp.Zero, "wtest/k", KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Deleted, t);

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "wtest/k", -1, aheadCommit, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Get, readType);
        Assert.NotNull(entry);
        Assert.Equal(7, entry.Revision);
        Assert.Equal(Encoding.UTF8.GetBytes("V1"), entry.Value);
    }

    /// <summary>
    /// An extend keeps the revision, so LastModified is the only thing that orders it after the committed head it
    /// replaces. It must stamp after the commit even when the commit came from a faster clock.
    /// </summary>
    [Fact]
    public async Task Extend_MeetingCommittedIntentFromFasterClock_StampsAfterIt()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        HLCTimestamp aheadCommit = new(0, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 10_000, 0);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        Inject(store, Intent("wtest/l", Encoding.UTF8.GetBytes("V1"), 7, PreparedIntentResolution.Committed) with { CommitTimestamp = aheadCommit },
            PreparedIntentResolution.Committed);

        (KeyValueResponseType t, _, _) = await node.Kahuna.LocateAndTryExtendKeyValue(
            HLCTimestamp.Zero, "wtest/l", 60_000, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Extended, t);

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "wtest/l", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Get, readType);
        Assert.NotNull(entry);
        Assert.Equal(7, entry.Revision);
        Assert.True(entry.LastModified > aheadCommit,
            $"the extend of revision 7 stamped {entry.LastModified}, not after its commit timestamp {aheadCommit}");
    }

    /// <summary>
    /// A transaction that writes over a committed intent from a faster clock stages its write on this leader. The
    /// coordinator mints the commit timestamp above the highest staged stamp, so the staged stamp must already be
    /// later than the commit the write replaces. Otherwise the committed revision can land before it.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TransactionalWrite_MeetingCommittedIntentFromFasterClock_StagesAfterIt(bool delete)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        HLCTimestamp aheadCommit = new(0, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 10_000, 0);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        Inject(store, Intent("wtest/m", Encoding.UTF8.GetBytes("V1"), 7, PreparedIntentResolution.Committed) with { CommitTimestamp = aheadCommit },
            PreparedIntentResolution.Committed);

        HLCTimestamp transactionId = new(0, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 0);

        (KeyValueResponseType t, long revision, HLCTimestamp stagedAt) = delete
            ? await node.Kahuna.LocateAndTryDeleteKeyValue(transactionId, "wtest/m", KeyValueDurability.Persistent, ct)
            : await node.Kahuna.LocateAndTrySetKeyValue(
                transactionId, "wtest/m", Encoding.UTF8.GetBytes("V2"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

        Assert.Equal(delete ? KeyValueResponseType.Deleted : KeyValueResponseType.Set, t);
        Assert.Equal(8, revision);
        Assert.True(stagedAt > aheadCommit,
            $"revision 8 staged at {stagedAt}, not after revision 7's commit timestamp {aheadCommit}");
    }

    /// <summary>
    /// An extend keeps the committed intent's revision. Until the intent settles it lingers in the store, and every
    /// latest read path must still answer from the extended head: the point get and the snapshot a transaction
    /// pins on its first read.
    /// </summary>
    [Fact]
    public async Task Extend_OverCommittedIntent_LatestReadsSeeTheExtend()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        Inject(store, Intent("wtest/n", Encoding.UTF8.GetBytes("V1"), 7, PreparedIntentResolution.Committed), PreparedIntentResolution.Committed);

        (KeyValueResponseType t, _, _) = await node.Kahuna.LocateAndTryExtendKeyValue(
            HLCTimestamp.Zero, "wtest/n", 60_000, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Extended, t);
        Assert.NotNull(store.Get("wtest/n"));

        (KeyValueResponseType latestType, ReadOnlyKeyValueEntry? latest) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "wtest/n", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Get, latestType);
        Assert.NotNull(latest);
        Assert.Equal(7, latest.Revision);
        Assert.NotEqual(HLCTimestamp.Zero, latest.Expires);

        HLCTimestamp transactionId = new(0, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 0);

        (KeyValueResponseType pinnedType, ReadOnlyKeyValueEntry? pinned) = await node.Kahuna.LocateAndTryGetValue(
            transactionId, "wtest/n", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Get, pinnedType);
        Assert.NotNull(pinned);
        Assert.Equal(7, pinned.Revision);
        Assert.Equal(latest.Expires, pinned.Expires);
    }

    /// <summary>
    /// An extend whose new expiry has elapsed makes the key absent. The committed intent it extended has no expiry,
    /// so a read or a scan that served the lingering intent would bring the expired key back.
    /// </summary>
    [Fact]
    public async Task Extend_OverCommittedIntent_ElapsedExpiry_ReadsAndScansDoNotServeTheIntent()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        Inject(store, Intent("wtest/o", Encoding.UTF8.GetBytes("V1"), 7, PreparedIntentResolution.Committed), PreparedIntentResolution.Committed);

        (KeyValueResponseType t, _, _) = await node.Kahuna.LocateAndTryExtendKeyValue(
            HLCTimestamp.Zero, "wtest/o", 1, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Extended, t);

        await Task.Delay(50, ct);

        Assert.NotNull(store.Get("wtest/o"));

        (KeyValueResponseType readType, _) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "wtest/o", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.DoesNotExist, readType);

        KeyValueGetByBucketResult bucket = await node.Kahuna.LocateAndGetByBucket(
            HLCTimestamp.Zero, "wtest", HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Get, bucket.Type);
        Assert.DoesNotContain(bucket.Items, i => i.Item1 == "wtest/o");

        KeyValueGetByRangeResult range = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, "wtest/", null, true, null, true, 100, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Get, range.Type);
        Assert.DoesNotContain(range.Items, i => i.Item1 == "wtest/o");
    }

    /// <summary>
    /// A snapshot read between the intent's commit and a later extend must see the committed version, not the
    /// extend: the extended head is not visible at that snapshot.
    /// </summary>
    [Fact]
    public async Task SnapshotRead_BetweenCommitAndExtend_SeesTheCommittedVersion()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        Inject(store, Intent("wtest/p", Encoding.UTF8.GetBytes("V1"), 7, PreparedIntentResolution.Committed), PreparedIntentResolution.Committed);

        HLCTimestamp beforeExtend = new(0, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 1, 0);

        (KeyValueResponseType t, _, _) = await node.Kahuna.LocateAndTryExtendKeyValue(
            HLCTimestamp.Zero, "wtest/p", 60_000, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Extended, t);

        (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "wtest/p", -1, beforeExtend, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Get, readType);
        Assert.NotNull(entry);
        Assert.Equal(7, entry.Revision);
        Assert.Equal(Encoding.UTF8.GetBytes("V1"), entry.Value);
        Assert.Equal(HLCTimestamp.Zero, entry.Expires);
    }
}
