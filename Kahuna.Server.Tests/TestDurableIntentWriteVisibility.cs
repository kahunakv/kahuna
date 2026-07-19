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
/// Deferred-settlement writer visibility (§6.3): a non-transactional write that meets a foreign durable prepared
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
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            EnableDurableIntentTransactions = true
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
}
