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
/// End-to-end coverage of the read-path §6 visibility wiring: with a lingering durable prepared intent (as would
/// exist under deferred settlement), a latest read consults the canonical outcome — a committed intent serves its
/// prepared value even though nothing materialized into MVCC, an undecided intent makes the read wait, and an
/// aborted intent is invisible. Intents are injected directly to simulate the pre-materialization window.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDurableIntentReadVisibility
{
    private readonly ILoggerFactory loggerFactory;

    public TestDurableIntentReadVisibility(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static PreparedIntent PendingIntent(string key, byte[] value) => IntentAt(key, value, 1);

    private static PreparedIntent IntentAt(string key, byte[] value, long revision) => new(
        TransactionId: new HLCTimestamp(0, 100, 0), Epoch: 1, Key: key, ManifestHash: 0, RecordAnchorKey: key,
        CommitTimestamp: new HLCTimestamp(0, 200, 0),
        State: KeyValueState.Set, Value: value, Bucket: null, Revision: revision, Expires: HLCTimestamp.Zero,
        NoRevision: false, BaseRevision: revision - 1, BaseState: KeyValueState.Set,
        // Far-future recovery deadline so the periodic recovery sweep never resolves it during the test.
        RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0), Resolution: PreparedIntentResolution.Pending);

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            EnableDurableIntentTransactions = true
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("vis/committed", ct);
        return node;
    }

    [Fact]
    public async Task LatestRead_ConsultsLingeringIntent_CommittedPendingAborted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            EnableDurableIntentTransactions = true
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("vis/committed", ct);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;

        // ── Committed lingering intent → the read serves the prepared value (no MVCC materialization) ──
        PreparedIntent committed = PendingIntent("vis/committed", Encoding.UTF8.GetBytes("V2"));
        store.Apply(new PrepareIntentCommand(committed));
        store.Apply(new ResolveIntentCommand(committed.TransactionId, committed.Epoch, "vis/committed", Commit: true));

        (KeyValueResponseType committedType, ReadOnlyKeyValueEntry? committedEntry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "vis/committed", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, committedType);
        Assert.Equal(Encoding.UTF8.GetBytes("V2"), committedEntry!.Value);

        // ── Undecided intent → the read waits/retries rather than returning a stale value ──
        store.Apply(new PrepareIntentCommand(PendingIntent("vis/pending", Encoding.UTF8.GetBytes("V?"))));

        // The actor replies WaitingForReplication; the client-facing locate path retries and surfaces MustRetry.
        (KeyValueResponseType pendingType, _) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "vis/pending", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.MustRetry, pendingType);

        // ── Aborted intent → invisible; the (absent) prior value is served ──
        PreparedIntent aborted = PendingIntent("vis/aborted", Encoding.UTF8.GetBytes("Vx"));
        store.Apply(new PrepareIntentCommand(aborted));
        store.Apply(new ResolveIntentCommand(aborted.TransactionId, aborted.Epoch, "vis/aborted", Commit: false));

        (KeyValueResponseType abortedType, _) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "vis/aborted", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, abortedType);

        // ── Exists mirrors the same contract on the still-lingering intents ──
        (KeyValueResponseType committedExists, _) = await node.Kahuna.LocateAndTryExistsValue(
            HLCTimestamp.Zero, "vis/committed", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Exists, committedExists);

        (KeyValueResponseType pendingExists, _) = await node.Kahuna.LocateAndTryExistsValue(
            HLCTimestamp.Zero, "vis/pending", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.MustRetry, pendingExists);

        (KeyValueResponseType abortedExists, _) = await node.Kahuna.LocateAndTryExistsValue(
            HLCTimestamp.Zero, "vis/aborted", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, abortedExists);
    }

    [Fact]
    public async Task ByRevisionRead_ConsultsCommittedIntentRevision()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;

        // A committed intent at revision 5 that has not materialized (absent from store and disk).
        PreparedIntent committed = IntentAt("vis/rev", Encoding.UTF8.GetBytes("R5"), 5);
        store.Apply(new PrepareIntentCommand(committed));
        store.Apply(new ResolveIntentCommand(committed.TransactionId, committed.Epoch, "vis/rev", Commit: true));

        // Reading exactly that revision must serve the intent's value, not DoesNotExist.
        (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "vis/rev", 5, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, getType);
        Assert.Equal(Encoding.UTF8.GetBytes("R5"), entry!.Value);

        (KeyValueResponseType existsType, _) = await node.Kahuna.LocateAndTryExistsValue(
            HLCTimestamp.Zero, "vis/rev", 5, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Exists, existsType);

        // A different revision the intent does not hold falls through to the ordinary (absent) lookup.
        (KeyValueResponseType otherRev, _) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "vis/rev", 4, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, otherRev);
    }

    [Fact]
    public async Task ByRevisionRead_UndecidedIntentRevision_Retry()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        store.Apply(new PrepareIntentCommand(IntentAt("vis/revpending", Encoding.UTF8.GetBytes("R?"), 3)));

        (KeyValueResponseType getType, _) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "vis/revpending", 3, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.MustRetry, getType);
    }

    [Fact]
    public async Task DeferredWindow_PendingIntentWithCommittedRecord_ServesCommittedValue()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        PreparedIntentStore intents = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        TransactionRecordStore records = ((KahunaManager)node.Kahuna).DurableTransactionRecordStore;

        HLCTimestamp txId = new(0, 100, 0);
        List<TransactionParticipantRef> manifest = [new("defer/k", KeyValueDurability.Persistent)];

        // Committed canonical decision, but the intent is left PENDING (not settled) — exactly the deferred window
        // between the durable decision and the off-critical-path resolution.
        records.Apply(new InitializeTransactionCommand(
            txId, 1, "coord", "defer/k", new(0, 200, 0), new(0, 9000, 0), 0, manifest, new(0, 150, 0), new(0, 50, 0)));
        records.Apply(new CommitTransactionCommand(txId, 1, 0, new(0, 150, 0), new(0, 150, 0)));
        Assert.Equal(TransactionDecision.Commit, records.Get(txId, 1)!.Decision);

        intents.Apply(new PrepareIntentCommand(IntentAt("defer/k", Encoding.UTF8.GetBytes("D1"), 1)));

        // The read finds a still-pending intent; consulting the canonical record (committed) serves its value.
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "defer/k", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.Equal(Encoding.UTF8.GetBytes("D1"), entry!.Value);
    }

    [Fact]
    public async Task DeferredWindow_PendingIntentWithUndecidedRecord_Retries()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        PreparedIntentStore intents = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        TransactionRecordStore records = ((KahunaManager)node.Kahuna).DurableTransactionRecordStore;

        HLCTimestamp txId = new(0, 100, 0);
        List<TransactionParticipantRef> manifest = [new("defer/u", KeyValueDurability.Persistent)];

        // Undecided canonical record + pending intent: the read must retry, never return a stale/undecided value.
        records.Apply(new InitializeTransactionCommand(
            txId, 1, "coord", "defer/u", new(0, 200, 0), new(0, 9000, 0), 0, manifest, new(0, 150, 0), new(0, 50, 0)));
        intents.Apply(new PrepareIntentCommand(IntentAt("defer/u", Encoding.UTF8.GetBytes("U?"), 1)));

        (KeyValueResponseType type, _) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "defer/u", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.MustRetry, type);
    }

    [Fact]
    public async Task CheckWriteIntent_FlagsUndecidedDurableIntent_NotCommittedOrOwn()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        HLCTimestamp otherTx = new(0, 500, 0); // != the intents' TransactionId (0,100,0)

        // Undecided foreign durable intent → a live concurrent writer → conflict.
        store.Apply(new PrepareIntentCommand(IntentAt("wi/pending", Encoding.UTF8.GetBytes("P"), 2)));
        Assert.Equal(KeyValueResponseType.Aborted, await node.Kahuna.LocateAndTryCheckWriteIntent(
            otherTx, "wi/pending", KeyValueDurability.Persistent, ct));

        // Committed foreign intent → not flagged here (revision validation handles staleness).
        PreparedIntent committed = IntentAt("wi/committed", Encoding.UTF8.GetBytes("C"), 2);
        store.Apply(new PrepareIntentCommand(committed));
        store.Apply(new ResolveIntentCommand(committed.TransactionId, committed.Epoch, "wi/committed", Commit: true));
        Assert.Equal(KeyValueResponseType.DoesNotExist, await node.Kahuna.LocateAndTryCheckWriteIntent(
            otherTx, "wi/committed", KeyValueDurability.Persistent, ct));

        // The intent's own transaction is not a foreign writer.
        Assert.Equal(KeyValueResponseType.DoesNotExist, await node.Kahuna.LocateAndTryCheckWriteIntent(
            new HLCTimestamp(0, 100, 0), "wi/pending", KeyValueDurability.Persistent, ct));
    }
}
