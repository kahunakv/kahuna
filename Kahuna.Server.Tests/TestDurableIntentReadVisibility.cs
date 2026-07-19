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

    private static PreparedIntent PendingIntent(string key, byte[] value) => new(
        TransactionId: new HLCTimestamp(0, 100, 0), Epoch: 1, Key: key, ManifestHash: 0, RecordAnchorKey: key,
        CommitTimestamp: new HLCTimestamp(0, 200, 0),
        State: KeyValueState.Set, Value: value, Bucket: null, Revision: 1, Expires: HLCTimestamp.Zero,
        NoRevision: false, BaseRevision: 0, BaseState: KeyValueState.Set,
        // Far-future recovery deadline so the periodic recovery sweep never resolves it during the test.
        RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0), Resolution: PreparedIntentResolution.Pending);

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
    }
}
