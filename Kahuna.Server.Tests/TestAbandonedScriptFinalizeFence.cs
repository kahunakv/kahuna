using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A durable finalize that ends unresolved can leave the transaction's record initialized and some of its
/// prepared intents installed: a one-phase bundle whose prepare is refused on one key (held by another
/// transaction's committed-but-unsettled intent) has already installed the bundle's other keys, and the
/// finalize answers MustRetry expecting a retry of the same identity to reuse them. A script transaction never
/// retries under the same identity, so those intents are abandoned when the script returns. Left alone they
/// stay undecided until presumed-abort recovery reaches the decision deadline, and every writer of those keys,
/// including the script's own re-run, is refused with MustRetry for the whole window.
///
/// The leftover state is built here through the raw one-phase wire operation on the partition leader, exactly
/// as a remote coordinator's bundle lands, because a live script only produces it when its node's replica of
/// the intent store lags the leader between the pre-flight and the refusal. The fence the script executor
/// applies on the way out is then driven directly.
/// </summary>
public sealed class TestAbandonedScriptFinalizeFence
{
    private readonly ILoggerFactory loggerFactory;

    public TestAbandonedScriptFinalizeFence(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static async Task WaitUntil(Func<bool> predicate, CancellationToken ct, int timeoutMs = 10_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;

        while (Environment.TickCount64 < deadline)
        {
            if (predicate())
                return;

            await Task.Delay(50, ct);
        }

        Assert.True(predicate(), "condition not met in time");
    }

    private static PreparedIntent RawIntent(HLCTimestamp txId, string key, string anchorKey, HLCTimestamp commitTimestamp, HLCTimestamp deadline) =>
        new(txId, 1, key, ManifestHash: 0, RecordAnchorKey: anchorKey, CommitTimestamp: commitTimestamp,
            State: KeyValueState.Set, Value: Encoding.UTF8.GetBytes("raw"), Bucket: KeyValueKeySpace.OfKey(key), Revision: 1, Expires: HLCTimestamp.Zero,
            NoRevision: false, BaseRevision: 0, BaseState: KeyValueState.Set, RecoveryDeadline: deadline,
            Resolution: PreparedIntentResolution.Pending);

    /// <summary>One transaction's raw one-phase bundle ([record init, prepare, commit decision]) over
    /// <paramref name="keys"/>, anchored on the first key, as the production bundle lays it out.</summary>
    private static (byte[] Init, byte[] Prepare, byte[] Decision, List<PreparedIntent> Intents) RawOnePhase(
        HLCTimestamp txId, string[] keys, HLCTimestamp now, HLCTimestamp opId, HLCTimestamp deadline)
    {
        string anchorKey = keys[0];

        List<TransactionParticipantRef> manifest = new(keys.Length);
        List<PreparedIntent> intents = new(keys.Length);
        List<PreparedIntentCommand> prepares = new(keys.Length);

        foreach (string key in keys)
        {
            manifest.Add(new TransactionParticipantRef(key, KeyValueDurability.Persistent));
            PreparedIntent intent = RawIntent(txId, key, anchorKey, now, deadline);
            intents.Add(intent);
            prepares.Add(new PrepareIntentCommand(intent));
        }

        byte[] init = TransactionRecordStore.SerializeDelta([new InitializeTransactionCommand(
            txId, 1, anchorKey, anchorKey, now, deadline, 0, manifest, opId, now)]);
        byte[] prepare = PreparedIntentStore.SerializeDelta(prepares);
        byte[] decision = TransactionRecordStore.SerializeDelta([new CommitTransactionCommand(txId, 1, 0, opId, now, keys)]);

        return (init, prepare, decision, intents);
    }

    [Fact]
    public async Task AbandonedFinalize_IsFencedAtOnce_AndItsInstalledIntentFreesTheKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        EmbeddedKahunaOptions options = new()
        {
            InitialPartitions = 1,
            DurableDeferredSettlement = true,
            // Recovery must not be what frees the key: the fence is.
            CollectionInterval = TimeSpan.FromMinutes(10)
        };

        await using EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);

        string scope = "fence" + Guid.NewGuid().ToString("N")[..8];
        string heldKey = scope + "/state";
        string freshKey = scope + "/member";

        await node.WaitForLeaderForKeyAsync(heldKey, ct);

        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        (int partition, long generation) = kahuna.KeyValues.LocateDurablePartition(heldKey);

        HLCTimestamp now = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        HLCTimestamp deadline = new(now.N, now.L + 60_000, now.C);

        // ── The holder: a committed transaction whose intent on the held key has not settled. ──
        HLCTimestamp holder = new(now.N, now.L - 5_000, now.C);
        HLCTimestamp holderOp = new(now.N, now.L - 4_999, now.C);
        (byte[] holderInit, byte[] holderPrepare, byte[] holderDecision, _) = RawOnePhase(holder, [heldKey], now, holderOp, deadline);

        DurableOnePhaseWireReply? committed = await node.Kahuna.DurableOnePhaseLocal(
            partition, holderInit, holderPrepare, holderDecision, holder, 1, holderOp, null, 0, ct);
        Assert.NotNull(committed);
        Assert.True(committed!.Value.PrepareAcknowledged);
        Assert.Equal((int)TransactionDecision.Commit, committed.Value.Decision);
        Assert.Equal(holder, kahuna.DurablePreparedIntentStore.Get(heldKey)!.TransactionId);

        // ── The abandoned attempt: its bundle is refused on the held key, but the fresh key's intent is installed
        // and the record stays Undecided — the state a script's finalize answers MustRetry from. ──
        HLCTimestamp abandoned = new(now.N, now.L - 3_000, now.C);
        HLCTimestamp abandonedOp = new(now.N, now.L - 2_999, now.C);
        (byte[] init, byte[] prepare, byte[] decision, List<PreparedIntent> intents) = RawOnePhase(abandoned, [heldKey, freshKey], now, abandonedOp, deadline);

        DurableOnePhaseWireReply? refused = await node.Kahuna.DurableOnePhaseLocal(
            partition, init, prepare, decision, abandoned, 1, abandonedOp, null, 0, ct);
        Assert.NotNull(refused);
        Assert.True(refused!.Value.BatchCommitted);
        Assert.False(refused.Value.PrepareAcknowledged);
        Assert.Equal((int)TransactionDecision.Undecided, refused.Value.Decision);
        Assert.Equal((int)BundledCommitVerdict.PrepareMissing, refused.Value.GatedVerdict);

        PreparedIntent? leftover = kahuna.DurablePreparedIntentStore.Get(freshKey);
        Assert.NotNull(leftover);
        Assert.Equal(abandoned, leftover!.TransactionId);
        Assert.Equal(PreparedIntentResolution.Pending, leftover.Resolution);
        Assert.Equal(TransactionDecision.Undecided, kahuna.DurableTransactionRecordStore.Get(abandoned, 1)!.Decision);

        // The hazard: a writer of the fresh key is refused while the abandoned intent stays undecided.
        (KeyValueResponseType blockedType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, freshKey, "x"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.MustRetry, blockedType);

        // ── The fence the script executor applies when its finalize ended unresolved. ──
        DurableFinalizeInput input = new(
            abandoned, 1, heldKey, heldKey, partition, generation, now, deadline, 0,
            [new TransactionParticipantRef(heldKey, KeyValueDurability.Persistent), new TransactionParticipantRef(freshKey, KeyValueDurability.Persistent)],
            [new DurablePartitionPrepare(partition, generation, intents)],
            now);

        TransactionContext context = new()
        {
            TransactionId = abandoned,
            CoordinatorKey = heldKey,
            Locking = KeyValueTransactionLocking.Pessimistic,
            Result = new() { Type = KeyValueResponseType.MustRetry, Reason = "Durable finalize could not complete; retry" },
            UnresolvedDurableFinalize = input
        };

        await kahuna.KeyValues.Coordinator.FenceAbandonedFinalize(context);

        // Durably aborted, its leftover rolled back, and the outcome the caller was given still truthful.
        Assert.Null(context.UnresolvedDurableFinalize);
        Assert.Equal(KeyValueResponseType.MustRetry, context.Result!.Type);
        Assert.Equal(TransactionDecision.Abort, kahuna.DurableTransactionRecordStore.Get(abandoned, 1)!.Decision);

        // The fresh key is free at once: the durable abort decides the leftover intent for every writer that
        // meets it, with no decision deadline to wait out.
        (KeyValueResponseType freedType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, freshKey, "x"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, freedType);

        // The fence's rollback resolution settles the leftover off the critical path (deferred settlement), well
        // before any recovery sweep could.
        await WaitUntil(() => kahuna.DurablePreparedIntentStore.Get(freshKey) is null or { Resolution: not PreparedIntentResolution.Pending }, ct);

        // A fence with nothing unresolved is a no-op.
        await kahuna.KeyValues.Coordinator.FenceAbandonedFinalize(context);
        Assert.Equal(KeyValueResponseType.MustRetry, context.Result!.Type);
    }
}
