using System.Text;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The replica stale-base veto: when a node applies a replicated validated-base prepare and its own fence
/// memory proves the base moved, that verdict is deterministically correct — heads record only settled commits,
/// in the same log order the prepare applies in — even when the acknowledging leader's frozen memory admitted
/// the prepare. The veto makes the replica's verdict count: it drives a best-effort abort at the transaction's
/// anchor, and the record state machine keeps a late veto harmless (an abort never overwrites a commit).
///
/// <para>Store-level tests pin the hook's trigger semantics (fires only on a stale flag, never on the restore
/// path); the embedded-node tests prove the wired end-to-end drive — abort recorded at the anchor — and the
/// late case — a pre-existing commit stands and is counted as an acknowledged stale-base commit.</para>
/// </summary>
public sealed class TestStaleBaseVeto
{
    private readonly ILoggerFactory loggerFactory;

    public TestStaleBaseVeto(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static PreparedIntent MakeIntent(
        string key, HLCTimestamp txId, long epoch, long revision, long baseRevision, KeyValueState baseState,
        long manifestHash = 0, string? anchorKey = null) => new(
        TransactionId: txId, Epoch: epoch, Key: key,
        ManifestHash: manifestHash, RecordAnchorKey: anchorKey ?? key,
        CommitTimestamp: new HLCTimestamp(txId.N, txId.L + 1, txId.C),
        State: KeyValueState.Set, Value: [1, 2, 3], Bucket: null,
        Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: baseRevision, BaseState: baseState,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    /// <summary>Runs one commit lifecycle through the store so its head is remembered by the fence memory.</summary>
    private static void CommitThroughStore(PreparedIntentStore store, PreparedIntent intent)
    {
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(intent)).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied,
            store.Apply(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true)).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied,
            store.Apply(new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key)).Outcome);
    }

    // Copy the serialized bytes so the apply cannot recognize them as this process's own proposal and must
    // decode them — the follower/decoded path is the one the veto exists for.
    private static RaftLog IntentLog(params PreparedIntentCommand[] commands) =>
        new() { LogType = ReplicationTypes.PreparedIntent, LogData = [.. PreparedIntentStore.SerializeDelta(commands)] };

    // ── store-level: the hook's trigger semantics ───────────────────────────────

    [Fact]
    public void StaleFlaggedPrepare_FiresTheVetoHook_WithIntentAndHead()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("veto/hot", Ts(1_000), epoch: 1, revision: 6, baseRevision: 5, KeyValueState.Set));

        List<(PreparedIntent Intent, long Head)> vetoes = [];
        store.AttachStaleBaseVetoer((intent, head) => vetoes.Add((intent, head)));

        PreparedIntent stale = MakeIntent("veto/hot", Ts(1_100), epoch: 1, revision: 6, baseRevision: 5, KeyValueState.Set);
        bool acked = store.ApplyDeltaAckPrepares(IntentLog(new PrepareIntentCommand(stale)));

        Assert.False(acked);
        (PreparedIntent vetoed, long head) = Assert.Single(vetoes);
        Assert.Equal(stale.TransactionId, vetoed.TransactionId);
        Assert.Equal("veto/hot", vetoed.Key);
        Assert.Equal(6, head);
    }

    [Fact]
    public void CurrentBasePrepare_DoesNotFireTheHook()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("veto/current", Ts(1_000), epoch: 1, revision: 6, baseRevision: 5, KeyValueState.Set));

        int vetoes = 0;
        store.AttachStaleBaseVetoer((_, _) => vetoes++);

        bool acked = store.ApplyDeltaAckPrepares(IntentLog(new PrepareIntentCommand(
            MakeIntent("veto/current", Ts(1_100), epoch: 1, revision: 7, baseRevision: 6, KeyValueState.Set))));

        Assert.True(acked);
        Assert.Equal(0, vetoes);
    }

    [Fact]
    public void ForeignHolderRejection_WithoutAStaleFlag_DoesNotFireTheHook()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("veto/held", Ts(1_000), epoch: 1, revision: 6, baseRevision: 5, KeyValueState.Set));

        // A live foreign intent now owns the key; the next prepare is a state-machine rejection, and the
        // fence is never evaluated for it (it is not a fresh install).
        Assert.Equal(TransactionApplyOutcome.Applied,
            store.Apply(new PrepareIntentCommand(
                MakeIntent("veto/held", Ts(1_100), epoch: 1, revision: 7, baseRevision: 6, KeyValueState.Set))).Outcome);

        int vetoes = 0;
        store.AttachStaleBaseVetoer((_, _) => vetoes++);

        bool acked = store.ApplyDeltaAckPrepares(IntentLog(new PrepareIntentCommand(
            MakeIntent("veto/held", Ts(1_200), epoch: 1, revision: 7, baseRevision: 6, KeyValueState.Set))));

        Assert.False(acked);
        Assert.Equal(0, vetoes);
    }

    [Fact]
    public void RestoreReplay_NeverFiresTheHook()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("veto/replay", Ts(1_000), epoch: 1, revision: 6, baseRevision: 5, KeyValueState.Set));

        int vetoes = 0;
        store.AttachStaleBaseVetoer((_, _) => vetoes++);

        // The same stale shape the veto tests above refuse — but arriving through the restore path, which is
        // replayed history whose transactions are long decided. Restore must apply without a verdict.
        Assert.True(store.Restore(partitionId: 1, IntentLog(new PrepareIntentCommand(
            MakeIntent("veto/replay", Ts(1_100), epoch: 1, revision: 6, baseRevision: 5, KeyValueState.Set)))));

        Assert.Equal(0, vetoes);
    }

    [Fact]
    public void BlindWrite_WithAMovedHead_DoesNotFireTheHook()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("veto/blind", Ts(1_000), epoch: 1, revision: 6, baseRevision: 5, KeyValueState.Set));

        int vetoes = 0;
        store.AttachStaleBaseVetoer((_, _) => vetoes++);

        // No validated base (last-writer-wins by design): the fence must not judge it, so no veto either.
        bool acked = store.ApplyDeltaAckPrepares(IntentLog(new PrepareIntentCommand(
            MakeIntent("veto/blind", Ts(1_100), epoch: 1, revision: 7,
                baseRevision: PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined))));

        Assert.True(acked);
        Assert.Equal(0, vetoes);
    }

    // ── embedded node: the wired end-to-end drive ───────────────────────────────

    private static EmbeddedKahunaOptions Options() => new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 4
    };

    /// <summary>Commits one durable script transaction and waits until its intents settle, so the key's
    /// committed head is recorded in the fence memory.</summary>
    private static async Task CommitAndSettleAsync(EmbeddedKahunaNode node, KahunaManager kahuna, string key, string value)
    {
        KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes($"BEGIN SET `{key}` '{value}' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, result.Type);

        await WaitUntil(() => kahuna.DurablePreparedIntentStore.Get(key) is null);
    }

    [Fact]
    public async Task ReplicaStaleVerdict_DrivesAnAbort_AtTheAnchor()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(Options(), loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("veto/e2e", ct);

        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        // Two settled commits move the key's committed head to a real revision the fence remembers.
        await CommitAndSettleAsync(node, kahuna, "veto/e2e", "v1");
        await CommitAndSettleAsync(node, kahuna, "veto/e2e", "v2");

        await WaitUntil(() => kahuna.DurablePreparedIntentStore.TryGetCommittedHead("veto/e2e", out _, out _));
        Assert.True(kahuna.DurablePreparedIntentStore.TryGetCommittedHead("veto/e2e", out long head, out _));

        // The poisoned-leader shape: a prepare validated against a base below the settled head enters the
        // replicated stream (a frozen leader admits it; this node's fence proves it stale). The transaction id
        // must sit near the fence watermark (real HLC time), or the retention staleness gate fires instead of
        // the head comparison.
        HLCTimestamp staleTx = new(0, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 0);
        PreparedIntent stale = MakeIntent("veto/e2e", staleTx, epoch: 1, revision: head, baseRevision: head - 1, KeyValueState.Set);

        long sentBefore = DurableTransactionMetrics.StaleBaseVetoesSentCount;
        long upheldBefore = DurableTransactionMetrics.StaleBaseVetoesUpheldCount;

        Assert.False(kahuna.DurablePreparedIntentStore.ApplyDeltaAckPrepares(
            IntentLog(new PrepareIntentCommand(stale))));

        // The wired hook drives the abort detached; the record state machine mints an abort tombstone from
        // absence at the (local) anchor.
        await WaitUntil(() =>
            kahuna.DurableTransactionRecordStore.Get(staleTx, 1) is { Decision: TransactionDecision.Abort });

        Assert.True(DurableTransactionMetrics.StaleBaseVetoesSentCount >= sentBefore + 1);
        await WaitUntil(() => DurableTransactionMetrics.StaleBaseVetoesUpheldCount >= upheldBefore + 1);
    }

    [Fact]
    public async Task LateVeto_NeverOverwritesACommit_AndCountsTheAcknowledgedFork()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(Options(), loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("veto/late", ct);

        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        await CommitAndSettleAsync(node, kahuna, "veto/late", "v1");
        await CommitAndSettleAsync(node, kahuna, "veto/late", "v2");
        await WaitUntil(() => kahuna.DurablePreparedIntentStore.TryGetCommittedHead("veto/late", out _, out _));
        Assert.True(kahuna.DurablePreparedIntentStore.TryGetCommittedHead("veto/late", out long head, out _));

        // The residual race, reproduced deterministically: the stale-base transaction's COMMIT decision is
        // already durable when the prepare applies here. Seed the canonical record first.
        HLCTimestamp forkTx = new(0, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), 0);
        const long epoch = 1;
        IReadOnlyList<TransactionParticipantRef> manifest = [new("veto/late", KeyValueDurability.Persistent)];
        HLCTimestamp commitTs = new(forkTx.N, forkTx.L + 1, forkTx.C);
        long hash = TransactionManifest.ComputeHash(forkTx, epoch, "veto/late", commitTs, manifest);

        Assert.True(kahuna.DurableTransactionRecordStore.Replicate(0, new RaftLog
        {
            LogType = ReplicationTypes.TransactionRecord,
            LogData = [.. TransactionRecordStore.SerializeDelta([
                new InitializeTransactionCommand(forkTx, epoch, "veto/late", "veto/late", commitTs,
                    new HLCTimestamp(forkTx.N, forkTx.L + 60_000, forkTx.C), hash, manifest, OpId: forkTx, CreatedAt: forkTx),
                new CommitTransactionCommand(forkTx, epoch, hash, OpId: commitTs, AttemptHlc: commitTs)])]
        }));

        PreparedIntent stale = MakeIntent("veto/late", forkTx, epoch, revision: head, baseRevision: head - 1,
            KeyValueState.Set, manifestHash: hash);

        long lateBefore = DurableTransactionMetrics.StaleBaseVetoesLateCount;

        Assert.False(kahuna.DurablePreparedIntentStore.ApplyDeltaAckPrepares(
            IntentLog(new PrepareIntentCommand(stale))));

        // The veto loses at the anchor, is counted as a confirmed acknowledged stale-base commit, and the
        // commit stands untouched.
        await WaitUntil(() => DurableTransactionMetrics.StaleBaseVetoesLateCount >= lateBefore + 1);
        Assert.Equal(TransactionDecision.Commit, kahuna.DurableTransactionRecordStore.Get(forkTx, epoch)!.Decision);
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
}
