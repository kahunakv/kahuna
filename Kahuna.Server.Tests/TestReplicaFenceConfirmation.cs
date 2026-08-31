using System.Text;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The pre-decision replica fence confirmation: before the finalizer proposes a commit, it reads the
/// staged-base fence verdict of every replica of the participant partitions. The prepare acknowledgement
/// folds only the LEADER's verdict, and a leader whose committed-head memory is frozen or freshly restored
/// admits exactly the prepares the healthy replicas refuse; the detached stale-base veto carried those
/// verdicts but raced the commit at the anchor, and a veto that lost the race only logged the acknowledged
/// stale-base commit. The confirmation orders the refusal ahead of the decision.
///
/// <para>Store-level tests pin the verdict semantics; the cluster tests prove the wired end-to-end flow —
/// a blind leader's stale-base commit is refused before the decision, and a healthy read-modify-write
/// commit pays no refusal.</para>
/// </summary>
public sealed class TestReplicaFenceConfirmation : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestReplicaFenceConfirmation(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    // ── store-level: the verdict semantics ──────────────────────────────────────

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static PreparedIntent MakeIntent(
        string key, HLCTimestamp txId, long epoch, long revision, long baseRevision, KeyValueState baseState) => new(
        TransactionId: txId, Epoch: epoch, Key: key,
        ManifestHash: 0, RecordAnchorKey: key,
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

    [Fact]
    public void PendingValidatedBaseIntent_BehindTheHead_AnswersStaleBase()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("rfc/stale", Ts(1_000), epoch: 1, revision: 6, baseRevision: 5, KeyValueState.Set));

        PreparedIntent stale = MakeIntent("rfc/stale", Ts(1_100), epoch: 1, revision: 7, baseRevision: 5, KeyValueState.Set);
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(stale)).Outcome);

        KeyValueStagedBaseVerdictEntry[] verdicts = store.EvaluateReplicaFenceVerdicts(stale.TransactionId, 1, ["rfc/stale"]);

        KeyValueStagedBaseVerdictEntry verdict = Assert.Single(verdicts);
        Assert.Equal(KeyValueStagedBaseVerdict.StaleBase, verdict.Verdict);
        Assert.Equal(6, verdict.HeadRevision);
    }

    [Fact]
    public void PendingValidatedBaseIntent_AtTheHead_AnswersClear()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("rfc/current", Ts(1_000), epoch: 1, revision: 6, baseRevision: 5, KeyValueState.Set));

        PreparedIntent current = MakeIntent("rfc/current", Ts(1_100), epoch: 1, revision: 7, baseRevision: 6, KeyValueState.Set);
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(current)).Outcome);

        KeyValueStagedBaseVerdictEntry[] verdicts = store.EvaluateReplicaFenceVerdicts(current.TransactionId, 1, ["rfc/current"]);

        Assert.Equal(KeyValueStagedBaseVerdict.Clear, Assert.Single(verdicts).Verdict);
    }

    [Fact]
    public void AbsentOrForeignIntent_AnswersNotApplied()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("rfc/foreign", Ts(1_000), epoch: 1, revision: 6, baseRevision: 5, KeyValueState.Set));

        // No intent at all: this node cannot attest.
        KeyValueStagedBaseVerdictEntry[] absent = store.EvaluateReplicaFenceVerdicts(Ts(1_100), 1, ["rfc/foreign"]);
        Assert.Equal(KeyValueStagedBaseVerdict.NotApplied, Assert.Single(absent).Verdict);

        // A live intent under another identity: not this transaction's prepare, so still no attestation.
        PreparedIntent holder = MakeIntent("rfc/foreign", Ts(1_200), epoch: 1, revision: 7, baseRevision: 6, KeyValueState.Set);
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(holder)).Outcome);

        KeyValueStagedBaseVerdictEntry[] foreign = store.EvaluateReplicaFenceVerdicts(Ts(1_300), 1, ["rfc/foreign"]);
        Assert.Equal(KeyValueStagedBaseVerdict.NotApplied, Assert.Single(foreign).Verdict);
    }

    [Fact]
    public void BlindWriteAndResolvedIntent_AnswerClear()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("rfc/blind", Ts(1_000), epoch: 1, revision: 6, baseRevision: 5, KeyValueState.Set));

        // A blind write carries no validated base: the fence has nothing to judge even behind the head.
        PreparedIntent blind = MakeIntent("rfc/blind", Ts(1_100), epoch: 1, revision: 7,
            PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined);
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(blind)).Outcome);

        KeyValueStagedBaseVerdictEntry[] verdicts = store.EvaluateReplicaFenceVerdicts(blind.TransactionId, 1, ["rfc/blind"]);
        Assert.Equal(KeyValueStagedBaseVerdict.Clear, Assert.Single(verdicts).Verdict);

        // A resolved intent: the canonical record owns the outcome, so there is no refusal left to give.
        Assert.Equal(TransactionApplyOutcome.Applied,
            store.Apply(new ResolveIntentCommand(blind.TransactionId, blind.Epoch, "rfc/blind", Commit: true)).Outcome);

        verdicts = store.EvaluateReplicaFenceVerdicts(blind.TransactionId, 1, ["rfc/blind"]);
        Assert.Equal(KeyValueStagedBaseVerdict.Clear, Assert.Single(verdicts).Verdict);
    }

    [Fact]
    public void ForgottenHeadMemory_AdmitsTheStaleShape()
    {
        PreparedIntentStore store = new();
        CommitThroughStore(store, MakeIntent("rfc/forget", Ts(1_000), epoch: 1, revision: 6, baseRevision: 5, KeyValueState.Set));
        Assert.True(store.TryGetCommittedHead("rfc/forget", out _, out _));

        store.ForgetCommittedHeadsForTesting();
        Assert.False(store.TryGetCommittedHead("rfc/forget", out _, out _));

        // The restore-blind window: with no head remembered, the stale shape is admitted — the exact
        // blindness the replica confirmation exists to compensate for.
        PreparedIntent stale = MakeIntent("rfc/forget", Ts(1_100), epoch: 1, revision: 7, baseRevision: 5, KeyValueState.Set);
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(stale)).Outcome);

        KeyValueStagedBaseVerdictEntry[] verdicts = store.EvaluateReplicaFenceVerdicts(stale.TransactionId, 1, ["rfc/forget"]);
        Assert.Equal(KeyValueStagedBaseVerdict.Clear, Assert.Single(verdicts).Verdict);
    }

    // ── cluster: the wired end-to-end flow ──────────────────────────────────────

    private static async Task<int> LeaderIndexOf(int partition, IRaft[] rafts, CancellationToken ct)
    {
        while (true)
        {
            for (int i = 0; i < rafts.Length; i++)
                if (await rafts[i].AmILeaderIfHosted(partition, ct))
                    return i;

            await Task.Delay(50, ct);
        }
    }

    /// <summary>
    /// The k145d2 / g152r1 shape, ordered: a read-modify-write validates its base, a competitor commits the
    /// same base inside the probe→prepare window, and the KEY partition's leader has an empty committed-head
    /// memory (the state a restart leaves), so its fence admits the stale prepare that every healthy replica
    /// refuses. Before the confirmation, the replicas' refusal rode a detached veto that raced the commit;
    /// now the finalizer reads their verdicts before the decision and the commit must abort — the
    /// competitor's write survives and the refusal is counted.
    /// </summary>
    [Fact]
    public async Task BlindLeaderStaleBaseCommit_IsRefusedBeforeTheDecision()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger,
                // Short staged-write intent lease so the competitor can slip into the probe→prepare
                // window after a small delay — the same lapse a paused coordinator suffers.
                configure: config => config.StagedWriteIntentLeaseMs = 200);

        IRaft[] rafts = [raft1, raft2, raft3];
        KahunaManager[] managers = [(KahunaManager)kahuna1, (KahunaManager)kahuna2, (KahunaManager)kahuna3];

        try
        {
            await RunUnderStableLeadership(raft1, 4, async () =>
            {
                string key = "rfc-e2e/" + Guid.NewGuid().ToString("N")[..8];

                // Seed through a durable script commit so every node's fence memory records the key's head.
                KeyValueTransactionResult seeded = await RetryOnMustRetry(
                    kahuna1, Encoding.UTF8.GetBytes($"BEGIN SET `{key}` '100' COMMIT END"), null, null);
                Assert.Equal(KeyValueResponseType.Set, seeded.Type);

                foreach (KahunaManager manager in managers)
                    await WaitUntilAsync(() => manager.DurablePreparedIntentStore.TryGetCommittedHead(key, out _, out _));

                int keyPartition = raft1.GetPartitionKey(key);
                int leaderIndex = await LeaderIndexOf(keyPartition, rafts, ct);

                // The victim: an optimistic read-modify-write. The registered read folds the base
                // observation; the staged write moves it into the written-base set, which read-set
                // validation deliberately skips — only the staged-base machinery guards it.
                (KeyValueResponseType startType, TransactionHandle victim) = await kahuna1.LocateAndStartTransaction(
                    new KeyValueTransactionOptions
                    {
                        CoordinatorKey = key + "/victim",
                        Locking = KeyValueTransactionLocking.Optimistic,
                        AsyncRelease = true,
                        Timeout = 60_000
                    }, ct);
                Assert.Equal(KeyValueResponseType.Set, startType);

                (KeyValueResponseType readType, ReadOnlyKeyValueEntry? readEntry) = await kahuna1.LocateAndTryGetValue(
                    victim.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
                    coordinatorKey: victim.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
                Assert.Equal(KeyValueResponseType.Get, readType);
                long baseRevision = readEntry!.Revision;

                (KeyValueResponseType writeType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                    victim.TransactionId, key, "99"u8.ToArray(), null, -1, KeyValueFlags.None, 0,
                    KeyValueDurability.Persistent, ct,
                    coordinatorKey: victim.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
                Assert.Equal(KeyValueResponseType.Set, writeType);

                // The interleaving, installed on every node (the finalize runs wherever the session's
                // coordinator key routes): after the victim's pre-propose validation passes and before its
                // prepares land, the competitor commits the same base and the key leader's fence memory is
                // emptied — the restore-blind window, deterministically.
                DurableTransactionFinalizer[] finalizers =
                    [.. managers.Select(static m => m.TransactionCoordinator.DurableFinalizerForTests)];

                Func<CancellationToken, Task> hook = async hookCt =>
                {
                    foreach (DurableTransactionFinalizer finalizer in finalizers)
                        finalizer.TestAfterPreValidationHook = null;

                    // Let the victim's staged in-memory write intent lapse, as a paused coordinator would.
                    await Task.Delay(400, hookCt);

                    KeyValueTransactionResult competitor = await RetryOnMustRetry(
                        kahuna1, Encoding.UTF8.GetBytes($"BEGIN SET `{key}` '101' COMMIT END"), null, null);
                    Assert.Equal(KeyValueResponseType.Set, competitor.Type);

                    // Every replica must hold the competitor's head before the victim's prepare applies,
                    // so the refusing verdict exists wherever the confirmation asks.
                    foreach (KahunaManager manager in managers)
                        await WaitUntilAsync(() =>
                            manager.DurablePreparedIntentStore.TryGetCommittedHead(key, out long head, out _)
                            && head > baseRevision);

                    managers[leaderIndex].DurablePreparedIntentStore.ForgetCommittedHeadsForTesting();
                };

                foreach (DurableTransactionFinalizer finalizer in finalizers)
                    finalizer.TestAfterPreValidationHook = hook;

                long refusalsBefore = DurableTransactionMetrics.ReplicaFenceRefusalsCount;

                try
                {
                    (KeyValueResponseType commitType, _) = await kahuna1.LocateAndCommitTransaction(victim, ct);

                    Assert.True(KeyValueResponseType.Aborted == commitType, $"expected Aborted, got {commitType}");
                }
                finally
                {
                    foreach (DurableTransactionFinalizer finalizer in finalizers)
                        finalizer.TestAfterPreValidationHook = null;
                }

                Assert.True(DurableTransactionMetrics.ReplicaFenceRefusalsCount > refusalsBefore,
                    "a replica's staged-base verdict must have refused the commit before the decision");

                // The competitor's write survived: the stale-base write was never acknowledged.
                (KeyValueResponseType finalType, ReadOnlyKeyValueEntry? finalEntry) = await kahuna1.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
                Assert.Equal(KeyValueResponseType.Get, finalType);
                Assert.Equal(baseRevision + 1, finalEntry!.Revision);
                Assert.Equal("101", Encoding.UTF8.GetString(finalEntry.Value!));
            });
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// The healthy path: a read-modify-write whose base is still current commits normally through the
    /// confirmation — every replica answers Clear (or NotApplied, which is never an objection) — and no
    /// refusal is counted. Guards the confirmation against false aborts.
    /// </summary>
    [Fact]
    public async Task HealthyReadModifyWrite_CommitsThroughTheConfirmation()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft raft1, IRaft raft2, IRaft raft3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        KahunaManager[] managers = [(KahunaManager)kahuna1, (KahunaManager)kahuna2, (KahunaManager)kahuna3];

        try
        {
            await RunUnderStableLeadership(raft1, 4, async () =>
            {
                string key = "rfc-ok/" + Guid.NewGuid().ToString("N")[..8];

                KeyValueTransactionResult seeded = await RetryOnMustRetry(
                    kahuna1, Encoding.UTF8.GetBytes($"BEGIN SET `{key}` '100' COMMIT END"), null, null);
                Assert.Equal(KeyValueResponseType.Set, seeded.Type);

                foreach (KahunaManager manager in managers)
                    await WaitUntilAsync(() => manager.DurablePreparedIntentStore.TryGetCommittedHead(key, out _, out _));

                long refusalsBefore = DurableTransactionMetrics.ReplicaFenceRefusalsCount;

                (KeyValueResponseType startType, TransactionHandle handle) = await kahuna1.LocateAndStartTransaction(
                    new KeyValueTransactionOptions
                    {
                        CoordinatorKey = key + "/rmw",
                        Locking = KeyValueTransactionLocking.Optimistic,
                        AsyncRelease = true,
                        Timeout = 60_000
                    }, ct);
                Assert.Equal(KeyValueResponseType.Set, startType);

                (KeyValueResponseType readType, ReadOnlyKeyValueEntry? readEntry) = await kahuna1.LocateAndTryGetValue(
                    handle.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
                    coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
                Assert.Equal(KeyValueResponseType.Get, readType);
                long baseRevision = readEntry!.Revision;

                (KeyValueResponseType writeType, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                    handle.TransactionId, key, "150"u8.ToArray(), null, -1, KeyValueFlags.None, 0,
                    KeyValueDurability.Persistent, ct,
                    coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
                Assert.Equal(KeyValueResponseType.Set, writeType);

                (KeyValueResponseType commitType, _) = await kahuna1.LocateAndCommitTransaction(handle, ct);
                Assert.Equal(KeyValueResponseType.Committed, commitType);

                Assert.Equal(refusalsBefore, DurableTransactionMetrics.ReplicaFenceRefusalsCount);

                (KeyValueResponseType finalType, ReadOnlyKeyValueEntry? finalEntry) = await kahuna1.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
                Assert.Equal(KeyValueResponseType.Get, finalType);
                Assert.Equal(baseRevision + 1, finalEntry!.Revision);
                Assert.Equal("150", Encoding.UTF8.GetString(finalEntry.Value!));
            });
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }
}
