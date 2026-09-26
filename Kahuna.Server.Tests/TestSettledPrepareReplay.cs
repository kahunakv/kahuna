using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The settled-identity memory of the prepared-intent store: a prepare whose transaction already settled on
/// the applying partition's log is a re-driven duplicate of a decided transaction (a proposal released as
/// not-leader at a step-down and re-proposed to the successor after its first copy landed, committed and
/// settled there). It must be rejected — never re-installed as a phantom pending intent, never judged by the
/// staged-base fence against a head that already holds its own commit, never vetoed — and the rejection must
/// be a replicated verdict: the same on a live replica, on restore, after a restart from the checkpoint, and
/// on a replica seeded from a whole-partition snapshot.
/// </summary>
public sealed class TestSettledPrepareReplay : IDisposable
{
    private const int Partition = 3;

    private readonly string dir = Path.Combine(Path.GetTempPath(), "kahuna-settled-" + Guid.NewGuid().ToString("N"));

    public TestSettledPrepareReplay() => Directory.CreateDirectory(dir);

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best effort */ }
    }

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static PreparedIntent MakeIntent(string key, long txPhysical, long revision, long baseRevision, long epoch = 1) => new(
        TransactionId: Ts(txPhysical), Epoch: epoch, Key: key,
        ManifestHash: 0, RecordAnchorKey: key,
        CommitTimestamp: Ts(txPhysical + 1),
        State: KeyValueState.Set, Value: [1], Bucket: null,
        Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: baseRevision, BaseState: KeyValueState.Set,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    private static RaftLog Log(params PreparedIntentCommand[] commands) =>
        new() { LogType = ReplicationTypes.PreparedIntent, LogData = [.. PreparedIntentStore.SerializeDelta(commands)] };

    private static RaftLog Prepare(PreparedIntent intent) => Log(new PrepareIntentCommand(intent));

    /// <summary>The transaction's full lifecycle as a partition applies it: prepare, resolve, remove.</summary>
    private static RaftLog[] Lifecycle(PreparedIntent intent, bool commit = true) =>
    [
        Prepare(intent),
        Log(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, commit)),
        Log(new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key))
    ];

    private static void Replicate(PreparedIntentStore store, IEnumerable<RaftLog> logs, int partitionId = Partition)
    {
        foreach (RaftLog log in logs)
            Assert.True(store.Replicate(partitionId, log));
    }

    /// <summary>Asserts the store rejected the replayed prepare without installing anything and named it settled.</summary>
    private static void AssertSettledRejection(PreparedIntentStore store, PreparedIntent replay, int partitionId = Partition)
    {
        Assert.False(store.ApplyDeltaAckPrepares(partitionId, Prepare(replay)));
        Assert.Null(store.Get(replay.Key));
        Assert.True(store.TryTakePrepareRejection(replay.TransactionId, replay.Epoch, replay.Key, out PrepareRejectionKind kind));
        Assert.Equal(PrepareRejectionKind.Settled, kind);
    }

    // ── the verdict ──────────────────────────────────────────────────────────────

    [Fact]
    public void ReplayAfterOwnSettlement_IsRejected_NotReinstalledOrFlaggedStale()
    {
        PreparedIntentStore store = new();
        PreparedIntent t = MakeIntent("s/k", 1_000, revision: 6, baseRevision: 5);
        Replicate(store, Lifecycle(t));
        Assert.Null(store.Get("s/k"));

        int vetoes = 0;
        store.AttachStaleBaseVetoer((_, _) => vetoes++);
        using MetricCapture capture = new("outcome",
            "kahuna.transactions.staged_base_prepare_rejections", "kahuna.durable_tx.settled_prepare_replays");

        // The re-driven copy of T's prepare lands after T settled. Before the memory existed this installed a
        // phantom pending intent, the fence refused it (head 6 > base 5: the transaction's own commit) and the
        // veto found the commit — the false "acknowledged stale-base commit".
        AssertSettledRejection(store, t);

        Assert.Equal(0, vetoes);
        Assert.Empty(capture.Samples("kahuna.transactions.staged_base_prepare_rejections"));
        Assert.Single(capture.Samples("kahuna.durable_tx.settled_prepare_replays"));
    }

    [Fact]
    public void ReplayAfterSuccessorsCommitted_IsStillSettled_NotStale()
    {
        // The soak shape: T commits and settles (head 6), a successor builds on it (head 7), then T's re-driven
        // prepare lands — the "gap of 2" the veto once reported as a fork.
        PreparedIntentStore store = new();
        PreparedIntent t = MakeIntent("s/gap", 1_000, revision: 6, baseRevision: 5);
        Replicate(store, Lifecycle(t));
        Replicate(store, Lifecycle(MakeIntent("s/gap", 1_200, revision: 7, baseRevision: 6)));
        Assert.True(store.TryGetCommittedHead("s/gap", out long head, out _));
        Assert.Equal(7, head);

        int vetoes = 0;
        store.AttachStaleBaseVetoer((_, _) => vetoes++);

        AssertSettledRejection(store, t);
        Assert.Equal(0, vetoes);
    }

    [Fact]
    public void ReplayAfterAbortSettlement_IsRejectedToo()
    {
        PreparedIntentStore store = new();
        PreparedIntent t = MakeIntent("s/abort", 1_000, revision: 6, baseRevision: 5);
        Replicate(store, Lifecycle(t, commit: false));
        Assert.False(store.TryGetCommittedHead("s/abort", out _, out _)); // an abort feeds no head

        // Without the memory an aborted transaction's replayed prepare would hold the key as a phantom until the
        // recovery sweep discarded it again.
        AssertSettledRejection(store, t);
    }

    [Fact]
    public void ReplayOfADifferentEpoch_OrTransaction_IsNotASettledReplay()
    {
        PreparedIntentStore store = new();
        PreparedIntent t = MakeIntent("s/id", 1_000, revision: 6, baseRevision: 5);
        Replicate(store, Lifecycle(t));

        // A later epoch of the same transaction id is a new attempt, and a different transaction on the same
        // key reading the current head is the ordinary next writer: both install.
        PreparedIntent nextEpoch = MakeIntent("s/id", 1_000, revision: 7, baseRevision: 6, epoch: 2);
        Assert.True(store.ApplyDeltaAckPrepares(Partition, Prepare(nextEpoch)));
        Assert.NotNull(store.Get("s/id"));
        Replicate(store, Lifecycle(nextEpoch).Skip(1));

        PreparedIntent other = MakeIntent("s/id", 1_500, revision: 8, baseRevision: 7);
        Assert.True(store.ApplyDeltaAckPrepares(Partition, Prepare(other)));
        Assert.NotNull(store.Get("s/id"));
    }

    [Fact]
    public void ReplayWhileTheResolvedIntentIsStillPresent_StaysAnIdempotentNoop()
    {
        PreparedIntentStore store = new();
        PreparedIntent t = MakeIntent("s/resolved", 1_000, revision: 6, baseRevision: 5);
        Replicate(store, Lifecycle(t).Take(2)); // prepared and resolved, not yet removed

        PreparedIntentApplyResult replay = store.Apply(new PrepareIntentCommand(t), Partition);

        Assert.Equal(TransactionApplyOutcome.IdempotentNoop, replay.Outcome);
        Assert.False(replay.SettledReplay);
        Assert.False(replay.StaleBase);
    }

    [Fact]
    public void Memory_IsPerLeg_ASiblingKeyThatDidNotSettleHere_IsNotAReplay()
    {
        // A multi-key transaction whose legs settle in separate entries (a helper settled the blocker only):
        // only the leg that settled is a replay. The sibling's prepare stays what it is — an idempotent
        // re-prepare while its resolved intent is present — and, once that leg settles too, a replay as well.
        PreparedIntentStore store = new();
        PreparedIntent a = MakeIntent("s/multi/a", 1_000, revision: 6, baseRevision: 5);
        PreparedIntent b = MakeIntent("s/multi/b", 1_000, revision: 3, baseRevision: 2);
        Replicate(store, [Prepare(a), Prepare(b)]);
        Replicate(store, Lifecycle(a).Skip(1));
        Replicate(store, [Log(new ResolveIntentCommand(b.TransactionId, b.Epoch, b.Key, Commit: true))]);

        AssertSettledRejection(store, a);
        PreparedIntentApplyResult sibling = store.Apply(new PrepareIntentCommand(b), Partition);
        Assert.Equal(TransactionApplyOutcome.IdempotentNoop, sibling.Outcome);
        Assert.False(sibling.SettledReplay);

        Replicate(store, [Log(new RemoveIntentCommand(b.TransactionId, b.Epoch, b.Key))]);
        AssertSettledRejection(store, b);
    }

    [Fact]
    public void ATransferredLeg_OfADecidedTransaction_OnAKeyThatNeverSettledHere_StillInstalls()
    {
        // A range move hands this partition a decided transaction's still-pending leg on another key while one
        // of its legs already settled here. The leg must install (the recovery sweep materializes it from the
        // record); rejecting it as a replay would lose a committed write.
        PreparedIntentStore destination = new();
        PreparedIntent settledHere = MakeIntent("s/moved/a", 1_000, revision: 6, baseRevision: 5);
        Replicate(destination, Lifecycle(settledHere));

        PreparedIntent moved = MakeIntent("s/moved/b", 1_000, revision: 3, baseRevision: 2);
        Assert.True(destination.ApplyDeltaAckPrepares(Partition, Prepare(moved)));
        Assert.NotNull(destination.Get("s/moved/b"));
        Assert.False(destination.TryTakePrepareRejection(moved.TransactionId, moved.Epoch, moved.Key, out _));
    }

    // ── replicated, not advisory ─────────────────────────────────────────────────

    [Fact]
    public void RestoreAndReplicatePaths_RejectTheReplayAlike()
    {
        PreparedIntentStore live = new();
        PreparedIntentStore restoring = new();
        PreparedIntent t = MakeIntent("s/paths", 1_000, revision: 6, baseRevision: 5);

        Replicate(live, Lifecycle(t));
        foreach (RaftLog log in Lifecycle(t))
            Assert.True(restoring.Restore(Partition, log));

        // The replay arrives through the follower apply on one node and the restore replay on the other; both
        // must leave the key free, or the two replicas' intent maps diverge.
        Assert.True(live.Replicate(Partition, Prepare(t)));
        Assert.True(restoring.Restore(Partition, Prepare(t)));
        Assert.Null(live.Get("s/paths"));
        Assert.Null(restoring.Get("s/paths"));
        Assert.Equal(live.SnapshotSettledIdentities(Partition), restoring.SnapshotSettledIdentities(Partition));
    }

    [Fact]
    public void Memory_IsScopedToTheApplyingPartitionsLog()
    {
        // A settlement that applied through partition 3's log says nothing about a prepare arriving on
        // partition 4's log: the verdict may depend only on the log it is judged on, because a node's other
        // slices are node-local.
        PreparedIntentStore store = new();
        PreparedIntent t = MakeIntent("s/scope", 1_000, revision: 6, baseRevision: 5);
        Replicate(store, Lifecycle(t), partitionId: 3);

        // On partition 4 the state machine installs it, and only the ADVISORY fence (which reads every slice)
        // refuses the acknowledgement as stale — the pre-existing behaviour, never the settled verdict.
        Assert.False(store.ApplyDeltaAckPrepares(4, Prepare(t)));
        Assert.NotNull(store.Get("s/scope"));
        Assert.True(store.TryTakePrepareRejection(t.TransactionId, t.Epoch, t.Key, out PrepareRejectionKind kind));
        Assert.Equal(PrepareRejectionKind.StaleBase, kind);
        Assert.Empty(store.SnapshotSettledIdentities(4));
        Assert.Single(store.SnapshotSettledIdentities(3));
    }

    [Fact]
    public void Retention_IsLogical_MeasuredFromTheSlicesWatermark()
    {
        PreparedIntentStore store = new();
        PreparedIntent t = MakeIntent("s/window", 1_000, revision: 6, baseRevision: 5);
        Replicate(store, Lifecycle(t));

        // Commits keep moving the watermark. Inside the window the identity is still remembered; once the
        // watermark is a full window past T's commit timestamp the identity is treated as absent — whether or
        // not the physical prune ran — and the replay degrades to the pre-existing install-and-judge behaviour.
        long inside = 1_000 + PreparedIntentStore.SettledIdentityRetentionMs - 10_000;
        Replicate(store, Lifecycle(MakeIntent("s/other", inside, revision: 2, baseRevision: 1)));
        AssertSettledRejection(store, t);

        long past = 1_000 + PreparedIntentStore.SettledIdentityRetentionMs + 10;
        Replicate(store, Lifecycle(MakeIntent("s/other", past, revision: 3, baseRevision: 2)));

        PreparedIntentApplyResult replay = store.Apply(new PrepareIntentCommand(t), Partition);
        Assert.False(replay.SettledReplay);
        Assert.Equal(TransactionApplyOutcome.Applied, replay.Outcome);
        Assert.DoesNotContain(store.SnapshotSettledIdentities(Partition), e => e.TransactionId == t.TransactionId);
    }

    [Fact]
    public void PhysicalPrune_DropsOnlyExpiredIdentities()
    {
        PreparedIntentStore store = new();
        Replicate(store, Lifecycle(MakeIntent("s/p1", 1_000, revision: 1, baseRevision: 0)));
        Replicate(store, Lifecycle(MakeIntent("s/p2", 30_000, revision: 1, baseRevision: 0)));
        Assert.Equal(2, store.SnapshotSettledIdentities(Partition).Count);

        // A commit far ahead moves the watermark past the first identity's window and across a prune bucket.
        Replicate(store, Lifecycle(MakeIntent("s/p3", 70_000, revision: 1, baseRevision: 0)));

        IReadOnlyList<PreparedIntentStore.SettledIdentityEntry> retained = store.SnapshotSettledIdentities(Partition);
        Assert.DoesNotContain(retained, e => e.TransactionId == Ts(1_000));
        Assert.Contains(retained, e => e.TransactionId == Ts(30_000));
        Assert.Contains(retained, e => e.TransactionId == Ts(70_000));
    }

    // ── persistence and transfer parity ──────────────────────────────────────────

    [Fact]
    public void Memory_SurvivesARestart_FromTheCheckpoint()
    {
        PreparedIntentStore live = new(dir, "rev", null);
        live.AttachPartitionResolver(_ => Partition);
        PreparedIntent t = MakeIntent("s/restart", 1_000, revision: 6, baseRevision: 5);
        Replicate(live, Lifecycle(t));
        Assert.True(live.PersistSnapshot(Partition));

        // The restarted node's WAL replay starts above T's settlement (compacted away): only the checkpointed
        // memory can tell it the replayed prepare is a duplicate of a settled transaction.
        PreparedIntentStore restarted = new(dir, "rev", null);
        restarted.AttachPartitionResolver(_ => Partition);
        Assert.Equal(live.SnapshotSettledIdentities(Partition), restarted.SnapshotSettledIdentities(Partition));

        AssertSettledRejection(restarted, t);
    }

    [Fact]
    public void Memory_RidesTheWholePartitionSection_ToASeededReplica()
    {
        PreparedIntentStore exporter = new();
        PreparedIntent t = MakeIntent("s/seed", 1_000, revision: 6, baseRevision: 5);
        Replicate(exporter, Lifecycle(t));

        byte[] section = exporter.SerializePartitionIntents(Partition, [.. exporter.SnapshotRange(null, null)]);
        PreparedIntentStore.PartitionIntentSection decoded = PreparedIntentStore.DeserializePartitionIntents(section);
        Assert.NotNull(decoded.Settled);
        Assert.Single(decoded.Settled!);

        PreparedIntentStore imported = new();
        imported.ImportPartitionIntents(Partition, decoded, requireLedger: true);
        Assert.Equal(exporter.SnapshotSettledIdentities(Partition), imported.SnapshotSettledIdentities(Partition));

        PreparedIntentStore replaced = new();
        replaced.ReplacePartitionIntents(Partition, decoded, requireLedger: true, _ => true);
        Assert.Equal(exporter.SnapshotSettledIdentities(Partition), replaced.SnapshotSettledIdentities(Partition));

        // The replay lands after the seeded replicas caught up: each rejects it exactly as the exporter does.
        AssertSettledRejection(exporter, t);
        AssertSettledRejection(imported, t);
        AssertSettledRejection(replaced, t);
    }

    [Fact]
    public void SectionWrittenBeforeTheMemoryExisted_LoadsWithAnEmptyMemory()
    {
        // The plain intent serializer predates the ledger and the memory: decoding it yields no settled entries,
        // and an install from it leaves the memory empty rather than failing.
        PreparedIntentStore.PartitionIntentSection legacy = PreparedIntentStore.DeserializePartitionIntents(PreparedIntentStore.SerializeIntents([]));
        Assert.Null(legacy.Settled);

        PreparedIntentStore node = new();
        node.ImportPartitionIntents(Partition, legacy, requireLedger: false);
        Assert.Empty(node.SnapshotSettledIdentities(Partition));
    }
}
