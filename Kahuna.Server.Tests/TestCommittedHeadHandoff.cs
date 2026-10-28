using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The committed-head hand-off of a range move at the store level. A split or merge moves a key's rows to another
/// partition, but the head its last transactional commit left in the staged-base fence memory lives in the SOURCE
/// partition's ledger slice. The hand-off seeds those heads into the destination's slice through the destination's
/// own log (<see cref="SeedCommittedHeadCommand"/>), so the fence on the destination judges a moved key from state
/// every replica of the destination holds at the same log position — not from each node's copy of the source
/// slice, which the destination's leader may hold behind its replicas or not at all.
/// </summary>
public sealed class TestCommittedHeadHandoff
{
    private const int SourcePartition = 3;

    private const int DestinationPartition = 9;

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

    // Copy the serialized bytes so the apply cannot recognize them as this process's own proposal and must decode
    // them — the follower's view of the hand-off entry.
    private static RaftLog IntentLog(params PreparedIntentCommand[] commands) =>
        new() { LogType = ReplicationTypes.PreparedIntent, LogData = [.. PreparedIntentStore.SerializeDelta(commands)] };

    private static SeedCommittedHeadCommand Seed(string key, long revision, long committedAt) =>
        new(key, revision, KeyValueState.Set, Ts(committedAt));

    /// <summary>Runs one commit lifecycle through the given partition's slice so its head is remembered there.</summary>
    private static void CommitOnPartition(PreparedIntentStore store, int partitionId, PreparedIntent intent)
    {
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(intent), partitionId).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied,
            store.Apply(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true), partitionId).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied,
            store.Apply(new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key), partitionId).Outcome);
    }

    // ── the seed command ────────────────────────────────────────────────────────

    [Fact]
    public void Seed_RecordsTheHeadInTheCarryingPartitionSlice()
    {
        PreparedIntentStore store = new();

        Assert.True(store.Replicate(DestinationPartition, IntentLog(Seed("hh/k", 12, 5_000))));

        Assert.True(store.TryGetLedgerHead(DestinationPartition, "hh/k", out long revision, out KeyValueState state, out HLCTimestamp committedAt));
        Assert.Equal(12, revision);
        Assert.Equal(KeyValueState.Set, state);
        Assert.Equal(Ts(5_000), committedAt);

        // The node-wide memory the advisory fence judges by sees it, and the slice's watermark moved with it.
        Assert.True(store.TryGetCommittedHead("hh/k", out long nodeWide, out _));
        Assert.Equal(12, nodeWide);
        Assert.Equal(Ts(5_000), store.GetLedgerWatermark(DestinationPartition));
        Assert.Equal(1, store.CommittedHeadCountForPartition(DestinationPartition));

        // No intent is touched: the seed is ledger state only.
        Assert.Null(store.Get("hh/k"));
        Assert.Equal(0, store.LiveIntentCount);
    }

    [Fact]
    public void Seed_IsMonotonicPerKey_AndIdempotent()
    {
        PreparedIntentStore store = new();

        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(Seed("hh/mono", 12, 5_000), DestinationPartition).Outcome);

        // A re-driven hand-off (the same seed again) and an older seed change nothing.
        Assert.Equal(TransactionApplyOutcome.IdempotentNoop, store.Apply(Seed("hh/mono", 12, 5_000), DestinationPartition).Outcome);
        Assert.Equal(TransactionApplyOutcome.IdempotentNoop, store.Apply(Seed("hh/mono", 11, 4_000), DestinationPartition).Outcome);
        Assert.True(store.TryGetLedgerHead(DestinationPartition, "hh/mono", out long revision, out _, out HLCTimestamp committedAt));
        Assert.Equal(12, revision);
        Assert.Equal(Ts(5_000), committedAt);

        // A newer seed advances it.
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(Seed("hh/mono", 13, 6_000), DestinationPartition).Outcome);
        Assert.True(store.TryGetLedgerHead(DestinationPartition, "hh/mono", out revision, out _, out _));
        Assert.Equal(13, revision);
    }

    [Fact]
    public void Seed_NeverRegressesAHeadTheDestinationAlreadyAdvanced()
    {
        PreparedIntentStore store = new();

        // A commit that settled on the destination's own log before a re-driven seed replays below it.
        CommitOnPartition(store, DestinationPartition, MakeIntent("hh/ahead", Ts(1_000), epoch: 1, revision: 14, baseRevision: 13, KeyValueState.Set));

        Assert.Equal(TransactionApplyOutcome.IdempotentNoop, store.Apply(Seed("hh/ahead", 12, 500), DestinationPartition).Outcome);

        Assert.True(store.TryGetLedgerHead(DestinationPartition, "hh/ahead", out long revision, out _, out _));
        Assert.Equal(14, revision);
    }

    [Fact]
    public void Seed_AppliesOnTheRestartReplayPath()
    {
        PreparedIntentStore store = new();

        Assert.True(store.Restore(DestinationPartition, IntentLog(Seed("hh/replay", 7, 3_000))));

        Assert.True(store.TryGetLedgerHead(DestinationPartition, "hh/replay", out long revision, out _, out _));
        Assert.Equal(7, revision);
    }

    // ── what the seed buys: the destination's fence judges moved keys from its own slice ──

    [Fact]
    public void StalePrepareOnTheDestination_IsRefusedByTheSeededHead_WithNoSourceSliceAtAll()
    {
        // The destination's memory holds nothing but the seed: the node never applied the source partition's log.
        PreparedIntentStore store = new();
        Assert.True(store.Replicate(DestinationPartition, IntentLog(Seed("hh/moved", 12, 5_000))));

        List<(PreparedIntent Intent, long Head)> vetoes = [];
        store.AttachStaleBaseVetoer((intent, head) => vetoes.Add((intent, head)));

        // Validated against revision 11 on the source before the move; the source's last commit was 12.
        PreparedIntent stale = MakeIntent("hh/moved", Ts(5_100), epoch: 1, revision: 12, baseRevision: 11, KeyValueState.Set);
        bool acked = store.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(new PrepareIntentCommand(stale)));

        Assert.False(acked);
        Assert.True(store.TryTakePrepareRejection(stale.TransactionId, 1, "hh/moved", out PrepareRejectionKind kind));
        Assert.Equal(PrepareRejectionKind.StaleBase, kind);

        (PreparedIntent vetoed, long head) = Assert.Single(vetoes);
        Assert.Equal(stale.TransactionId, vetoed.TransactionId);
        Assert.Equal(12, head);
    }

    [Fact]
    public void CurrentPrepareOnTheDestination_IsAcknowledgedOverTheSeededHead()
    {
        PreparedIntentStore store = new();
        Assert.True(store.Replicate(DestinationPartition, IntentLog(Seed("hh/current", 12, 5_000))));

        int vetoes = 0;
        store.AttachStaleBaseVetoer((_, _) => vetoes++);

        // Validated against the seeded head itself: the ordinary read-modify-write after the move.
        PreparedIntent current = MakeIntent("hh/current", Ts(5_100), epoch: 1, revision: 13, baseRevision: 12, KeyValueState.Set);
        bool acked = store.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(new PrepareIntentCommand(current)));

        Assert.True(acked);
        Assert.False(store.TryTakePrepareRejection(current.TransactionId, 1, "hh/current", out _));
        Assert.Equal(0, vetoes);
    }

    [Fact]
    public void RevisionCollisionOnTheDestination_IsRefusedByTheSeededHead()
    {
        PreparedIntentStore store = new();
        Assert.True(store.Replicate(DestinationPartition, IntentLog(Seed("hh/collide", 12, 5_000))));

        // A blind write that staged revision 12 under an exclusion the move took away: the seeded head already
        // holds another transaction's commit at 12.
        PreparedIntent blind = MakeIntent("hh/collide", Ts(5_100), epoch: 1, revision: 12,
            PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined);
        bool acked = store.ApplyDeltaAckPrepares(DestinationPartition, IntentLog(new PrepareIntentCommand(blind)));

        Assert.False(acked);
        Assert.True(store.TryTakePrepareRejection(blind.TransactionId, 1, "hh/collide", out PrepareRejectionKind kind));
        Assert.Equal(PrepareRejectionKind.StaleBase, kind);
    }

    // ── the gather side ─────────────────────────────────────────────────────────

    [Fact]
    public void SnapshotCommittedHeadsRange_TakesThePerKeyMaximumAcrossSlices_WithinTheBounds()
    {
        PreparedIntentStore store = new();

        // Two slices remember "r/a" (a range that moved before): the higher revision is the one the fence
        // judges by, so it is the one handed on.
        Assert.True(store.Replicate(SourcePartition, IntentLog(Seed("r/a", 5, 1_000), Seed("r/b", 7, 2_000), Seed("s/a", 1, 3_000))));
        Assert.True(store.Replicate(SourcePartition + 1, IntentLog(Seed("r/a", 6, 4_000), Seed("r/c", 2, 5_000))));

        IReadOnlyList<CommittedHeadLedgerEntry> heads = store.SnapshotCommittedHeadsRange("r/", "r0");

        Dictionary<string, CommittedHeadLedgerEntry> byKey = heads.ToDictionary(h => h.Key);
        Assert.Equal(3, byKey.Count);
        Assert.Equal(6, byKey["r/a"].Revision);
        Assert.Equal(Ts(4_000), byKey["r/a"].CommittedAt);
        Assert.Equal(7, byKey["r/b"].Revision);
        Assert.Equal(2, byKey["r/c"].Revision);
        Assert.DoesNotContain("s/a", byKey.Keys);

        // Open bounds: everything.
        Assert.Equal(4, store.SnapshotCommittedHeadsRange(null, null).Count);
    }

    [Fact]
    public void CommittedHeadsPage_RoundTripsThroughItsWireForm()
    {
        CommittedHeadLedgerEntry[] heads =
        [
            new("w/a", 5, KeyValueState.Set, Ts(1_000)),
            new("w/b", 9, KeyValueState.Deleted, new HLCTimestamp(2, 2_000, 3))
        ];

        IReadOnlyList<CommittedHeadLedgerEntry> decoded = PreparedIntentStore.DeserializeCommittedHeads(PreparedIntentStore.SerializeCommittedHeads(heads));

        Assert.Equal(heads, decoded);
    }

    // ── the wire form of the seed ───────────────────────────────────────────────

    /// <summary>The live decoder reads the wire directly; the generated proto messages are the reference decoder.
    /// A seed must read identically through both, in a one-seed delta (per-command form) and in a multi-seed
    /// delta, where the writer hoists the (empty) shared header and the seed's own commit timestamp must survive
    /// as its own field.</summary>
    [Fact]
    public void SeedDelta_ReadsIdenticallyThroughBothDecoders_AndKeepsEachSeedsTimestamp()
    {
        AssertDecodersAgree(PreparedIntentStore.SerializeDelta([Seed("hh/one", 12, 5_000)]));

        byte[] hoisted = PreparedIntentStore.SerializeDelta([
            new SeedCommittedHeadCommand("hh/two", 12, KeyValueState.Set, new HLCTimestamp(1, 5_000, 2)),
            new SeedCommittedHeadCommand("hh/three", 3, KeyValueState.Deleted, new HLCTimestamp(2, 6_000, 4))]);
        AssertDecodersAgree(hoisted);

        PreparedIntentStore store = new();
        Assert.True(store.Replicate(DestinationPartition, new RaftLog { LogType = ReplicationTypes.PreparedIntent, LogData = [.. hoisted] }));

        Assert.True(store.TryGetLedgerHead(DestinationPartition, "hh/two", out long two, out KeyValueState twoState, out HLCTimestamp twoAt));
        Assert.Equal((12L, KeyValueState.Set, new HLCTimestamp(1, 5_000, 2)), (two, twoState, twoAt));

        Assert.True(store.TryGetLedgerHead(DestinationPartition, "hh/three", out long three, out KeyValueState threeState, out HLCTimestamp threeAt));
        Assert.Equal((3L, KeyValueState.Deleted, new HLCTimestamp(2, 6_000, 4)), (three, threeState, threeAt));
    }

    private static void AssertDecodersAgree(byte[] bytes)
    {
        PreparedIntentCommand[] direct = PreparedIntentStore.DecodeDelta([.. bytes]);
        PreparedIntentDeltaMessage reference = ReplicationSerializer.UnserializePreparedIntentDeltaMessage(bytes);

        Assert.Equal(reference.Commands.Count, direct.Length);

        for (int i = 0; i < direct.Length; i++)
        {
            PreparedIntentCommand expected = PreparedIntentStore.ToCommand(reference.Commands[i], reference.Header);
            Assert.Equal(PreparedIntentCommandKindMessage.PreparedIntentSeedHead, reference.Commands[i].Kind);
            Assert.Equal(expected, direct[i]);
        }
    }
}
