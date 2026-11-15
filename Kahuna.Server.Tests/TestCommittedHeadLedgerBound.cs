using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The size bound of a committed-head ledger slice. A slice that passes its head budget raises its size floor:
/// the oldest heads leave, and a transaction that began before the floor is refused, so a pruned head is never
/// read as "no commit happened". The floor is replicated state: a replica that applied the log, one that
/// reloaded a checkpoint and one that installed a snapshot hold the same floor and the same heads.
/// </summary>
public sealed class TestCommittedHeadLedgerBound : IDisposable
{
    private const int Partition = 3;

    // Commit times 10 s apart, so each head has its own second and the 15 s minimum horizon never binds.
    private const long StepMs = 10_000;

    private const long BaseMs = 100_000;

    private readonly string dir = Path.Combine(Path.GetTempPath(), "kahuna-ledger-bound-" + Guid.NewGuid().ToString("N"));

    public TestCommittedHeadLedgerBound() => Directory.CreateDirectory(dir);

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best effort */ }
    }

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static PreparedIntent MakeIntent(string key, long txPhysical, long revision, long baseRevision = PreparedIntent.UnknownBaseRevision) => new(
        TransactionId: Ts(txPhysical), Epoch: 1, Key: key,
        ManifestHash: 0, RecordAnchorKey: key,
        CommitTimestamp: Ts(txPhysical + 1),
        State: KeyValueState.Set, Value: [1], Bucket: null,
        Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: baseRevision, BaseState: baseRevision == PreparedIntent.UnknownBaseRevision ? KeyValueState.Undefined : KeyValueState.Set,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    private static RaftLog[] CommitLog(PreparedIntent intent) =>
    [
        Log(new PrepareIntentCommand(intent)),
        Log(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true)),
        Log(new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key))
    ];

    private static RaftLog Log(params PreparedIntentCommand[] commands) =>
        new() { LogType = ReplicationTypes.PreparedIntent, LogData = [.. PreparedIntentStore.SerializeDelta(commands)] };

    private static void Apply(PreparedIntentStore store, IEnumerable<RaftLog> logs)
    {
        foreach (RaftLog log in logs)
            Assert.True(store.Replicate(Partition, log));
    }

    /// <summary>The log of a load that commits one new key every 10 s of commit time: key <c>b/NN</c> at
    /// <c>BaseMs + NN * StepMs</c>.</summary>
    private static List<RaftLog> LoadLog(int from, int to)
    {
        List<RaftLog> logs = [];
        for (int i = from; i < to; i++)
            logs.AddRange(CommitLog(MakeIntent($"b/{i:D2}", BaseMs + i * StepMs, revision: 1)));
        return logs;
    }

    private static string[] KeysOf(PreparedIntentStore store) =>
        [.. store.SnapshotLedger(Partition).Select(static head => head.Key)];

    private static string[] Keys(int from, int to) =>
        [.. Enumerable.Range(from, to - from).Select(static i => $"b/{i:D2}")];

    private static void AssertSameSlice(PreparedIntentStore expected, PreparedIntentStore actual)
    {
        Assert.Equal(expected.SnapshotLedger(Partition), actual.SnapshotLedger(Partition));
        Assert.Equal(expected.GetLedgerWatermark(Partition), actual.GetLedgerWatermark(Partition));
        Assert.Equal(expected.GetLedgerSizeFloor(Partition), actual.GetLedgerSizeFloor(Partition));
    }

    // ── the bound ────────────────────────────────────────────────────────────────

    [Fact]
    public void SliceOverItsBudget_DropsItsOldestHeads_AndRaisesTheFloor()
    {
        PreparedIntentStore store = new();
        store.ConfigureCommittedHeadLedgerBound(maxEntries: 8);

        // The ninth head passes the budget: the six newest stay (three quarters of 8), the three oldest leave.
        Apply(store, LoadLog(0, 9));
        Assert.Equal(Keys(3, 9), KeysOf(store));

        // The floor is the second after the newest head that left (b/02, committed at 120_001).
        Assert.Equal(121_000, store.GetLedgerSizeFloor(Partition));
        Assert.Equal(6, store.SnapshotLedgerSizes()[0].Entries);

        // Two more heads fit; the third passes the budget again.
        Apply(store, LoadLog(9, 11));
        Assert.Equal(Keys(3, 11), KeysOf(store));

        Apply(store, LoadLog(11, 12));
        Assert.Equal(Keys(6, 12), KeysOf(store));
        Assert.Equal(151_000, store.GetLedgerSizeFloor(Partition));
    }

    [Fact]
    public void SustainedLoadOfNewKeys_NeverHoldsMoreThanTheBudget()
    {
        PreparedIntentStore store = new();
        store.ConfigureStagedBaseFence(retentionMs: 3_600_000);
        store.ConfigureCommittedHeadLedgerBound(maxEntries: 100);

        // 5,000 new keys inside one retention horizon: without the bound the slice holds all of them.
        long before = store.SnapshotLedgerSizes().Count;
        Assert.Equal(0, before);

        for (int i = 0; i < 5_000; i++)
        {
            Apply(store, CommitLog(MakeIntent($"s/{i}", BaseMs + i * 500L, revision: 1)));
            Assert.True(store.CommittedHeadCountForPartition(Partition) <= 100, $"slice held more than its budget after key {i}");
        }

        (int _, long entries, long bytes) = store.SnapshotLedgerSizes()[0];
        Assert.InRange(entries, 75, 100);
        Assert.Equal(entries, store.SnapshotLedger(Partition).Count);

        // The byte gauge follows the heads that left.
        Assert.InRange(bytes, 1, 100 * 200);
    }

    [Fact]
    public void BoundOff_KeepsEveryHeadInsideTheRetentionHorizon()
    {
        PreparedIntentStore store = new();
        store.ConfigureCommittedHeadLedgerBound(maxEntries: 0);

        Apply(store, LoadLog(0, 20));

        Assert.Equal(20, store.SnapshotLedger(Partition).Count);
        Assert.Equal(0, store.GetLedgerSizeFloor(Partition));
    }

    [Fact]
    public void HeadMovedByALaterCommit_CountsOnce()
    {
        PreparedIntentStore store = new();
        store.ConfigureCommittedHeadLedgerBound(maxEntries: 8);

        // Eight keys, then 40 more commits of the same eight keys: the slice never passes the budget.
        Apply(store, LoadLog(0, 8));
        for (int round = 1; round <= 5; round++)
        {
            for (int i = 0; i < 8; i++)
                Apply(store, CommitLog(MakeIntent($"b/{i:D2}", BaseMs + (round * 8 + i) * StepMs, revision: round + 1)));
        }

        Assert.Equal(Keys(0, 8), KeysOf(store));
        Assert.Equal(0, store.GetLedgerSizeFloor(Partition));
    }

    [Fact]
    public void FloorStaysTheMinimumHorizonBehindTheNewestCommit()
    {
        PreparedIntentStore store = new();
        store.ConfigureCommittedHeadLedgerBound(maxEntries: 8);

        // 30 heads inside 3 s: the floor may not come nearer than the minimum horizon, so the slice stays over
        // its budget and no transaction of the last seconds is refused.
        for (int i = 0; i < 30; i++)
            Apply(store, CommitLog(MakeIntent($"m/{i:D2}", BaseMs + i * 100, revision: 1)));

        Assert.Equal(30, store.SnapshotLedger(Partition).Count);
        Assert.Equal(0, store.GetLedgerSizeFloor(Partition));

        // Once the newest commit is the minimum horizon ahead of them, they leave.
        long later = BaseMs + 3_000 + PreparedIntentStore.LedgerSizeBoundMinRetentionMs + 1_000;
        Apply(store, CommitLog(MakeIntent("m/later", later, revision: 1)));

        Assert.Equal(["m/later"], KeysOf(store));
        Assert.True(store.GetLedgerSizeFloor(Partition) <= later + 1 - PreparedIntentStore.LedgerSizeBoundMinRetentionMs);
        Assert.True(store.GetLedgerSizeFloor(Partition) >= BaseMs + 3_000);
    }

    // ── the staleness gate ───────────────────────────────────────────────────────

    [Fact]
    public void TransactionOlderThanTheFloor_IsRefused_AndAYoungerOneIsAcknowledged()
    {
        PreparedIntentStore store = new();
        store.ConfigureCommittedHeadLedgerBound(maxEntries: 8);

        Apply(store, LoadLog(0, 9));
        long floor = store.GetLedgerSizeFloor(Partition);
        Assert.Equal(121_000, floor);

        // b/01 was replaced at 110_001 and its head left. A transaction that began before the floor may hold
        // the base that commit replaced: the memory cannot say, so the acknowledgement is refused.
        PreparedIntentApplyResult stale = store.Apply(new PrepareIntentCommand(
            MakeIntent("b/01", txPhysical: floor - 1, revision: 1, baseRevision: 0)));
        Assert.Equal(TransactionApplyOutcome.Applied, stale.Outcome);
        Assert.True(stale.StaleBase, "a transaction that began before the size floor cannot have its base verified");

        // A transaction that began at the floor read after every commit the bound dropped.
        PreparedIntentApplyResult young = store.Apply(new PrepareIntentCommand(
            MakeIntent("b/02", txPhysical: floor, revision: 2, baseRevision: 1)));
        Assert.Equal(TransactionApplyOutcome.Applied, young.Outcome);
        Assert.False(young.StaleBase);

        // A blind write has no base to verify, so the gate does not apply to it.
        PreparedIntentApplyResult blind = store.Apply(new PrepareIntentCommand(
            MakeIntent("b/00", txPhysical: floor - 1, revision: 2)));
        Assert.False(blind.StaleBase);
    }

    [Fact]
    public void RetainedHead_StillRefusesAMovedBase()
    {
        PreparedIntentStore store = new();
        store.ConfigureCommittedHeadLedgerBound(maxEntries: 8);

        Apply(store, LoadLog(0, 9));
        Apply(store, CommitLog(MakeIntent("b/08", BaseMs + 9 * StepMs, revision: 2, baseRevision: 1)));

        // The transaction began after the floor and validated revision 1 of a key whose head is now 2.
        PreparedIntentApplyResult result = store.Apply(new PrepareIntentCommand(
            MakeIntent("b/08", txPhysical: BaseMs + 8 * StepMs + 500, revision: 2, baseRevision: 1)));

        Assert.True(result.StaleBase);
    }

    // ── replica parity ───────────────────────────────────────────────────────────

    [Fact]
    public void CheckpointReload_KeepsTheFloor_AndReplayReachesTheSameSlice()
    {
        List<RaftLog> log = LoadLog(0, 20);

        PreparedIntentStore live = new();
        live.ConfigureCommittedHeadLedgerBound(maxEntries: 8);
        Apply(live, log);

        // A node that wrote a checkpoint after 13 keys, restarted, and replayed its whole log over it.
        PreparedIntentStore first = new(dir, "rev", null);
        first.ConfigureCommittedHeadLedgerBound(maxEntries: 8);
        first.AttachPartitionResolver(_ => Partition);
        Apply(first, log.Take(13 * 3));
        long floorAtCheckpoint = first.GetLedgerSizeFloor(Partition);
        Assert.True(floorAtCheckpoint > 0);
        Assert.True(first.PersistSnapshot(Partition));

        PreparedIntentStore restarted = new(dir, "rev", null);
        restarted.ConfigureCommittedHeadLedgerBound(maxEntries: 8);
        Assert.Equal(floorAtCheckpoint, restarted.GetLedgerSizeFloor(Partition));
        AssertSameSlice(first, restarted);

        Apply(restarted, log);
        AssertSameSlice(live, restarted);
    }

    [Fact]
    public void SnapshotInstall_CarriesTheFloor_AndTheTailReachesTheSameSlice()
    {
        List<RaftLog> log = LoadLog(0, 20);

        PreparedIntentStore live = new();
        live.ConfigureCommittedHeadLedgerBound(maxEntries: 8);
        Apply(live, log);

        PreparedIntentStore exporter = new();
        exporter.ConfigureCommittedHeadLedgerBound(maxEntries: 8);
        Apply(exporter, log.Take(13 * 3));
        byte[] section = exporter.SerializePartitionIntents(Partition, [.. exporter.SnapshotRange(null, null)]);

        PreparedIntentStore installed = new();
        installed.ConfigureCommittedHeadLedgerBound(maxEntries: 8);
        installed.ImportPartitionIntents(Partition, PreparedIntentStore.DeserializePartitionIntents(section), requireLedger: true);
        AssertSameSlice(exporter, installed);
        Assert.True(installed.GetLedgerSizeFloor(Partition) > 0);

        // The receiver replays entries from below the snapshot's position, then the tail.
        Apply(installed, log.Skip(10 * 3));
        AssertSameSlice(live, installed);

        // The installed floor refuses what the exporter refuses.
        PreparedIntentApplyResult stale = installed.Apply(new PrepareIntentCommand(
            MakeIntent("b/01", txPhysical: installed.GetLedgerSizeFloor(Partition) - 1, revision: 1, baseRevision: 0)));
        Assert.True(stale.StaleBase);
    }

    [Fact]
    public void UnhostPurge_ReleasesTheNodeWideFloor()
    {
        PreparedIntentStore store = new();
        store.ConfigureCommittedHeadLedgerBound(maxEntries: 8);

        Apply(store, LoadLog(0, 9));
        long floor = store.GetLedgerSizeFloor(Partition);

        store.PurgePartitionLedger(Partition);
        Assert.Equal(0, store.GetLedgerSizeFloor(Partition));

        // The slice that raised the floor is gone, so its floor refuses nothing on this node.
        PreparedIntentApplyResult result = store.Apply(new PrepareIntentCommand(
            MakeIntent("b/01", txPhysical: floor - 1, revision: 1, baseRevision: 0)));
        Assert.False(result.StaleBase);
    }
}
