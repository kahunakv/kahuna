using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The committed-head ledger as replicated state: one slice per partition, fed only by that partition's
/// applied log, persisted with the partition's intent snapshot, installed with a whole-partition snapshot,
/// and pruned by a rule the verdicts cannot observe. These tests pin the parity the one-phase bundled commit
/// gate depends on — a replica that applied a log, one that replayed it after a restart, and one that
/// installed a snapshot and then applied the tail hold the same slice at the same log position — plus the
/// fail-closed load contract for snapshots written before the ledger existed.
/// </summary>
public sealed class TestCommittedHeadLedger : IDisposable
{
    private const int Partition = 3;

    private readonly string dir = Path.Combine(Path.GetTempPath(), "kahuna-ledger-" + Guid.NewGuid().ToString("N"));

    public TestCommittedHeadLedger() => Directory.CreateDirectory(dir);

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best effort */ }
    }

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static PreparedIntent MakeIntent(string key, long txPhysical, long revision, long baseRevision = PreparedIntent.UnknownBaseRevision,
        KeyValueState state = KeyValueState.Set) => new(
        TransactionId: Ts(txPhysical), Epoch: 1, Key: key,
        ManifestHash: 0, RecordAnchorKey: key,
        CommitTimestamp: Ts(txPhysical + 1),
        State: state, Value: state == KeyValueState.Set ? [1] : null, Bucket: null,
        Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: baseRevision, BaseState: baseRevision == PreparedIntent.UnknownBaseRevision ? KeyValueState.Undefined : KeyValueState.Set,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    /// <summary>One transaction's full lifecycle as the three log entries a partition applies, encoded as a
    /// follower would receive them (the bytes are copied so the producer-side command cache is defeated).</summary>
    private static RaftLog[] CommitLog(PreparedIntent intent) =>
    [
        Log(new PrepareIntentCommand(intent)),
        Log(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true)),
        Log(new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key))
    ];

    private static RaftLog Log(params PreparedIntentCommand[] commands) =>
        new() { LogType = ReplicationTypes.PreparedIntent, LogData = [.. PreparedIntentStore.SerializeDelta(commands)] };

    private static void Apply(PreparedIntentStore store, IEnumerable<RaftLog> logs, int partitionId = Partition)
    {
        foreach (RaftLog log in logs)
            Assert.True(store.Replicate(partitionId, log));
    }

    private static void AssertSameSlice(PreparedIntentStore expected, PreparedIntentStore actual, int partitionId = Partition)
    {
        Assert.Equal(expected.SnapshotLedger(partitionId), actual.SnapshotLedger(partitionId));
        Assert.Equal(expected.GetLedgerWatermark(partitionId), actual.GetLedgerWatermark(partitionId));
    }

    // ── feed and scoping ─────────────────────────────────────────────────────────

    [Fact]
    public void Settlement_FeedsTheApplyingPartitionsSlice_MonotonicPerKey()
    {
        PreparedIntentStore store = new();

        Apply(store, CommitLog(MakeIntent("l/k1", 1_000, revision: 6)));
        Apply(store, CommitLog(MakeIntent("l/k1", 1_200, revision: 9)));
        Apply(store, CommitLog(MakeIntent("l/k2", 1_100, revision: 2)), partitionId: 4);

        Assert.True(store.TryGetLedgerHead(Partition, "l/k1", out long revision, out KeyValueState state, out HLCTimestamp committedAt));
        Assert.Equal(9, revision);
        Assert.Equal(KeyValueState.Set, state);
        Assert.Equal(Ts(1_201), committedAt);
        Assert.Equal(Ts(1_201), store.GetLedgerWatermark(Partition));

        // The other partition's settlement fed its own slice only.
        Assert.False(store.TryGetLedgerHead(Partition, "l/k2", out _, out _, out _));
        Assert.True(store.TryGetLedgerHead(4, "l/k2", out _, out _, out _));
        Assert.Equal(Ts(1_101), store.GetLedgerWatermark(4));

        // The advisory view spans every slice.
        Assert.True(store.TryGetCommittedHead("l/k2", out long advisory, out _));
        Assert.Equal(2, advisory);
        Assert.Equal(2, store.CommittedHeadCount);
    }

    [Fact]
    public void AbortedSettlement_FeedsNothing()
    {
        PreparedIntentStore store = new();
        PreparedIntent intent = MakeIntent("l/aborted", 1_000, revision: 6);

        Apply(store, [Log(new PrepareIntentCommand(intent)),
            Log(new ResolveIntentCommand(intent.TransactionId, 1, intent.Key, Commit: false)),
            Log(new RemoveIntentCommand(intent.TransactionId, 1, intent.Key))]);

        Assert.False(store.TryGetLedgerHead(Partition, "l/aborted", out _, out _, out _));
        Assert.Equal(HLCTimestamp.Zero, store.GetLedgerWatermark(Partition));
    }

    // ── parity across the ways a replica reaches a log position ──────────────────

    [Fact]
    public void LeaderAndFollower_ApplyingTheSameLog_HoldTheSameSlice()
    {
        PreparedIntentStore leader = new();
        PreparedIntentStore follower = new();

        List<RaftLog> log = [];
        for (int i = 0; i < 50; i++)
            log.AddRange(CommitLog(MakeIntent("l/k" + (i % 7), 1_000 + i * 10, revision: i + 1)));

        Apply(leader, log);
        Apply(follower, log);

        AssertSameSlice(leader, follower);
        Assert.Equal(7, leader.SnapshotLedger(Partition).Count);
    }

    [Fact]
    public void ReplayAfterRestart_FromThePersistedSlice_HoldsTheSameSlice()
    {
        PreparedIntentStore live = new(dir, "rev", null);
        live.AttachPartitionResolver(_ => Partition);

        List<RaftLog> prefix = [];
        for (int i = 0; i < 20; i++)
            prefix.AddRange(CommitLog(MakeIntent("l/k" + (i % 5), 1_000 + i * 10, revision: i + 1)));

        List<RaftLog> tail = [];
        for (int i = 20; i < 40; i++)
            tail.AddRange(CommitLog(MakeIntent("l/k" + (i % 5), 1_000 + i * 10, revision: i + 1)));

        Apply(live, prefix);
        Assert.True(live.PersistSnapshot(Partition));
        IReadOnlyList<(string, long, KeyValueState, HLCTimestamp)> atCheckpoint = live.SnapshotLedger(Partition);
        HLCTimestamp watermarkAtCheckpoint = live.GetLedgerWatermark(Partition);
        Apply(live, tail);

        // A restarted node reloads exactly the slice the checkpoint captured, then replays the tail — and the
        // entries the capture already reflected, which a replayed settlement records idempotently.
        PreparedIntentStore restarted = new(dir, "rev", null);
        restarted.AttachPartitionResolver(_ => Partition);
        Assert.Equal(atCheckpoint, restarted.SnapshotLedger(Partition));
        Assert.Equal(watermarkAtCheckpoint, restarted.GetLedgerWatermark(Partition));

        Apply(restarted, prefix.Skip(30)); // a re-delivered overlap: idempotent
        Apply(restarted, tail);

        AssertSameSlice(live, restarted);
    }

    [Fact]
    public void SnapshotInstall_ThenTheTail_HoldsTheSameSlice()
    {
        PreparedIntentStore exporter = new();
        List<RaftLog> prefix = [];
        for (int i = 0; i < 20; i++)
            prefix.AddRange(CommitLog(MakeIntent("l/k" + (i % 5), 1_000 + i * 10, revision: i + 1)));
        Apply(exporter, prefix);

        // A lagging follower seeded from the exporter's whole-partition snapshot, then applying the tail.
        byte[] section = exporter.SerializePartitionIntents(Partition, [.. exporter.SnapshotRange(null, null)]);
        PreparedIntentStore installed = new();
        installed.ImportPartitionIntents(Partition, PreparedIntentStore.DeserializePartitionIntents(section), requireLedger: true);
        AssertSameSlice(exporter, installed);

        List<RaftLog> tail = [];
        for (int i = 20; i < 40; i++)
            tail.AddRange(CommitLog(MakeIntent("l/k" + (i % 5), 1_000 + i * 10, revision: i + 1)));
        Apply(exporter, tail);
        Apply(installed, tail);

        AssertSameSlice(exporter, installed);
    }

    [Fact]
    public void SnapshotInstall_ReplacesTheSlice_AndAnInstallWithoutALedger_EmptiesOrRefusesIt()
    {
        PreparedIntentStore node = new();
        Apply(node, CommitLog(MakeIntent("l/stale", 1_000, revision: 3)));

        PreparedIntentStore exporter = new();
        Apply(exporter, CommitLog(MakeIntent("l/current", 2_000, revision: 8)));
        byte[] withLedger = exporter.SerializePartitionIntents(Partition, []);

        node.ImportPartitionIntents(Partition, PreparedIntentStore.DeserializePartitionIntents(withLedger), requireLedger: true);
        Assert.False(node.TryGetLedgerHead(Partition, "l/stale", out _, out _, out _));
        Assert.True(node.TryGetLedgerHead(Partition, "l/current", out _, out _, out _));

        // A section from a build without the ledger (the plain intent serializer) carries no ledger marker.
        byte[] legacy = PreparedIntentStore.SerializeIntents([]);
        PreparedIntentStore.PartitionIntentSection decoded = PreparedIntentStore.DeserializePartitionIntents(legacy);
        Assert.Null(decoded.Ledger);

        Assert.Throws<InvalidDataException>(() => node.ImportPartitionIntents(Partition, decoded, requireLedger: true));
        Assert.True(node.TryGetLedgerHead(Partition, "l/current", out _, out _, out _), "a refused install must leave the slice untouched");

        node.ImportPartitionIntents(Partition, decoded, requireLedger: false);
        Assert.False(node.TryGetLedgerHead(Partition, "l/current", out _, out _, out _), "without the ledger the slice is what that build would hold: empty");
    }

    // ── persistence contract ──────────────────────────────────────────────────────

    [Fact]
    public void Slice_SurvivesColdRestart_WithWatermark()
    {
        PreparedIntentStore store = new(dir, "rev", null);
        store.AttachPartitionResolver(_ => Partition);
        Apply(store, CommitLog(MakeIntent("l/a", 1_000, revision: 4, baseRevision: 3)));
        Apply(store, CommitLog(MakeIntent("l/b", 1_500, revision: 1, state: KeyValueState.Deleted)));
        Assert.True(store.PersistSnapshot(Partition));

        PreparedIntentStore reloaded = new(dir, "rev", null);
        Assert.True(reloaded.TryGetLedgerHead(Partition, "l/a", out long revision, out KeyValueState state, out HLCTimestamp committedAt));
        Assert.Equal((4, KeyValueState.Set, Ts(1_001)), (revision, state, committedAt));
        Assert.True(reloaded.TryGetLedgerHead(Partition, "l/b", out revision, out state, out _));
        Assert.Equal((1, KeyValueState.Deleted), (revision, state));
        Assert.Equal(Ts(1_501), reloaded.GetLedgerWatermark(Partition));

        // The reloaded slice serves the advisory fence too: a stale prepare is refused straight after restart.
        PreparedIntentApplyResult stale = reloaded.Apply(new PrepareIntentCommand(MakeIntent("l/a", 1_600, revision: 4, baseRevision: 3)), Partition);
        Assert.True(stale.StaleBase);
    }

    [Fact]
    public void SliceMutation_RearmsTheSnapshotRewrite()
    {
        PreparedIntentStore store = new(dir, "rev", null);
        store.AttachPartitionResolver(_ => Partition);
        Apply(store, CommitLog(MakeIntent("l/a", 1_000, revision: 4)));
        Assert.True(store.PersistSnapshot(Partition));

        string path = Directory.GetFiles(dir, "preparedintent_rev_p*.snapshot").Single();
        File.Delete(path);
        Assert.True(store.PersistSnapshot(Partition));
        Assert.False(File.Exists(path), "an unchanged slice must not rewrite the file");

        // A settlement that only touches the ledger (the intent is gone again) must still re-arm the rewrite.
        Apply(store, CommitLog(MakeIntent("l/a", 1_100, revision: 5)));
        Assert.True(store.PersistSnapshot(Partition));
        Assert.True(File.Exists(path));

        PreparedIntentStore reloaded = new(dir, "rev", null);
        Assert.True(reloaded.TryGetLedgerHead(Partition, "l/a", out long revision, out _, out _));
        Assert.Equal(5, revision);
    }

    [Fact]
    public void SnapshotWithoutALedger_LoadsEmpty_AndFailsClosedWhenTheGateIsEnabled()
    {
        // A file written by a build that predates the ledger: intents only, no marker.
        PreparedIntent intent = MakeIntent("l/legacy", 1_000, revision: 2);
        File.WriteAllBytes(Path.Combine(dir, $"preparedintent_rev_p{Partition}.snapshot"), PreparedIntentStore.SerializeIntents([intent]));

        PreparedIntentStore reloaded = new(dir, "rev", null);
        Assert.NotNull(reloaded.Get("l/legacy"));
        Assert.False(reloaded.TryGetLedgerHead(Partition, "l/legacy", out _, out _, out _));

        reloaded.ConfigureOnePhaseApplyTimeValidation(false);
        InvalidDataException refused = Assert.Throws<InvalidDataException>(() => reloaded.ConfigureOnePhaseApplyTimeValidation(true));
        Assert.Contains($"partition(s) {Partition}", refused.Message);

        // Once the store has rewritten the file with its ledger section, the next start accepts the option.
        reloaded.AttachPartitionResolver(_ => Partition);
        Assert.True(reloaded.PersistSnapshot(Partition));
        PreparedIntentStore rewritten = new(dir, "rev", null);
        rewritten.ConfigureOnePhaseApplyTimeValidation(true);
    }

    // ── retention ─────────────────────────────────────────────────────────────────

    [Fact]
    public void PhysicalPrune_RunsAtRetentionBucketTransitions_AndDropsOnlyExpiredHeads()
    {
        PreparedIntentStore store = new();
        store.ConfigureStagedBaseFence(retentionMs: 1_000);

        Apply(store, CommitLog(MakeIntent("l/old", 1_000, revision: 1)));    // committed at 1_001
        Apply(store, CommitLog(MakeIntent("l/mid", 1_800, revision: 1)));    // committed at 1_801
        Assert.Equal(2, store.SnapshotLedger(Partition).Count);

        // Watermark 2_301 crosses into a new bucket (250 ms): prune below 1_301 drops l/old only.
        Apply(store, CommitLog(MakeIntent("l/new", 2_300, revision: 1)));
        IReadOnlyList<(string Key, long, KeyValueState, HLCTimestamp)> retained = store.SnapshotLedger(Partition);
        Assert.Equal(["l/mid", "l/new"], retained.Select(static r => r.Key).ToArray());

        Assert.Single(store.SnapshotLedgerSizes());
        Assert.Equal(2, store.SnapshotLedgerSizes()[0].Entries);
        Assert.True(store.SnapshotLedgerSizes()[0].Bytes > 0);
    }

    [Fact]
    public void UnhostPurge_DropsTheSlice()
    {
        PreparedIntentStore store = new();
        Apply(store, CommitLog(MakeIntent("l/a", 1_000, revision: 1)));
        Apply(store, CommitLog(MakeIntent("l/b", 1_000, revision: 1)), partitionId: 4);

        store.PurgePartitionLedger(Partition);

        Assert.False(store.TryGetLedgerHead(Partition, "l/a", out _, out _, out _));
        Assert.True(store.TryGetLedgerHead(4, "l/b", out _, out _, out _));
        Assert.Equal(1, store.CommittedHeadCount);
    }
}
