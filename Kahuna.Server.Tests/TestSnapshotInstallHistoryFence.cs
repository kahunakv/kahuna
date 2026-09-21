using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// What a whole-partition snapshot install leaves behind in the prepared-intent store, and how the advisory
/// staged-base fence reads it afterwards (the state a leader kill leaves behind).
///
/// <para>A whole-partition snapshot is newer than the WAL boundary it is installed at: the exporter walks its
/// state well after the boundary and the receiver replays every retained entry above the boundary on top of
/// the installed state, through the live apply path, against a ledger that is already ahead of those entries.
/// The section therefore carries the exporter's applied position when it was written; entries at or below it
/// are history the installed state already reflects, and the fence does not judge them — otherwise every
/// replayed prepare of a since-committed transaction was refused, vetoed, and (the veto finding the commit)
/// counted as a late veto in the loss witness.</para>
///
/// <para>And the install REPLACES the partition's intent set instead of merging into it: a pending intent this
/// node retained from before the install, whose settlement lies below the boundary and is never replayed, would
/// otherwise stay a phantom holder of its key for good, rejecting every later prepare of the key as a foreign
/// holder while the node's replicas admit it.</para>
/// </summary>
public sealed class TestSnapshotInstallHistoryFence : IDisposable
{
    private const int Partition = 5;

    private readonly string dir = Path.Combine(Path.GetTempPath(), "kahuna-install-history-" + Guid.NewGuid().ToString("N"));

    public TestSnapshotInstallHistoryFence() => Directory.CreateDirectory(dir);

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best effort */ }
    }

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static PreparedIntent MakeIntent(string key, long txPhysical, long revision, long baseRevision) => new(
        TransactionId: Ts(txPhysical), Epoch: 1, Key: key,
        ManifestHash: 0, RecordAnchorKey: key,
        CommitTimestamp: Ts(txPhysical + 1),
        State: KeyValueState.Set, Value: [1], Bucket: null,
        Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: baseRevision, BaseState: KeyValueState.Set,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    /// <summary>An indexed log entry as a follower receives it (bytes copied, so the producer-side command
    /// cache is defeated).</summary>
    private static RaftLog Log(long id, params PreparedIntentCommand[] commands) =>
        new() { Id = id, LogType = ReplicationTypes.PreparedIntent, LogData = [.. PreparedIntentStore.SerializeDelta(commands)] };

    /// <summary>One transaction's lifecycle as three consecutive indexed entries starting at <paramref name="firstId"/>.</summary>
    private static RaftLog[] CommitLog(long firstId, PreparedIntent intent) =>
    [
        Log(firstId, new PrepareIntentCommand(intent)),
        Log(firstId + 1, new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true)),
        Log(firstId + 2, new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key))
    ];

    /// <summary>Applies through the live follower path — the one that folds the fence verdict and fires the veto.</summary>
    private static bool ApplyLive(PreparedIntentStore store, RaftLog log) => store.ApplyDeltaAckPrepares(Partition, log);

    private static void ApplyLive(PreparedIntentStore store, IEnumerable<RaftLog> logs)
    {
        foreach (RaftLog log in logs)
            ApplyLive(store, log);
    }

    /// <summary>An exporter that committed key <c>h/k</c> twice (revisions 6 then 7) at log ids 1..6, and the
    /// section it exports for the partition: reflected through log id 6.</summary>
    private static (PreparedIntentStore Exporter, PreparedIntentStore.PartitionIntentSection Section) ExportedHistory()
    {
        PreparedIntentStore exporter = new();
        ApplyLive(exporter, CommitLog(1, MakeIntent("h/k", 1_000, revision: 6, baseRevision: 5)));
        ApplyLive(exporter, CommitLog(4, MakeIntent("h/k", 1_100, revision: 7, baseRevision: 6)));

        byte[] bytes = exporter.SerializePartitionIntents(Partition, [.. exporter.SnapshotRange(null, null)]);
        PreparedIntentStore.PartitionIntentSection section = PreparedIntentStore.DeserializePartitionIntents(bytes);

        Assert.Equal(6, section.ReflectedThroughIndex);
        return (exporter, section);
    }

    // ── the history window ───────────────────────────────────────────────────────

    [Fact]
    public void ReplayBelowTheInstalledPosition_IsNotJudged_AndNeverVetoes()
    {
        (_, PreparedIntentStore.PartitionIntentSection section) = ExportedHistory();

        PreparedIntentStore installed = new();
        List<(string Key, long Head)> vetoes = [];
        installed.AttachStaleBaseVetoer((intent, head) => vetoes.Add((intent.Key, head)));
        installed.ReplacePartitionIntents(Partition, section, requireLedger: true, isOwned: _ => true);

        Assert.Equal(6, installed.GetLedgerReflectedThroughIndex(Partition));
        Assert.True(installed.IsHistoricalApply(Partition, 6));
        Assert.False(installed.IsHistoricalApply(Partition, 7));
        Assert.True(installed.TryGetCommittedHead("h/k", out long head, out _));
        Assert.Equal(7, head);

        // The receiver's WAL boundary is below the exporter's position, so the first transaction's prepare
        // (validated at base 5, long since committed and superseded) replays through the live path against
        // a ledger whose head is already 7. Judged, it would be "stale"; it is history, so it is neither
        // refused nor vetoed, and the transition still installs.
        RaftLog[] replay = CommitLog(1, MakeIntent("h/k", 1_000, revision: 6, baseRevision: 5));
        Assert.True(ApplyLive(installed, replay[0]), "a replayed prepare below the reflected position must be acknowledged as applied");
        Assert.NotNull(installed.Get("h/k"));
        Assert.Empty(vetoes);

        ApplyLive(installed, replay.Skip(1));
        ApplyLive(installed, CommitLog(4, MakeIntent("h/k", 1_100, revision: 7, baseRevision: 6)));
        Assert.Empty(vetoes);
        Assert.Null(installed.Get("h/k"));

        // Past the reflected position the fence is live again: a genuinely stale base is refused and vetoed.
        Assert.False(ApplyLive(installed, Log(7, new PrepareIntentCommand(MakeIntent("h/k", 1_200, revision: 7, baseRevision: 5)))));
        Assert.Equal([("h/k", 7L)], vetoes);
    }

    [Fact]
    public void ReplayBelowTheInstalledPosition_StillAppliesTheTransitions_SameSliceAsTheExporter()
    {
        (PreparedIntentStore exporter, PreparedIntentStore.PartitionIntentSection section) = ExportedHistory();

        PreparedIntentStore installed = new();
        installed.ReplacePartitionIntents(Partition, section, requireLedger: true, isOwned: _ => true);

        // The whole window replays (ids 1..6), then a live tail (ids 7..9) that both replicas apply.
        ApplyLive(installed, CommitLog(1, MakeIntent("h/k", 1_000, revision: 6, baseRevision: 5)));
        ApplyLive(installed, CommitLog(4, MakeIntent("h/k", 1_100, revision: 7, baseRevision: 6)));

        RaftLog[] tail = CommitLog(7, MakeIntent("h/k", 1_200, revision: 8, baseRevision: 7));
        ApplyLive(exporter, tail);
        ApplyLive(installed, tail);

        Assert.Equal(exporter.SnapshotLedger(Partition), installed.SnapshotLedger(Partition));
        Assert.Equal(exporter.GetLedgerWatermark(Partition), installed.GetLedgerWatermark(Partition));
        Assert.Equal(0, installed.LiveIntentCount);
    }

    [Fact]
    public void UnindexedApplies_AreNeverHistory()
    {
        (_, PreparedIntentStore.PartitionIntentSection section) = ExportedHistory();

        PreparedIntentStore installed = new();
        installed.ReplacePartitionIntents(Partition, section, requireLedger: true, isOwned: _ => true);

        // The pure/in-memory configuration applies without a log index; the fence judges as before.
        PreparedIntentApplyResult result = installed.Apply(
            new PrepareIntentCommand(MakeIntent("h/k", 1_200, revision: 7, baseRevision: 5)), Partition);

        Assert.Equal(TransactionApplyOutcome.Applied, result.Outcome);
        Assert.True(result.StaleBase);
        Assert.False(installed.IsHistoricalApply(Partition, 0));
    }

    [Fact]
    public void AnotherPartitionsLog_IsNotHistory()
    {
        (_, PreparedIntentStore.PartitionIntentSection section) = ExportedHistory();

        PreparedIntentStore installed = new();
        installed.ReplacePartitionIntents(Partition, section, requireLedger: true, isOwned: _ => true);

        Assert.True(installed.IsHistoricalApply(Partition, 3));
        Assert.False(installed.IsHistoricalApply(Partition + 1, 3));
    }

    [Fact]
    public void ReflectedPosition_NeverRegresses_OnARedeliveredOrOlderSection()
    {
        (PreparedIntentStore exporter, PreparedIntentStore.PartitionIntentSection older) = ExportedHistory();

        ApplyLive(exporter, CommitLog(7, MakeIntent("h/k", 1_200, revision: 8, baseRevision: 7)));
        PreparedIntentStore.PartitionIntentSection newer = PreparedIntentStore.DeserializePartitionIntents(
            exporter.SerializePartitionIntents(Partition, [.. exporter.SnapshotRange(null, null)]));
        Assert.Equal(9, newer.ReflectedThroughIndex);

        PreparedIntentStore installed = new();
        installed.ReplacePartitionIntents(Partition, newer, requireLedger: true, isOwned: _ => true);
        installed.ReplacePartitionIntents(Partition, older, requireLedger: true, isOwned: _ => true);

        Assert.Equal(9, installed.GetLedgerReflectedThroughIndex(Partition));
    }

    [Fact]
    public void ReflectedPosition_SurvivesTheCheckpointAndARestart()
    {
        (_, PreparedIntentStore.PartitionIntentSection section) = ExportedHistory();

        PreparedIntentStore installed = new(dir, "rev", null);
        installed.AttachPartitionResolver(_ => Partition);
        installed.ReplacePartitionIntents(Partition, section, requireLedger: true, isOwned: _ => true);
        Assert.True(installed.PersistSnapshot(Partition));

        // A restart mid catch-up reloads the slice with its position: the remaining window is still history.
        PreparedIntentStore restarted = new(dir, "rev", null);
        restarted.AttachPartitionResolver(_ => Partition);

        Assert.Equal(6, restarted.GetLedgerReflectedThroughIndex(Partition));
        Assert.True(restarted.IsHistoricalApply(Partition, 6));
        Assert.False(restarted.IsHistoricalApply(Partition, 7));
        Assert.Equal(installed.SnapshotLedger(Partition), restarted.SnapshotLedger(Partition));

        // A checkpoint of a slice that was never installed advertises no history.
        PreparedIntentStore fresh = new();
        ApplyLive(fresh, CommitLog(1, MakeIntent("h/k", 1_000, revision: 6, baseRevision: 5)));
        Assert.Equal(0, fresh.GetLedgerReflectedThroughIndex(Partition));
        Assert.False(fresh.IsHistoricalApply(Partition, 1));
    }

    [Fact]
    public void AnExporterThatNeverAppliedAnIndexedEntry_AdvertisesNoHistory()
    {
        PreparedIntentStore exporter = new();
        exporter.Apply(new PrepareIntentCommand(MakeIntent("h/pending", 1_000, revision: 3, baseRevision: 2)), Partition);

        PreparedIntentStore.PartitionIntentSection section = PreparedIntentStore.DeserializePartitionIntents(
            exporter.SerializePartitionIntents(Partition, [.. exporter.SnapshotRange(null, null)]));

        Assert.Equal(0, section.ReflectedThroughIndex);
    }

    // ── replace, not merge ───────────────────────────────────────────────────────

    [Fact]
    public void Install_DropsPendingIntentsThePartitionNoLongerHas_AndKeepsOtherPartitions()
    {
        // The node held three pending intents of the partition before it fell behind: one the exporter still
        // holds identically, one the exporter now holds under a later transaction, one the exporter settled
        // and removed below the boundary. Plus an intent of another partition the install must not touch.
        PreparedIntentStore node = new();
        PreparedIntent same = MakeIntent("p/same", 1_000, revision: 4, baseRevision: 3);
        PreparedIntent superseded = MakeIntent("p/superseded", 1_010, revision: 4, baseRevision: 3);
        PreparedIntent phantom = MakeIntent("p/phantom", 1_020, revision: 4, baseRevision: 3);
        PreparedIntent foreign = MakeIntent("q/keep", 1_030, revision: 4, baseRevision: 3);
        foreach (PreparedIntent intent in new[] { same, superseded, phantom, foreign })
            Assert.Equal(TransactionApplyOutcome.Applied, node.Apply(new PrepareIntentCommand(intent), Partition).Outcome);

        PreparedIntentStore exporter = new();
        PreparedIntent later = MakeIntent("p/superseded", 2_000, revision: 5, baseRevision: 4);
        exporter.Apply(new PrepareIntentCommand(same), Partition);
        exporter.Apply(new PrepareIntentCommand(later), Partition);
        PreparedIntentStore.PartitionIntentSection section = PreparedIntentStore.DeserializePartitionIntents(
            exporter.SerializePartitionIntents(Partition, [.. exporter.SnapshotRange(null, null)]));

        node.ReplacePartitionIntents(Partition, section, requireLedger: true, isOwned: key => key.StartsWith("p/", StringComparison.Ordinal));

        Assert.Equal(same.TransactionId, node.Get("p/same")!.TransactionId);
        Assert.Equal(later.TransactionId, node.Get("p/superseded")!.TransactionId);
        Assert.Null(node.Get("p/phantom"));
        Assert.NotNull(node.Get("q/keep"));

        // The key the phantom held is free again: a later transaction's prepare — which the node's replicas
        // admit — is admitted here too instead of being rejected as held by a foreign transaction.
        PreparedIntentApplyResult next = node.Apply(new PrepareIntentCommand(MakeIntent("p/phantom", 3_000, revision: 5, baseRevision: 4)), Partition);
        Assert.Equal(TransactionApplyOutcome.Applied, next.Outcome);
        Assert.False(next.StaleBase);
    }

    [Fact]
    public void Install_WithAMergeInsteadOfAReplace_WouldHaveKeptThePhantom()
    {
        // The regression pinned: the merging import keeps a locally held pending intent the section lacks.
        PreparedIntentStore node = new();
        node.Apply(new PrepareIntentCommand(MakeIntent("p/phantom", 1_020, revision: 4, baseRevision: 3)), Partition);

        PreparedIntentStore.PartitionIntentSection section = PreparedIntentStore.DeserializePartitionIntents(
            new PreparedIntentStore().SerializePartitionIntents(Partition, []));

        node.ImportPartitionIntents(Partition, section, requireLedger: true);
        Assert.NotNull(node.Get("p/phantom"));

        Assert.Equal(TransactionApplyOutcome.Rejected,
            node.Apply(new PrepareIntentCommand(MakeIntent("p/phantom", 3_000, revision: 5, baseRevision: 4)), Partition).Outcome);

        node.ReplacePartitionIntents(Partition, section, requireLedger: true, isOwned: _ => true);
        Assert.Null(node.Get("p/phantom"));
    }

    [Fact]
    public void Install_ReplacesTheLedgerSlice_AndRefusesALedgerlessSectionWhenRequired()
    {
        PreparedIntentStore node = new();
        ApplyLive(node, CommitLog(1, MakeIntent("p/old", 1_000, revision: 3, baseRevision: 2)));

        (_, PreparedIntentStore.PartitionIntentSection section) = ExportedHistory();
        node.ReplacePartitionIntents(Partition, section, requireLedger: true, isOwned: _ => true);

        Assert.False(node.TryGetLedgerHead(Partition, "p/old", out _, out _, out _));
        Assert.True(node.TryGetLedgerHead(Partition, "h/k", out long head, out _, out _));
        Assert.Equal(7, head);

        PreparedIntentStore.PartitionIntentSection legacy = PreparedIntentStore.DeserializePartitionIntents(PreparedIntentStore.SerializeIntents([]));
        Assert.Null(legacy.Ledger);
        Assert.Throws<InvalidDataException>(() => node.ReplacePartitionIntents(Partition, legacy, requireLedger: true, isOwned: _ => true));

        // Refused before anything moved.
        Assert.True(node.TryGetLedgerHead(Partition, "h/k", out _, out _, out _));

        node.ReplacePartitionIntents(Partition, legacy, requireLedger: false, isOwned: _ => true);
        Assert.False(node.TryGetLedgerHead(Partition, "h/k", out _, out _, out _));
        Assert.Equal(0, node.GetLedgerReflectedThroughIndex(Partition));
    }
}
