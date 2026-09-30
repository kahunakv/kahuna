using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// What a node's own prepared-intent checkpoint leaves behind for the restart that reloads it, and how the
/// WAL replay after that restart reads the reloaded set.
///
/// <para>A restart replays the partition's log from its application-durability floor, and that floor can sit
/// far below the checkpoint: the floor waits for the slowest channel (the key-value flusher on a stalled disk),
/// the checkpoint captures the intent set as it stands. The entries between the two are history the reloaded
/// set already reflects — exactly the window a whole-partition snapshot install leaves for its receiver — and
/// they must replay as such. Replayed through the live path they re-execute against a set from the future: a
/// prepare the live apply refused because a competitor held its key finds the key free (the competitor settled
/// before the checkpoint) and installs a phantom holder no replica ever had, which then refuses every later
/// transaction of the key as foreign-held. CamusDB fault soak sn1 (2026-09-30): a leader killed 28 s after such
/// a refusal came back with two of its keys frozen below their peers' heads, led again, and lost ~100 s of
/// committed writes on both rows.</para>
/// </summary>
public sealed class TestRestartReplayHistoryFence : IDisposable
{
    private const int Partition = 5;

    private readonly string dir = Path.Combine(Path.GetTempPath(), "kahuna-restart-history-" + Guid.NewGuid().ToString("N"));

    public TestRestartReplayHistoryFence() => Directory.CreateDirectory(dir);

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

    /// <summary>An indexed log entry as it is applied live and replayed after a restart (bytes copied, so the
    /// producer-side command cache is defeated).</summary>
    private static RaftLog Log(long id, params PreparedIntentCommand[] commands) =>
        new() { Id = id, LogType = ReplicationTypes.PreparedIntent, LogData = [.. PreparedIntentStore.SerializeDelta(commands)] };

    private static RaftLog Prepare(long id, PreparedIntent intent) => Log(id, new PrepareIntentCommand(intent));

    private static RaftLog[] Settle(long firstId, PreparedIntent intent) =>
    [
        Log(firstId, new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true)),
        Log(firstId + 1, new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key))
    ];

    private static PreparedIntentStore Persisted(string dir)
    {
        PreparedIntentStore store = new(dir, "rev", null);
        store.AttachPartitionResolver(_ => Partition);
        return store;
    }

    /// <summary>The live follower path — folds the fence verdict, memoes refusals, fires vetoes.</summary>
    private static bool ApplyLive(PreparedIntentStore store, RaftLog log) => store.ApplyDeltaAckPrepares(Partition, log);

    /// <summary>The restart path: the WAL replay of an entry from the durability floor up.</summary>
    private static void Replay(PreparedIntentStore store, params RaftLog[] logs)
    {
        foreach (RaftLog log in logs)
            Assert.True(store.Restore(Partition, log));
    }

    // The shape of sn1 on one key: H holds it (1), X's prepare is refused as foreign-held (2), H settles
    // (3, 4), T1 takes the key (5) and settles (6, 7). Then the node checkpoints and dies; the durability
    // floor is still at 1, so the restart replays 2..7 over the reloaded set.
    private readonly PreparedIntent holder = MakeIntent("k", 1_000, revision: 6, baseRevision: 5);
    private readonly PreparedIntent refused = MakeIntent("k", 1_010, revision: 6, baseRevision: 5);
    private readonly PreparedIntent next = MakeIntent("k", 1_020, revision: 7, baseRevision: 6);

    private RaftLog[] History() =>
    [
        Prepare(1, holder),
        Prepare(2, refused),
        .. Settle(3, holder),
        Prepare(5, next),
        .. Settle(6, next)
    ];

    private void RunLive(PreparedIntentStore store)
    {
        RaftLog[] history = History();

        Assert.True(ApplyLive(store, history[0]));
        Assert.False(ApplyLive(store, history[1]), "X's prepare is refused live: H holds the key");
        Assert.True(store.TryTakePrepareRejection(refused.TransactionId, 1, "k", out _));

        foreach (RaftLog log in history.Skip(2))
            Assert.True(ApplyLive(store, log));

        Assert.Null(store.Get("k"));
        Assert.Equal(7, store.GetAppliedLogIndex(Partition));
    }

    [Fact]
    public void ReplayBelowTheCheckpointsCertifiedPosition_InstallsNoPhantom()
    {
        PreparedIntentStore before = Persisted(dir);
        RunLive(before);

        // The checkpoint certifies the position applied through the store before its walk (the durability
        // tracker's prepared-intent ceiling): the file is exact for every entry at or below it.
        Assert.True(before.PersistSnapshot(Partition, appliedThroughIndex: 7));

        PreparedIntentStore restarted = Persisted(dir);
        Assert.Equal(7, restarted.GetLedgerFullyReflectedThroughIndex(Partition));
        Assert.Equal(7, restarted.GetLedgerReflectedThroughIndex(Partition));
        Assert.True(restarted.IsFullyReflectedApply(Partition, 7));
        Assert.False(restarted.IsHistoricalApply(Partition, 8));
        Assert.Equal(0, restarted.LiveIntentCount);

        // The replay from the floor: X's prepare finds the key free — H settled before the checkpoint — and
        // must install nothing, because the reloaded set is exact for it. T1's lifecycle folds the same way.
        RaftLog[] history = History();
        Replay(restarted, history.Skip(1).ToArray());

        Assert.Null(restarted.Get("k"));
        Assert.Equal(0, restarted.LiveIntentCount);

        // Past the certified position the store is live again, and the key is free for the next transaction,
        // exactly as it is on the node's replicas.
        PreparedIntent after = MakeIntent("k", 1_030, revision: 8, baseRevision: 7);
        Assert.True(ApplyLive(restarted, Prepare(8, after)));
        Assert.Equal(after.TransactionId, restarted.Get("k")!.TransactionId);
    }

    [Fact]
    public void ReplayThroughTheLivePath_WouldHaveInstalledThePhantom()
    {
        // The regression pinned: a checkpoint that certifies no position leaves the replay on the live path,
        // where X's refused prepare installs over the settled holder's key and every later prepare of the key
        // is refused as foreign-held.
        PreparedIntentStore before = Persisted(dir);
        RunLive(before);
        Assert.True(before.PersistSnapshot(Partition));

        PreparedIntentStore restarted = Persisted(dir);
        Assert.Equal(0, restarted.GetLedgerFullyReflectedThroughIndex(Partition));

        RaftLog[] history = History();
        Replay(restarted, history.Skip(1).ToArray());

        Assert.Equal(refused.TransactionId, restarted.Get("k")!.TransactionId);
        Assert.False(ApplyLive(restarted, Prepare(8, MakeIntent("k", 1_030, revision: 8, baseRevision: 7))));
    }

    [Fact]
    public void CertifiedPosition_NeverExceedsTheStoresOwn_AndTheWalksEndIsTheReflectedOne()
    {
        PreparedIntentStore before = Persisted(dir);
        RunLive(before);

        // An intent prepared after the certified cut and still live when the file is written: the file holds
        // it, and its position is the reflected one — replaying it lands in the tear window, where a prepare
        // the walk missed still installs, and one it held folds idempotently.
        PreparedIntent late = MakeIntent("k", 1_030, revision: 8, baseRevision: 7);
        Assert.True(ApplyLive(before, Prepare(8, late)));
        Assert.True(before.PersistSnapshot(Partition, appliedThroughIndex: 7));

        PreparedIntentStore restarted = Persisted(dir);
        Assert.Equal(7, restarted.GetLedgerFullyReflectedThroughIndex(Partition));
        Assert.Equal(8, restarted.GetLedgerReflectedThroughIndex(Partition));
        Assert.Equal(late.TransactionId, restarted.Get("k")!.TransactionId);

        Replay(restarted, History().Skip(1).ToArray());
        Assert.Equal(late.TransactionId, restarted.Get("k")!.TransactionId);
        Replay(restarted, Prepare(8, late));
        Assert.Equal(late.TransactionId, restarted.Get("k")!.TransactionId);
        Assert.Equal(1, restarted.LiveIntentCount);
    }

    [Fact]
    public void AnUnchangedSet_AtALaterCertifiedPosition_StillRewritesTheFile()
    {
        PreparedIntentStore before = Persisted(dir);
        RunLive(before);
        Assert.True(before.PersistSnapshot(Partition, appliedThroughIndex: 5));

        // Nothing mutated since, but the certified position moved: the file must say so, or the entries
        // between 5 and 7 replay as live history on the next restart.
        Assert.True(before.PersistSnapshot(Partition, appliedThroughIndex: 7));

        PreparedIntentStore restarted = Persisted(dir);
        Assert.Equal(7, restarted.GetLedgerFullyReflectedThroughIndex(Partition));
    }

    [Fact]
    public void AnInstalledSlicesPositions_AreNeverLoweredByTheCheckpoint()
    {
        // A node seeded by a whole-partition snapshot keeps the exporter's positions across its own
        // checkpoints, whatever it certifies itself.
        PreparedIntentStore exporter = new();
        foreach (RaftLog log in History())
            ApplyLive(exporter, log);
        PreparedIntentStore.PartitionIntentSection section = PreparedIntentStore.DeserializePartitionIntents(
            exporter.SerializePartitionIntents(Partition, [.. exporter.SnapshotRange(null, null)], exporter.GetAppliedLogIndex(Partition)));
        Assert.Equal(7, section.FullyReflectedThroughIndex);

        PreparedIntentStore installed = Persisted(dir);
        installed.ReplacePartitionIntents(Partition, section, requireLedger: true, isOwned: _ => true);
        Assert.True(installed.PersistSnapshot(Partition, appliedThroughIndex: 3));

        PreparedIntentStore restarted = Persisted(dir);
        Assert.Equal(7, restarted.GetLedgerFullyReflectedThroughIndex(Partition));
        Assert.Equal(7, restarted.GetLedgerReflectedThroughIndex(Partition));
    }

    [Fact]
    public void TheAppliedPosition_IsPublishedOnlyAfterTheEntryIsInTheMap()
    {
        // The position an exporter or a checkpoint reads before its walk certifies that every intent prepared
        // at or below it is in the map: an entry must not count as applied until its commands are.
        PreparedIntentStore store = new();
        long observedAtApply = -1;
        PreparedIntent? intentAtApply = null;

        store.AttachStaleBaseVetoer((_, _) => { });
        store.AttachCommittedSettleObserver(_ =>
        {
            observedAtApply = store.GetAppliedLogIndex(Partition);
            intentAtApply = store.Get("k");
        });

        ApplyLive(store, Prepare(1, holder));
        Assert.Equal(1, store.GetAppliedLogIndex(Partition));

        // The settle observer runs inside the resolve's apply, after the map changed and before the entry is
        // marked applied: the position it sees is still the previous entry's.
        ApplyLive(store, Settle(2, holder)[0]);
        Assert.Equal(1, observedAtApply);
        Assert.NotNull(intentAtApply);
        Assert.Equal(2, store.GetAppliedLogIndex(Partition));
    }
}
