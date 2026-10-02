using System.Diagnostics.Metrics;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// How a restart replay resolves the by-reference materializations (records and materializing resolves) of the
/// history window — the entries between the application-durability floor the replay starts from and the position
/// the node's own prepared-intent checkpoint certified as fully reflected.
///
/// <para>In that window a replayed prepare that finds no live intent installs nothing: the reloaded intent set is
/// exact for it, and installing would create a phantom holder. But the window's data side is not reflected
/// anywhere on the node (the floor sits below it precisely because those rows never reached the backend), and a
/// commit there is materialized by a record that carries no value and names the intent the replay just declined
/// to install. So the folded prepare is kept as replay history — present for that record to resolve from, never a
/// holder — and dropped when its settle replays. A record with no source anywhere is verified against the
/// backend row before it is counted as a value the restart left missing; the partition's restore-finished hook
/// reports the tally and gates the partition when anything is unresolved. CamusDB fault soak sn2 (2026-09-30):
/// a restart that replayed a 245K-entry window logged 82,536 such misses over all 2,000 keys and then led.</para>
/// </summary>
[Collection("MaterializationMissMetrics")]
public sealed class TestRestartReplayByReferenceMaterialization : IDisposable
{
    private const int Partition = 5;

    private const string UnresolvedCounter = "kahuna.kv.restore_by_reference_unresolved";

    private const string ResolvedCounter = "kahuna.kv.restore_by_reference_resolved";

    private readonly string dir = Path.Combine(Path.GetTempPath(), "kahuna-replay-byref-" + Guid.NewGuid().ToString("N"));

    public TestRestartReplayByReferenceMaterialization() => Directory.CreateDirectory(dir);

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best effort */ }
    }

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static PreparedIntent MakeIntent(string key, long txPhysical, long revision, byte[]? value, KeyValueState state = KeyValueState.Set) => new(
        TransactionId: Ts(txPhysical), Epoch: 1, Key: key,
        ManifestHash: 0, RecordAnchorKey: key,
        CommitTimestamp: Ts(txPhysical + 1),
        State: state, Value: value, Bucket: null,
        Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: revision - 1, BaseState: KeyValueState.Set,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    private static RaftLog Delta(long id, params PreparedIntentCommand[] commands) =>
        new() { Id = id, LogType = ReplicationTypes.PreparedIntent, LogData = [.. PreparedIntentStore.SerializeDelta(commands)] };

    private static RaftLog Prepare(long id, PreparedIntent intent) => Delta(id, new PrepareIntentCommand(intent));

    private static RaftLog Resolve(long id, PreparedIntent intent, bool materializeOnResolve = false) =>
        Delta(id, new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true, materializeOnResolve, intent.CommitTimestamp));

    private static RaftLog Remove(long id, PreparedIntent intent) =>
        Delta(id, new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key));

    private static RaftLog Record(long id, PreparedIntent intent) =>
        new()
        {
            Id = id, Type = RaftLogType.Committed, LogType = ReplicationTypes.KeyValues,
            LogData = [.. PreparedIntentMaterializer.ToKeyValueRecord(intent with { Resolution = PreparedIntentResolution.Committed }, new KeyValueMessage(), byReference: true)]
        };

    private PreparedIntentStore Persisted()
    {
        PreparedIntentStore store = new(dir, "rev", null);
        store.AttachPartitionResolver(_ => Partition);
        return store;
    }

    /// <summary>The live follower path, then a checkpoint that certifies everything applied so far.</summary>
    private static void ApplyLiveAndCheckpoint(PreparedIntentStore store, long certifiedThrough, params RaftLog[] logs)
    {
        foreach (RaftLog log in logs)
            Assert.True(store.ApplyDeltaAckPrepares(Partition, log));

        Assert.True(store.PersistSnapshot(Partition, appliedThroughIndex: certifiedThrough));
    }

    /// <summary>The restart path: a prepared-intent delta replayed from the WAL.</summary>
    private static void Replay(PreparedIntentStore store, params RaftLog[] logs)
    {
        foreach (RaftLog log in logs)
            Assert.True(store.Restore(Partition, log));
    }

    /// <summary>Sums a named counter's increments on the "Kahuna" meter while the action runs, by its
    /// <c>source</c> tag (an untagged increment sums under the empty string). The meter is process-wide and test
    /// classes run in parallel, so a test asserts at-least on it and takes exact counts from the restorer's own
    /// summary.</summary>
    private static Dictionary<string, long> MeasureBySource(string instrumentName, Action action)
    {
        Dictionary<string, long> totals = new(StringComparer.Ordinal);
        using MeterListener listener = new();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "Kahuna" && instrument.Name == instrumentName)
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
        {
            string source = "";
            foreach (KeyValuePair<string, object?> tag in tags)
                if (tag.Key == "source")
                    source = tag.Value?.ToString() ?? "";

            lock (totals)
                totals[source] = totals.GetValueOrDefault(source) + measurement;
        });
        listener.Start();

        action();

        listener.Dispose();
        return totals;
    }

    /// <summary>A flushed row as the backend holds it: the last-modified stamp is the commit timestamp.</summary>
    private static PersistenceRequestItem Row(string key, byte[]? value, long revision, HLCTimestamp lastModified, KeyValueState state = KeyValueState.Set) =>
        new(key, value, revision, 0, 0, 0, 0, 0, 0, lastModified.N, lastModified.L, lastModified.C, (int)state);

    private sealed class RestorerInstaller(KeyValueRestorer restorer) : IResolvedIntentInstaller
    {
        public void Install(int partitionId, long logIndex, PreparedIntent intent, bool replay) =>
            restorer.RestoreResolvedIntent(partitionId, logIndex, intent);

        public void CompleteEntry(int partitionId, long logIndex, bool replay) =>
            restorer.CompleteResolvedIntentEntry(partitionId, logIndex);

        public void NoteUnresolvedOnReplay(int partitionId, long logIndex, HLCTimestamp transactionId, long epoch, string key, HLCTimestamp commitTimestamp) =>
            restorer.NoteUnresolvedMaterializingResolve(partitionId, logIndex, transactionId, epoch, key, commitTimestamp);
    }

    // ── store: what the fully reflected fence keeps ─────────────────────────────

    [Fact]
    public void FoldedPrepareInTheHistoryWindow_IsKeptAsReplayHistory_NeverAsALiveIntent()
    {
        PreparedIntent committed = MakeIntent("k", 1_000, revision: 6, value: [1]);

        // Live: prepare (1), record (2, not a delta), settle (3, 4); the checkpoint certifies through 4.
        PreparedIntentStore before = Persisted();
        ApplyLiveAndCheckpoint(before, certifiedThrough: 4, Prepare(1, committed), Resolve(3, committed), Remove(4, committed));
        Assert.Null(before.Get("k"));

        PreparedIntentStore restarted = Persisted();
        Assert.True(restarted.IsFullyReflectedApply(Partition, 4));
        Assert.Equal(0, restarted.ReplayHistoryIntentCount);

        // The replayed prepare installs nothing live and is kept for the record that names it.
        Replay(restarted, Prepare(1, committed));
        Assert.Null(restarted.Get("k"));
        Assert.Null(restarted.GetByIdentity(committed.TransactionId, committed.Epoch, "k"));
        Assert.Equal(0, restarted.LiveIntentCount);
        Assert.Equal(1, restarted.ReplayHistoryIntentCount);
        Assert.True(restarted.TryGetReplayHistoryIntent(Partition, committed.TransactionId, committed.Epoch, "k", out PreparedIntent? history));
        Assert.Equal(6, history!.Revision);
        Assert.Equal(new byte[] { 1 }, history.Value);

        // Another partition's replay does not see it.
        Assert.False(restarted.TryGetReplayHistoryIntent(Partition + 1, committed.TransactionId, committed.Epoch, "k", out _));

        // The resolve leaves it (the record precedes the settle, but a duplicate record may follow the resolve);
        // the removal drops it.
        Replay(restarted, Resolve(3, committed));
        Assert.Equal(1, restarted.ReplayHistoryIntentCount);
        Replay(restarted, Remove(4, committed));
        Assert.Equal(0, restarted.ReplayHistoryIntentCount);
        Assert.False(restarted.TryGetReplayHistoryIntent(Partition, committed.TransactionId, committed.Epoch, "k", out _));
    }

    [Fact]
    public void RefusedPrepareInTheHistoryWindow_IsNeitherAPhantomNorKeptPastTheReplay()
    {
        // The phantom shape: H holds the key (1), X's prepare is refused live (2), H settles (3, 4).
        PreparedIntent holder = MakeIntent("k", 1_000, revision: 6, value: [1]);
        PreparedIntent refused = MakeIntent("k", 1_010, revision: 6, value: [2]);

        PreparedIntentStore before = Persisted();
        Assert.True(before.ApplyDeltaAckPrepares(Partition, Prepare(1, holder)));
        Assert.False(before.ApplyDeltaAckPrepares(Partition, Prepare(2, refused)));
        ApplyLiveAndCheckpoint(before, certifiedThrough: 4, Resolve(3, holder), Remove(4, holder));

        PreparedIntentStore restarted = Persisted();

        // The floor sits at 1: X's prepare replays over a free key and must still not install. It is kept as
        // history — X never produced a record, so nothing reads it — and the restore-finished clear drops it.
        Replay(restarted, Prepare(2, refused), Resolve(3, holder), Remove(4, holder));
        Assert.Null(restarted.Get("k"));
        Assert.Equal(0, restarted.LiveIntentCount);
        Assert.Equal(1, restarted.ReplayHistoryIntentCount);

        Assert.Equal(1, restarted.ClearReplayHistory(Partition));
        Assert.Equal(0, restarted.ReplayHistoryIntentCount);
        Assert.Equal(0, restarted.ClearReplayHistory(Partition));

        // Above the window the store is live again: the key is free for the next transaction.
        PreparedIntent next = MakeIntent("k", 1_030, revision: 7, value: [3]);
        Assert.True(restarted.ApplyDeltaAckPrepares(Partition, Prepare(5, next)));
        Assert.Equal(next.TransactionId, restarted.Get("k")!.TransactionId);
    }

    [Fact]
    public void LivePrepareAboveTheWindow_IsNotKeptAsHistory()
    {
        PreparedIntent committed = MakeIntent("k", 1_000, revision: 6, value: [1]);

        PreparedIntentStore before = Persisted();
        ApplyLiveAndCheckpoint(before, certifiedThrough: 0);

        PreparedIntentStore restarted = Persisted();
        Replay(restarted, Prepare(1, committed));

        Assert.Equal(1, restarted.LiveIntentCount);
        Assert.Equal(0, restarted.ReplayHistoryIntentCount);
    }

    // ── restorer: the record resolves from the history ──────────────────────────

    /// <summary>The sn2 shape on one key: the prepare, the record and the settle all lie inside the history
    /// window (the flusher was behind, the checkpoint certified past them), and the snapshot holds the
    /// transaction only as a settled intent retained for its flush. The record must resolve — from the replayed
    /// prepare itself, so that no release of the retained set can take the source away.</summary>
    [Fact]
    public void Restorer_RecordInsideTheHistoryWindow_ResolvesFromTheReplayedPrepare()
    {
        PreparedIntent committed = MakeIntent("acct/1", 1_000, revision: 9, value: [4, 5, 6]);

        PreparedIntentStore before = Persisted();
        // No unflushed-row probe: nothing is retained, so the replayed prepare is the record's only source.
        ApplyLiveAndCheckpoint(before, certifiedThrough: 4, Prepare(1, committed), Resolve(3, committed), Remove(4, committed));

        PreparedIntentStore restarted = Persisted();
        Assert.Equal(0, restarted.SettledIntentsAwaitingFlushCount);

        (KeyValueRestorer restorer, UnflushedKeyValueWritesIndex overlay, _, IDisposable lifetime) =
            KeyValueRestorerHarness.Build(out _, restarted);

        using (lifetime)
        {
            Dictionary<string, long> resolved = MeasureBySource(ResolvedCounter, () =>
            {
                Replay(restarted, Prepare(1, committed));
                Assert.True(restorer.Restore(Partition, Record(2, committed)));
                Replay(restarted, Resolve(3, committed), Remove(4, committed));
            });

            Assert.True(resolved.GetValueOrDefault("history") >= 1, "the history source was expected on the resolved counter");
            Assert.True(overlay.TryGet("acct/1", out UnflushedKeyValueWrite replayed));
            Assert.Equal(9, replayed.Revision);
            Assert.Equal(new byte[] { 4, 5, 6 }, replayed.Value);
            Assert.Equal(KeyValueState.Set, replayed.State);
            Assert.Equal(Ts(1_001), replayed.LastModified);

            KeyValueRestorer.RestoreSummary summary = restorer.CompleteRestore(Partition);
            Assert.Equal(1, summary.FromHistory);
            Assert.Equal(0, summary.Unresolved);
            Assert.Equal(1, summary.Records);
            Assert.Equal(0, restarted.ReplayHistoryIntentCount);
        }
    }

    [Fact]
    public void Restorer_MaterializingResolveInsideTheHistoryWindow_InstallsFromTheReplayedPrepare()
    {
        PreparedIntent committed = MakeIntent("acct/1", 1_000, revision: 9, value: [7]);

        PreparedIntentStore before = Persisted();
        ApplyLiveAndCheckpoint(before, certifiedThrough: 3, Prepare(1, committed), Resolve(2, committed, materializeOnResolve: true), Remove(3, committed));

        PreparedIntentStore restarted = Persisted();
        (KeyValueRestorer restorer, UnflushedKeyValueWritesIndex overlay, _, IDisposable lifetime) =
            KeyValueRestorerHarness.Build(out _, restarted);

        using (lifetime)
        {
            restarted.AttachResolvedIntentInstaller(new RestorerInstaller(restorer));

            Replay(restarted, Prepare(1, committed), Resolve(2, committed, materializeOnResolve: true), Remove(3, committed));

            Assert.True(overlay.TryGet("acct/1", out UnflushedKeyValueWrite installed));
            Assert.Equal(9, installed.Revision);
            Assert.Equal(new byte[] { 7 }, installed.Value);
            Assert.Equal(0, restarted.ReplayHistoryIntentCount);

            KeyValueRestorer.RestoreSummary summary = restorer.CompleteRestore(Partition);
            Assert.Equal(1, summary.FromHistory);
            Assert.Equal(0, summary.Unresolved);
        }
    }

    // ── restorer: no source anywhere ────────────────────────────────────────────

    [Fact]
    public void Restorer_RecordWithNoSource_IsDurableWhenTheBackendRowIsAtOrAboveIt()
    {
        PreparedIntent committed = MakeIntent("acct/1", 1_000, revision: 9, value: [1]);

        (KeyValueRestorer restorer, UnflushedKeyValueWritesIndex overlay, _, IDisposable lifetime) =
            KeyValueRestorerHarness.Build(out MemoryPersistenceBackend backend);

        using (lifetime)
        {
            // The row flushed before the crash at a later revision; the record is a duplicate of a durable copy.
            Assert.True(backend.StoreKeyValues([Row("acct/1", [2], 10, Ts(2_000))]));

            Dictionary<string, long> resolved = MeasureBySource(ResolvedCounter, () =>
                Assert.True(restorer.Restore(Partition, Record(2, committed))));
            Assert.True(resolved.GetValueOrDefault("durable") >= 1, "the durable source was expected on the resolved counter");

            Assert.False(overlay.TryGet("acct/1", out _));

            KeyValueRestorer.RestoreSummary summary = restorer.CompleteRestore(Partition);
            Assert.Equal(1, summary.Durable);
            Assert.Equal(0, summary.Unresolved);
        }
    }

    [Fact]
    public void Restorer_RecordWithNoSourceAndNoDurableRow_IsCountedAsUnresolved_WithTheRangeAndTheKeys()
    {
        PreparedIntent first = MakeIntent("acct/1", 1_000, revision: 9, value: [1]);
        PreparedIntent second = MakeIntent("acct/1", 1_020, revision: 10, value: [2]);
        PreparedIntent other = MakeIntent("acct/2", 1_040, revision: 3, value: [3]);

        (KeyValueRestorer restorer, UnflushedKeyValueWritesIndex overlay, _, IDisposable lifetime) =
            KeyValueRestorerHarness.Build(out MemoryPersistenceBackend backend);

        using (lifetime)
        {
            // A row below the record's revision proves nothing.
            Assert.True(backend.StoreKeyValues([Row("acct/1", [0], 8, Ts(500))]));

            Dictionary<string, long> unresolved = MeasureBySource(UnresolvedCounter, () =>
            {
                Assert.True(restorer.Restore(Partition, Record(20, first)));
                Assert.True(restorer.Restore(Partition, Record(25, second)));
                Assert.True(restorer.Restore(Partition, Record(30, other)));
            });

            Assert.True(unresolved.GetValueOrDefault("") >= 3, "the unresolved counter was expected to count every miss");
            Assert.False(overlay.TryGet("acct/1", out _));
            Assert.False(overlay.TryGet("acct/2", out _));

            KeyValueRestorer.RestoreSummary summary = restorer.CompleteRestore(Partition);
            Assert.Equal(3, summary.Unresolved);
            Assert.Equal(20, summary.FirstUnresolvedLogIndex);
            Assert.Equal(30, summary.LastUnresolvedLogIndex);
            Assert.Equal(2, summary.UnresolvedKeys);
            Assert.Equal(3, summary.Records);

            // The tally closes with the restore.
            Assert.Equal(default, restorer.CompleteRestore(Partition));
        }
    }

    [Fact]
    public void Restorer_RecordWithNoSource_ForAKeyThePartitionNoLongerOwns_IsNotUnresolved()
    {
        PreparedIntent moved = MakeIntent("moved/1", 1_000, revision: 9, value: [1]);
        PreparedIntent held = MakeIntent("held/1", 1_020, revision: 9, value: [2]);

        // The un-host purge took moved/1 with its range; held/1 is this partition's and genuinely missing.
        (KeyValueRestorer restorer, _, _, IDisposable lifetime) =
            KeyValueRestorerHarness.Build(out _, keyOwner: key => key.StartsWith("moved/", StringComparison.Ordinal) ? Partition + 1 : Partition);

        using (lifetime)
        {
            Assert.True(restorer.Restore(Partition, Record(20, moved)));
            Assert.True(restorer.Restore(Partition, Record(21, held)));

            KeyValueRestorer.RestoreSummary summary = restorer.CompleteRestore(Partition);
            Assert.Equal(1, summary.Foreign);
            Assert.Equal(1, summary.Unresolved);
            Assert.Equal(21, summary.FirstUnresolvedLogIndex);
            Assert.Equal(1, summary.UnresolvedKeys);
            Assert.Equal(2, summary.Records);
        }
    }

    [Fact]
    public void Restorer_RecordWithNoSource_SameRevisionDelete_IsNotCoveredByTheEarlierSet()
    {
        // A delete reuses the revision of the set it follows; a durable set at that revision does not hold it.
        PreparedIntent deleted = MakeIntent("acct/1", 1_020, revision: 9, value: null, KeyValueState.Deleted);

        (KeyValueRestorer restorer, _, _, IDisposable lifetime) = KeyValueRestorerHarness.Build(out MemoryPersistenceBackend backend);

        using (lifetime)
        {
            Assert.True(backend.StoreKeyValues([Row("acct/1", [1], 9, Ts(1_001))]));
            Assert.True(restorer.Restore(Partition, Record(20, deleted)));

            KeyValueRestorer.RestoreSummary summary = restorer.CompleteRestore(Partition);
            Assert.Equal(0, summary.Durable);
            Assert.Equal(1, summary.Unresolved);
        }
    }

    [Fact]
    public void Restorer_MaterializingResolveWithNoSource_IsDurableByCommitTimestamp_OrUnresolved()
    {
        PreparedIntent flushed = MakeIntent("acct/1", 1_000, revision: 9, value: [1]);
        PreparedIntent missing = MakeIntent("acct/2", 1_020, revision: 9, value: [2]);

        (KeyValueRestorer restorer, _, PreparedIntentStore intents, IDisposable lifetime) =
            KeyValueRestorerHarness.Build(out MemoryPersistenceBackend backend);

        using (lifetime)
        {
            intents.AttachResolvedIntentInstaller(new RestorerInstaller(restorer));

            // acct/1's row is durable at this commit's timestamp; acct/2's row predates its commit.
            Assert.True(backend.StoreKeyValues([Row("acct/1", [1], 9, flushed.CommitTimestamp), Row("acct/2", [0], 8, Ts(900))]));

            Dictionary<string, long> unresolved = MeasureBySource(UnresolvedCounter, () =>
            {
                Assert.True(intents.Restore(Partition, Resolve(2, flushed, materializeOnResolve: true)));
                Assert.True(intents.Restore(Partition, Resolve(3, missing, materializeOnResolve: true)));
            });

            Assert.True(unresolved.GetValueOrDefault("") >= 1, "the unresolved counter was expected to count the miss");

            KeyValueRestorer.RestoreSummary summary = restorer.CompleteRestore(Partition);
            Assert.Equal(1, summary.Durable);
            Assert.Equal(1, summary.Unresolved);
            Assert.Equal(3, summary.FirstUnresolvedLogIndex);
            Assert.Equal(1, summary.UnresolvedKeys);
        }
    }
}
