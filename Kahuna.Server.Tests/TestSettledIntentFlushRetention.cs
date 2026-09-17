using System.Diagnostics.Metrics;
using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// A by-reference materialization record carries no value: a restart replay applies it from the prepared intent
/// it names. The settle removes that intent from the live set, and the next intent-snapshot rewrite drops it
/// from disk, while the committed row the record materialized may still be queued for the background flush.
/// The durability floor certifies the prepare delta through the snapshot channel, so the replay window can start
/// above the prepare and at or below the record — and then the value is nowhere on the node. These tests pin
/// the repair: the store retains a settled committed intent whose row is not yet durable, persists it with the
/// snapshot outside the live set, and releases it once the row's flush is confirmed; the restorer resolves the
/// record from it; and a real node killed in that window serves the committed value after its restart.
/// </summary>
public sealed class TestSettledIntentFlushRetention : IDisposable
{
    private static HLCTimestamp Ts(long physical) => new(0, physical, 0);

    private const int PartitionId = 5;

    private const string MissCounter = "kahuna.kv.materialization_intent_missing";

    private static readonly string[] SingleAcctOneRelease = ["acct/1"];

    private readonly ILoggerFactory loggerFactory;

    private readonly string dir = Path.Combine(Path.GetTempPath(), "kahuna-settled-retention-" + Guid.NewGuid().ToString("N"));

    public TestSettledIntentFlushRetention(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
        Directory.CreateDirectory(dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best effort */ }
    }

    private static PreparedIntent Intent(string key, long revision, byte[]? value, long txn = 1_000, long epoch = 1, KeyValueState state = KeyValueState.Set) =>
        new(
            TransactionId: Ts(txn), Epoch: epoch, Key: key, ManifestHash: 42, RecordAnchorKey: "anchor",
            CommitTimestamp: Ts(txn + 234),
            State: state, Value: value, Bucket: null, Revision: revision, Expires: Ts(50_000),
            NoRevision: false, BaseRevision: revision - 1, BaseState: KeyValueState.Set,
            RecoveryDeadline: Ts(txn + 6_000), Resolution: PreparedIntentResolution.Pending);

    /// <summary>The unflushed overlay as the store sees it: the newest queued revision per key.</summary>
    private static Func<string, long, bool> ProbeOver(Dictionary<string, long> unflushed) =>
        (key, revision) => unflushed.TryGetValue(key, out long queued) && queued >= revision;

    private static void Settle(PreparedIntentStore store, PreparedIntent intent, bool commit)
    {
        store.Apply(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: commit));
        store.Apply(new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key));
    }

    // Sums the increments of a named counter on the "Kahuna" meter emitted while the action runs.
    private static async Task<long> MeasureCounter(string instrumentName, Func<Task> action)
    {
        long total = 0;
        using MeterListener listener = new();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "Kahuna" && instrument.Name == instrumentName)
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, measurement, _, _) => Interlocked.Add(ref total, measurement));
        listener.Start();

        await action();

        listener.Dispose();
        return Interlocked.Read(ref total);
    }

    // ── store: retention decision ───────────────────────────────────────────────

    [Fact]
    public void RemovedCommittedIntent_IsRetainedWhileItsRowIsUnflushed_AndReleasedByTheFlush()
    {
        Dictionary<string, long> unflushed = new(StringComparer.Ordinal);
        PreparedIntentStore store = new();
        store.AttachUnflushedRowProbe(ProbeOver(unflushed));

        PreparedIntent intent = Intent("acct/1", revision: 9, value: [1, 2, 3]);
        store.Apply(new PrepareIntentCommand(intent));

        // The materialization queued the row; the settle then removes the intent.
        unflushed["acct/1"] = 9;
        Settle(store, intent, commit: true);

        Assert.Null(store.Get("acct/1"));
        Assert.Equal(0, store.Count);
        Assert.Empty(store.Snapshot());
        Assert.Equal(1, store.SettledIntentsAwaitingFlushCount);

        Assert.True(store.TryGetSettledIntentAwaitingFlush(intent.TransactionId, intent.Epoch, "acct/1", out PreparedIntent? retained));
        Assert.Equal(9, retained!.Revision);
        Assert.Equal(new byte[] { 1, 2, 3 }, retained.Value);

        // A different attempt of the same key is not it.
        Assert.False(store.TryGetSettledIntentAwaitingFlush(intent.TransactionId, intent.Epoch + 1, "acct/1", out _));

        // The confirmed flush drops the key from the overlay and releases the retained intent.
        unflushed.Remove("acct/1");
        store.ReleaseSettledIntentsAwaitingFlush("acct/1");

        Assert.Equal(0, store.SettledIntentsAwaitingFlushCount);
        Assert.False(store.TryGetSettledIntentAwaitingFlush(intent.TransactionId, intent.Epoch, "acct/1", out _));
    }

    [Fact]
    public void RemovedCommittedIntent_WithADurableRow_IsNotRetained()
    {
        Dictionary<string, long> unflushed = new(StringComparer.Ordinal);
        PreparedIntentStore store = new();
        store.AttachUnflushedRowProbe(ProbeOver(unflushed));

        PreparedIntent intent = Intent("acct/2", revision: 4, value: [4]);
        store.Apply(new PrepareIntentCommand(intent));

        // The flush landed inside the materialize→settle gap: nothing is queued for the key any more.
        Settle(store, intent, commit: true);

        Assert.Equal(0, store.SettledIntentsAwaitingFlushCount);
    }

    [Fact]
    public void RemovedAbortedIntent_IsNeverRetained()
    {
        Dictionary<string, long> unflushed = new(StringComparer.Ordinal) { ["acct/3"] = 7 };
        PreparedIntentStore store = new();
        store.AttachUnflushedRowProbe(ProbeOver(unflushed));

        PreparedIntent intent = Intent("acct/3", revision: 7, value: [7]);
        store.Apply(new PrepareIntentCommand(intent));

        // An aborted intent materializes nothing; even an unflushed head at the key (another writer's) is not
        // this intent's row.
        Settle(store, intent, commit: false);

        Assert.Equal(0, store.SettledIntentsAwaitingFlushCount);
    }

    [Fact]
    public void WithoutAProbe_NothingIsRetained()
    {
        PreparedIntentStore store = new();

        PreparedIntent intent = Intent("acct/4", revision: 2, value: [2]);
        store.Apply(new PrepareIntentCommand(intent));
        Settle(store, intent, commit: true);

        Assert.Equal(0, store.SettledIntentsAwaitingFlushCount);
    }

    // ── store: snapshot round trip ──────────────────────────────────────────────

    [Fact]
    public void RetainedIntent_SurvivesTheSnapshotRewrite_OutsideTheLiveSet_UntilReleased()
    {
        Dictionary<string, long> unflushed = new(StringComparer.Ordinal);

        PreparedIntentStore store = new(dir, "rev", null);
        store.AttachPartitionResolver(_ => PartitionId);
        store.AttachUnflushedRowProbe(ProbeOver(unflushed));

        PreparedIntent first = Intent("acct/1", revision: 9, value: [9], txn: 1_000);
        PreparedIntent live = Intent("acct/2", revision: 3, value: [3], txn: 2_000);
        store.Apply(new PrepareIntentCommand(first));
        store.Apply(new PrepareIntentCommand(live));

        unflushed["acct/1"] = 9;
        Settle(store, first, commit: true);

        // The rewrite after the settle: the live set no longer holds acct/1, but the file must.
        Assert.True(store.PersistSnapshot(PartitionId));

        PreparedIntentStore reloaded = new(dir, "rev", null);
        reloaded.AttachPartitionResolver(_ => PartitionId);

        Assert.Null(reloaded.Get("acct/1"));
        Assert.NotNull(reloaded.Get("acct/2"));
        Assert.Equal(1, reloaded.Count);
        Assert.Equal(1, reloaded.SettledIntentsAwaitingFlushCount);
        Assert.True(reloaded.TryGetSettledIntentAwaitingFlush(first.TransactionId, first.Epoch, "acct/1", out PreparedIntent? retained));
        Assert.Equal(9, retained!.Revision);
        Assert.Equal(new byte[] { 9 }, retained.Value);
        Assert.Equal(KeyValueState.Set, retained.State);

        // The flush lands on the original node: the release re-dirties the partition and the next rewrite drops it.
        unflushed.Remove("acct/1");
        store.ReleaseSettledIntentsAwaitingFlush("acct/1");
        Assert.True(store.PersistSnapshot(PartitionId));

        PreparedIntentStore afterRelease = new(dir, "rev", null);
        Assert.Equal(0, afterRelease.SettledIntentsAwaitingFlushCount);
        Assert.Equal(1, afterRelease.Count);
    }

    [Fact]
    public void RetainedIntents_AreScopedToTheirPartitionSnapshot()
    {
        Dictionary<string, long> unflushed = new(StringComparer.Ordinal) { ["p1/k"] = 1, ["p2/k"] = 1 };

        PreparedIntentStore store = new(dir, "rev", null);
        store.AttachPartitionResolver(key => key.StartsWith("p1/", StringComparison.Ordinal) ? 1 : 2);
        store.AttachUnflushedRowProbe(ProbeOver(unflushed));

        PreparedIntent one = Intent("p1/k", revision: 1, value: [1], txn: 1_000);
        PreparedIntent two = Intent("p2/k", revision: 1, value: [2], txn: 2_000);
        store.Apply(new PrepareIntentCommand(one));
        store.Apply(new PrepareIntentCommand(two));
        Settle(store, one, commit: true);
        Settle(store, two, commit: true);

        // Only partition 1's file is written; partition 2's retained intent must not leak into it.
        Assert.True(store.PersistSnapshot(1));

        PreparedIntentStore reloaded = new(dir, "rev", null);
        Assert.Equal(1, reloaded.SettledIntentsAwaitingFlushCount);
        Assert.True(reloaded.TryGetSettledIntentAwaitingFlush(one.TransactionId, one.Epoch, "p1/k", out _));
        Assert.False(reloaded.TryGetSettledIntentAwaitingFlush(two.TransactionId, two.Epoch, "p2/k", out _));
    }

    // ── store: release after a restart replay ───────────────────────────────────

    [Fact]
    public void ReleaseAfterRestore_DropsEntriesWhoseRowIsDurable_AndKeepsRequeuedOnes()
    {
        Dictionary<string, long> unflushed = new(StringComparer.Ordinal) { ["acct/1"] = 9, ["acct/2"] = 5 };

        PreparedIntentStore store = new(dir, "rev", null);
        store.AttachPartitionResolver(_ => PartitionId);
        store.AttachUnflushedRowProbe(ProbeOver(unflushed));

        PreparedIntent requeued = Intent("acct/1", revision: 9, value: [9], txn: 1_000);
        PreparedIntent durable = Intent("acct/2", revision: 5, value: [5], txn: 2_000);
        store.Apply(new PrepareIntentCommand(requeued));
        store.Apply(new PrepareIntentCommand(durable));
        Settle(store, requeued, commit: true);
        Settle(store, durable, commit: true);
        Assert.True(store.PersistSnapshot(PartitionId));

        // The restarted node: its replay redelivered acct/1's record (row queued again) but started above
        // acct/2's (its row was durable before the crash).
        Dictionary<string, long> afterReplay = new(StringComparer.Ordinal) { ["acct/1"] = 9 };
        PreparedIntentStore reloaded = new(dir, "rev", null);
        reloaded.AttachPartitionResolver(_ => PartitionId);
        reloaded.AttachUnflushedRowProbe(ProbeOver(afterReplay));
        Assert.Equal(2, reloaded.SettledIntentsAwaitingFlushCount);

        // Another partition's restore completing does not touch this partition's entries.
        Assert.Equal(0, reloaded.ReleaseSettledIntentsWithDurableRows(PartitionId + 1));
        Assert.Equal(2, reloaded.SettledIntentsAwaitingFlushCount);

        Assert.Equal(1, reloaded.ReleaseSettledIntentsWithDurableRows(PartitionId));
        Assert.Equal(1, reloaded.SettledIntentsAwaitingFlushCount);
        Assert.True(reloaded.TryGetSettledIntentAwaitingFlush(requeued.TransactionId, requeued.Epoch, "acct/1", out _));
        Assert.False(reloaded.TryGetSettledIntentAwaitingFlush(durable.TransactionId, durable.Epoch, "acct/2", out _));

        // The release re-dirtied the partition: the next rewrite no longer carries the durable one.
        Assert.True(reloaded.PersistSnapshot(PartitionId));
        PreparedIntentStore rewritten = new(dir, "rev", null);
        Assert.Equal(1, rewritten.SettledIntentsAwaitingFlushCount);
    }

    [Fact]
    public void UnhostPurge_DropsRetainedIntentsOfTheRange()
    {
        Dictionary<string, long> unflushed = new(StringComparer.Ordinal) { ["gone/k"] = 1, ["kept/k"] = 1 };
        PreparedIntentStore store = new();
        store.AttachUnflushedRowProbe(ProbeOver(unflushed));

        PreparedIntent gone = Intent("gone/k", revision: 1, value: [1], txn: 1_000);
        PreparedIntent kept = Intent("kept/k", revision: 1, value: [2], txn: 2_000);
        store.Apply(new PrepareIntentCommand(gone));
        store.Apply(new PrepareIntentCommand(kept));
        Settle(store, gone, commit: true);
        Settle(store, kept, commit: true);

        store.PurgeWhere(key => key.StartsWith("gone/", StringComparison.Ordinal));

        Assert.Equal(1, store.SettledIntentsAwaitingFlushCount);
        Assert.False(store.TryGetSettledIntentAwaitingFlush(gone.TransactionId, gone.Epoch, "gone/k", out _));
        Assert.True(store.TryGetSettledIntentAwaitingFlush(kept.TransactionId, kept.Epoch, "kept/k", out _));
    }

    [Fact]
    public void OverlayRelease_ReachesTheStoreOnlyWhenTheKeyLeavesTheOverlay()
    {
        UnflushedKeyValueWritesIndex overlay = new();
        List<string> released = [];
        overlay.AttachReleaseObserver(released.Add);

        overlay.Record("acct/1", [1], revision: 9, Ts(1), Ts(1), Ts(9), KeyValueState.Set, noRevision: false);
        overlay.Record("acct/1", [2], revision: 10, Ts(1), Ts(1), Ts(10), KeyValueState.Set, noRevision: false);

        // The older head's flush lands while the newer one is still queued: the key stays covered, no release.
        overlay.RemoveFlushed("acct/1", flushedRevision: 9, flushedLastModified: Ts(9));
        Assert.Empty(released);
        Assert.True(overlay.TryGet("acct/1", out _));

        overlay.RemoveFlushed("acct/1", flushedRevision: 10, flushedLastModified: Ts(10));
        Assert.Equal(SingleAcctOneRelease, released);
        Assert.False(overlay.TryGet("acct/1", out _));

        // Nothing to remove, nothing to release.
        overlay.RemoveFlushed("acct/1", flushedRevision: 10, flushedLastModified: Ts(10));
        Assert.Single(released);
    }

    // ── restorer: the replay window starts between the prepare and the record ──

    private sealed class RestorerHarness : IDisposable
    {
        private readonly IDisposable lifetime;

        public readonly KeyValueRestorer Restorer;

        public readonly UnflushedKeyValueWritesIndex Overlay = new();

        public RestorerHarness(PreparedIntentStore intents)
        {
            lifetime = TestActorSystemLifetime.Create(out Nixie.ActorSystem actorSystem);

            UnflushedOverlayPersistenceBackend decorated = new(new MemoryPersistenceBackend(), Overlay, new UnflushedLockWritesIndex());

            RaftManager raft = new(
                new RaftConfiguration
                {
                    NodeName = "settled-restore", NodeId = 1, Host = "localhost", Port = 0,
                    InitialPartitions = 1, EnableQuiescence = false, PartitionExecutorPoolSize = 1
                },
                new Kommander.Discovery.StaticDiscovery([]),
                new InMemoryWAL(NullLogger<IRaft>.Instance),
                new Kommander.Communication.Memory.InMemoryCommunication(),
                new HybridLogicalClock(),
                NullLogger<IRaft>.Instance);

            Kahuna.Server.Configuration.KahunaConfiguration config =
                Kahuna.Server.Configuration.ConfigurationValidator.Validate(new()
                {
                    LocksWorkers = 1, KeyValueWorkers = 1, BackgroundWriterWorkers = 1, Storage = "memory",
                    CacheEntryTtl = TimeSpan.FromMinutes(5), CacheEntriesToRemove = 1000,
                    MaxEntriesPerActor = 50_000, MaxBytesPerActor = 256L * 1024 * 1024, CollectBatchMax = 1000,
                    RevisionRetention = 16, DirtyObjectsWriterDelay = 30_000
                });

            Nixie.IActorRef<BackgroundWriterActor, BackgroundWriteRequest> writer =
                actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
                    "settled-restore-bg", raft, raft.ReadScheduler, decorated,
                    null!, null!, new TransactionRecordStore(), intents,
                    config, NullLogger<IKahuna>.Instance, new FlushNotificationSink(), null!);

            Restorer = new(
                writer, raft, new CompletionReceiptStore(), NullLogger<IKahuna>.Instance,
                Overlay, durabilityTracker: null, preparedIntentStore: intents);
        }

        public void Dispose() => lifetime.Dispose();
    }

    private static RaftLog KvLog(long id, byte[] record) =>
        new() { Id = id, Type = RaftLogType.Committed, LogType = ReplicationTypes.KeyValues, LogData = [.. record] };

    [Fact]
    public async Task Restorer_ReplayStartingAfterThePrepare_ResolvesTheRecordFromTheRetainedSettledIntent()
    {
        // ── Before the crash: prepare, materialize (row queued), settle, snapshot rewrite. ──
        Dictionary<string, long> unflushed = new(StringComparer.Ordinal);
        PreparedIntentStore before = new(dir, "rev", null);
        before.AttachPartitionResolver(_ => PartitionId);
        before.AttachUnflushedRowProbe(ProbeOver(unflushed));

        PreparedIntent intent = Intent("acct/1", revision: 9, value: [4, 5, 6]);
        before.Apply(new PrepareIntentCommand(intent));
        unflushed["acct/1"] = 9;
        Settle(before, intent, commit: true);
        Assert.True(before.PersistSnapshot(PartitionId));

        // ── After the crash: the replay window starts at the record; the prepare is never replayed. ──
        PreparedIntentStore after = new(dir, "rev", null);
        after.AttachPartitionResolver(_ => PartitionId);
        Assert.Null(after.Get("acct/1"));

        using RestorerHarness harness = new(after);

        byte[] record = PreparedIntentMaterializer.ToKeyValueRecord(
            intent with { Resolution = PreparedIntentResolution.Committed }, new KeyValueMessage(), byReference: true);

        long missed = await MeasureCounter(MissCounter, () =>
        {
            Assert.True(harness.Restorer.Restore(PartitionId, KvLog(200, record)));

            // The settle replays after the record, over an intent that is no longer live: a no-op.
            Settle(after, intent, commit: true);
            return Task.CompletedTask;
        });

        Assert.Equal(0, missed);
        Assert.True(harness.Overlay.TryGet("acct/1", out UnflushedKeyValueWrite replayed));
        Assert.Equal(9, replayed.Revision);
        Assert.Equal(new byte[] { 4, 5, 6 }, replayed.Value);
        Assert.Equal(KeyValueState.Set, replayed.State);
        Assert.Equal(Ts(1_234), replayed.LastModified);

        // The replay re-queued the row, so the retained intent stays until that flush is confirmed.
        after.AttachUnflushedRowProbe((key, revision) => harness.Overlay.TryGet(key, out UnflushedKeyValueWrite w) && w.Revision >= revision);
        Assert.Equal(0, after.ReleaseSettledIntentsWithDurableRows(PartitionId));
        Assert.Equal(1, after.SettledIntentsAwaitingFlushCount);
    }

    [Fact]
    public async Task Restorer_RetainedIntentAtADifferentRevision_StillRefusesTheApply()
    {
        Dictionary<string, long> unflushed = new(StringComparer.Ordinal) { ["acct/1"] = 11 };
        PreparedIntentStore store = new();
        store.AttachUnflushedRowProbe(ProbeOver(unflushed));

        PreparedIntent intent = Intent("acct/1", revision: 9, value: [4, 5, 6]);
        store.Apply(new PrepareIntentCommand(intent with { Revision = 11, Value = [9] }));
        Settle(store, intent, commit: true);
        Assert.Equal(1, store.SettledIntentsAwaitingFlushCount);

        using RestorerHarness harness = new(store);

        byte[] record = PreparedIntentMaterializer.ToKeyValueRecord(intent, new KeyValueMessage(), byReference: true);

        long missed = await MeasureCounter(MissCounter, () =>
        {
            Assert.True(harness.Restorer.Restore(PartitionId, KvLog(10, record)));
            return Task.CompletedTask;
        });

        Assert.Equal(1, missed);
        Assert.False(harness.Overlay.TryGet("acct/1", out _));
    }

    // ── end to end: kill inside the window, restart, read ───────────────────────

    /// <summary>
    /// Forwards everything to the real backend, but fails every key-value flush batch while armed. The
    /// background writer retains the batch and reports the flush failed; the durability floors and the store
    /// snapshots still advance — the exact node state the incident's restart replayed from.
    /// </summary>
    private sealed class FlushFailingBackend(IPersistenceBackend inner) : IPersistenceBackend, IDisposable
    {
        public volatile bool FailKeyValueFlushes;

        public int FailedBatches;

        public bool StoreKeyValues(List<PersistenceRequestItem> items)
        {
            if (FailKeyValueFlushes)
            {
                Interlocked.Increment(ref FailedBatches);
                return false;
            }

            return inner.StoreKeyValues(items);
        }

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreDurabilityFloors(IReadOnlyList<(int PartitionId, long Floor)> floors) => inner.StoreDurabilityFloors(floors);
        public long GetDurabilityFloor(int partitionId) => inner.GetDurabilityFloor(partitionId);
        public bool RemoveDurabilityFloor(int partitionId) => inner.RemoveDurabilityFloor(partitionId);
        public bool TryRecoverFromStorageFailure() => false;
        public LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public KeyValueEntry?[] GetKeyValues(string[] keyNames) => inner.GetKeyValues(keyNames);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public KeyValueHydration GetKeyValueWithRecentRevisions(string keyName, int recentRevisions) =>
            inner.GetKeyValueWithRecentRevisions(keyName, recentRevisions);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string Key, ReadOnlyKeyValueEntry Current, ReadOnlyKeyValueEntry? Snapshot)> GetKeyValueByPrefixAtOrBefore(
            string prefixKeyName, HLCTimestamp readTimestamp, Func<bool>? shouldAbort = null) =>
            inner.GetKeyValueByPrefixAtOrBefore(prefixKeyName, readTimestamp, shouldAbort);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            inner.GetKeyValueByRange(prefix, startKey, limit);
        public KeyValueScanPage ScanKeyValues(string? cursor, int limit) => inner.ScanKeyValues(cursor, limit);
        public LockScanPage ScanLocks(string? cursor, int limit) => inner.ScanLocks(cursor, limit);
        public bool DeleteKeyValues(IReadOnlyList<string> keys) => inner.DeleteKeyValues(keys);
        public bool DeleteLocks(IReadOnlyList<string> resources) => inner.DeleteLocks(resources);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge,
            int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge,
            int batchSize, HLCTimestamp floorTimestamp, TimeSpan timeBudget, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, timeBudget, out result);
        public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public CheckpointResult CreateCheckpointAsOf(string destinationPath, long appliedIndex, HLCTimestamp cut, CancellationToken ct = default) =>
            inner.CreateCheckpointAsOf(destinationPath, appliedIndex, cut, ct);
        public bool SupportsExactAsOfCheckpoint => inner.SupportsExactAsOfCheckpoint;
        public HLCTimestamp GetPrunedHistoryFloor() => inner.GetPrunedHistoryFloor();

        public void Dispose()
        {
            if (inner is IDisposable disposable)
                disposable.Dispose();
        }
    }

    private static EmbeddedKahunaOptions PersistentOptions(string storagePath, string walPath, Func<IPersistenceBackend, IPersistenceBackend>? decorator) => new()
    {
        InitialPartitions = 1,
        Storage = "sqlite",
        StoragePath = storagePath,
        StorageRevision = "settled-retention",
        WalStorage = "sqlite",
        WalPath = walPath,
        WalRevision = "settled-retention-wal",
        WalSyncWrites = true,
        DurableMaterializeByReference = true,
        // No periodic flush: every flush is explicit, so the state at the kill is exactly "replicated and
        // settled, row never in the backend".
        DirtyObjectsWriterDelay = 600_000,
        CollectionInterval = TimeSpan.FromMinutes(10),
        PersistenceBackendDecorator = decorator
    };

    /// <summary>
    /// The incident's shape on one node. A by-reference transaction commits and settles; the row's flush is
    /// refused, so the explicit flush leaves the row queued while it rewrites the intent snapshot (without the
    /// live intent) and persists a durability floor above the prepare delta. The node is killed. The restart's
    /// replay starts above the prepare and redelivers the record, which must resolve its value from the
    /// retained settled intent: the committed values are readable, no miss is reported, and once the row
    /// finally flushes the retained intents are released.
    /// </summary>
    [Fact]
    public async Task Restart_ReplayStartingBetweenPrepareAndMaterialize_ServesTheCommittedValues()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string storagePath = CreateTempDir("kahuna-settled-store-");
        string walPath = CreateTempDir("kahuna-settled-wal-");

        const string k1 = "settled/row-1";
        const string k2 = "settled/row-2";

        try
        {
            long missed = await MeasureCounter(MissCounter, async () =>
            {
                // ── Phase 1: commit by reference, let the settle land, fail the flush, kill. ──
                {
                    FlushFailingBackend? blocker = null;
                    await using EmbeddedKahunaNode node = new(
                        PersistentOptions(storagePath, walPath, inner => blocker = new FlushFailingBackend(inner)), loggerFactory);
                    await node.StartAsync(ct);
                    await node.WaitForLeaderForKeyAsync(k1, ct);

                    KahunaManager kahuna = (KahunaManager)node.Kahuna;
                    PreparedIntentStore intents = kahuna.DurablePreparedIntentStore;

                    blocker!.FailKeyValueFlushes = true;

                    KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
                        Encoding.UTF8.GetBytes($"BEGIN SET `{k1}` 'alpha' SET `{k2}` 'beta' COMMIT END"), null, null);
                    Assert.Equal(KeyValueResponseType.Set, result.Type);

                    // The (deferred) settlement removes both intents from the live set.
                    await WaitUntil(() => intents.Get(k1) is null && intents.Get(k2) is null, ct);

                    // Their rows are queued but not durable, so both are retained.
                    Assert.Equal(2, intents.SettledIntentsAwaitingFlushCount);

                    // The explicit flush cannot land the rows, but it still rewrites the store snapshots and
                    // persists the durability floors — above the prepare, below the records.
                    await Assert.ThrowsAsync<IOException>(() => node.FlushAsync());
                    Assert.True(blocker.FailedBatches > 0, "the key-value flush was expected to be refused");
                    Assert.True(kahuna.DurabilityProvider.GetDurablyAppliedIndex(1) > 0, "the flush cycle was expected to persist a durability floor");

                    // Disposing without a successful flush discards the queued rows — a kill.
                }

                // ── Phase 2: restart over the same durable state with a healthy backend. ──
                {
                    await using EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath, decorator: null), loggerFactory);
                    await node.StartAsync(ct);
                    await node.WaitForLeaderForKeyAsync(k1, ct);

                    Assert.Equal("alpha", await ReadAsync(node, k1, ct));
                    Assert.Equal("beta", await ReadAsync(node, k2, ct));

                    KahunaManager kahuna = (KahunaManager)node.Kahuna;

                    // The replay re-queued the rows; a real flush lands them and releases the retained intents.
                    await node.FlushAsync();
                    Assert.Equal(0, kahuna.DurablePreparedIntentStore.SettledIntentsAwaitingFlushCount);
                }
            });

            Assert.Equal(0, missed);
        }
        finally
        {
            TryDeleteDir(storagePath);
            TryDeleteDir(walPath);
        }
    }

    private static async Task<string> ReadAsync(EmbeddedKahunaNode node, string key, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 30_000;
        KeyValueResponseType type = KeyValueResponseType.MustRetry;

        while (Environment.TickCount64 < deadline)
        {
            (type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

            if (type == KeyValueResponseType.Get)
                return Encoding.UTF8.GetString(entry!.Value!);

            if (type != KeyValueResponseType.MustRetry)
                break;

            await Task.Delay(100, ct);
        }

        throw new Xunit.Sdk.XunitException($"key {key} did not read back after the restart: {type}");
    }

    private static async Task WaitUntil(Func<bool> condition, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 30_000;

        while (!condition())
        {
            if (Environment.TickCount64 >= deadline)
                throw new TimeoutException("condition not met within 30 s");

            await Task.Delay(50, ct);
        }
    }

    private static string CreateTempDir(string prefix)
    {
        string path = Path.Combine(Path.GetTempPath(), prefix + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDeleteDir(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, recursive: true); }
        catch { /* best-effort cleanup */ }
    }
}
