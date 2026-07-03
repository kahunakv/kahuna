
using System.Text;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for the persistent bucket scan (GetByBucket / TryGetByBucketHandler) covering the
/// detach-from-actor-mailbox path. Stage 1 is the in-memory scan; stage 2 runs
/// GetKeyValueByPrefix off-actor; stage 3 merges the disk page against the current store.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestTryGetByBucketHandler
{
    // ── Stage-1 shortcut: all matches resident in memory, no disk needed ────────────────

    [Fact]
    public async Task InMemoryBucketScan_EphemeralDurability_ReturnsMatchesWithoutDiskRead()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("bucket-memory");

        scheduler.Start();
        try
        {
            // Backend that panics if any disk read is attempted — proves the ephemeral
            // path stays entirely in memory.
            NeverReadBackend backend = new();

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "bucket-mem-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            // Pre-populate two in-memory ephemeral keys under the "usr" bucket.
            await actorRef.Ask(MakeSet("usr/alice", Encoding.UTF8.GetBytes("alice-val"), 1), TimeSpan.FromSeconds(5));
            await actorRef.Ask(MakeSet("usr/bob", Encoding.UTF8.GetBytes("bob-val"), 2), TimeSpan.FromSeconds(5));

            // Ephemeral scan — never touches disk.
            KeyValueResponse? resp = await actorRef.Ask(
                MakeBucketScan("usr", KeyValueDurability.Ephemeral), TimeSpan.FromSeconds(5));

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.Items);
            Assert.Equal(2, resp.Items!.Count);
        }
        finally { scheduler.Stop(); }
    }

    // ── Stage 2+3: disk-only entries served via detach path ──────────────────────────────

    [Fact]
    public async Task PersistentBucketScan_DiskOnlyEntries_DetachPathResolvesGet()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("bucket-disk");

        scheduler.Start();
        try
        {
            // Disk has two entries under "doc/" prefix; memory store is empty.
            PrefixBackend backend = new([
                ("doc/a", Encoding.UTF8.GetBytes("aaa"), 1L, KeyValueState.Set),
                ("doc/b", Encoding.UTF8.GetBytes("bbb"), 2L, KeyValueState.Set),
            ]);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "bucket-disk-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(MakeBucketScan("doc"), TimeSpan.FromSeconds(5));

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.Items);
            Assert.Equal(2, resp.Items!.Count);
            // Items must be lexicographically ordered.
            Assert.Equal("doc/a", resp.Items[0].Item1);
            Assert.Equal("doc/b", resp.Items[1].Item1);
        }
        finally { scheduler.Stop(); }
    }

    // ── Stage 3 merge: write lands during detach, resident wins when revision is >= disk ──

    [Fact]
    public async Task PersistentBucketScan_WriteLandsDuringDetach_ResidentWinsOnEqualRevision()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("bucket-merge");

        scheduler.Start();
        try
        {
            ManualResetEventSlim gate = new(false);
            ManualResetEventSlim entered = new(false);

            // Disk returns revision 0 for "cfg/k" (old, pre-write version).
            // The in-memory TrySet that lands during stage 2 assigns revision 0 (first write),
            // so resident.Revision (0) >= disk.Revision (0) → stage 3 prefers the resident.
            BlockingPrefixBackend backend = new(gate, entered, [
                ("cfg/k", Encoding.UTF8.GetBytes("disk-old"), 0L, KeyValueState.Set),
            ]);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "bucket-merge-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            // Start the scan (will block in stage 2 until gate opens).
            Task<KeyValueResponse?> scanTask = actorRef.Ask(MakeBucketScan("cfg"), TimeSpan.FromSeconds(10));

            // Wait until the disk read has started, then write a new value for "cfg/k".
            // The key is not in memory before the scan (stage 1 finds nothing), so this
            // write lands between stage 1 and stage 3 — exactly the concurrent-write scenario.
            entered.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await actorRef.Ask(MakeSet("cfg/k", Encoding.UTF8.GetBytes("in-mem-new"), 0), TimeSpan.FromSeconds(5));

            // Ephemeral TryGet as sentinel: drains the actor queue so the set above has
            // been processed before we release the gate and stage 3 runs.
            await actorRef.Ask(MakeEphemeralGet("sentinel"), TimeSpan.FromSeconds(5));
            gate.Set();

            KeyValueResponse? resp = await scanTask;

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.Items);
            Assert.Single(resp.Items!);
            // Stage 3 must return the resident (in-memory) value: resident.Revision (0) >=
            // disk.Revision (0) means the resident entry wins, so "disk-old" is never served.
            Assert.Equal("in-mem-new", Encoding.UTF8.GetString(resp.Items[0].Item2.Value!));
        }
        finally { scheduler.Stop(); }
    }

    // ── Deleted disk entry is excluded from the result ───────────────────────────────────

    [Fact]
    public async Task PersistentBucketScan_DeletedDiskEntry_Excluded()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("bucket-deleted");

        scheduler.Start();
        try
        {
            PrefixBackend backend = new([
                ("data/x", Encoding.UTF8.GetBytes("x-val"), 1L, KeyValueState.Set),
                ("data/y", null, 2L, KeyValueState.Deleted),
            ]);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "bucket-deleted-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(MakeBucketScan("data"), TimeSpan.FromSeconds(5));

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.Items);
            // Tombstone must be excluded.
            Assert.Single(resp.Items!);
            Assert.Equal("data/x", resp.Items[0].Item1);
        }
        finally { scheduler.Stop(); }
    }

    // ── Coalescing must not bleed across read shapes ─────────────────────────────────────

    /// <summary>
    /// Two concurrent bucket scans of the same prefix — one latest, one snapshot — must each
    /// produce the result appropriate to their own read context. The snapshot scan must not
    /// coalesce onto the latest scan's continuation and silently receive current data.
    ///
    /// Mechanism: the backend blocks until BOTH disk reads have started, proving that two
    /// independent disk reads were dispatched (coalescing would produce only one).
    /// The entry's LastModified is set after the snapshot's readTimestamp, so the snapshot
    /// scan excludes it (no revision history available), while the latest scan includes it.
    /// </summary>
    [Fact]
    public async Task ConcurrentLatestAndSnapshotBucketScans_SamePrefix_ResolveIndependently()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("bucket-snapshot-isolation");

        scheduler.Start();
        try
        {
            // readTimestamp = 1; entry's LastModified = 1000 (well after the snapshot).
            // Latest scan should include it; snapshot scan should exclude it (no revision history).
            HLCTimestamp readTs = new(0, 1, 0);
            HLCTimestamp lastModified = new(0, 1000, 0);

            TwoReadBlockingPrefixBackend backend = new([
                ("ord/x", Encoding.UTF8.GetBytes("current"), 1L, KeyValueState.Set, lastModified),
            ]);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "bucket-snap-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            // Dispatch both scans before either completes; they will race to start their disk reads.
            Task<KeyValueResponse?> latestTask = actorRef.Ask(MakeBucketScan("ord"), TimeSpan.FromSeconds(10));
            Task<KeyValueResponse?> snapshotTask = actorRef.Ask(MakeSnapshotBucketScan("ord", readTs), TimeSpan.FromSeconds(10));

            // Wait for both disk reads to enter the backend — if coalescing occurred only
            // one disk read would start, and this wait would time out.
            backend.BothEntered.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            backend.Gate.Set();

            KeyValueResponse? latest = await latestTask;
            KeyValueResponse? snapshot = await snapshotTask;

            // Latest scan sees the entry.
            Assert.NotNull(latest);
            Assert.Equal(KeyValueResponseType.Get, latest!.Type);
            Assert.Single(latest.Items!);
            Assert.Equal("ord/x", latest.Items![0].Item1);

            // Snapshot scan excludes the entry (LastModified > readTs, no on-disk revision history).
            Assert.NotNull(snapshot);
            Assert.Equal(KeyValueResponseType.Get, snapshot!.Type);
            Assert.Empty(snapshot.Items!);
        }
        finally { scheduler.Stop(); }
    }

    /// <summary>
    /// A transactional bucket scan must not coalesce onto a concurrent plain scan's continuation.
    /// Coalescing would cause the transactional scan to evaluate entries under txId=0 instead of
    /// its own txId, skipping MVCC-entry creation and breaking read-your-writes / conflict detection.
    ///
    /// Verified by counting disk reads: plain and transactional scans of the same prefix must each
    /// produce an independent disk read (2 total), not share one.
    /// </summary>
    [Fact]
    public async Task ConcurrentPlainAndTransactionalBucketScans_SamePrefix_ResolveIndependently()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("bucket-txn-isolation");

        scheduler.Start();
        try
        {
            HLCTimestamp txId = new(0, 42, 0);

            TwoReadBlockingPrefixBackend backend = new([
                ("inv/a", Encoding.UTF8.GetBytes("item-a"), 1L, KeyValueState.Set, HLCTimestamp.Zero),
            ]);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "bucket-txn-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            // Plain scan (coalesceable) then transactional scan (not coalesceable — must stay private).
            Task<KeyValueResponse?> plainTask = actorRef.Ask(MakeBucketScan("inv"), TimeSpan.FromSeconds(10));
            Task<KeyValueResponse?> txnTask = actorRef.Ask(MakeTransactionalBucketScan("inv", txId), TimeSpan.FromSeconds(10));

            // Two independent disk reads must start — coalescing would prevent the second.
            backend.BothEntered.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            backend.Gate.Set();

            KeyValueResponse? plain = await plainTask;
            KeyValueResponse? txn = await txnTask;

            // Both scans must complete successfully and return the entry independently.
            Assert.NotNull(plain);
            Assert.Equal(KeyValueResponseType.Get, plain!.Type);
            Assert.Single(plain.Items!);

            Assert.NotNull(txn);
            Assert.Equal(KeyValueResponseType.Get, txn!.Type);
            Assert.Single(txn.Items!);
        }
        finally { scheduler.Stop(); }
    }

    // ── Snapshot bucket scan: disk-fallback when in-memory archive is trimmed ────────────

    /// <summary>
    /// A snapshot <c>GetByBucket</c> at timestamp T returns a key whose current disk revision
    /// was created after T, but whose revision at T is found via
    /// <c>GetKeyValueRevisionAtOrBefore</c> in the off-actor stage-2 task.
    ///
    /// Also verifies that a key created after T (no history at T) is correctly excluded.
    /// </summary>
    [Fact]
    public async Task SnapshotBucketScan_DiskHistoryFallback_ReturnsAsOfRevisionAndExcludesPostSnapshotKey()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("bucket-snapshot-disk-fallback");

        scheduler.Start();
        try
        {
            HLCTimestamp readTs = new(0, 500, 0);

            // k1: current disk revision has LastModified = 1000 > readTs.
            //     GetKeyValueRevisionAtOrBefore should find the revision at T=100 <= readTs.
            HLCTimestamp k1CurrentTs = new(0, 1000, 0);
            HLCTimestamp k1AsOfTs    = new(0, 100, 0);

            // k2: current disk revision has LastModified = 2000 > readTs, and no history at T.
            //     Should be excluded.
            HLCTimestamp k2CurrentTs = new(0, 2000, 0);

            SnapshotRevisionBackend backend = new(
                prefix: "db",
                currentRows:
                [
                    ("db/k1", "v1-current"u8.ToArray(), 5L, k1CurrentTs, KeyValueState.Set),
                    ("db/k2", "v2-current"u8.ToArray(), 3L, k2CurrentTs, KeyValueState.Set),
                ],
                revisionAtOrBefore: new Dictionary<string, (long Revision, HLCTimestamp Ts, byte[] Value)>
                {
                    ["db/k1"] = (1L, k1AsOfTs, "v1-asat"u8.ToArray())
                    // db/k2 has no history at readTs
                });

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "bucket-snap-disk-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeSnapshotBucketScan("db", readTs), TimeSpan.FromSeconds(5));

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.Items);

            // Only db/k1 at revision 1 (as-of T) should be returned.
            Assert.Single(resp.Items!);
            Assert.Equal("db/k1", resp.Items[0].Item1);
            Assert.Equal(1L, resp.Items[0].Item2.Revision);
            Assert.Equal("v1-asat", Encoding.UTF8.GetString(resp.Items[0].Item2.Value!));
        }
        finally { scheduler.Stop(); }
    }

    /// <summary>
    /// A snapshot bucket scan does not block the actor mailbox during the off-actor stage-2
    /// disk read. An ephemeral get dispatched while stage 2 is in flight is processed before
    /// stage 3 runs, proving the mailbox remained free.
    /// </summary>
    [Fact]
    public async Task SnapshotBucketScan_Stage2RunsOffActor_MailboxFreeForOtherMessages()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("bucket-snapshot-mailbox");

        scheduler.Start();
        try
        {
            HLCTimestamp readTs      = new(0, 500, 0);
            HLCTimestamp currentTs   = new(0, 1000, 0);

            ManualResetEventSlim gate    = new(false);
            ManualResetEventSlim entered = new(false);

            SnapshotRevisionBackend backend = new(
                prefix: "mail",
                currentRows: [("mail/a", "val"u8.ToArray(), 2L, currentTs, KeyValueState.Set)],
                revisionAtOrBefore: new Dictionary<string, (long, HLCTimestamp, byte[])>
                {
                    ["mail/a"] = (1L, new HLCTimestamp(0, 100, 0), "val-old"u8.ToArray())
                },
                gate: gate,
                gateEntered: entered);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "bucket-mailbox-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            // Dispatch the snapshot scan — stage 2 will block in GetKeyValueByPrefix.
            Task<KeyValueResponse?> scanTask = actorRef.Ask(
                MakeSnapshotBucketScan("mail", readTs), TimeSpan.FromSeconds(10));

            // Wait until stage 2 has started its disk read.
            entered.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            // Send an unrelated ephemeral get and drain it — proves the mailbox is not blocked.
            KeyValueResponse? sentinel = await actorRef.Ask(
                MakeEphemeralGet("sentinel"), TimeSpan.FromSeconds(5));
            Assert.NotNull(sentinel);

            // Release stage 2.
            gate.Set();

            KeyValueResponse? resp = await scanTask;

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.Single(resp.Items!);
            Assert.Equal("mail/a", resp.Items![0].Item1);
        }
        finally { scheduler.Stop(); }
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private static KeyValueRequest MakeEphemeralGet(string key)
    {
        return new KeyValueRequest(
            KeyValueRequestType.TryGet,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            key,
            null, null, -1,
            KeyValueFlags.None, 0,
            HLCTimestamp.Zero,
            KeyValueDurability.Ephemeral, 0, 0, null);
    }

    // ── Back-pressure on the scan enqueue surfaces as MustRetry and leaks no in-flight entry ─

    [Fact]
    public async Task PersistentBucketScan_EnqueueRejectedByBackpressure_ReturnsMustRetry_NoLeak()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("bucket-backpressure");

        scheduler.Start();
        try
        {
            // Disk-only entries, empty memory store, so the scan takes the detach path and dispatches
            // a backend read — which the scheduler rejects with back-pressure.
            PrefixBackend backend = new([
                ("doc/a", Encoding.UTF8.GetBytes("aaa"), 1L, KeyValueState.Set),
                ("doc/b", Encoding.UTF8.GetBytes("bbb"), 2L, KeyValueState.Set),
            ]);

            RecordingReadScheduler rejecting = new(rejectWithBackpressure: true);
            SchedulerOverridingRaft decoratedRaft = new(raft, rejecting);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "bucket-backpressure-actor", null!, null!, backend, decoratedRaft,
                    new KeySpaceRegistry(), new RangeMapStore(decoratedRaft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(MakeBucketScan("doc"), TimeSpan.FromSeconds(5));

            // Rejected enqueue must surface as retryable, not a faulted actor or a lost promise.
            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.MustRetry, resp!.Type);

            // The catch path must remove the in-flight registration it added in stage 1, so no dead
            // continuation is left for later arrivals to coalesce onto.
            Assert.Equal(0, ((KeyValueActor)actorRef.Runner.Actor!).PendingReadsCount);
        }
        finally { scheduler.Stop(); }
    }

    private static KeyValueRequest MakeBucketScan(
        string prefix,
        KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        return new KeyValueRequest(
            KeyValueRequestType.GetByBucket,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            prefix,
            null, null, -1,
            KeyValueFlags.None, 0,
            HLCTimestamp.Zero,
            durability, 0, 0, null);
    }

    private static KeyValueRequest MakeSnapshotBucketScan(string prefix, HLCTimestamp readTimestamp)
    {
        return new KeyValueRequest(
            KeyValueRequestType.GetByBucket,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            prefix,
            null, null, -1,
            KeyValueFlags.None, 0,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent, 0, 0, null)
        { ReadTimestamp = readTimestamp };
    }

    private static KeyValueRequest MakeTransactionalBucketScan(string prefix, HLCTimestamp transactionId)
    {
        return new KeyValueRequest(
            KeyValueRequestType.GetByBucket,
            transactionId,
            HLCTimestamp.Zero,
            prefix,
            null, null, -1,
            KeyValueFlags.None, 0,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent, 0, 0, null);
    }

    private static KeyValueRequest MakeSet(string key, byte[] value, long revision)
    {
        return new KeyValueRequest(
            KeyValueRequestType.TrySet,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            key,
            value,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            KeyValueDurability.Ephemeral,
            0,
            0,
            null
        );
    }

    private static (RaftManager Raft, FairReadScheduler Scheduler, KahunaConfiguration Config, ILogger<IKahuna> Logger)
        CreateRaftAndConfig(string nodeName)
    {
        KahunaConfiguration config = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            CacheEntryTtl = TimeSpan.FromMinutes(5),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = 50_000,
            MaxBytesPerActor = 256L * 1024 * 1024,
            CollectBatchMax = 1000,
            RevisionRetention = 16
        });

        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = nodeName,
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger
        );

        return (raft, (FairReadScheduler)raft.ReadScheduler, config, logger);
    }

    // ── inner backends ───────────────────────────────────────────────────────────────────

    /// <summary>Backend that throws if any disk read is attempted — proves stage-1 sufficiency.</summary>
    private sealed class NeverReadBackend : IPersistenceBackend, IDisposable
    {
        private readonly MemoryPersistenceBackend inner = new();

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValue(string keyName) => throw new InvalidOperationException("disk read not expected");
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => throw new InvalidOperationException("disk read not expected");
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) => inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => throw new InvalidOperationException("disk read not expected");
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) => inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) => inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) => inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose() => inner.Dispose();
    }

    /// <summary>Backend that returns a fixed list of prefix entries from "disk".</summary>
    private sealed class PrefixBackend : IPersistenceBackend, IDisposable
    {
        private readonly MemoryPersistenceBackend inner = new();
        private readonly List<(string Key, ReadOnlyKeyValueEntry Entry)> diskEntries;

        internal PrefixBackend(IEnumerable<(string Key, byte[]? Value, long Revision, KeyValueState State)> entries)
        {
            diskEntries = entries.Select(e => (e.Key, new ReadOnlyKeyValueEntry(
                e.Value, e.Revision,
                HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, e.State))).ToList();
        }

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) => inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => diskEntries.Where(e => e.Key.StartsWith(prefixKeyName, StringComparison.Ordinal)).Select(e => (e.Key, e.Entry)).ToList();
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) => inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) => inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) => inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose() => inner.Dispose();
    }

    /// <summary>
    /// Backend that blocks until two concurrent GetKeyValueByPrefix calls have both entered,
    /// then releases both. Used to verify that two independent disk reads were dispatched
    /// instead of one coalesced read.
    /// </summary>
    private sealed class TwoReadBlockingPrefixBackend : IPersistenceBackend, IDisposable
    {
        private readonly MemoryPersistenceBackend inner = new();
        private readonly List<(string Key, ReadOnlyKeyValueEntry Entry)> diskEntries;
        private int enterCount;

        internal ManualResetEventSlim BothEntered { get; } = new(false);
        internal ManualResetEventSlim Gate { get; } = new(false);

        internal TwoReadBlockingPrefixBackend(
            IEnumerable<(string Key, byte[]? Value, long Revision, KeyValueState State, HLCTimestamp LastModified)> entries)
        {
            diskEntries = entries.Select(e => (e.Key, new ReadOnlyKeyValueEntry(
                e.Value, e.Revision,
                HLCTimestamp.Zero, HLCTimestamp.Zero, e.LastModified, e.State))).ToList();
        }

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) => inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName)
        {
            if (Interlocked.Increment(ref enterCount) >= 2)
                BothEntered.Set();
            Gate.Wait();
            return diskEntries
                .Where(e => e.Key.StartsWith(prefixKeyName, StringComparison.Ordinal))
                .Select(e => (e.Key, e.Entry)).ToList();
        }

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) => inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) => inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) => inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose()
        {
            inner.Dispose();
            BothEntered.Dispose();
            Gate.Dispose();
        }
    }

    /// <summary>
    /// Backend with fixed current disk rows and a per-key revision-at-or-before lookup table.
    /// Supports a gate to block the prefix read and verify the actor mailbox stays free.
    /// </summary>
    private sealed class SnapshotRevisionBackend : IPersistenceBackend, IDisposable
    {
        private readonly MemoryPersistenceBackend inner = new();
        private readonly string prefix;
        private readonly List<(string Key, ReadOnlyKeyValueEntry Entry)> currentRows;
        private readonly Dictionary<string, (long Revision, HLCTimestamp Ts, byte[] Value)> revisionAtOrBefore;
        private readonly ManualResetEventSlim? gate;
        private readonly ManualResetEventSlim? gateEntered;

        internal SnapshotRevisionBackend(
            string prefix,
            IEnumerable<(string Key, byte[] Value, long Revision, HLCTimestamp LastModified, KeyValueState State)> currentRows,
            Dictionary<string, (long Revision, HLCTimestamp Ts, byte[] Value)> revisionAtOrBefore,
            ManualResetEventSlim? gate = null,
            ManualResetEventSlim? gateEntered = null)
        {
            this.prefix = prefix;
            this.currentRows = currentRows.Select(e => (e.Key, new ReadOnlyKeyValueEntry(
                e.Value, e.Revision, HLCTimestamp.Zero, HLCTimestamp.Zero, e.LastModified, e.State))).ToList();
            this.revisionAtOrBefore = revisionAtOrBefore;
            this.gate = gate;
            this.gateEntered = gateEntered;
        }

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) => inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string dest, long idx, HLCTimestamp ts) => inner.CreateCheckpoint(dest, idx, ts);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string p, string? s, int l) => inner.GetKeyValueByRange(p, s, l);

        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp)
        {
            if (!revisionAtOrBefore.TryGetValue(keyName, out (long Revision, HLCTimestamp Ts, byte[] Value) hit))
                return null;
            if (hit.Revision > maxRevision || hit.Ts.CompareTo(readTimestamp) > 0)
                return null;
            return new KeyValueEntry
            {
                Value        = hit.Value,
                Revision     = hit.Revision,
                LastModified = hit.Ts,
                State        = KeyValueState.Set
            };
        }

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName)
        {
            gateEntered?.Set();
            gate?.Wait();
            return currentRows.Where(e => e.Key.StartsWith(prefixKeyName, StringComparison.Ordinal))
                              .Select(e => (e.Key, e.Entry)).ToList();
        }

        public void Dispose() => inner.Dispose();
    }

    /// <summary>
    /// Backend that blocks GetKeyValueByPrefix on a gate, allowing a concurrent write to land
    /// before stage 3 runs, so the merge-by-revision path can be exercised.
    /// </summary>
    private sealed class BlockingPrefixBackend : IPersistenceBackend, IDisposable
    {
        private readonly MemoryPersistenceBackend inner = new();
        private readonly ManualResetEventSlim gate;
        private readonly ManualResetEventSlim entered;
        private readonly List<(string Key, ReadOnlyKeyValueEntry Entry)> diskEntries;

        internal BlockingPrefixBackend(
            ManualResetEventSlim gate,
            ManualResetEventSlim entered,
            IEnumerable<(string Key, byte[]? Value, long Revision, KeyValueState State)> entries)
        {
            this.gate = gate;
            this.entered = entered;
            diskEntries = entries.Select(e => (e.Key, new ReadOnlyKeyValueEntry(
                e.Value, e.Revision,
                HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, e.State))).ToList();
        }

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) => inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName)
        {
            // Signal to the test that we've entered the disk read, then block until released.
            entered.Set();
            gate.Wait();
            return diskEntries.Where(e => e.Key.StartsWith(prefixKeyName, StringComparison.Ordinal))
                              .Select(e => (e.Key, e.Entry)).ToList();
        }

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) => inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) => inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) => inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose() => inner.Dispose();
    }
}
