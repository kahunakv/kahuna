
using System.Text;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
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
/// Unit tests for the disk-history fallback path in snapshot point reads (TryGet and TryExists).
///
/// The in-memory revision archive is bounded (RevisionRetention = 16). Once a key has been
/// superseded more times than the archive capacity, the as-of revision is trimmed from memory
/// but still lives in the persisted revision history. These tests verify that TryGetHandler and
/// TryExistsHandler fall back to GetKeyValueRevisionAtOrBefore when the in-memory archive
/// misses, and that a timestamp before the key existed still returns DoesNotExist.
///
/// Uses a pre-configured backend (no full cluster, no background-writer timing dependency) to
/// exercise the fallback directly.
/// </summary>
[Collection("ClusterTests")]
public class TestSnapshotPointReadFromDiskHistory
{
    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>
    /// A snapshot TryGet at T, when the entry's current revision is newer than T and the
    /// in-memory revision archive does not contain the as-of revision, must return the as-of
    /// value from the persisted revision history.
    ///
    /// Also verifies that a T before the key was first written returns DoesNotExist.
    /// </summary>
    [Fact]
    public async Task PointGet_AtSnapshot_InMemoryMiss_FallsBackToDisk()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig();

        // Use explicit, well-ordered timestamps to avoid any HLC wall-clock ambiguity.
        HLCTimestamp beforeCreate = new(1, 1000L, 0);  // before the key existed
        HLCTimestamp snapshotT    = new(1, 2000L, 0);  // key existed at this point
        HLCTimestamp currentT     = new(1, 3000L, 0);  // current (latest) revision

        const string key = "snapshot/point-get";
        const long   asOfRevision  = 1L;
        const long   latestRevision = 20L;

        // Backend: GetKeyValue returns the current (latest) entry; GetKeyValueRevisionAtOrBefore
        // returns the as-of entry when queried with a timestamp >= snapshotT, null otherwise.
        PointReadHistoryBackend backend = new(
            key, latestRevision, currentT, B("value-latest"),
            asOfRevision, snapshotT, B("value-at-snapshot"));

        scheduler.Start();
        try
        {
            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "snap-get-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            // Snapshot get at T — in-memory misses, disk fallback must return as-of revision.
            KeyValueResponse? resp = await actorRef.Ask(
                MakeSnapshotGet(key, snapshotT), TimeSpan.FromSeconds(5));

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.Entry);
            Assert.Equal("value-at-snapshot", Encoding.UTF8.GetString(resp.Entry!.Value!));
            Assert.Equal(asOfRevision, resp.Entry!.Revision);

            // Snapshot get before the key existed — disk fallback returns null → DoesNotExist.
            KeyValueResponse? noResp = await actorRef.Ask(
                MakeSnapshotGet(key, beforeCreate), TimeSpan.FromSeconds(5));

            Assert.NotNull(noResp);
            Assert.Equal(KeyValueResponseType.DoesNotExist, noResp!.Type);
        }
        finally { scheduler.Stop(); }
    }

    /// <summary>
    /// A snapshot TryExists at T, when the in-memory archive misses the as-of revision, must
    /// return Exists with the correct revision number from the persisted revision history.
    /// A T before the key was created must return DoesNotExist.
    /// </summary>
    [Fact]
    public async Task PointExists_AtSnapshot_InMemoryMiss_FallsBackToDisk()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig();

        HLCTimestamp beforeCreate = new(1, 1000L, 0);
        HLCTimestamp snapshotT    = new(1, 2000L, 0);
        HLCTimestamp currentT     = new(1, 3000L, 0);

        const string key = "snapshot/point-exists";
        const long   asOfRevision  = 1L;
        const long   latestRevision = 20L;

        PointReadHistoryBackend backend = new(
            key, latestRevision, currentT, B("value-latest"),
            asOfRevision, snapshotT, B("value-at-snapshot"));

        scheduler.Start();
        try
        {
            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "snap-exists-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            // Snapshot exists at T — disk fallback must report Exists with the as-of revision.
            KeyValueResponse? resp = await actorRef.Ask(
                MakeSnapshotExists(key, snapshotT), TimeSpan.FromSeconds(5));

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Exists, resp!.Type);
            Assert.NotNull(resp.Entry);
            Assert.Equal(asOfRevision, resp.Entry!.Revision);
            // TryExists suppresses the value — only revision/metadata are returned.
            Assert.Null(resp.Entry!.Value);

            // Snapshot exists before the key existed — disk fallback returns null → DoesNotExist.
            KeyValueResponse? noResp = await actorRef.Ask(
                MakeSnapshotExists(key, beforeCreate), TimeSpan.FromSeconds(5));

            Assert.NotNull(noResp);
            Assert.Equal(KeyValueResponseType.DoesNotExist, noResp!.Type);
        }
        finally { scheduler.Stop(); }
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private static KeyValueRequest MakeSnapshotGet(string key, HLCTimestamp readTimestamp)
    {
        KeyValueRequest req = new(KeyValueRequestType.TryGet,
            HLCTimestamp.Zero, HLCTimestamp.Zero,
            key, null, null, -1,
            KeyValueFlags.None, 0,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent, 0, 0, null);
        req.ReadTimestamp = readTimestamp;
        return req;
    }

    private static KeyValueRequest MakeSnapshotExists(string key, HLCTimestamp readTimestamp)
    {
        KeyValueRequest req = new(KeyValueRequestType.TryExists,
            HLCTimestamp.Zero, HLCTimestamp.Zero,
            key, null, null, -1,
            KeyValueFlags.None, 0,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent, 0, 0, null);
        req.ReadTimestamp = readTimestamp;
        return req;
    }

    private static (RaftManager Raft, FairReadScheduler Scheduler, KahunaConfiguration Config, ILogger<IKahuna> Logger)
        CreateRaftAndConfig()
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
                NodeName = "snap-point-read",
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

    // ── inner backend ────────────────────────────────────────────────────────────────────

    /// <summary>
    /// A backend that simulates a key with a trimmed in-memory revision archive:
    /// GetKeyValue returns the latest committed revision (LastModified > snapshotT), while
    /// GetKeyValueRevisionAtOrBefore returns the as-of revision for any readTimestamp >= the
    /// as-of LastModified, and null for any readTimestamp before it (simulating no history at
    /// that point in time).
    /// </summary>
    private sealed class PointReadHistoryBackend : IPersistenceBackend
    {
        private readonly MemoryPersistenceBackend inner = new();
        private readonly string key;
        private readonly KeyValueEntry currentEntry;
        private readonly KeyValueEntry asOfEntry;
        private readonly HLCTimestamp asOfLastModified;

        internal PointReadHistoryBackend(
            string key,
            long latestRevision, HLCTimestamp latestLastModified, byte[] latestValue,
            long asOfRevision, HLCTimestamp asOfLastModified, byte[] asOfValue)
        {
            this.key = key;
            this.asOfLastModified = asOfLastModified;

            // The current entry returned by GetKeyValue — has no Revisions dict (simulates
            // a disk-loaded entry where only the current state is present, not the history).
            currentEntry = new()
            {
                Value = latestValue,
                Revision = latestRevision,
                LastModified = latestLastModified,
                State = KeyValueState.Set
            };

            // The as-of entry returned by GetKeyValueRevisionAtOrBefore.
            asOfEntry = new()
            {
                Value = asOfValue,
                Revision = asOfRevision,
                LastModified = asOfLastModified,
                State = KeyValueState.Set
            };
        }

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public LockEntry? GetLock(string resource) => inner.GetLock(resource);

        public KeyValueEntry? GetKeyValue(string keyName) =>
            string.Equals(keyName, key, StringComparison.Ordinal) ? currentEntry : null;

        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) =>
            inner.GetKeyValueRevision(keyName, revision);

        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp)
        {
            if (!string.Equals(keyName, key, StringComparison.Ordinal))
                return null;
            // Return the as-of entry only if readTimestamp is at or after its LastModified.
            if (readTimestamp.CompareTo(asOfLastModified) >= 0 && asOfEntry.Revision <= maxRevision)
                return asOfEntry;
            return null;
        }

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) =>
            inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
    }
}

/// <summary>
/// Integration tests for the disk-history fallback (F1a). These exercise the full chain:
/// real writes → in-memory revision trim (RevisionRetention=2) → background-writer flush to the
/// real MemoryPersistenceBackend → snapshot point-read that falls back to
/// GetKeyValueRevisionAtOrBefore and returns the correct as-of revision from disk.
///
/// Complements TestSnapshotPointReadFromDiskHistory (handler-wiring unit tests) by proving the
/// end-to-end path: the trim really happens, the revisions really land on disk after a flush,
/// and the handler's disk fallback really serves the correct historical value.
/// </summary>
[Collection("ClusterTests")]
public class TestSnapshotPointReadDiskFallbackIntegration : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);
    private static string S(byte[]? b) => b is null ? "" : Encoding.UTF8.GetString(b);

    public TestSnapshotPointReadDiskFallbackIntegration(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger   = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    /// <summary>
    /// Writes RevisionRetention+2 versions of a key, waits for the background writer to flush
    /// all revisions to the persistence backend, then reads as-of the first write's timestamp.
    ///
    /// After RevisionRetention+2 writes the in-memory archive no longer contains the first
    /// revision (it was trimmed by RemoveExpiredRevisions). The snapshot point-read must fall
    /// back to GetKeyValueRevisionAtOrBefore and return the first-write value from disk.
    /// A read at a timestamp before the key existed must return DoesNotExist.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task PointGet_AfterTrim_FallsBackToDiskHistory(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(4)] int partitions)
    {
        // RevisionRetention=2 means the in-memory archive keeps at most 2 old revisions.
        // DirtyObjectsWriterDelay=100 flushes the background writer every 100 ms so the test
        // doesn't have to wait 5 seconds for the default 5-second flush cycle.
        const int retention = 2;

        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger, cfg =>
            {
                cfg.RevisionRetention = retention;
                cfg.DirtyObjectsWriterDelay = 100;
            });

        try
        {
            string key = "snapDiskFallback/" + Guid.NewGuid().ToString("N")[..8];

            // ── Write V1, capture its timestamp as the snapshot point ──────────────────────────
            (KeyValueResponseType t, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, B("v1"),
                null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, t);

            // Read back V1 so we can capture its exact LastModified timestamp.
            (_, ReadOnlyKeyValueEntry? entry1) = await kahuna2.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.NotNull(entry1);
            HLCTimestamp snapshotT = entry1!.LastModified;

            // Capture a timestamp strictly before V1 to test the "key did not exist" case.
            HLCTimestamp beforeCreate = new(snapshotT.N, snapshotT.L - 1, snapshotT.C);

            // ── Write RevisionRetention+2 more versions to push V1 out of the in-memory archive ─
            // With retention=2 and (retention+2) total supersessions the as-of revision for
            // snapshotT is trimmed from the in-memory Revisions dict by RemoveExpiredRevisions.
            for (int i = 2; i <= retention + 2; i++)
            {
                await Task.Delay(2, TestContext.Current.CancellationToken); // HLC monotonicity
                (t, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, B($"v{i}"),
                    null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Set, t);
            }

            // ── Wait for the background writer to flush all revisions to the persistence backend ─
            // DirtyObjectsWriterDelay=100 ms → two timer ticks (200 ms) is a comfortable margin.
            await Task.Delay(300, TestContext.Current.CancellationToken);

            // ── Snapshot read at snapshotT — in-memory archive misses, must serve V1 from disk ──
            (KeyValueResponseType rt, ReadOnlyKeyValueEntry? snapEntry) =
                await kahuna1.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, snapshotT,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

            Assert.Equal(KeyValueResponseType.Get, rt);
            Assert.NotNull(snapEntry);
            Assert.Equal("v1", S(snapEntry!.Value));
            Assert.Equal(1L, snapEntry.Revision);

            // ── Snapshot read before the key existed — disk fallback must return DoesNotExist ───
            (KeyValueResponseType nrt, _) =
                await kahuna1.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, beforeCreate,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

            Assert.Equal(KeyValueResponseType.DoesNotExist, nrt);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// Same chain as PointGet_AfterTrim_FallsBackToDiskHistory but exercises TryExists instead
    /// of TryGet: the handler must return Exists with the correct revision, not the value.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task PointExists_AfterTrim_FallsBackToDiskHistory(
        [CombinatorialValues("memory")] string storage,
        [CombinatorialValues(4)] int partitions)
    {
        const int retention = 2;

        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger, cfg =>
            {
                cfg.RevisionRetention = retention;
                cfg.DirtyObjectsWriterDelay = 100;
            });

        try
        {
            string key = "snapDiskFallbackEx/" + Guid.NewGuid().ToString("N")[..8];

            (KeyValueResponseType t, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, B("v1"),
                null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, t);

            (_, ReadOnlyKeyValueEntry? entry1) = await kahuna2.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.NotNull(entry1);
            HLCTimestamp snapshotT = entry1!.LastModified;
            HLCTimestamp beforeCreate = new(snapshotT.N, snapshotT.L - 1, snapshotT.C);

            for (int i = 2; i <= retention + 2; i++)
            {
                await Task.Delay(2, TestContext.Current.CancellationToken);
                (t, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, B($"v{i}"),
                    null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
                Assert.Equal(KeyValueResponseType.Set, t);
            }

            await Task.Delay(300, TestContext.Current.CancellationToken);

            (KeyValueResponseType et, ReadOnlyKeyValueEntry? existsEntry) =
                await kahuna1.LocateAndTryExistsValue(
                    HLCTimestamp.Zero, key, -1, snapshotT,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

            Assert.Equal(KeyValueResponseType.Exists, et);
            Assert.NotNull(existsEntry);
            Assert.Equal(1L, existsEntry!.Revision);

            (KeyValueResponseType net, _) =
                await kahuna1.LocateAndTryExistsValue(
                    HLCTimestamp.Zero, key, -1, beforeCreate,
                    KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

            Assert.Equal(KeyValueResponseType.DoesNotExist, net);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
