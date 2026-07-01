
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
/// Tests for TryScanByPrefixFromDiskHandler covering the detach-from-actor-mailbox path
/// added in R5. The entire GetKeyValueByPrefix (plus optional per-key snapshot projection)
/// runs off-actor in stage 2; stage 3 filters deleted/expired entries and resolves.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestTryScanByPrefixFromDiskHandler
{
    // ── Disk returns entries → resolved as Get after detach ───────────────────────────────

    [Fact]
    public async Task PrefixFromDiskScan_DiskReturnsEntries_ResolvesGet()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("pfx-hit");

        scheduler.Start();
        try
        {
            PrefixBackend backend = new([
                ("svc/a", Encoding.UTF8.GetBytes("va"), 1L, KeyValueState.Set),
                ("svc/b", Encoding.UTF8.GetBytes("vb"), 2L, KeyValueState.Set),
            ]);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "pfx-hit-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(MakePrefixScan("svc/"), TimeSpan.FromSeconds(5));

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.Items);
            Assert.Equal(2, resp.Items!.Count);
        }
        finally { scheduler.Stop(); }
    }

    // ── Disk returns nothing → empty result ───────────────────────────────────────────────

    [Fact]
    public async Task PrefixFromDiskScan_DiskEmpty_ResolvesEmptyList()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("pfx-empty");

        scheduler.Start();
        try
        {
            PrefixBackend backend = new([]);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "pfx-empty-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(MakePrefixScan("nothing/"), TimeSpan.FromSeconds(5));

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.Items);
            Assert.Empty(resp.Items!);
        }
        finally { scheduler.Stop(); }
    }

    // ── Deleted entries are excluded from the result ──────────────────────────────────────

    [Fact]
    public async Task PrefixFromDiskScan_DeletedEntry_Excluded()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("pfx-deleted");

        scheduler.Start();
        try
        {
            PrefixBackend backend = new([
                ("ns/live", Encoding.UTF8.GetBytes("alive"), 1L, KeyValueState.Set),
                ("ns/dead", null, 2L, KeyValueState.Deleted),
            ]);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "pfx-deleted-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(MakePrefixScan("ns/"), TimeSpan.FromSeconds(5));

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.Items);
            // Tombstone must not appear in the result.
            Assert.Equal(1, resp.Items!.Count);
            Assert.Equal("ns/live", resp.Items[0].Item1);
        }
        finally { scheduler.Stop(); }
    }

    // ── Backend fault → MustRetry ─────────────────────────────────────────────────────────

    [Fact]
    public async Task PrefixFromDiskScan_BackendFaults_ResolvesMustRetry()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("pfx-fault");

        scheduler.Start();
        try
        {
            FaultingBackend backend = new();

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "pfx-fault-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(MakePrefixScan("err/"), TimeSpan.FromSeconds(5));

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.MustRetry, resp!.Type);
        }
        finally { scheduler.Stop(); }
    }

    // ── Coalescing: N concurrent identical scans issue exactly one disk read ──────────────

    [Fact]
    public async Task ConcurrentIdenticalScans_ExactlyOneDiskRead_AllReceiveResults()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("pfx-coalesce");

        scheduler.Start();
        try
        {
            ManualResetEventSlim gate = new(false);
            ManualResetEventSlim entered = new(false);
            CountingPrefixBackend backend = new(gate, entered, [
                ("app/x", Encoding.UTF8.GetBytes("xval"), 1L, KeyValueState.Set),
            ]);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "pfx-coalesce-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            const int n = 5;
            Task<KeyValueResponse?>[] asks = Enumerable.Range(0, n)
                .Select(_ => actorRef.Ask(MakePrefixScan("app/"), TimeSpan.FromSeconds(10)))
                .ToArray();

            // Wait until the first read has entered the backend, then send a sentinel
            // ephemeral TryGet (never touches disk, completes immediately on the actor thread)
            // to drain the actor mailbox — by the time the sentinel returns, all N prefix
            // scans have been processed and coalesced onto the single continuation.
            entered.Wait(TimeSpan.FromSeconds(5));
            KeyValueResponse? sentinel = await actorRef.Ask(
                MakeEphemeralGet("sentinel"), TimeSpan.FromSeconds(10));

            gate.Set();

            KeyValueResponse?[] results = await Task.WhenAll(asks);

            // Exactly one backend call despite N concurrent identical requests.
            Assert.Equal(1, backend.PrefixReadCount);

            // All callers must receive the same Get result.
            foreach (KeyValueResponse? r in results)
            {
                Assert.NotNull(r);
                Assert.Equal(KeyValueResponseType.Get, r!.Type);
                Assert.Equal(1, r.Items!.Count);
            }
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

    private static KeyValueRequest MakePrefixScan(string prefix)
    {
        return new KeyValueRequest(
            KeyValueRequestType.ScanByPrefixFromDisk,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            prefix,
            null, null, -1,
            KeyValueFlags.None, 0,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent, 0, 0, null);
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

    private sealed class PrefixBackend : IPersistenceBackend
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
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, out RevisionPruneResult result) => inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) => inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
    }

    private sealed class FaultingBackend : IPersistenceBackend
    {
        private readonly MemoryPersistenceBackend inner = new();

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) => inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => throw new InvalidOperationException("simulated disk fault");
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) => inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, out RevisionPruneResult result) => inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) => inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
    }

    private sealed class CountingPrefixBackend : IPersistenceBackend
    {
        private readonly MemoryPersistenceBackend inner = new();
        private readonly ManualResetEventSlim gate;
        private readonly ManualResetEventSlim entered;
        private readonly List<(string Key, ReadOnlyKeyValueEntry Entry)> diskEntries;
        private int prefixReadCount;

        internal int PrefixReadCount => prefixReadCount;

        internal CountingPrefixBackend(
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
            Interlocked.Increment(ref prefixReadCount);
            entered.Set();
            gate.Wait();
            return diskEntries.Where(e => e.Key.StartsWith(prefixKeyName, StringComparison.Ordinal))
                              .Select(e => (e.Key, e.Entry)).ToList();
        }

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) => inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, out RevisionPruneResult result) => inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) => inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
    }
}
