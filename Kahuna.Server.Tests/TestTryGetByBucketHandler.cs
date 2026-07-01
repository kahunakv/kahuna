
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
/// detach-from-actor-mailbox path added in R5. Stage 1 is the in-memory scan; stage 2 runs
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
            entered.Wait(TimeSpan.FromSeconds(5));
            await actorRef.Ask(MakeSet("cfg/k", Encoding.UTF8.GetBytes("in-mem-new"), 0), TimeSpan.FromSeconds(5));

            // Ephemeral TryGet as sentinel: drains the actor queue so the set above has
            // been processed before we release the gate and stage 3 runs.
            await actorRef.Ask(MakeEphemeralGet("sentinel"), TimeSpan.FromSeconds(5));
            gate.Set();

            KeyValueResponse? resp = await scanTask;

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Get, resp!.Type);
            Assert.NotNull(resp.Items);
            Assert.Equal(1, resp.Items!.Count);
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
            Assert.Equal(1, resp.Items!.Count);
            Assert.Equal("data/x", resp.Items[0].Item1);
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
    private sealed class NeverReadBackend : IPersistenceBackend
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
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, out RevisionPruneResult result) => inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) => inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
    }

    /// <summary>Backend that returns a fixed list of prefix entries from "disk".</summary>
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

    /// <summary>
    /// Backend that blocks GetKeyValueByPrefix on a gate, allowing a concurrent write to land
    /// before stage 3 runs, so the merge-by-revision path can be exercised.
    /// </summary>
    private sealed class BlockingPrefixBackend : IPersistenceBackend
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
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, out RevisionPruneResult result) => inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) => inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
    }
}
