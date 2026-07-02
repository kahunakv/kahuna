
using System.Text;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
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
/// Tests for <see cref="TryExistsHandler"/> covering the by-revision path that detaches
/// GetKeyValueRevision off the actor mailbox. The non-by-revision path (which still uses the
/// blocking GetKeyValueEntry) is not exercised here.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestTryExistsHandler
{
    // ── By-revision: stage-1 shortcuts (no disk, no detach) ──────────────────────────────

    [Fact]
    public async Task ByRevision_CurrentRevisionMatch_ShortcutReturnsExistsWithoutDetach()
    {
        (TryExistsHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        KeyValueEntry resident = new()
        {
            Value = Encoding.UTF8.GetBytes("v1"),
            Revision = 5,
            LastModified = now,
            LastUsed = now,
            State = KeyValueState.Set
        };
        context.InsertStoreEntry("exists-key", resident);

        // actorContext is null! — detach path would NullReferenceException; clean response
        // proves the stage-1 shortcut was taken.
        KeyValueResponse resp = await handler.Execute(MakeExists("exists-key", 5));

        Assert.Equal(KeyValueResponseType.Exists, resp.Type);
        Assert.Equal(5L, resp.Entry!.Revision);
    }

    [Fact]
    public async Task ByRevision_InMemoryRevisionDict_ShortcutReturnsExists()
    {
        (TryExistsHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        KeyValueEntry resident = new()
        {
            Value = Encoding.UTF8.GetBytes("current"),
            Revision = 10,
            LastModified = now,
            LastUsed = now,
            State = KeyValueState.Set,
            Revisions = new Dictionary<long, KeyValueRevisionEntry>
            {
                [2] = new() { Value = null, State = KeyValueState.Set }
            }
        };
        context.InsertStoreEntry("exists-rev-key", resident);

        KeyValueResponse resp = await handler.Execute(MakeExists("exists-rev-key", 2));

        Assert.Equal(KeyValueResponseType.Exists, resp.Type);
        Assert.Equal(2L, resp.Entry!.Revision);
    }

    [Fact]
    public async Task ByRevision_Ephemeral_MissingRevision_ReturnsDoesNotExist()
    {
        (TryExistsHandler handler, _, _) = CreateHandler();

        KeyValueResponse resp = await handler.Execute(
            MakeExists("no-key", 99, KeyValueDurability.Ephemeral));

        Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);
    }

    // ── Latest-point: persistent cache miss → detach → resolves via ResumeRead ─────────────
    //
    // TryExists for a non-by-revision key that is not in cache and durability is Persistent
    // must detach GetKeyValue off the actor mailbox via PointReadContinuation(Exists). The
    // response shape must be Exists (not Get), and the value field must be null.

    [Fact]
    public async Task LatestPoint_PersistentCacheMiss_DetachPath_ResolvesExists()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("exists-latest-detach");

        scheduler.Start();
        try
        {
            MemoryPersistenceBackend backend = new();
            backend.StoreKeyValues(
            [
                new PersistenceRequestItem(
                    key: "live-key",
                    value: Encoding.UTF8.GetBytes("live-value"),
                    revision: 2,
                    expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
                    lastUsedNode: 0, lastUsedPhysical: 1000, lastUsedCounter: 0,
                    lastModifiedNode: 0, lastModifiedPhysical: 1000, lastModifiedCounter: 0,
                    state: (int)KeyValueState.Set)
            ]);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "exists-latest-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeExists("live-key", compareRevision: -1),
                TimeSpan.FromSeconds(5));

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Exists, resp!.Type);
            Assert.Equal(2L, resp.Entry!.Revision);
            Assert.Null(resp.Entry.Value); // Exists must not carry the value
        }
        finally { scheduler.Stop(); }
    }

    [Fact]
    public async Task LatestPoint_PersistentCacheMiss_KeyNotOnDisk_ResolvesDoesNotExist()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("exists-latest-notfound");

        scheduler.Start();
        try
        {
            MemoryPersistenceBackend backend = new(); // empty — GetKeyValue returns null

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "exists-notfound-actor2", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeExists("absent-key", compareRevision: -1),
                TimeSpan.FromSeconds(5));

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.DoesNotExist, resp!.Type);
        }
        finally { scheduler.Stop(); }
    }

    // ── Shape-axis isolation: Get(key) and Exists(key) latest-point in flight simultaneously
    //
    // PendingReads uses (key, -1, false) for latest TryGet and (key, -1, true) for latest
    // TryExists so they occupy separate slots. A TryGet and TryExists for the same key must
    // resolve to their own shapes even when both are simultaneously in flight (cache miss).

    [Fact]
    public async Task LatestGet_And_LatestExists_SameKey_ResolveToCorrectShapes()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("latest-shape-isolation");

        scheduler.Start();
        try
        {
            ManualResetEventSlim gate = new(false);
            BlockingLatestBackend backend = new(gate, Encoding.UTF8.GetBytes("the-value"), revision: 9);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "latest-shape-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            Task<KeyValueResponse?> getAsk = actorRef.Ask(
                MakeGet("shape-key"),
                TimeSpan.FromSeconds(10));
            Task<KeyValueResponse?> existsAsk = actorRef.Ask(
                MakeExists("shape-key", compareRevision: -1),
                TimeSpan.FromSeconds(10));

            // Drain mailbox: both dispatches done before gate is released.
            KeyValueResponse? sentinel = await actorRef.Ask(
                MakeExists("sentinel", compareRevision: -1, durability: KeyValueDurability.Ephemeral),
                TimeSpan.FromSeconds(10));
            Assert.Equal(KeyValueResponseType.DoesNotExist, sentinel!.Type);

            gate.Set();

            KeyValueResponse?[] results = await Task.WhenAll(getAsk, existsAsk);

            // TryGet → Get with value.
            Assert.NotNull(results[0]);
            Assert.Equal(KeyValueResponseType.Get, results[0]!.Type);
            Assert.NotNull(results[0]!.Entry!.Value);
            Assert.Equal("the-value", Encoding.UTF8.GetString(results[0]!.Entry!.Value!));

            // TryExists → Exists with null value.
            Assert.NotNull(results[1]);
            Assert.Equal(KeyValueResponseType.Exists, results[1]!.Type);
            Assert.Null(results[1]!.Entry!.Value);
            Assert.Equal(9L, results[1]!.Entry!.Revision);
        }
        finally { scheduler.Stop(); }
    }

    // ── By-revision: persistent disk miss → detach → resolves via ResumeRead ─────────────

    [Fact]
    public async Task ByRevision_PersistentDiskMiss_DetachPath_ResolvesExists()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("exists-detach");

        scheduler.Start();
        try
        {
            RevisionBackend backend = new(targetRevision: 4, value: Encoding.UTF8.GetBytes("ignored"));

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "exists-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeExists("hist-key", 4),
                TimeSpan.FromSeconds(5));

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.Exists, resp!.Type);
            Assert.Equal(4L, resp.Entry!.Revision);
            // Exists must suppress the value field.
            Assert.Null(resp.Entry.Value);
        }
        finally { scheduler.Stop(); }
    }

    [Fact]
    public async Task ByRevision_PersistentDiskMiss_RevisionNotOnDisk_ResolvesDoesNotExist()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config,
            ILogger<IKahuna> logger) = CreateRaftAndConfig("exists-notfound");

        scheduler.Start();
        try
        {
            RevisionBackend backend = new(targetRevision: -1, value: null);

            using ActorSystem actorSystem = new();
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "exists-notfound-actor", null!, null!, backend, raft,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            KeyValueResponse? resp = await actorRef.Ask(
                MakeExists("hist-key", 99),
                TimeSpan.FromSeconds(5));

            Assert.NotNull(resp);
            Assert.Equal(KeyValueResponseType.DoesNotExist, resp!.Type);
        }
        finally { scheduler.Stop(); }
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Backend whose GetKeyValue blocks on a ManualResetEvent gate and returns a specific entry.
    /// Used to keep latest-point TryGet and TryExists reads in-flight simultaneously so the
    /// shape-isolation test can verify they occupy separate PendingReads slots.
    /// </summary>
    private sealed class BlockingLatestBackend : IPersistenceBackend, IDisposable
    {
        private readonly MemoryPersistenceBackend inner = new();
        private readonly ManualResetEventSlim gate;
        private readonly byte[] value;
        private readonly long revision;

        internal BlockingLatestBackend(ManualResetEventSlim gate, byte[] value, long revision)
        {
            this.gate = gate;
            this.value = value;
            this.revision = revision;
        }

        public KeyValueEntry? GetKeyValue(string keyName)
        {
            gate.Wait();
            return new() { Value = value, Revision = revision, State = KeyValueState.Set };
        }

        public KeyValueEntry? GetKeyValueRevision(string keyName, long rev) => null;
        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public Kahuna.Server.Locks.Data.LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose() => inner.Dispose();
    }

    private sealed class RevisionBackend : IPersistenceBackend, IDisposable
    {
        private readonly MemoryPersistenceBackend inner = new();
        private readonly long targetRevision;
        private readonly byte[]? value;

        internal RevisionBackend(long targetRevision, byte[]? value)
        {
            this.targetRevision = targetRevision;
            this.value = value;
        }

        public KeyValueEntry? GetKeyValue(string keyName) => null;

        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision)
        {
            if (revision == targetRevision && value is not null)
                return new() { Value = value, Revision = revision, State = KeyValueState.Set };
            return null;
        }

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public Kahuna.Server.Locks.Data.LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose() => inner.Dispose();
    }

    private static KeyValueRequest MakeGet(
        string key,
        KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        return new KeyValueRequest(
            KeyValueRequestType.TryGet,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            key,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            durability,
            0,
            0,
            null
        );
    }

    private static KeyValueRequest MakeExists(
        string key,
        long compareRevision,
        KeyValueDurability durability = KeyValueDurability.Persistent)
    {
        return new KeyValueRequest(
            KeyValueRequestType.TryExists,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            key,
            null,
            null,
            compareRevision,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            durability,
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

    private static (TryExistsHandler, KeyValueContext, RaftManager) CreateHandler()
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

        BTree<string, KeyValueEntry> store = new(32);
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "tryexists-test",
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

        MemoryPersistenceBackend backend = new();

        KeyValueContext context = new(
            null!,
            store,
            new Dictionary<string, KeyValueWriteIntent>(),
            new Dictionary<string, List<KeyValueRangeLock>>(),
            new Dictionary<int, KeyValueProposal>(),
            null!,
            null!,
            backend,
            raft,
            new KeySpaceRegistry(),
            new RangeMapStore(raft, null, null, logger),
            config,
            logger
        );

        return (new TryExistsHandler(context), context, raft);
    }
}
