
using System.Text;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
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
/// End-to-end tests for the detached snapshot point read (<c>SnapshotReadContinuation</c>), driven
/// through a real spawned actor and a started read scheduler like the plain-miss detach tests in
/// <see cref="TestTryGetHandler"/>. Covers the three stage-2 shapes — the hydrated head answers, a
/// hydrated archived revision answers, the persisted history answers — plus a resident archive miss,
/// an absent key, a faulting backend and the TryExists shape. CamusDB feature 80af367a.
/// </summary>
public sealed class TestSnapshotReadContinuation : RaftTrackingTest
{
    private const int Retention = 16;

    private readonly ILoggerFactory loggerFactory;

    public TestSnapshotReadContinuation(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static HLCTimestamp At(long physical) => new(0, physical, 0);

    /// <summary>Revision <paramref name="revision"/> committed at physical time <c>revision * 1000</c>.</summary>
    private static PersistenceRequestItem Item(string key, long revision) =>
        new(key,
            Encoding.UTF8.GetBytes("val" + revision),
            revision: revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: revision * 1000, lastModifiedCounter: 0,
            state: (int)KeyValueState.Set);

    private static KeyValueRequest Make(KeyValueRequestType type, string key, HLCTimestamp readTimestamp) =>
        new(type, HLCTimestamp.Zero, HLCTimestamp.Zero, key, null, null, -1, KeyValueFlags.None, 0,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, default) { ReadTimestamp = readTimestamp };

    private (RaftManager Raft, FairReadScheduler Scheduler, KahunaConfiguration Config, ILogger<IKahuna> Logger) Infra(string name)
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
            RevisionRetention = Retention
        });

        ILogger<IRaft> raftLogger = loggerFactory.CreateLogger<IRaft>();
        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = name, NodeId = 1, Host = "localhost", Port = 0, InitialPartitions = 1,
                EnableQuiescence = false, PartitionExecutorPoolSize = 1
            },
            new StaticDiscovery([]), new InMemoryWAL(raftLogger), new InMemoryCommunication(),
            new HybridLogicalClock(), raftLogger);

        FairReadScheduler scheduler = (FairReadScheduler)Track(raft).ReadScheduler;
        scheduler.Start();
        return (raft, scheduler, config, loggerFactory.CreateLogger<IKahuna>());
    }

    private static IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> Spawn(
        ActorSystem actorSystem, string name, IPersistenceBackend backend, RaftManager raft, KahunaConfiguration config, ILogger<IKahuna> logger) =>
        actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
            name, null!, null!, backend, raft, raft.ReadScheduler, new KeySpaceRegistry(),
            new RangeMapStore(raft, null, null, logger), config, logger);

    private static async Task<KeyValueResponse> Ask(IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor, KeyValueRequest request)
    {
        KeyValueResponse? resp = await actor.Ask(request, TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.NotNull(resp);
        return resp!;
    }

    [Fact]
    public async Task SnapshotMiss_HydratesHeadWithArchive_ThenServesEveryWindowWithoutWalkingTheChain()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config, ILogger<IKahuna> logger) = Infra("snap-hydrate");
        try
        {
            CountingBackend backend = new();
            const string key = "acct/1";
            for (long r = 1; r <= 40; r++)
                backend.StoreKeyValues([Item(key, r)]);

            using IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor = Spawn(actorSystem, "snap-hydrate-actor", backend, raft, config, logger);

            // Cache miss at a snapshot between revisions 39 and 40: the hydrated head (40) is too new,
            // the newest archived revision (39) answers — one hydration read, no history walk.
            KeyValueResponse resp = await Ask(actor, Make(KeyValueRequestType.TryGet, key, At(39_500)));
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(39L, resp.Entry!.Revision);
            Assert.Equal("val39", Encoding.UTF8.GetString(resp.Entry.Value!));
            Assert.Equal(1, backend.Hydrations);
            Assert.Equal(0, backend.AsOfReads);

            // Resident now. A snapshot inside the archive window (revision 30) is an in-memory hit.
            resp = await Ask(actor, Make(KeyValueRequestType.TryGet, key, At(30_500)));
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(30L, resp.Entry!.Revision);
            Assert.Equal(1, backend.Hydrations);
            Assert.Equal(0, backend.AsOfReads);

            // A snapshot older than the archive window (revision 5) needs the persisted history: one
            // detached as-of read with the ceiling just below the archive, never on the actor thread.
            resp = await Ask(actor, Make(KeyValueRequestType.TryGet, key, At(5_500)));
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(5L, resp.Entry!.Revision);
            Assert.Equal("val5", Encoding.UTF8.GetString(resp.Entry.Value!));
            Assert.Equal(1, backend.AsOfReads);

            // Before the first revision: nothing existed.
            resp = await Ask(actor, Make(KeyValueRequestType.TryGet, key, At(500)));
            Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);

            // No snapshot: the head.
            resp = await Ask(actor, Make(KeyValueRequestType.TryGet, key, HLCTimestamp.Zero));
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(40L, resp.Entry!.Revision);

            KeyValueActor kv = (KeyValueActor)actor.Runner.Actor!;
            Assert.Equal(0, kv.PendingReadsCount);
            Assert.Equal(0, kv.PendingSnapshotReadsCount);
        }
        finally
        {
            scheduler.Stop();
        }
    }

    [Fact]
    public async Task SnapshotMiss_HeadAnswersWhenNotNewerThanSnapshot()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config, ILogger<IKahuna> logger) = Infra("snap-head");
        try
        {
            CountingBackend backend = new();
            const string key = "acct/2";
            for (long r = 1; r <= 3; r++)
                backend.StoreKeyValues([Item(key, r)]);

            using IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor = Spawn(actorSystem, "snap-head-actor", backend, raft, config, logger);

            KeyValueResponse resp = await Ask(actor, Make(KeyValueRequestType.TryGet, key, At(10_000)));
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(3L, resp.Entry!.Revision);
            Assert.Equal(0, backend.AsOfReads);
        }
        finally
        {
            scheduler.Stop();
        }
    }

    [Fact]
    public async Task SnapshotMiss_OlderThanArchiveWindow_ReadsPersistedHistoryOffTheActor()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config, ILogger<IKahuna> logger) = Infra("snap-history");
        try
        {
            CountingBackend backend = new();
            const string key = "acct/3";
            for (long r = 1; r <= 40; r++)
                backend.StoreKeyValues([Item(key, r)]);

            using IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor = Spawn(actorSystem, "snap-history-actor", backend, raft, config, logger);

            // Cold key, snapshot far below the archive window: hydration plus one as-of read, both in
            // the same scheduler task, with the as-of ceiling below the revisions already examined.
            KeyValueResponse resp = await Ask(actor, Make(KeyValueRequestType.TryGet, key, At(12_500)));
            Assert.Equal(KeyValueResponseType.Get, resp.Type);
            Assert.Equal(12L, resp.Entry!.Revision);
            Assert.Equal(1, backend.Hydrations);
            Assert.Equal(1, backend.AsOfReads);
            Assert.Equal(40 - 1 - Retention, backend.LastAsOfCeiling);
        }
        finally
        {
            scheduler.Stop();
        }
    }

    [Fact]
    public async Task SnapshotMiss_AbsentKey_DoesNotExist_AndExistsShapeCarriesNoValue()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config, ILogger<IKahuna> logger) = Infra("snap-absent");
        try
        {
            CountingBackend backend = new();
            const string key = "acct/4";
            for (long r = 1; r <= 3; r++)
                backend.StoreKeyValues([Item(key, r)]);

            using IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor = Spawn(actorSystem, "snap-absent-actor", backend, raft, config, logger);

            KeyValueResponse resp = await Ask(actor, Make(KeyValueRequestType.TryGet, "acct/nothing", At(10_000)));
            Assert.Equal(KeyValueResponseType.DoesNotExist, resp.Type);

            resp = await Ask(actor, Make(KeyValueRequestType.TryExists, key, At(2_500)));
            Assert.Equal(KeyValueResponseType.Exists, resp.Type);
            Assert.Equal(2L, resp.Entry!.Revision);
            Assert.Null(resp.Entry.Value);
        }
        finally
        {
            scheduler.Stop();
        }
    }

    [Fact]
    public async Task SnapshotMiss_BackendFaults_ResolvesMustRetryNotDoesNotExist()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config, ILogger<IKahuna> logger) = Infra("snap-fault");
        try
        {
            CountingBackend backend = new() { FailReads = true };

            using IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor = Spawn(actorSystem, "snap-fault-actor", backend, raft, config, logger);

            KeyValueResponse resp = await Ask(actor, Make(KeyValueRequestType.TryGet, "acct/5", At(10_000)));
            Assert.Equal(KeyValueResponseType.MustRetry, resp.Type);
            KeyValueActor kv = (KeyValueActor)actor.Runner.Actor!;
            Assert.Equal(0, kv.PendingReadsCount);
            Assert.Equal(0, kv.PendingSnapshotReadsCount);
        }
        finally
        {
            scheduler.Stop();
        }
    }

    /// <summary>Memory backend that counts the two detached read shapes and can be made to fault.</summary>
    private sealed class CountingBackend : IPersistenceBackend, IDisposable
    {
        private readonly MemoryPersistenceBackend inner = new();
        private int hydrations;
        private int asOfReads;

        internal int Hydrations => hydrations;
        internal int AsOfReads => asOfReads;
        internal long LastAsOfCeiling { get; private set; } = long.MinValue;
        internal bool FailReads { get; init; }

        public KeyValueEntry? GetKeyValue(string keyName) =>
            FailReads ? throw new IOException("simulated backend read failure") : inner.GetKeyValue(keyName);

        public KeyValueHydration GetKeyValueWithRecentRevisions(string keyName, int recentRevisions)
        {
            Interlocked.Increment(ref hydrations);
            if (FailReads)
                throw new IOException("simulated backend read failure");
            return ((IPersistenceBackend)inner).GetKeyValueWithRecentRevisions(keyName, recentRevisions);
        }

        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp)
        {
            Interlocked.Increment(ref asOfReads);
            LastAsOfCeiling = maxRevision;
            return inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        }

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public Kahuna.Server.Locks.Data.LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retentionCount, retentionAge, batchSize, floorTimestamp, out result);
        public Kahuna.Server.Persistence.Pitr.CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose() => inner.Dispose();
    }
}
