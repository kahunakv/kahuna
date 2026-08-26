using System.Text;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
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
/// Covers the Kahuna-owned backend I/O schedulers introduced to decouple data-plane reads/writes from
/// Kommander's WAL read pool:
/// <list type="bullet">
///   <item><see cref="BackendReadBackpressure_OnSynchronousReadPath_ResolvesMustRetry"/> — when the
///     dedicated backend read scheduler rejects an enqueue at its per-partition depth limit, a point
///     write's read-before-write (a synchronous await path) surfaces MustRetry rather than faulting the
///     actor.</item>
///   <item><see cref="CustomBackendPoolSizes_NodeRunsAndShutsDownCleanly"/> — a node configured with
///     explicit backend read/write pool sizes runs KV traffic through the dedicated pools and disposes
///     without hanging (the schedulers stop after the actor system drains).</item>
/// </list>
/// </summary>
public sealed class TestBackendIoScheduler : RaftTrackingTest
{
    /// <summary>
    /// A persistence backend that blocks the first <see cref="GetKeyValue"/> call until released, so a
    /// single in-flight read can be pinned on a worker thread while the test drives a second read into the
    /// scheduler's per-partition depth limit. All other operations delegate to an in-memory backend.
    /// </summary>
    private sealed class GatingBackend(IPersistenceBackend inner) : IPersistenceBackend, IDisposable
    {
        public readonly ManualResetEventSlim Entered = new(false);
        public readonly ManualResetEventSlim Release = new(false);
        private int gated;

        public void Dispose()
        {
            Entered.Dispose();
            Release.Dispose();
        }

        public KeyValueEntry? GetKeyValue(string keyName)
        {
            // Gate only the first read: signal that a worker is now parked inside the backend (so the
            // scheduler's queue depth for this partition is at 1) and block until the test releases it.
            if (Interlocked.Exchange(ref gated, 1) == 0)
            {
                Entered.Set();
                Release.Wait();
            }

            return inner.GetKeyValue(keyName);
        }

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            inner.GetKeyValueByRange(prefix, startKey, limit);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retainCount, TimeSpan retainAge,
            int batchSize, HLCTimestamp floor, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retainCount, retainAge, batchSize, floor, out result);
        public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
    }

    [Fact]
    public async Task BackendReadBackpressure_OnSynchronousReadPath_ResolvesMustRetry()
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
            new RaftConfiguration { NodeName = "backend-io-bp", NodeId = 1, Host = "localhost", Port = 0, InitialPartitions = 1, EnableQuiescence = false, PartitionExecutorPoolSize = 1 },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger);

        // A dedicated backend read scheduler with a single worker and a per-partition depth of 1: once one
        // read is parked inside the gating backend, the next enqueue for the same partition is rejected.
        FairReadScheduler backendReadScheduler = new(raftLogger, workerCount: 1, maxQueueDepthPerPartition: 1);
        backendReadScheduler.Start();

        MemoryPersistenceBackend memory = new();
        memory.StoreKeyValues(
        [
            new PersistenceRequestItem(
                key: "tenant/blocker",
                value: Encoding.UTF8.GetBytes("blocker-value"),
                revision: 1,
                expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
                lastUsedNode: 0, lastUsedPhysical: 1000, lastUsedCounter: 0,
                lastModifiedNode: 0, lastModifiedPhysical: 1000, lastModifiedCounter: 0,
                state: (int)KeyValueState.Set)
        ]);
        GatingBackend backend = new(memory);

        using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);

        try
        {
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "backend-io-bp-actor",
                    null!,  // backgroundWriter (never touched: read + rejected read-before-write only)
                    null!,  // writeAggregator
                    backend,
                    raft,
                    backendReadScheduler,
                    new KeySpaceRegistry(),
                    new RangeMapStore(Track(raft), null, null, logger),
                    config,
                    logger,
                    null!, null!, null!, null!);

            // Blocker: a persistent cache-miss get detaches off the actor and parks a worker inside the
            // backend. We hold its task; it resolves only once the gate is released.
            Task<KeyValueResponse?> blocker = actorRef.Ask(
                MakeGet("tenant/blocker"),
                TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            // Wait until the worker is actually inside the backend (depth for the partition is now 1).
            Assert.True(backend.Entered.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken), "blocking read never reached the backend");

            // Probe: a persistent set of a different (uncached) key does a read-before-write on the same
            // partition. Its enqueue hits the depth limit and throws; the actor's boundary maps that to
            // MustRetry rather than faulting.
            KeyValueResponse? probe = await actorRef.Ask(
                MakeSet("tenant/probe", "probe-value"),
                TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.NotNull(probe);
            Assert.Equal(KeyValueResponseType.MustRetry, probe!.Type);

            // Release the blocker and confirm it completes normally (deterministic drain, no hang).
            backend.Release.Set();
            KeyValueResponse? blockerResult = await blocker;
            Assert.NotNull(blockerResult);
            Assert.Equal(KeyValueResponseType.Get, blockerResult!.Type);
            Assert.Equal("blocker-value", Encoding.UTF8.GetString(blockerResult.Entry!.Value!));
        }
        finally
        {
            backend.Release.Set();
            backendReadScheduler.Stop();
            backendReadScheduler.Dispose();
            if (raft is IDisposable d) d.Dispose();
            backend.Dispose();
        }
    }

    [Fact]
    public async Task CustomBackendPoolSizes_NodeRunsAndShutsDownCleanly()
    {
        // Explicit, deliberately small backend pool sizes exercise the config → KahunaManager wiring and
        // prove a node runs KV traffic through the dedicated pools and then disposes without hanging.
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(100),
            StartElectionTimeout = 500,
            EndElectionTimeout = 1500,
            BackendReadIOThreads = 2,
            BackendWriteIOThreads = 1,
            BackendReadQueueDepth = 256
        }, NullLoggerFactory.Instance);

        await node.StartAsync(TestContext.Current.CancellationToken);
        await node.WaitForLeaderForKeyAsync("tenant/table/k", TestContext.Current.CancellationToken);

        // A persistent write (routes through the dedicated writer pool on flush) and a persistent read of a
        // key that is not yet cached (routes through the dedicated read pool) exercise both schedulers.
        (KeyValueResponseType setType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, "tenant/table/k", Encoding.UTF8.GetBytes("v"), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, setType);

        await node.FlushAsync();

        (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "tenant/table/k", -1, HLCTimestamp.Zero,
            KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Get, getType);
        Assert.Equal("v", Encoding.UTF8.GetString(entry!.Value!));

        // Dispose must complete promptly: the backend schedulers are stopped in KahunaManager.Dispose,
        // which runs after the actor system drains, so in-flight backend I/O finishes rather than hanging.
        Task dispose = node.DisposeAsync().AsTask();
        Assert.True(await Task.WhenAny(dispose, Task.Delay(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken)) == dispose,
            "node dispose hung — backend scheduler shutdown did not drain");
        await dispose;
    }

    [Fact]
    public async Task SaturatingOneScheduler_DoesNotRejectOrDelayAnother()
    {
        // The whole point of a dedicated backend pool is isolation from Kommander's WAL read pool: driving
        // one FairReadScheduler to its per-partition depth limit must not affect a second instance. Model the
        // two pools as two instances; saturate the "backend" one and prove the "WAL" one still serves reads.
        ILogger<IRaft> logger = NullLoggerFactory.Instance.CreateLogger<IRaft>();

        using ManualResetEventSlim gate = new(false);
        FairReadScheduler backend = new(logger, workerCount: 1, maxQueueDepthPerPartition: 2);
        FairReadScheduler wal = new(logger, workerCount: 1, maxQueueDepthPerPartition: 2);
        backend.Start();
        wal.Start();
        try
        {
            // Occupy the backend worker and fill its single partition queue so the next enqueue is rejected.
            Task blocked = backend.EnqueueTask(1, () => { gate.Wait(); return 0; });
            _ = backend.EnqueueTask(1, () => 0); // queued (depth now at the limit)
            Assert.Throws<ReadBackpressureExceededException>(() => { _ = backend.EnqueueTask(1, () => 0); });

            // The unrelated (WAL) scheduler is completely unaffected — it accepts and completes immediately.
            Assert.Equal(42, await wal.EnqueueTask(0, () => 42));

            gate.Set();
            await blocked;
        }
        finally
        {
            gate.Set();
            backend.Stop(); backend.Dispose();
            wal.Stop(); wal.Dispose();
        }
    }

    [Fact]
    public async Task StopDrainsAcceptedInFlightWork()
    {
        // KahunaManager.Dispose relies on FairReadScheduler.Stop() draining every accepted operation before
        // its threads exit — otherwise an in-flight backend read at shutdown would be silently dropped (its
        // awaiter left hanging) instead of completing or faulting deterministically. Assert the contract.
        ILogger<IRaft> logger = NullLoggerFactory.Instance.CreateLogger<IRaft>();
        FairReadScheduler scheduler = new(logger, workerCount: 1, maxQueueDepthPerPartition: 64);
        scheduler.Start();

        int ran = 0;
        // Accept work, then immediately stop: the accepted operation must still run to completion.
        Task<int> slow = scheduler.EnqueueTask(1, () => { Thread.Sleep(200); Interlocked.Increment(ref ran); return 7; });
        Task<int> queued = scheduler.EnqueueTask(1, () => { Interlocked.Increment(ref ran); return 8; });

        scheduler.Stop();

        Assert.Equal(7, await slow);
        Assert.Equal(8, await queued);
        Assert.Equal(2, ran);
        scheduler.Dispose();
    }

    private static KeyValueRequest MakeGet(string key) => new(
        KeyValueRequestType.TryGet, HLCTimestamp.Zero, HLCTimestamp.Zero, key, null, null, -1,
        KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 1, default);

    private static KeyValueRequest MakeSet(string key, string value) => new(
        KeyValueRequestType.TrySet, HLCTimestamp.Zero, HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
        KeyValueFlags.Set, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 1, default);
}
