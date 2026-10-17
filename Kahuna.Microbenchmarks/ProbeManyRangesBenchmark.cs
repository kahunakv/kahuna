using System.Text;
using BenchmarkDotNet.Attributes;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
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

namespace Kahuna.Microbenchmarks;

/// <summary>
/// One real key-value actor over a started read scheduler, asked for 64 bounded seeks of one key space, 11 rows each
/// (the non-unique index probes of 64 outer rows of a join), the two ways they can reach it: one range page per seek
/// with every page in flight at once, and one batched range probe that answers all the seeks in one actor turn.
/// <para>Three backends: the in-memory backend, which reads pages on the actor thread; the same data behind a wrapper
/// that answers "not in memory", which forces the scheduler path; and RocksDB. On the scheduler path a single page
/// costs one actor turn, one scheduler hand-off and one resume turn; the probe costs one of each for the whole batch.
/// The work-item column (<see cref="ThreadingDiagnoserAttribute"/>) and the allocation column show the difference.
/// Range pages never make rows resident, so every invocation reads the backend.</para>
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class ProbeManyRangesBenchmark : IDisposable
{
    public enum BackendKind
    {
        MemoryOnActor,
        MemoryOnScheduler,
        RocksDb
    }

    private const int Seeks = 64;

    private const int Keys = 1_024;

    private const int RowsPerKey = 11;

    private const string Space = "bench/seek";

    private const string Prefix = Space + "/";

    [Params(BackendKind.MemoryOnActor, BackendKind.MemoryOnScheduler, BackendKind.RocksDb)]
    public BackendKind Backend;

    private RaftManager raft = null!;

    private IDisposable actorSystemLifetime = null!;

    private KeyValueActorRing ring = null!;

    private IPersistenceBackend backend = null!;

    private string? directory;

    private KeyValueProbeRange[][] batches = null!;

    private int next;

    [GlobalSetup]
    public void Setup()
    {
        KahunaConfiguration config = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            CacheEntryTtl = TimeSpan.FromMinutes(30),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = 5_000_000,
            MaxBytesPerActor = 4L * 1024 * 1024 * 1024,
            CollectBatchMax = 1000,
            RevisionRetention = 16
        });

        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;

        raft = new RaftManager(
            new RaftConfiguration
            {
                NodeName = "probe-ranges-bench-" + Backend,
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false,
                PartitionExecutorPoolSize = 1
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger);

        ((FairReadScheduler)raft.ReadScheduler).Start();

        switch (Backend)
        {
            case BackendKind.RocksDb:
                directory = Path.Combine(Path.GetTempPath(), "kahuna-seek-bench-" + Guid.NewGuid().ToString("N")[..8]);
                Directory.CreateDirectory(directory);
                backend = new RocksDbPersistenceBackend(directory, "bench");
                break;
            case BackendKind.MemoryOnScheduler:
                backend = new SchedulerOnlyBackend(new MemoryPersistenceBackend());
                break;
            default:
                backend = new MemoryPersistenceBackend();
                break;
        }

        Seed(backend);

        ActorSystem actorSystem = new();
        actorSystemLifetime = actorSystem;

        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor = actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
            "probe-ranges-bench-kv", null!, null!, backend, raft, raft.ReadScheduler,
            new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

        KeyValueActor? instance = null;
        while (instance is null)
        {
            instance = ((ActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>)actor).Runner.Actor as KeyValueActor;
            if (instance is null)
                Thread.Sleep(10);
        }

        ring = new KeyValueActorRing([actor], [instance.GetContext().PublishedHeads]);

        batches = new KeyValueProbeRange[Keys / Seeks][];
        for (int b = 0; b < batches.Length; b++)
        {
            batches[b] = new KeyValueProbeRange[Seeks];
            for (int s = 0; s < Seeks; s++)
            {
                int key = b * Seeks + s;
                batches[b][s] = new($"{Prefix}k{key:D4}/", true, $"{Prefix}k{key:D4}0", false, 64);
            }
        }

        // One batch each way before measuring, so the actor's code paths are warm.
        SinglePagesPerSeek().GetAwaiter().GetResult();
        OneBatchedRangeProbe().GetAwaiter().GetResult();
    }

    private static void Seed(IPersistenceBackend backend)
    {
        byte[] value = Encoding.UTF8.GetBytes("{\"id\":1,\"name\":\"range probe benchmark row\",\"balance\":1000}");
        List<PersistenceRequestItem> rows = new(Keys * RowsPerKey);

        for (int key = 0; key < Keys; key++)
            for (int row = 0; row < RowsPerKey; row++)
                rows.Add(new PersistenceRequestItem(
                    key: $"{Prefix}k{key:D4}/{row:D6}", value: value, revision: 1,
                    expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
                    lastUsedNode: 0, lastUsedPhysical: 1000, lastUsedCounter: 0,
                    lastModifiedNode: 0, lastModifiedPhysical: 1000, lastModifiedCounter: 0,
                    state: (int)KeyValueState.Set));

        if (!backend.StoreKeyValues(rows))
            throw new InvalidOperationException("seed failed");
    }

    /// <summary>Every seek read through one range page each, all in flight at once.</summary>
    [Benchmark(Baseline = true)]
    public async Task<int> SinglePagesPerSeek()
    {
        KeyValueProbeRange[] seeks = batches[next++ % batches.Length];

        Task<int>[] pages = new Task<int>[seeks.Length];
        for (int i = 0; i < seeks.Length; i++)
            pages[i] = Page(seeks[i]);

        int rows = 0;
        foreach (Task<int> page in pages)
            rows += await page;

        return rows;
    }

    /// <summary>Every seek read through one batched range probe.</summary>
    [Benchmark]
    public async Task<int> OneBatchedRangeProbe()
    {
        KeyValueProbeRange[] seeks = batches[next++ % batches.Length];

        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryProbeManyRanges, HLCTimestamp.Zero, HLCTimestamp.Zero, Prefix, null, null, -1,
            KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, default);

        request.ProbeRanges = seeks;

        try
        {
            KeyValueResponse? response = await KeyValueActorRouters.AskKeyValueActor(ring, request);
            KeyValueProbeRangeResult[] results = response?.ProbeRangeResults ?? throw new InvalidOperationException("probe refused");

            int rows = 0;
            foreach (KeyValueProbeRangeResult result in results)
            {
                if (result.Items.Count != RowsPerKey)
                    throw new InvalidOperationException($"seek answered {result.Type} with {result.Items.Count} rows");
                rows += result.Items.Count;
            }

            return rows;
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    private async Task<int> Page(KeyValueProbeRange seek)
    {
        KeyValueRequest request = KeyValueRequestPool.RentRange(
            HLCTimestamp.Zero, Prefix, seek.StartKey, seek.StartInclusive, seek.EndKey, seek.EndInclusive, seek.Limit,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, default);

        try
        {
            KeyValueResponse? response = await KeyValueActorRouters.AskKeyValueActor(ring, request);

            if (response?.Type != KeyValueResponseType.Get || response.RangeResult!.Items.Count != RowsPerKey)
                throw new InvalidOperationException("scan page refused: " + response?.Type);

            return response.RangeResult.Items.Count;
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    [GlobalCleanup]
    public void Cleanup() => Dispose();

    public void Dispose()
    {
        ((FairReadScheduler)raft.ReadScheduler).Stop();
        actorSystemLifetime.Dispose();
        raft.Dispose();
        (backend as IDisposable)?.Dispose();

        if (directory is not null)
        {
            try
            {
                Directory.Delete(directory, recursive: true);
            }
            catch (IOException)
            {
                // Best effort: the temp directory is reclaimed by the system otherwise.
            }
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// The in-memory backend behind a wrapper that answers "not in memory", so the handlers take the scheduler path
    /// over the same data. Every read forwards to the inner backend, the bounded page read included.
    /// </summary>
    private sealed class SchedulerOnlyBackend(MemoryPersistenceBackend inner) : IPersistenceBackend, IDisposable
    {
        public bool ServesReadsFromMemory => false;

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public LockEntry? GetLock(string resource) => inner.GetLock(resource);
        public KeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public KeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public KeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            inner.GetKeyValueRevisionAtOrBefore(keyName, maxRevision, readTimestamp);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) => inner.GetKeyValueByPrefix(prefixKeyName);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            inner.GetKeyValueByRange(prefix, startKey, limit);
        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(
            string prefix, string? startKey, string? endKey, bool endInclusive, int limit, ref KeyValueScanTally tally) =>
            inner.GetKeyValueByRange(prefix, startKey, endKey, endInclusive, limit, ref tally);
        public bool PruneKeyValueRevisions(IReadOnlyCollection<string>? keys, int retainCount, TimeSpan retainAge, int batchSize, HLCTimestamp floor, out RevisionPruneResult result) =>
            inner.PruneKeyValueRevisions(keys, retainCount, retainAge, batchSize, floor, out result);
        public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            inner.CreateCheckpoint(destinationPath, appliedIndex, appliedTime);
        public void Dispose() => inner.Dispose();
    }
}
