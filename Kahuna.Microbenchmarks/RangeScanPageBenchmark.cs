using System.Text;
using BenchmarkDotNet.Attributes;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
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
/// One real key-value actor over a started backend read scheduler and the in-memory backend, asked for one
/// persistent range scan page per invocation, read the two ways a page can reach the backend: on a scheduler
/// worker and back to the actor through a thread-pool continuation and a resume message (the path every
/// backend took, and the disk backends still take), or on the actor thread inside the turn that received the
/// request (the in-memory backend now). The scheduler path is forced by a wrapper that hides the backend's
/// in-memory nature from the handler; the data and the page are the same either way.
/// <para>Two page shapes: a bounded seek of one index key with 11 rows (a join probe), and a 512-row page of a
/// prefix scan continued from a cursor (a table scan). The hand-offs show in the work-item and lock-contention
/// columns of <see cref="ThreadingDiagnoserAttribute"/>; the latency shows in the mean.</para>
/// <para>The prepared-intent store the page's merge consults holds <see cref="LiveIntentsOutsideWindow"/> live
/// intents on keys no page touches: the cost a page pays for intents it never meets.</para>
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class RangeScanPageBenchmark : IDisposable
{
    private const string Space = "bench/scan";

    private const string Prefix = Space + "/";

    private const int Keys = 1_000;

    private const int RowsPerKey = 11;

    private const int PageSize = 512;

    /// <summary>True reads the page on the actor thread (the in-memory backend); false forces the scheduler path.</summary>
    [Params(false, true)]
    public bool ReadOnActor;

    /// <summary>Live prepared intents in the store, all on keys outside every page the benchmark reads.</summary>
    [Params(0, 1_000)]
    public int LiveIntentsOutsideWindow;

    private RaftManager raft = null!;

    private IDisposable actorSystemLifetime = null!;

    private KeyValueActorRing ring = null!;

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
                NodeName = "range-page-bench-" + ReadOnActor,
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

        MemoryPersistenceBackend memory = new();
        Seed(memory);

        IPersistenceBackend backend = ReadOnActor ? memory : new SchedulerOnlyBackend(memory);

        PreparedIntentStore intentStore = new();
        for (int i = 0; i < LiveIntentsOutsideWindow; i++)
        {
            string key = $"bench/other/{i:D6}";
            PreparedIntent intent = new(
                TransactionId: new HLCTimestamp(0, 1_000 + i, 0), Epoch: 1, Key: key, ManifestHash: 1, RecordAnchorKey: key,
                CommitTimestamp: new HLCTimestamp(0, 2_000 + i, 0), State: KeyValueState.Set, Value: [1], Bucket: "bench/other",
                Revision: 1, Expires: HLCTimestamp.Zero, NoRevision: false, BaseRevision: 0, BaseState: KeyValueState.Set,
                RecoveryDeadline: new HLCTimestamp(0, 9_000_000, 0), Resolution: PreparedIntentResolution.Committed);

            if (intentStore.Apply(new PrepareIntentCommand(intent)).Outcome != TransactionApplyOutcome.Applied)
                throw new InvalidOperationException("intent seed failed for " + key);
        }

        ActorSystem actorSystem = new();
        actorSystemLifetime = actorSystem;

        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor = actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
            "range-page-bench-kv", null!, null!, backend, raft, raft.ReadScheduler,
            new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger,
            null!, null!, intentStore, null!);

        KeyValueActor? instance = null;
        while (instance is null)
        {
            instance = ((ActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>)actor).Runner.Actor as KeyValueActor;
            if (instance is null)
                Thread.Sleep(10);
        }

        ring = new KeyValueActorRing([actor], [instance.GetContext().PublishedHeads]);

        // One page of each shape before measuring, so the actor's code paths are warm.
        BoundedSeek11Rows().GetAwaiter().GetResult();
        CursorPage512Rows().GetAwaiter().GetResult();
    }

    private static void Seed(IPersistenceBackend backend)
    {
        byte[] value = Encoding.UTF8.GetBytes("{\"id\":1,\"name\":\"range page benchmark row\",\"balance\":1000}");
        List<PersistenceRequestItem> rows = new(Keys * RowsPerKey);

        for (int key = 0; key < Keys; key++)
            for (int row = 0; row < RowsPerKey; row++)
                rows.Add(new PersistenceRequestItem(
                    key: RowKey(key, row), value: value, revision: 1,
                    expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
                    lastUsedNode: 0, lastUsedPhysical: 1000, lastUsedCounter: 0,
                    lastModifiedNode: 0, lastModifiedPhysical: 1000, lastModifiedCounter: 0,
                    state: (int)KeyValueState.Set));

        if (!backend.StoreKeyValues(rows))
            throw new InvalidOperationException("seed failed");
    }

    private static string RowKey(int key, int row) => $"{Prefix}k{key:D4}/{row:D6}";

    /// <summary>One index key's 11 rows: the bounded seek a join probe issues, page size 512.</summary>
    [Benchmark]
    public Task<int> BoundedSeek11Rows()
    {
        int key = next++ % Keys;
        string start = $"{Prefix}k{key:D4}/";
        string end = $"{Prefix}k{key:D4}0";
        return Scan(start, true, end, false);
    }

    /// <summary>A 512-row page of a prefix scan continued from a cursor (an exclusive start at a row that is present).</summary>
    [Benchmark]
    public Task<int> CursorPage512Rows()
    {
        int key = next++ % (Keys - (PageSize / RowsPerKey) - 1);
        return Scan(RowKey(key, RowsPerKey - 1), false, null, false);
    }

    private async Task<int> Scan(string startKey, bool startInclusive, string? endKey, bool endInclusive)
    {
        KeyValueRequest request = KeyValueRequestPool.RentRange(
            HLCTimestamp.Zero, Prefix, startKey, startInclusive, endKey, endInclusive, PageSize,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, default);

        try
        {
            KeyValueResponse? response = await KeyValueActorRouters.AskKeyValueActor(ring, request);

            if (response?.Type != KeyValueResponseType.Get)
                throw new InvalidOperationException("scan page refused: " + response?.Type);

            return response.RangeResult!.Items.Count;
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
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// The in-memory backend behind a wrapper that answers "not in memory", so the handler takes the scheduler
    /// path over the same data. Every read forwards to the inner backend, the bounded page read included, so the
    /// two paths differ only in where the page is read.
    /// </summary>
    private sealed class SchedulerOnlyBackend(MemoryPersistenceBackend inner) : IPersistenceBackend
    {
        public bool ServesReadsFromMemory => false;

        public bool StoreLocks(List<PersistenceRequestItem> items) => inner.StoreLocks(items);
        public bool StoreKeyValues(List<PersistenceRequestItem> items) => inner.StoreKeyValues(items);
        public LockRow? GetLock(string resource) => inner.GetLock(resource);
        public ReadOnlyKeyValueEntry? GetKeyValue(string keyName) => inner.GetKeyValue(keyName);
        public ReadOnlyKeyValueEntry? GetKeyValueRevision(string keyName, long revision) => inner.GetKeyValueRevision(keyName, revision);
        public ReadOnlyKeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
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
    }
}
