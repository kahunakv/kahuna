using System.Text;
using BenchmarkDotNet.Attributes;
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

namespace Kahuna.Microbenchmarks;

/// <summary>
/// One real key-value actor over a started read scheduler, read the two ways a batch of point probes can reach it:
/// one ask per key with every ask in flight at once (what a batch get does on the leader today), and one batched
/// probe that answers all the keys in one actor turn. Each batch is 64 keys of one key space.
/// <para>A single ask per key costs one actor turn and at least two thread-pool work items; a key that is not
/// resident adds a scheduler hand-off and a resume turn. The probe costs one turn for the batch, plus one hand-off
/// and one resume turn when any key is not resident. The work-item column (<see cref="ThreadingDiagnoserAttribute"/>)
/// and the allocation column show the difference; the actor turns are 64 against 1 by construction.</para>
/// </summary>
public abstract class ProbeManyBenchmarkBase : IDisposable
{
    protected const int BatchSize = 64;

    protected const string Space = "bench/probe";

    private IDisposable actorSystemLifetime = null!;

    private protected RaftManager Raft { get; private set; } = null!;

    private protected KeyValueActorRing Ring { get; private set; } = null!;

    private protected IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> Actor { get; private set; } = null!;

    private protected IPersistenceBackend Backend { get; private set; } = null!;

    private protected readonly HLCTimestamp LockOwner = new(1, 1, 1);

    private protected void Start(string name, IPersistenceBackend backend)
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

        Raft = new RaftManager(
            new RaftConfiguration
            {
                NodeName = name,
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

        ((FairReadScheduler)Raft.ReadScheduler).Start();

        Backend = backend;

        ActorSystem actorSystem = new();
        actorSystemLifetime = actorSystem;

        Actor = actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
            name + "-kv", null!, null!, backend, Raft, Raft.ReadScheduler,
            new KeySpaceRegistry(), new RangeMapStore(Raft, null, null, logger), config, logger);

        KeyValueActor? instance = null;
        while (instance is null)
        {
            instance = ((ActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>)Actor).Runner.Actor as KeyValueActor;
            if (instance is null)
                Thread.Sleep(10);
        }

        Ring = new KeyValueActorRing([Actor], [instance.GetContext().PublishedHeads]);
    }

    private protected static string[] KeyWindow(int first, int count)
    {
        string[] keys = new string[count];
        for (int i = 0; i < count; i++)
            keys[i] = Space + "/" + (first + i).ToString("D8");
        return keys;
    }

    private protected void Seed(string[] keys)
    {
        byte[] value = Encoding.UTF8.GetBytes("{\"id\":1,\"name\":\"probe benchmark row\",\"balance\":1000}");
        List<PersistenceRequestItem> rows = new(keys.Length);

        foreach (string key in keys)
            rows.Add(new PersistenceRequestItem(
                key: key, value: value, revision: 1,
                expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
                lastUsedNode: 0, lastUsedPhysical: 1000, lastUsedCounter: 0,
                lastModifiedNode: 0, lastModifiedPhysical: 1000, lastModifiedCounter: 0,
                state: (int)KeyValueState.Set));

        if (!Backend.StoreKeyValues(rows))
            throw new InvalidOperationException("seed failed");
    }

    /// <summary>Every key read through one ask each, all in flight at once.</summary>
    private protected async Task<long> SingleAsks(string[] keys, bool sharedLock)
    {
        Task<long>[] reads = new Task<long>[keys.Length];
        for (int i = 0; i < keys.Length; i++)
            reads[i] = SingleAsk(keys[i], sharedLock);

        long revisions = 0;
        foreach (Task<long> read in reads)
            revisions += await read;

        return revisions;
    }

    private async Task<long> SingleAsk(string key, bool sharedLock)
    {
        if (sharedLock)
        {
            KeyValueRequest lockRequest = KeyValueRequestPool.Rent(
                KeyValueRequestType.TryAcquireExclusiveRangeLock, LockOwner, HLCTimestamp.Zero, Space, null, null, -1,
                KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, default);

            lockRequest.StartKey = key;
            lockRequest.StartInclusive = true;
            lockRequest.EndKey = key;
            lockRequest.EndInclusive = true;
            lockRequest.RangeLockMode = RangeLockMode.Shared;

            try
            {
                KeyValueResponse? granted = await KeyValueActorRouters.AskKeyValueActor(Ring, lockRequest);
                if (granted?.Type != KeyValueResponseType.Locked)
                    throw new InvalidOperationException("lock refused for " + key);
            }
            finally
            {
                KeyValueRequestPool.Return(lockRequest);
            }
        }

        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryGet, sharedLock ? LockOwner : HLCTimestamp.Zero, HLCTimestamp.Zero, key, null, null, -1,
            KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, default);

        try
        {
            KeyValueResponse? response = await KeyValueActorRouters.AskKeyValueActor(Ring, request);
            return response?.Entry?.Revision ?? -1;
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    /// <summary>Every key read through one batched probe.</summary>
    private protected async Task<long> OneProbe(string[] keys, bool sharedLock)
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryProbeMany, sharedLock ? LockOwner : HLCTimestamp.Zero, HLCTimestamp.Zero, keys[0], null, null, -1,
            KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, default);

        request.ProbeKeys = keys;
        request.ProbeLock = sharedLock ? KeyValueProbeLock.Shared : KeyValueProbeLock.None;

        try
        {
            KeyValueResponse? response = await KeyValueActorRouters.AskKeyValueActor(Ring, request);
            KeyValueProbeResult[] results = response?.ProbeResults ?? throw new InvalidOperationException("probe refused");

            long revisions = 0;
            foreach (KeyValueProbeResult result in results)
                revisions += result.Entry?.Revision ?? -1;

            return revisions;
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    private protected void EvictAll()
    {
        KeyValueRequest evict = new(
            KeyValueRequestType.EvictPartition, HLCTimestamp.Zero, HLCTimestamp.Zero, Space + "/x", null, null, -1,
            KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 1, default);

        Actor.Ask(evict).GetAwaiter().GetResult();
    }

    public virtual void Dispose()
    {
        ((FairReadScheduler)Raft.ReadScheduler).Stop();
        actorSystemLifetime.Dispose();
        Raft.Dispose();
        GC.SuppressFinalize(this);
    }
}

/// <summary>64 resident keys: the per-key fixed cost a probe batch removes, with and without a Shared lock per key.</summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class ProbeManyResidentBenchmark : ProbeManyBenchmarkBase
{
    private string[] keys = null!;

    [Params(false, true)]
    public bool SharedLock;

    [GlobalSetup]
    public void Setup()
    {
        Start("probe-resident-bench", new MemoryPersistenceBackend());
        keys = KeyWindow(0, BatchSize);
        Seed(keys);

        // Make every key resident and, when locking, take the locks once, so each measured call is a re-entry.
        OneProbe(keys, SharedLock).GetAwaiter().GetResult();
    }

    [Benchmark(Baseline = true)]
    public Task<long> SingleAsksPerKey() => SingleAsks(keys, SharedLock);

    [Benchmark]
    public Task<long> OneBatchedProbe() => OneProbe(keys, SharedLock);

    [GlobalCleanup]
    public void Cleanup() => Dispose();
}

/// <summary>
/// 64 keys on a RocksDB backend, half of them not resident: the probe reads all its misses with one
/// <c>MultiGet</c> and resumes once, where single asks pay a scheduler hand-off and a resume turn per miss. Every
/// iteration evicts the actor's store and makes the warm half resident again, so each measured batch meets the
/// same mix; an invocation reads <see cref="Windows"/> distinct batches.
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class ProbeManyHalfColdBenchmark : ProbeManyBenchmarkBase
{
    private const int Windows = 32;

    private string directory = null!;

    private string[][] warm = null!;

    private string[][] batches = null!;

    [GlobalSetup]
    public void Setup()
    {
        directory = Path.Combine(Path.GetTempPath(), "kahuna-probe-bench-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(directory);

        Start("probe-cold-bench", new RocksDbPersistenceBackend(directory, "bench"));

        warm = new string[Windows][];
        batches = new string[Windows][];

        for (int w = 0; w < Windows; w++)
        {
            string[] window = KeyWindow(w * BatchSize, BatchSize);
            Seed(window);

            // Interleave warm and cold keys, as the outer keys of a join arrive.
            warm[w] = [.. window.Where((_, i) => i % 2 == 0)];
            batches[w] = window;
        }
    }

    [IterationSetup]
    public void MakeHalfResident()
    {
        EvictAll();

        foreach (string[] keys in warm)
            OneProbe(keys, sharedLock: false).GetAwaiter().GetResult();
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = Windows)]
    public async Task<long> SingleAsksPerKey()
    {
        long revisions = 0;
        foreach (string[] keys in batches)
            revisions += await SingleAsks(keys, sharedLock: false);
        return revisions;
    }

    [Benchmark(OperationsPerInvoke = Windows)]
    public async Task<long> OneBatchedProbe()
    {
        long revisions = 0;
        foreach (string[] keys in batches)
            revisions += await OneProbe(keys, sharedLock: false);
        return revisions;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        Dispose();
        (Backend as IDisposable)?.Dispose();

        try
        {
            Directory.Delete(directory, recursive: true);
        }
        catch (IOException)
        {
            // Best effort: the temp directory is reclaimed by the system otherwise.
        }
    }
}
