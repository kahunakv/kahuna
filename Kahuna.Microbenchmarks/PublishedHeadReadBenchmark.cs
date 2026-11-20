
using System.Text;
using BenchmarkDotNet.Attributes;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// A resident latest point get of a quiet key, served two ways against one real key-value actor: through the
/// actor's mailbox (rent a request, ask, await the pooled reply) and from the actor's published head record on
/// the calling thread. The keys are set once through the actor, so both paths read the same resident state;
/// the actor path also touches the entry, as it does in production. <c>Readers</c> issues that many gets at
/// once per operation, so the mailbox queueing the published path removes shows up at more than one caller.
/// With <c>Pinned</c> the gets are the repeat reads of one transaction that read every key before: the actor
/// answers them from the transaction's pin, and the record answers them because it shows that pin at its head.
/// </summary>
[MemoryDiagnoser]
public class PublishedHeadReadBenchmark : IDisposable
{
    private const int Keys = 1024;

    [Params(1, 8)]
    public int Readers;

    [Params(false, true)]
    public bool Pinned;

    // The transaction whose repeat reads are measured, or zero for plain gets.
    private HLCTimestamp transaction;

    private IDisposable actorSystemLifetime = null!;
    private RaftManager raft = null!;
    private KeyValueActorRing ring = null!;
    private PublishedHeadReader reader = null!;
    private string[] keys = null!;
    private Task<KeyValueResponse?>[] inFlight = null!;
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
            CacheEntryTtl = TimeSpan.FromMinutes(5),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = 50_000,
            MaxBytesPerActor = 256L * 1024 * 1024,
            CollectBatchMax = 1000,
            RevisionRetention = 16
        });

        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;

        raft = new RaftManager(
            new RaftConfiguration
            {
                NodeName = "published-head-bench",
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

        ActorSystem actorSystem = new();
        actorSystemLifetime = actorSystem;

        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
            actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                "published-head-bench-kv",
                null!,
                null!,
                new MemoryPersistenceBackend(),
                raft,
                raft.ReadScheduler,
                new KeySpaceRegistry(),
                new RangeMapStore(raft, null, null, logger),
                config,
                logger);

        KeyValueActor? instance = null;
        while (instance is null)
        {
            instance = ((ActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>)actorRef).Runner.Actor as KeyValueActor;
            if (instance is null)
                Thread.Sleep(10);
        }

        PublishedHeadTable table = instance.GetContext().PublishedHeads;
        ring = new KeyValueActorRing([actorRef], [table]);
        reader = new PublishedHeadReader(ring, ring, null, new PreparedIntentStore(), raft, enabled: true);

        keys = new string[Keys];
        byte[] value = Encoding.UTF8.GetBytes("{\"balance\":1000,\"owner\":\"bench\"}");
        for (int i = 0; i < Keys; i++)
        {
            keys[i] = "bench/account/" + i;
            KeyValueResponse? set = ring.Ask(new KeyValueRequest(
                KeyValueRequestType.TrySet, HLCTimestamp.Zero, HLCTimestamp.Zero, keys[i], value, null, -1,
                KeyValueFlags.Set, 0, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, 0, 0, default)).GetAwaiter().GetResult();

            if (set?.Type != KeyValueResponseType.Set)
                throw new InvalidOperationException("seed failed for " + keys[i]);
        }

        for (int i = 0; i < Keys; i++)
        {
            if (!reader.TryRead(HLCTimestamp.Zero, keys[i], -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _))
                throw new InvalidOperationException("record not published for " + keys[i]);
        }

        if (Pinned)
        {
            // One transaction reads every key once. Each read sends a pin to the actor; the gets measured below
            // are its repeat reads, after every pin landed.
            transaction = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

            for (int i = 0; i < Keys; i++)
            {
                if (!reader.TryRead(transaction, keys[i], -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out _))
                    throw new InvalidOperationException("first read not served for " + keys[i]);
            }

            for (int i = 0; i < Keys; i++)
            {
                while (!(table.TryGet(keys[i], out PublishedHead head) && !head.IsRetracted && head.MvccStateOf(transaction) == PublishedMvccState.PinAtHead))
                    Thread.Sleep(1);
            }
        }

        inFlight = new Task<KeyValueResponse?>[8];
    }

    private string NextKey() => keys[(next++) & (Keys - 1)];

    [Benchmark(Baseline = true)]
    public async Task<long> ThroughTheActor()
    {
        if (Readers == 1)
        {
            KeyValueResponse? response = await AskActor(NextKey());
            return response!.Entry!.Revision;
        }

        for (int i = 0; i < Readers; i++)
            inFlight[i] = AskActor(NextKey()).AsTask();

        long revisions = 0;
        for (int i = 0; i < Readers; i++)
            revisions += (await inFlight[i])!.Entry!.Revision;

        return revisions;
    }

    [Benchmark]
    public long FromThePublishedRecord()
    {
        long revisions = 0;
        for (int i = 0; i < Readers; i++)
        {
            if (!reader.TryRead(transaction, NextKey(), -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, KeyValueReadMode.Default, out ReadOnlyKeyValueEntry? entry))
                throw new InvalidOperationException("record not served");

            revisions += entry!.Revision;
        }

        return revisions;
    }

    private async ValueTask<KeyValueResponse?> AskActor(string key)
    {
        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TryGet, transaction, HLCTimestamp.Zero, key, null, null, -1,
            KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, 0, 0, default);

        try
        {
            return await KeyValueActorRouters.AskKeyValueActor(ring, request);
        }
        finally
        {
            KeyValueRequestPool.Return(request);
        }
    }

    [GlobalCleanup]
    public void Dispose()
    {
        actorSystemLifetime.Dispose();
        raft.Dispose();
        GC.SuppressFinalize(this);
    }
}
