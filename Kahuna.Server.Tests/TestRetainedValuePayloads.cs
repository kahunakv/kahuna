
using System.Text;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
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

namespace Kahuna.Server.Tests;

/// <summary>
/// Committed value payloads on the pinned object heap. With the option off the actor installs the request's
/// wire array as the head (zero copy from the wire). With it on, the head is a pinned copy with identical bytes,
/// the wire array is left to die young, and every reader keeps sharing the one stored array: the published head
/// record, the actor's own response, the revision archive and the snapshot read all hand out the same instance,
/// so the zero-copy contract between the store and the responses is unchanged. Payloads below the threshold stay
/// on the normal heap. A head install allocates nothing beyond the copy.
/// </summary>
public sealed class TestRetainedValuePayloads : RaftTrackingTest
{
    private const int MinBytes = 64;

    [Fact]
    public async Task OptionOff_TheHeadIsTheWireArray()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this, pinned: false);
        const string key = "pin/off";
        byte[] wire = Payload(512, 1);

        KeyValueResponse? set = await rig.AskAsync(Set(key, wire));
        Assert.Equal(KeyValueResponseType.Set, set!.Type);

        KeyValueEntry resident = rig.Context.Store.Get(key)!;
        Assert.Same(wire, resident.Value);
        Assert.Same(wire, resident.Published!.Value);
    }

    [Fact]
    public async Task OptionOn_TheHeadIsACopy_SharedByEveryReader()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this, pinned: true);
        const string key = "pin/on";
        byte[] wire = Payload(512, 2);

        KeyValueResponse? set = await rig.AskAsync(Set(key, wire));
        Assert.Equal(KeyValueResponseType.Set, set!.Type);

        KeyValueEntry resident = rig.Context.Store.Get(key)!;
        byte[] stored = resident.Value!;
        Assert.NotSame(wire, stored);
        Assert.Equal(wire, stored);

        // The published record and the actor's response wrap the stored array itself: no copy on read.
        Assert.Same(stored, resident.Published!.Value);

        KeyValueResponse? viaActor = await rig.AskAsync(Get(key));
        Assert.Equal(KeyValueResponseType.Get, viaActor!.Type);
        Assert.Same(stored, viaActor.Entry!.Value);

        Assert.True(rig.Reader.TryRead(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, out ReadOnlyKeyValueEntry? published));
        Assert.Same(stored, published!.Value);
    }

    [Fact]
    public async Task OptionOn_APayloadBelowTheThreshold_StaysTheWireArray()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this, pinned: true);
        const string key = "pin/small";
        byte[] wire = Payload(MinBytes - 1, 3);

        await rig.AskAsync(Set(key, wire));

        Assert.Same(wire, rig.Context.Store.Get(key)!.Value);
    }

    [Fact]
    public async Task OptionOn_RevisionAndSnapshotReads_ReturnTheArchivedBytes()
    {
        await using ActorRig rig = await ActorRig.CreateAsync(this, pinned: true);
        const string key = "pin/history";
        byte[] first = Payload(512, 4);
        byte[] second = Payload(512, 5);

        await rig.AskAsync(Set(key, first));
        KeyValueResponse? afterFirst = await rig.AskAsync(Get(key));
        HLCTimestamp firstStamp = afterFirst!.Entry!.LastModified;
        byte[] firstStored = rig.Context.Store.Get(key)!.Value!;

        await rig.AskAsync(Set(key, second));
        KeyValueEntry resident = rig.Context.Store.Get(key)!;
        Assert.Equal(second, resident.Value);
        Assert.NotSame(second, resident.Value);

        // The archive kept the first head's stored array, not a second copy of it.
        Assert.True(resident.Revisions!.TryGetValue(0, out KeyValueRevisionEntry archived));
        Assert.Same(firstStored, archived.Value);

        KeyValueResponse? byRevision = await rig.AskAsync(Get(key, revision: 0));
        Assert.Equal(KeyValueResponseType.Get, byRevision!.Type);
        Assert.Equal(first, byRevision.Entry!.Value);
        Assert.Same(firstStored, byRevision.Entry.Value);

        KeyValueRequest snapshot = Get(key);
        snapshot.ReadTimestamp = firstStamp;
        KeyValueResponse? asOf = await rig.AskAsync(snapshot);
        Assert.Equal(KeyValueResponseType.Get, asOf!.Type);
        Assert.Equal(first, asOf.Entry!.Value);
        Assert.Same(firstStored, asOf.Entry.Value);

        KeyValueResponse? latest = await rig.AskAsync(Get(key));
        Assert.Equal(second, latest!.Entry!.Value);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void HeadInstall_AllocatesNothingBeyondTheCopy(bool pinned)
    {
        (KeyValueContext context, RaftManager raft) = CreateBareContext(pinned);
        ApplyHandler handler = new(context);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        const int payloadLength = 4096;

        // Warm the path once so tiering and lazily-created statics do not count against the measured call.
        handler.Apply(new KeyValueEntry { Revision = -1 }, Proposal("alloc/warm", Payload(payloadLength, 6), now));

        KeyValueEntry entry = new() { Revision = -1 };
        KeyValueProposal proposal = Proposal("alloc/measured", Payload(payloadLength, 7), now);

        long before = GC.GetAllocatedBytesForCurrentThread();
        handler.Apply(entry, proposal);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(proposal.Value, entry.Value);
        if (pinned)
        {
            Assert.NotSame(proposal.Value, entry.Value);
            Assert.InRange(allocated, payloadLength, payloadLength + 128);
        }
        else
        {
            Assert.Same(proposal.Value, entry.Value);
            Assert.InRange(allocated, 0, 128);
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private static byte[] Payload(int length, byte seed)
    {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++)
            bytes[i] = (byte)(seed + i);
        return bytes;
    }

    private static KeyValueRequest Set(string key, byte[] value) =>
        new(KeyValueRequestType.TrySet, HLCTimestamp.Zero, HLCTimestamp.Zero, key, value, null, -1,
            KeyValueFlags.Set, 0, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, 0, 0, default);

    private static KeyValueRequest Get(string key, long revision = -1) =>
        new(KeyValueRequestType.TryGet, HLCTimestamp.Zero, HLCTimestamp.Zero, key, null, null, revision,
            KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, 0, 0, default);

    private static KeyValueProposal Proposal(string key, byte[] value, HLCTimestamp now) =>
        new(KeyValueRequestType.TrySet, key, value, 0, noRevision: false, HLCTimestamp.Zero, now, now,
            KeyValueState.Set, KeyValueDurability.Persistent);

    private static KahunaConfiguration Configuration(bool pinned) => ConfigurationValidator.Validate(new()
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
        RevisionRetention = 16,
        PinnedValuePayloads = pinned,
        PinnedValuePayloadMinBytes = MinBytes
    });

    private (KeyValueContext, RaftManager) CreateBareContext(bool pinned)
    {
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = Track(new RaftManager(
            new RaftConfiguration
            {
                NodeName = "retained-payloads",
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
            raftLogger));

        KeyValueContext context = new(
            null!,
            new KeyValueStore(),
            new Dictionary<string, KeyValueWriteIntent>(),
            new Dictionary<string, List<KeyValueRangeLock>>(),
            new Dictionary<int, KeyValueProposal>(),
            null!,
            null!,
            new MemoryPersistenceBackend(),
            raft,
            raft.ReadScheduler,
            new KeySpaceRegistry(),
            new RangeMapStore(raft, null, null, logger),
            Configuration(pinned),
            logger);

        return (context, raft);
    }

    /// <summary>Exposes the shared head-advance routine so a test can drive one install directly.</summary>
    private sealed class ApplyHandler(KeyValueContext context) : BaseHandler(context)
    {
        public void Apply(KeyValueEntry entry, KeyValueProposal proposal) =>
            ApplyCommittedHead(entry, proposal, HLCTimestamp.Zero);
    }

    /// <summary>One real ephemeral key-value actor behind a one-actor ring, with the published-head reader wired to it.</summary>
    private sealed class ActorRig : IAsyncDisposable
    {
        public KeyValueActorRing Ring = null!;
        public KeyValueContext Context = null!;
        public PublishedHeadReader Reader = null!;
        private IDisposable actorSystemLifetime = null!;

        public static async Task<ActorRig> CreateAsync(TestRetainedValuePayloads owner, bool pinned)
        {
            ActorRig rig = new();
            KahunaConfiguration config = Configuration(pinned);

            ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;
            ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;

            RaftManager raft = owner.Track(new RaftManager(
                new RaftConfiguration
                {
                    NodeName = "retained-payloads-rig",
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
                raftLogger));

            rig.actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);

            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    "retained-payloads-kv",
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
            for (int i = 0; i < 500 && instance is null; i++)
            {
                instance = ((ActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>)actorRef).Runner.Actor as KeyValueActor;
                if (instance is null)
                    await Task.Delay(10);
            }

            rig.Context = instance!.GetContext();
            rig.Ring = new KeyValueActorRing([actorRef], [rig.Context.PublishedHeads]);

            KeyValueActorRing other = new([actorRef], [new PublishedHeadTable()]);
            rig.Reader = new PublishedHeadReader(
                rig.Ring,
                other,
                new Persistence.UnflushedKeyValueWritesIndex(),
                new KeyValues.Transactions.PreparedIntentStore(),
                raft,
                enabled: true);

            return rig;
        }

        public Task<KeyValueResponse?> AskAsync(KeyValueRequest request) => Ring.Ask(request);

        public ValueTask DisposeAsync()
        {
            actorSystemLifetime.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
