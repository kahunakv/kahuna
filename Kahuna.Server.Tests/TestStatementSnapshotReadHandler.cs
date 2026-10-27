
using System.Text;
using Google.Protobuf;
using Kahuna.Server.Communication.Internode.Grpc;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.Communication.Grpc;
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
/// The statement read mode at the actor and on the wire: the safe-time wait behind a foreign staged write, which
/// the routed read loop would otherwise hide behind its retries; the handler-level refusal of a statement read
/// without a timestamp; the mode on every forwarded request; and the operation digest that binds it.
/// </summary>
public sealed class TestStatementSnapshotReadHandler : RaftTrackingTest
{
    private const string Space = "stmt";

    private static string Key(string name) => $"{Space}/{name}";

    /// <summary>
    /// A foreign transaction staged a write on the key, so the key holds a live write intent whose commit
    /// timestamp will be at or above the staged write's stamp. A statement read at a timestamp at or above that
    /// stamp waits (<c>WaitingForReplication</c>): the commit may still land at or before the snapshot. A statement
    /// read at a timestamp below the stamp cannot be overtaken by the commit and reads the committed state, even
    /// when its timestamp is above the writer's transaction id. The writer's own statement read answers its
    /// staged write and waits for nothing.
    /// </summary>
    [Fact]
    public async Task ForeignStagedWrite_WaitsWhenItMayCommitInsideTheSnapshot_FallsThroughOtherwise()
    {
        await using ActorRig rig = ActorRig.Create(this, "stmt-safe-time");
        string key = Key("k");

        KeyValueResponse? seeded = await rig.SetEphemeral(key, "v1");
        Assert.Equal(KeyValueResponseType.Set, seeded!.Type);

        HLCTimestamp beforeWriter = rig.Now();
        HLCTimestamp writer = rig.Now();
        HLCTimestamp reader = rig.Now();
        HLCTimestamp beforeStage = rig.Now();

        KeyValueResponse? staged = await rig.SetTransactional(key, "staged", writer);
        Assert.Equal(KeyValueResponseType.Set, staged!.Type);

        HLCTimestamp afterStage = rig.Now();

        KeyValueResponse? below = await rig.Get(key, reader, beforeWriter, KeyValueReadMode.StatementSnapshot);
        Assert.Equal(KeyValueResponseType.Get, below!.Type);
        Assert.Equal("v1", Encoding.UTF8.GetString(below.Entry!.Value!));

        // Above the writer's transaction id but below its staged write: the commit lands at or above the stamp,
        // so it can never overtake this snapshot, and the read falls through to the committed state.
        KeyValueResponse? belowStage = await rig.Get(key, reader, beforeStage, KeyValueReadMode.StatementSnapshot);
        Assert.Equal(KeyValueResponseType.Get, belowStage!.Type);
        Assert.Equal("v1", Encoding.UTF8.GetString(belowStage.Entry!.Value!));

        KeyValueResponse? above = await rig.Get(key, reader, afterStage, KeyValueReadMode.StatementSnapshot);
        Assert.Equal(KeyValueResponseType.WaitingForReplication, above!.Type);

        KeyValueResponse? own = await rig.Get(key, writer, afterStage, KeyValueReadMode.StatementSnapshot);
        Assert.Equal(KeyValueResponseType.Get, own!.Type);
        Assert.Equal("staged", Encoding.UTF8.GetString(own.Entry!.Value!));

        // The batched probe applies the same rules per key.
        KeyValueResponse probe = await rig.Probe([key], reader, afterStage, KeyValueReadMode.StatementSnapshot);
        Assert.Equal(KeyValueResponseType.WaitingForReplication, probe.ProbeResults![0].Type);

        KeyValueResponse belowStageProbe = await rig.Probe([key], reader, beforeStage, KeyValueReadMode.StatementSnapshot);
        Assert.Equal(KeyValueResponseType.Get, belowStageProbe.ProbeResults![0].Type);

        KeyValueResponse ownProbe = await rig.Probe([key], writer, afterStage, KeyValueReadMode.StatementSnapshot);
        Assert.Equal(KeyValueResponseType.Get, ownProbe.ProbeResults![0].Type);
        Assert.Equal("staged", Encoding.UTF8.GetString(ownProbe.ProbeResults[0].Entry!.Value!));
    }

    /// <summary>
    /// The handlers refuse a statement read without a timestamp on their own, so a request that reaches an actor
    /// without passing the routed entry is refused the same way.
    /// </summary>
    [Fact]
    public async Task Handlers_RefuseAStatementReadWithoutATimestamp()
    {
        await using ActorRig rig = ActorRig.Create(this, "stmt-shape");
        HLCTimestamp tx = rig.Now();

        KeyValueResponse? get = await rig.Get(Key("k"), tx, HLCTimestamp.Zero, KeyValueReadMode.StatementSnapshot);
        Assert.Equal(KeyValueResponseType.InvalidInput, get!.Type);

        KeyValueResponse probe = await rig.Probe([Key("k")], tx, HLCTimestamp.Zero, KeyValueReadMode.StatementSnapshot);
        Assert.Equal(KeyValueResponseType.InvalidInput, probe.Type);

        KeyValueResponse? page = await rig.Page(tx, HLCTimestamp.Zero, KeyValueReadMode.StatementSnapshot);
        Assert.Equal(KeyValueResponseType.InvalidInput, page!.Type);

        KeyValueResponse? seeks = await rig.ProbeRanges(tx, HLCTimestamp.Zero, KeyValueReadMode.StatementSnapshot);
        Assert.Equal(KeyValueResponseType.InvalidInput, seeks!.Type);
    }

    // ── Wire ──────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void ForwardedRequests_CarryTheReadMode_AndDecodeDefaultWhenAbsent()
    {
        HLCTimestamp tx = new(1, 100, 2);
        HLCTimestamp readAt = new(1, 90, 0);

        GrpcTryProbeManyRequest probe = GrpcTryProbeManyRequest.Parser.ParseFrom(
            ProbeManyWire.ToRequest(tx, readAt, ["s/a"], KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, KeyValueReadMode.StatementSnapshot).ToByteArray());
        Assert.Equal(KeyValueReadMode.StatementSnapshot, (KeyValueReadMode)probe.ReadMode);

        GrpcTryProbeManyRequest probeDefault = GrpcTryProbeManyRequest.Parser.ParseFrom(
            ProbeManyWire.ToRequest(tx, readAt, ["s/a"], KeyValueProbeLock.None, 0, KeyValueDurability.Persistent).ToByteArray());
        Assert.Equal(KeyValueReadMode.Default, (KeyValueReadMode)probeDefault.ReadMode);

        GrpcTryProbeManyRangesRequest ranges = GrpcTryProbeManyRangesRequest.Parser.ParseFrom(
            ProbeManyRangesWire.ToRequest(tx, readAt, "s", [new KeyValueProbeRange(null, true, null, false, 5)], KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, KeyValueReadMode.StatementSnapshot).ToByteArray());
        Assert.Equal(KeyValueReadMode.StatementSnapshot, (KeyValueReadMode)ranges.ReadMode);

        GrpcTryGetKeyValueRequest get = GrpcTryGetKeyValueRequest.Parser.ParseFrom(
            new GrpcTryGetKeyValueRequest { Key = "s/a", ReadMode = GrpcKeyValueReadMode.GrpcReadModeStatementSnapshot }.ToByteArray());
        Assert.Equal(KeyValueReadMode.StatementSnapshot, (KeyValueReadMode)get.ReadMode);

        GrpcGetByRangeRequest page = GrpcGetByRangeRequest.Parser.ParseFrom(
            new GrpcGetByRangeRequest { Prefix = "s/", ReadMode = GrpcKeyValueReadMode.GrpcReadModeStatementSnapshot }.ToByteArray());
        Assert.Equal(KeyValueReadMode.StatementSnapshot, (KeyValueReadMode)page.ReadMode);

        // A request from a sender that does not know the field decodes as the default shape.
        GrpcGetByRangeRequest legacy = GrpcGetByRangeRequest.Parser.ParseFrom(new GrpcGetByRangeRequest { Prefix = "s/" }.ToByteArray());
        Assert.Equal(KeyValueReadMode.Default, (KeyValueReadMode)legacy.ReadMode);
    }

    /// <summary>
    /// The registration digest binds the mode, so a same-id retry in another mode is a different declaration, and
    /// the default mode leaves every existing digest unchanged.
    /// </summary>
    [Fact]
    public void OperationDigest_BindsTheReadMode_DefaultIsUnchanged()
    {
        HLCTimestamp readAt = new(1, 90, 0);
        KeyValueProbeRange[] seeks = [new("s/a", true, "s/b", false, 3)];

        Assert.Equal(
            OperationDigest.ForRead(OperationKind.Get, "s/a", -1, readAt, KeyValueDurability.Persistent),
            OperationDigest.ForRead(OperationKind.Get, "s/a", -1, readAt, KeyValueDurability.Persistent, KeyValueReadMode.Default));
        Assert.NotEqual(
            OperationDigest.ForRead(OperationKind.Get, "s/a", -1, readAt, KeyValueDurability.Persistent),
            OperationDigest.ForRead(OperationKind.Get, "s/a", -1, readAt, KeyValueDurability.Persistent, KeyValueReadMode.StatementSnapshot));

        Assert.Equal(
            OperationDigest.ForProbeMany(["s/a"], readAt, KeyValueProbeLock.None, 0, KeyValueDurability.Persistent),
            OperationDigest.ForProbeMany(["s/a"], readAt, KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, KeyValueReadMode.Default));
        Assert.NotEqual(
            OperationDigest.ForProbeMany(["s/a"], readAt, KeyValueProbeLock.None, 0, KeyValueDurability.Persistent),
            OperationDigest.ForProbeMany(["s/a"], readAt, KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, KeyValueReadMode.StatementSnapshot));

        Assert.NotEqual(
            OperationDigest.ForRangeScan("s/", null, true, null, false, 10, readAt, KeyValueDurability.Persistent),
            OperationDigest.ForRangeScan("s/", null, true, null, false, 10, readAt, KeyValueDurability.Persistent, KeyValueReadMode.StatementSnapshot));
        Assert.NotEqual(
            OperationDigest.ForProbeManyRanges("s", seeks, readAt, KeyValueProbeLock.None, 0, KeyValueDurability.Persistent),
            OperationDigest.ForProbeManyRanges("s", seeks, readAt, KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, KeyValueReadMode.StatementSnapshot));
    }

    // ── Rig ───────────────────────────────────────────────────────────────────────────────────

    /// <summary>A spawned key-value actor over a memory backend; ephemeral keys need no replication.</summary>
    private sealed class ActorRig : IAsyncDisposable
    {
        private readonly IDisposable actorSystemLifetime;
        private readonly FairReadScheduler scheduler;
        private readonly MemoryPersistenceBackend backend;

        private RaftManager Raft { get; }
        private IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> Actor { get; }

        private ActorRig(RaftManager raft, FairReadScheduler scheduler, MemoryPersistenceBackend backend, IDisposable lifetime, IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor)
        {
            Raft = raft;
            this.scheduler = scheduler;
            this.backend = backend;
            actorSystemLifetime = lifetime;
            Actor = actor;
        }

        public static ActorRig Create(TestStatementSnapshotReadHandler test, string name)
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
                RevisionRetention = 16,
                ReadContinuationTimeout = 30_000,
                MaxProbeBatchSize = 512
            });

            ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
            ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

            RaftManager raft = test.Track(new RaftManager(
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
                raftLogger
            ));

            FairReadScheduler scheduler = (FairReadScheduler)raft.ReadScheduler;
            scheduler.Start();

            MemoryPersistenceBackend backend = new();
            IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);

            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor =
                actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                    name + "-actor", null!, null!, backend, raft, raft.ReadScheduler,
                    new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

            return new(raft, scheduler, backend, lifetime, actor);
        }

        public HLCTimestamp Now() => Raft.HybridLogicalClock.TrySendOrLocalEvent(Raft.GetLocalNodeId());

        private Task<KeyValueResponse?> Ask(KeyValueRequest request) =>
            Actor.Ask(request, TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        public Task<KeyValueResponse?> Get(string key, HLCTimestamp transactionId, HLCTimestamp readTimestamp, KeyValueReadMode readMode) =>
            Ask(new KeyValueRequest(
                KeyValueRequestType.TryGet, transactionId, HLCTimestamp.Zero, key, null, null, -1,
                KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, 0, 0, default)
            {
                ReadTimestamp = readTimestamp,
                ReadMode = readMode
            });

        public async Task<KeyValueResponse> Probe(string[] keys, HLCTimestamp transactionId, HLCTimestamp readTimestamp, KeyValueReadMode readMode)
        {
            KeyValueResponse? response = await Ask(new KeyValueRequest(
                KeyValueRequestType.TryProbeMany, transactionId, HLCTimestamp.Zero, keys[0], null, null, -1,
                KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, 0, 0, default)
            {
                ReadTimestamp = readTimestamp,
                ReadMode = readMode,
                ProbeKeys = keys
            });
            Assert.NotNull(response);
            return response!;
        }

        public Task<KeyValueResponse?> Page(HLCTimestamp transactionId, HLCTimestamp readTimestamp, KeyValueReadMode readMode) =>
            Ask(new KeyValueRequest(
                KeyValueRequestType.GetByRange, transactionId, HLCTimestamp.Zero, Space, null, null, -1,
                KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, 0, 0, default)
            {
                Limit = 10,
                ReadTimestamp = readTimestamp,
                ReadMode = readMode
            });

        public Task<KeyValueResponse?> ProbeRanges(HLCTimestamp transactionId, HLCTimestamp readTimestamp, KeyValueReadMode readMode) =>
            Ask(new KeyValueRequest(
                KeyValueRequestType.TryProbeManyRanges, transactionId, HLCTimestamp.Zero, Space, null, null, -1,
                KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, 0, 0, default)
            {
                ReadTimestamp = readTimestamp,
                ReadMode = readMode,
                ProbeRanges = [new KeyValueProbeRange(null, true, null, false, 10)]
            });

        /// <summary>A committed write outside any transaction: an ephemeral set applies on the actor at once.</summary>
        public Task<KeyValueResponse?> SetEphemeral(string key, string value) =>
            Ask(new KeyValueRequest(
                KeyValueRequestType.TrySet, HLCTimestamp.Zero, HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
                KeyValueFlags.Set, 0, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, 0, 0, default));

        /// <summary>A staged write of <paramref name="transactionId"/>: it plants the key's write intent.</summary>
        public Task<KeyValueResponse?> SetTransactional(string key, string value, HLCTimestamp transactionId) =>
            Ask(new KeyValueRequest(
                KeyValueRequestType.TrySet, transactionId, HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
                KeyValueFlags.Set, 0, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, 0, 0, default));

        public ValueTask DisposeAsync()
        {
            scheduler.Stop();
            actorSystemLifetime.Dispose();
            backend.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
