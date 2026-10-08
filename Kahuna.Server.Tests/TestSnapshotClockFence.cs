
using System.Text;
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
/// The clock fence between a snapshot read and the writes that follow it, driven through a real
/// <see cref="KeyValueActor"/>.
///
/// <para>A snapshot read at <c>T</c> promises that every write staged on the serving node afterwards is
/// stamped above <c>T</c>, which the actor keeps by folding <c>T</c> into the node's clock before it answers.
/// A <c>T</c> that leads the clock by more than the fence bound cannot be folded, and the only safe answer
/// is to refuse the read: served unfenced, a later write stamped below <c>T</c> would make a second read at
/// <c>T</c> disagree with the first. That is what a forward wall-clock jump on one cluster node produces,
/// because the snapshots it mints lead every other node until its next Raft message spreads the jump.</para>
/// </summary>
public class TestSnapshotClockFence : RaftTrackingTest
{
    private const int FarAheadMs = 20_000;

    private static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

    private static string S(byte[]? b) => b is null ? "" : Encoding.UTF8.GetString(b);

    /// <summary>
    /// A snapshot far ahead of the node's clock is refused with MustRetry, not served; a write stamped below it
    /// in the meantime is then visible to it (no promise was made), and once the clock has caught up the same
    /// read is served fenced, so a write that follows it is stamped above it and stays invisible.
    /// </summary>
    [Fact]
    public async Task SnapshotRead_FarAheadOfClock_IsRefused_ThenServedFencedOnceClockCatchesUp()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config, ILogger<IKahuna> logger) = CreateRaftAndConfig();
        const string key = "fence/far-ahead";

        scheduler.Start();
        try
        {
            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor = Spawn(actorSystem, "fence-far-ahead", raft, config, logger);

            await SetAsync(actor, key, "v1");

            HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
            HLCTimestamp farAhead = now + FarAheadMs;

            KeyValueResponse refusedGet = await GetAtAsync(actor, key, farAhead);
            Assert.Equal(KeyValueResponseType.MustRetry, refusedGet.Type);

            KeyValueResponse refusedExists = await ExistsAtAsync(actor, key, farAhead);
            Assert.Equal(KeyValueResponseType.MustRetry, refusedExists.Type);

            // The refusal folded nothing: the clock still trails the snapshot, so a write lands below it.
            HLCTimestamp afterRefusal = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
            Assert.True(afterRefusal.L < farAhead.L - FarAheadMs / 2, $"clock {afterRefusal} must not have been dragged to {farAhead}");

            await SetAsync(actor, key, "v2");

            // The jump reaches this node (a Raft message from the node that minted the snapshot folds it in).
            raft.HybridLogicalClock.ReceiveEvent(raft.GetLocalNodeId(), farAhead);

            KeyValueResponse first = await GetAtAsync(actor, key, farAhead);
            Assert.Equal(KeyValueResponseType.Get, first.Type);
            Assert.Equal("v2", S(first.Entry!.Value));

            // Served fenced: a write after the read is stamped above the snapshot and stays invisible to it.
            await SetAsync(actor, key, "v3");

            KeyValueResponse second = await GetAtAsync(actor, key, farAhead);
            Assert.Equal(KeyValueResponseType.Get, second.Type);
            Assert.Equal("v2", S(second.Entry!.Value));

            KeyValueResponse latest = await GetAtAsync(actor, key, HLCTimestamp.Zero);
            Assert.Equal("v3", S(latest.Entry!.Value));
        }
        finally { scheduler.Stop(); }
    }

    /// <summary>
    /// A snapshot inside the fence bound is served, and serving it moves the node's clock past it, so a write
    /// that follows the read cannot land inside the snapshot.
    /// </summary>
    [Fact]
    public async Task SnapshotRead_WithinFenceBound_AdvancesClockAndStaysRepeatable()
    {
        (RaftManager raft, FairReadScheduler scheduler, KahunaConfiguration config, ILogger<IKahuna> logger) = CreateRaftAndConfig();
        const string key = "fence/within-bound";

        scheduler.Start();
        try
        {
            using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor = Spawn(actorSystem, "fence-within", raft, config, logger);

            await SetAsync(actor, key, "v1");

            HLCTimestamp ahead = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId()) + 1_000;

            KeyValueResponse first = await GetAtAsync(actor, key, ahead);
            Assert.Equal(KeyValueResponseType.Get, first.Type);
            Assert.Equal("v1", S(first.Entry!.Value));

            HLCTimestamp afterRead = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
            Assert.True(afterRead.CompareTo(ahead) > 0, $"clock {afterRead} must be past the served snapshot {ahead}");

            await SetAsync(actor, key, "v2");

            KeyValueResponse second = await GetAtAsync(actor, key, ahead);
            Assert.Equal(KeyValueResponseType.Get, second.Type);
            Assert.Equal("v1", S(second.Entry!.Value));
        }
        finally { scheduler.Stop(); }
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private static IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> Spawn(
        ActorSystem actorSystem, string name, RaftManager raft, KahunaConfiguration config, ILogger<IKahuna> logger)
    {
        return actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
            name, null!, null!, new MemoryPersistenceBackend(), raft,
            raft.ReadScheduler, new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);
    }

    /// <summary>An ephemeral set: applied on the actor's clock without a Raft proposal, which is all the fence needs.</summary>
    private static async Task SetAsync(IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor, string key, string value)
    {
        KeyValueRequest req = new(KeyValueRequestType.TrySet,
            HLCTimestamp.Zero, HLCTimestamp.Zero,
            key, B(value), null, -1,
            KeyValueFlags.Set, 0,
            HLCTimestamp.Zero,
            KeyValueDurability.Ephemeral, 0, 0, default);

        KeyValueResponse? resp = await actor.Ask(req, TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.NotNull(resp);
        Assert.Equal(KeyValueResponseType.Set, resp!.Type);
    }

    private static async Task<KeyValueResponse> GetAtAsync(IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor, string key, HLCTimestamp readTimestamp)
    {
        KeyValueRequest req = new(KeyValueRequestType.TryGet,
            HLCTimestamp.Zero, HLCTimestamp.Zero,
            key, null, null, -1,
            KeyValueFlags.None, 0,
            HLCTimestamp.Zero,
            KeyValueDurability.Ephemeral, 0, 0, default);
        req.ReadTimestamp = readTimestamp;

        KeyValueResponse? resp = await actor.Ask(req, TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.NotNull(resp);
        return resp!;
    }

    private static async Task<KeyValueResponse> ExistsAtAsync(IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor, string key, HLCTimestamp readTimestamp)
    {
        KeyValueRequest req = new(KeyValueRequestType.TryExists,
            HLCTimestamp.Zero, HLCTimestamp.Zero,
            key, null, null, -1,
            KeyValueFlags.None, 0,
            HLCTimestamp.Zero,
            KeyValueDurability.Ephemeral, 0, 0, default);
        req.ReadTimestamp = readTimestamp;

        KeyValueResponse? resp = await actor.Ask(req, TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.NotNull(resp);
        return resp!;
    }

    private (RaftManager Raft, FairReadScheduler Scheduler, KahunaConfiguration Config, ILogger<IKahuna> Logger) CreateRaftAndConfig()
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
                NodeName = "snapshot-clock-fence",
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false, PartitionExecutorPoolSize = 1
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger
        );

        return (raft, (FairReadScheduler)Track(raft).ReadScheduler, config, logger);
    }
}
