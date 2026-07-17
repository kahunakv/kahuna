using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for the off-mailbox two-phase-commit worker (<see cref="KeyValuePhaseTwoActor"/>): a
/// participant's prepare/commit/rollback Raft round trip runs on a dedicated worker rather than
/// parking the KeyValueActor mailbox, and the outcome returns to the participant actor via a
/// <c>CompletePhaseTwo</c> completion that resolves the caller's promise.
///
/// This first slice drives a Prepare and a Commit through the worker end to end against an embedded
/// node and asserts the completion resolves the promise with the right terminal response.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestPhaseTwoOffMailbox
{
    private readonly ILoggerFactory loggerFactory;

    public TestPhaseTwoOffMailbox(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static KahunaConfiguration Config() => ConfigurationValidator.Validate(new()
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
        Phase2CommitTimeout = 5000
    });

    private static byte[] SerializeSet(string key, long revision)
    {
        KeyValueMessage kvm = new()
        {
            Type = (int)KeyValueRequestType.TrySet,
            Key = key,
            Revision = revision
        };

        return ReplicationSerializer.Serialize(kvm);
    }

    /// <summary>
    /// A Prepare dispatched to the worker replicates the proposal off the mailbox and the completion
    /// resolves the promise with Prepared carrying a non-zero ticket.
    /// </summary>
    [Fact]
    public async Task Prepare_DrivenThroughWorker_ResolvesPreparedWithTicket()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);
        await node.StartAsync(ct);

        const int partitionId = 1;
        await node.Raft.WaitForLeader(partitionId, ct);

        using ActorSystem actorSystem = new();
        (IActorRef<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest> worker,
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> replyActor) = SpawnWorkerAndSink(actorSystem, node.Raft, logger);

        TaskCompletionSource<KeyValueResponse?> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        worker.Send(KeyValuePhaseTwoRequest.ForPrepare(1, partitionId, SerializeSet("phasetwo/prepare", 0), replyActor, promise));

        KeyValueResponse? response = await promise.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);

        Assert.NotNull(response);
        Assert.Equal(KeyValueResponseType.Prepared, response!.Type);
        Assert.NotEqual(HLCTimestamp.Zero, response.Ticket);
    }

    /// <summary>
    /// A Commit for a previously prepared ticket, dispatched to the worker, commits off the mailbox
    /// and the completion resolves the promise with Committed.
    /// </summary>
    [Fact]
    public async Task Commit_DrivenThroughWorker_ResolvesCommitted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);
        await node.StartAsync(ct);

        const int partitionId = 1;
        await node.Raft.WaitForLeader(partitionId, ct);

        // Prepare a real proposal to obtain a committable ticket, mirroring what a participant's
        // prepare produces before phase two.
        RaftReplicationResult prepared = await node.Raft.ReplicateLogs(
            partitionId, ReplicationTypes.KeyValues, SerializeSet("phasetwo/commit", 0), autoCommit: false, cancellationToken: ct);

        Assert.True(prepared.Success);
        Assert.NotEqual(HLCTimestamp.Zero, prepared.TicketId);

        using ActorSystem actorSystem = new();
        (IActorRef<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest> worker,
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> replyActor) = SpawnWorkerAndSink(actorSystem, node.Raft, logger);

        TaskCompletionSource<KeyValueResponse?> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        worker.Send(KeyValuePhaseTwoRequest.ForCommit(1, partitionId, prepared.TicketId, replyActor, promise));

        KeyValueResponse? response = await promise.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);

        Assert.NotNull(response);
        Assert.Equal(KeyValueResponseType.Committed, response!.Type);
    }

    private static (IActorRef<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest>, IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>)
        SpawnWorkerAndSink(ActorSystem actorSystem, IRaft raft, ILogger<IKahuna> logger)
    {
        KahunaConfiguration config = Config();

        IActorRef<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest> worker =
            actorSystem.Spawn<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest>("phasetwo-worker", raft, config, logger);

        // A minimal participant actor acts as the completion sink: it receives the CompletePhaseTwo
        // message and resolves the promise. It exercises no store/backend state on this path.
        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> replyActor =
            actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                "phasetwo-reply", null!, null!, new MemoryPersistenceBackend(), raft,
                new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

        return (worker, replyActor);
    }
}
