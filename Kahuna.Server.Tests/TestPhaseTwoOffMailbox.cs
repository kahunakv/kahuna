using System.Text;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
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

    /// <summary>
    /// A confirmed commit completion applies the mutation once; a duplicate completion for the same
    /// phaseTwoId finds no pending entry and does not re-apply.
    /// </summary>
    [Fact]
    public async Task Commit_DuplicateCompletion_AppliesOnce()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        KahunaConfiguration config = Config();

        RaftManager raft = CreateRaft("phasetwo-commit-idem");
        MemoryPersistenceBackend backend = new();

        using ActorSystem actorSystem = new();
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> bgWriter =
            actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
                "bg-commit", raft, backend,
                null!, null!, null!,
                config, logger, new FlushNotificationSink());

        BTree<string, KeyValueEntry> store = new(32);
        KeyValueContext context = new(
            null!, store, new(), new(), new(), bgWriter, null!, backend, raft,
            new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

        const string key = "phasetwo/commit-idem";
        HLCTimestamp txId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // A prepared entry: current revision 4, with a write intent + MVCC staged for txId.
        KeyValueEntry entry = new()
        {
            Value = "old"u8.ToArray(),
            Revision = 4,
            State = KeyValueState.Set,
            LastUsed = txId,
            LastModified = txId,
            WriteIntent = new() { TransactionId = txId, Expires = txId + 15000 },
            MvccEntries = new() { [txId] = new KeyValueMvccEntry { Value = "new"u8.ToArray(), Revision = 5, State = KeyValueState.Set } }
        };
        store.Insert(key, entry);

        KeyValueProposal proposal = new(
            KeyValueRequestType.TrySet, key, "new"u8.ToArray(), 5, false,
            HLCTimestamp.Zero, txId, txId, KeyValueState.Set, KeyValueDurability.Persistent);

        int phaseTwoId = context.NextPhaseTwoId();
        context.PendingPhaseTwos[phaseTwoId] = PendingPhaseTwo.ForCommit(
            txId, key, KeyValueDurability.Persistent, proposal, txId, txId, 1, null, null);

        CompletePhaseTwoHandler handler = new(context);
        TaskCompletionSource<KeyValueResponse?> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        KeyValueRequest completion = KeyValueRequest.ForCompletePhaseTwo(
            new PhaseTwoCompletionData(phaseTwoId, PhaseTwoOpKind.Commit, true, RaftOperationStatus.Success, 42, HLCTimestamp.Zero),
            promise);

        // First completion applies: entry advances to the committed revision, intent + MVCC cleared.
        KeyValueResponse first = handler.Execute(completion);
        Assert.Equal(KeyValueResponseType.Committed, first.Type);
        Assert.Equal(5, entry.Revision);
        Assert.Equal("new"u8.ToArray(), entry.Value);
        Assert.Null(entry.WriteIntent);
        Assert.True(entry.MvccEntries is null || !entry.MvccEntries.ContainsKey(txId));
        Assert.Empty(context.PendingPhaseTwos);

        KeyValueResponse? resolved = await promise.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);
        Assert.Equal(KeyValueResponseType.Committed, resolved!.Type);

        // Duplicate completion: the pending entry is gone, so no re-apply happens.
        KeyValueResponse second = handler.Execute(completion);
        Assert.Equal(KeyValueResponseType.Committed, second.Type);
        Assert.Equal(5, entry.Revision);
        Assert.Null(entry.WriteIntent);
        Assert.Empty(context.PendingPhaseTwos);
    }

    /// <summary>
    /// A rollback completion clears the participant state once; a duplicate completion is a no-op.
    /// </summary>
    [Fact]
    public void Rollback_DuplicateCompletion_IsIdempotent()
    {
        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        KahunaConfiguration config = Config();

        RaftManager raft = CreateRaft("phasetwo-rollback-idem");
        MemoryPersistenceBackend backend = new();

        BTree<string, KeyValueEntry> store = new(32);
        KeyValueContext context = new(
            null!, store, new(), new(), new(), null!, null!, backend, raft,
            new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

        const string key = "phasetwo/rollback-idem";
        HLCTimestamp txId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        KeyValueEntry entry = new()
        {
            Value = "old"u8.ToArray(),
            Revision = 4,
            State = KeyValueState.Set,
            LastUsed = txId,
            LastModified = txId,
            WriteIntent = new() { TransactionId = txId, Expires = txId + 15000 },
            MvccEntries = new() { [txId] = new KeyValueMvccEntry { Value = "new"u8.ToArray(), Revision = 5, State = KeyValueState.Set } }
        };
        store.Insert(key, entry);

        int phaseTwoId = context.NextPhaseTwoId();
        context.PendingPhaseTwos[phaseTwoId] = PendingPhaseTwo.ForRollback(
            txId, key, KeyValueDurability.Persistent, txId, txId, 1);

        CompletePhaseTwoHandler handler = new(context);
        TaskCompletionSource<KeyValueResponse?> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        KeyValueRequest completion = KeyValueRequest.ForCompletePhaseTwo(
            new PhaseTwoCompletionData(phaseTwoId, PhaseTwoOpKind.Rollback, true, RaftOperationStatus.Success, 7, HLCTimestamp.Zero),
            promise);

        // First completion rolls back: intent + MVCC cleared, value left untouched.
        KeyValueResponse first = handler.Execute(completion);
        Assert.Equal(KeyValueResponseType.RolledBack, first.Type);
        Assert.Null(entry.WriteIntent);
        Assert.True(entry.MvccEntries is null || !entry.MvccEntries.ContainsKey(txId));
        Assert.Equal("old"u8.ToArray(), entry.Value);
        Assert.Equal(4, entry.Revision);
        Assert.Empty(context.PendingPhaseTwos);
        Assert.True(context.WasRolledBackHere(txId));

        // Duplicate completion: no pending entry, still a coherent RolledBack, no state change.
        KeyValueResponse second = handler.Execute(completion);
        Assert.Equal(KeyValueResponseType.RolledBack, second.Type);
        Assert.Null(entry.WriteIntent);
        Assert.Equal(4, entry.Revision);
    }

    /// <summary>
    /// Delivering the same completion twice (double .Send) applies once — the registry removal on the
    /// first completion makes the second a no-op with a stable result.
    /// </summary>
    [Fact]
    public void DuplicatePhaseTwoCompletion_IsIdempotent()
    {
        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        KahunaConfiguration config = Config();
        RaftManager raft = CreateRaft("phasetwo-dup");

        KeyValueContext context = DirectContext(raft, config, logger);

        const string key = "phasetwo/dup";
        HLCTimestamp txId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        KeyValueEntry entry = new()
        {
            Value = "v"u8.ToArray(),
            Revision = 4,
            State = KeyValueState.Set,
            LastUsed = txId,
            LastModified = txId,
            WriteIntent = new() { TransactionId = txId, Expires = txId + 15000 },
            MvccEntries = new() { [txId] = new KeyValueMvccEntry { Value = "v"u8.ToArray(), Revision = 4, State = KeyValueState.Set } }
        };
        context.Store.Insert(key, entry);

        int phaseTwoId = context.NextPhaseTwoId();
        context.PendingPhaseTwos[phaseTwoId] = PendingPhaseTwo.ForRollback(
            txId, key, KeyValueDurability.Persistent, txId, txId, 1);

        CompletePhaseTwoHandler handler = new(context);
        KeyValueRequest completion = KeyValueRequest.ForCompletePhaseTwo(
            new PhaseTwoCompletionData(phaseTwoId, PhaseTwoOpKind.Rollback, true, RaftOperationStatus.Success, 3, HLCTimestamp.Zero),
            null);

        KeyValueResponse first = handler.Execute(completion);
        Assert.Equal(KeyValueResponseType.RolledBack, first.Type);
        Assert.Null(entry.WriteIntent);
        Assert.True(context.WasRolledBackHere(txId));
        Assert.Empty(context.PendingPhaseTwos);

        // Duplicate: pending entry gone, no re-processing, stable RolledBack.
        KeyValueResponse second = handler.Execute(completion);
        Assert.Equal(KeyValueResponseType.RolledBack, second.Type);
        Assert.Null(entry.WriteIntent);
        Assert.Empty(context.PendingPhaseTwos);
    }

    /// <summary>
    /// Second line of defence: a commit completion whose tx was already rolled back on this node must
    /// not report a false Committed — the recent-decision set overrides the raw completion payload.
    /// </summary>
    [Fact]
    public void CommitCompletion_AfterRollbackRecordedHere_ReturnsErrored()
    {
        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        KahunaConfiguration config = Config();
        RaftManager raft = CreateRaft("phasetwo-commit-over-rollback");

        KeyValueContext context = DirectContext(raft, config, logger);

        const string key = "phasetwo/commit-over-rollback";
        HLCTimestamp txId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // The write intent is already gone and a rollback was recorded here for this tx.
        context.Store.Insert(key, new KeyValueEntry { Value = "v"u8.ToArray(), Revision = 4, State = KeyValueState.Set });
        context.RecordRolledBack(txId);

        int phaseTwoId = context.NextPhaseTwoId();
        KeyValueProposal proposal = new(
            KeyValueRequestType.TrySet, key, "new"u8.ToArray(), 5, false,
            HLCTimestamp.Zero, txId, txId, KeyValueState.Set, KeyValueDurability.Persistent);
        context.PendingPhaseTwos[phaseTwoId] = PendingPhaseTwo.ForCommit(
            txId, key, KeyValueDurability.Persistent, proposal, txId, txId, 1, null, null);

        CompletePhaseTwoHandler handler = new(context);
        KeyValueRequest completion = KeyValueRequest.ForCompletePhaseTwo(
            new PhaseTwoCompletionData(phaseTwoId, PhaseTwoOpKind.Commit, true, RaftOperationStatus.Success, 9, HLCTimestamp.Zero),
            null);

        KeyValueResponse response = handler.Execute(completion);

        Assert.Equal(KeyValueResponseType.Errored, response.Type);
    }

    /// <summary>
    /// Symmetric second line of defence: a rollback completion whose tx already committed on this node
    /// must not report a false RolledBack.
    /// </summary>
    [Fact]
    public void RollbackCompletion_AfterCommitRecordedHere_ReturnsErrored()
    {
        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        KahunaConfiguration config = Config();
        RaftManager raft = CreateRaft("phasetwo-rollback-over-commit");

        KeyValueContext context = DirectContext(raft, config, logger);

        const string key = "phasetwo/rollback-over-commit";
        HLCTimestamp txId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        context.Store.Insert(key, new KeyValueEntry { Value = "v"u8.ToArray(), Revision = 5, State = KeyValueState.Set });
        context.RecordCommitted(txId);

        int phaseTwoId = context.NextPhaseTwoId();
        context.PendingPhaseTwos[phaseTwoId] = PendingPhaseTwo.ForRollback(
            txId, key, KeyValueDurability.Persistent, txId, txId, 1);

        CompletePhaseTwoHandler handler = new(context);
        KeyValueRequest completion = KeyValueRequest.ForCompletePhaseTwo(
            new PhaseTwoCompletionData(phaseTwoId, PhaseTwoOpKind.Rollback, true, RaftOperationStatus.Success, 2, HLCTimestamp.Zero),
            null);

        KeyValueResponse response = handler.Execute(completion);

        Assert.Equal(KeyValueResponseType.Errored, response.Type);
    }

    /// <summary>
    /// The BeforeRaftCallHook holds a commit in flight until released: while gated the caller's promise
    /// is unresolved; releasing the gate lets the commit finish. Proves the seam gates deterministically.
    /// </summary>
    [Fact]
    public async Task InFlightGateHook_HoldsCommitUntilReleased()
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

        RaftReplicationResult prepared = await node.Raft.ReplicateLogs(
            partitionId, ReplicationTypes.KeyValues, SerializeSet("phasetwo/gate", 0), autoCommit: false, cancellationToken: ct);
        Assert.True(prepared.Success);

        using ActorSystem actorSystem = new();
        (IActorRef<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest> worker,
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> replyActor) = SpawnWorkerAndSink(actorSystem, node.Raft, logger);

        KeyValuePhaseTwoActor workerInstance = (worker.Runner.Actor as KeyValuePhaseTwoActor)!;
        Assert.NotNull(workerInstance);

        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        workerInstance.BeforeRaftCallHook = async () =>
        {
            entered.TrySetResult();
            await gate.Task;
        };

        TaskCompletionSource<KeyValueResponse?> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        worker.Send(KeyValuePhaseTwoRequest.ForCommit(1, partitionId, prepared.TicketId, replyActor, promise));

        // The worker reaches the gate — the op is in flight, before the Raft call.
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);

        // While gated the commit has not completed.
        Assert.False(promise.Task.IsCompleted);

        // Release — the commit finishes.
        gate.SetResult();
        KeyValueResponse? response = await promise.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);

        Assert.NotNull(response);
        Assert.Equal(KeyValueResponseType.Committed, response!.Type);
    }

    /// <summary>
    /// While a transaction's prepare is gated in flight on the off-mailbox worker, an unrelated read on
    /// the same single KeyValueActor completes promptly — proving the participant handler no longer parks
    /// the actor mailbox on its phase-two Raft round trip. Releasing the gate lets the transaction commit.
    /// </summary>
    [Fact]
    public async Task PrepareCommitRollback_DoNotParkMailbox()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            KeyValueWorkers = 1
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("pp", ct);

        KeyValuePhaseTwoActor worker = ((KahunaManager)node.Kahuna).FirstPhaseTwoWorker!;
        Assert.NotNull(worker);

        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        worker.BeforeRaftCallHook = async () =>
        {
            entered.TrySetResult();
            await gate.Task;
        };

        // Run a transaction whose prepare dispatches to the (gated) worker; do not await it yet.
        Task<KeyValueTransactionResult> txTask = node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET pp 'v1' COMMIT END"), null, null);

        // The worker reaches the gate — the prepare is in flight, off the actor mailbox.
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);

        // While the phase-two op is outstanding, an unrelated read on the same single actor completes
        // promptly. If the mailbox were parked on the prepare, this would time out.
        (KeyValueResponseType getType, _) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, "qq", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct)
            .WaitAsync(TimeSpan.FromSeconds(5), ct);
        Assert.Equal(KeyValueResponseType.DoesNotExist, getType);

        // Release — the transaction commits.
        gate.SetResult();
        KeyValueTransactionResult result = await txTask.WaitAsync(TimeSpan.FromSeconds(15), ct);
        Assert.Equal(KeyValueResponseType.Set, result.Type);

        worker.BeforeRaftCallHook = null;
    }

    private static KeyValueContext DirectContext(RaftManager raft, KahunaConfiguration config, ILogger<IKahuna> logger)
    {
        return new KeyValueContext(
            null!, new BTree<string, KeyValueEntry>(32), new(), new(), new(),
            null!, null!, new MemoryPersistenceBackend(), raft,
            new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);
    }

    private static RaftManager CreateRaft(string nodeName)
    {
        return new RaftManager(
            new RaftConfiguration
            {
                NodeName = nodeName,
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false
            },
            new StaticDiscovery([]),
            new InMemoryWAL(Microsoft.Extensions.Logging.Abstractions.NullLogger<IRaft>.Instance),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<IRaft>.Instance
        );
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
