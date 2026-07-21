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

        using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
        (IActorRef<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest> worker,
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> replyActor) = SpawnWorkerAndSink(actorSystem, node.Raft, logger);

        TaskCompletionSource<KeyValueResponse?> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        worker.Send(KeyValuePhaseTwoRequest.ForPrepare(1, partitionId, SerializeSet("phasetwo/prepare", 0), KeyValuePhaseTwoRequest.NoDeadline, 0, replyActor, promise));

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

        using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
        (IActorRef<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest> worker,
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> replyActor) = SpawnWorkerAndSink(actorSystem, node.Raft, logger);

        TaskCompletionSource<KeyValueResponse?> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        worker.Send(KeyValuePhaseTwoRequest.ForCommit(1, partitionId, prepared.TicketId, KeyValuePhaseTwoRequest.NoDeadline, replyActor, promise));

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

        using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> bgWriter =
            actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
                "bg-commit", raft, backend,
                null!, null!, null!, null!,
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
            txId, key, KeyValueDurability.Persistent, proposal, txId, txId, 1, null);

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
            txId, key, KeyValueDurability.Persistent, proposal, txId, txId, 1, null);

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

        using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
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
        worker.Send(KeyValuePhaseTwoRequest.ForCommit(1, partitionId, prepared.TicketId, KeyValuePhaseTwoRequest.NoDeadline, replyActor, promise));

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
    /// The committed-log apply (InvalidateOrApply) must defer to a live 2PC write intent — the phase-two
    /// completion owns the apply. When the intent has expired (leadership lost), it clears it and applies.
    /// </summary>
    [Fact]
    public void InvalidateOrApply_SkipsUnderLiveWriteIntent_AppliesUnderExpired()
    {
        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        RaftManager raft = CreateRaft("invalidate-writeintent");
        KeyValueContext context = DirectContext(raft, Config(), logger);

        const string key = "phasetwo/invalidate";
        HLCTimestamp txId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        KeyValueEntry entry = new()
        {
            Value = "old"u8.ToArray(),
            Revision = 4,
            State = KeyValueState.Set,
            LastUsed = txId,
            LastModified = txId,
            WriteIntent = new() { TransactionId = txId, Expires = txId + 15000 }
        };
        context.Store.Insert(key, entry);

        InvalidateOrApplyHandler handler = new(context);

        // Live write intent: the committed-log apply must not advance the entry.
        handler.Execute(KeyValueRequest.ForInvalidateOrApply(key, 5, "new"u8.ToArray(), HLCTimestamp.Zero, txId, txId, KeyValueState.Set));
        Assert.Equal(4, entry.Revision);
        Assert.Equal("old"u8.ToArray(), entry.Value);
        Assert.NotNull(entry.WriteIntent);

        // Expired write intent (leadership lost before the completion): clear it and apply.
        entry.WriteIntent!.Expires = HLCTimestamp.Zero;
        handler.Execute(KeyValueRequest.ForInvalidateOrApply(key, 5, "new"u8.ToArray(), HLCTimestamp.Zero, txId, txId, KeyValueState.Set));
        Assert.Equal(5, entry.Revision);
        Assert.Equal("new"u8.ToArray(), entry.Value);
        Assert.Null(entry.WriteIntent);
    }

    /// <summary>
    /// B2 regression: on the leader the committed-log apply is enqueued before the phase-two completion.
    /// With the live-write-intent guard, InvalidateOrApply defers, so the completion archives the correct
    /// superseded revision — not the already-advanced value.
    /// </summary>
    [Fact]
    public void InvalidateOrApplyBeforeCompletion_DoesNotCorruptRevisionArchive()
    {
        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        KahunaConfiguration config = Config();
        RaftManager raft = CreateRaft("invalidate-ordering");
        MemoryPersistenceBackend backend = new();

        using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> bgWriter =
            actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
                "bg-order", raft, backend, null!, null!, null!, null!, config, logger, new FlushNotificationSink());

        BTree<string, KeyValueEntry> store = new(32);
        KeyValueContext context = new(
            null!, store, new(), new(), new(), bgWriter, null!, backend, raft,
            new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

        const string key = "phasetwo/ordering";
        HLCTimestamp txId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Prepared entry: current revision 4 "old", live write intent + MVCC staging revision 5 "new".
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
            txId, key, KeyValueDurability.Persistent, proposal, txId, txId, 1, null);

        // Leader ordering: the committed-log apply arrives BEFORE the phase-two completion.
        InvalidateOrApplyHandler invalidate = new(context);
        invalidate.Execute(KeyValueRequest.ForInvalidateOrApply(key, 5, "new"u8.ToArray(), HLCTimestamp.Zero, txId, txId, KeyValueState.Set));

        // The live write intent deferred the apply — the entry is still at the superseded revision.
        Assert.Equal(4, entry.Revision);
        Assert.Equal("old"u8.ToArray(), entry.Value);

        // The completion then applies once, archiving the superseded revision correctly.
        CompletePhaseTwoHandler complete = new(context);
        TaskCompletionSource<KeyValueResponse?> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        KeyValueResponse resp = complete.Execute(KeyValueRequest.ForCompletePhaseTwo(
            new PhaseTwoCompletionData(phaseTwoId, PhaseTwoOpKind.Commit, true, RaftOperationStatus.Success, 5, HLCTimestamp.Zero),
            promise));

        Assert.Equal(KeyValueResponseType.Committed, resp.Type);
        Assert.Equal(5, entry.Revision);
        Assert.Equal("new"u8.ToArray(), entry.Value);
        Assert.Null(entry.WriteIntent);

        // Revision 4 was archived with its OLD value; the current version 5 is not archived under itself.
        Assert.NotNull(entry.Revisions);
        Assert.True(entry.Revisions!.TryGetValue(4, out KeyValueRevisionEntry archived));
        Assert.Equal("old"u8.ToArray(), archived.Value);
        Assert.False(entry.Revisions.ContainsKey(5));
    }

    /// <summary>
    /// R2 (B7): the worker must not fabricate a success because the node left the cluster after the
    /// request queued. With a non-joined raft it still runs the Raft op (reaching the hook) and the
    /// doomed call resolves to a non-Committed outcome — never a fabricated commit.
    /// </summary>
    [Fact]
    public async Task Worker_NotJoined_DoesNotFabricateSuccess()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        RaftManager raft = CreateRaft("phasetwo-notjoined");
        Assert.False(raft.Joined);

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
            Phase2CommitTimeout = 500
        });

        using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
        IActorRef<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest> worker =
            actorSystem.Spawn<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest>("notjoined-worker", raft, logger);
        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> replyActor =
            actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                "notjoined-reply", null!, null!, new MemoryPersistenceBackend(), raft,
                new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

        KeyValuePhaseTwoActor workerInstance = (worker.Runner.Actor as KeyValuePhaseTwoActor)!;
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        workerInstance.BeforeRaftCallHook = () =>
        {
            entered.TrySetResult();
            return Task.CompletedTask;
        };

        TaskCompletionSource<KeyValueResponse?> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        worker.Send(KeyValuePhaseTwoRequest.ForCommit(
            1, 1, raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId()),
            KeyValuePhaseTwoRequest.NoDeadline, replyActor, promise));

        // The worker reaches the Raft-call path even when not joined (before R2 it short-circuited).
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);

        // The doomed Raft op resolves to a non-Committed result — never a fabricated success.
        KeyValueResponse? response = await promise.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
        Assert.NotNull(response);
        Assert.NotEqual(KeyValueResponseType.Committed, response!.Type);
    }

    /// <summary>
    /// R3 (S3): a joined commit with no phase-two worker wired must drive CommitLogs (durable), not
    /// apply locally and fabricate success. A bogus (never-prepared) ticket makes CommitLogs fail, so
    /// the commit resolves non-Committed and retains its state — proving CommitLogs is actually driven.
    /// </summary>
    [Fact]
    public async Task NullRouterCommit_DrivesCommitLogs_NotFabricatedSuccess()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();

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
            Phase2CommitTimeout = 500
        });

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);
        await node.StartAsync(ct);
        const int partitionId = 1;
        await node.Raft.WaitForLeader(partitionId, ct);

        MemoryPersistenceBackend backend = new();
        using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> bgWriter =
            actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
                "bg-r3", node.Raft, backend, null!, null!, null!, null!, config, logger, new FlushNotificationSink());

        // Note: PhaseTwoRouter defaults to null here — the fallback path under test.
        BTree<string, KeyValueEntry> store = new(32);
        KeyValueContext context = new(
            null!, store, new(), new(), new(), bgWriter, null!, backend, node.Raft,
            new KeySpaceRegistry(), new RangeMapStore(node.Raft, null, null, logger), config, logger);

        const string key = "r3/key";
        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
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

        HLCTimestamp bogusTicket = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        KeyValueRequest commit = new(
            KeyValueRequestType.TryCommitMutations, txId, HLCTimestamp.Zero, key, null, null, -1,
            KeyValueFlags.None, 0, bogusTicket, KeyValueDurability.Persistent, 0, 0, null);

        TryCommitMutationsHandler handler = new(context);
        KeyValueResponse resp = await handler.Execute(commit);

        // CommitLogs on a never-prepared ticket fails — the commit is not fabricated as Committed.
        Assert.NotEqual(KeyValueResponseType.Committed, resp.Type);
        // State retained for retry (durability was not silently dropped).
        Assert.NotNull(entry.WriteIntent);
    }

    /// <summary>
    /// R6 (B6): if the after-Raft apply throws, the completion must still resolve the caller's promise
    /// (retryable) rather than leak it. Forced here with a null background writer inside ApplyConfirmedCommit.
    /// </summary>
    [Fact]
    public void Completion_ApplyException_ResolvesPromiseRetryable_NotLeaked()
    {
        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        RaftManager raft = CreateRaft("apply-exception");
        KeyValueContext context = DirectContext(raft, Config(), logger); // null background writer

        const string key = "phasetwo/apply-throws";
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
        context.Store.Insert(key, entry);

        KeyValueProposal proposal = new(
            KeyValueRequestType.TrySet, key, "new"u8.ToArray(), 5, false,
            HLCTimestamp.Zero, txId, txId, KeyValueState.Set, KeyValueDurability.Persistent);
        int phaseTwoId = context.NextPhaseTwoId();
        context.PendingPhaseTwos[phaseTwoId] = PendingPhaseTwo.ForCommit(
            txId, key, KeyValueDurability.Persistent, proposal, txId, txId, 1, null);

        CompletePhaseTwoHandler handler = new(context);
        TaskCompletionSource<KeyValueResponse?> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // ApplyConfirmedCommit reaches context.BackgroundWriter.Send (null) and throws; the handler must
        // catch it and resolve the promise retryable instead of letting it propagate and leak the promise.
        KeyValueResponse resp = handler.Execute(KeyValueRequest.ForCompletePhaseTwo(
            new PhaseTwoCompletionData(phaseTwoId, PhaseTwoOpKind.Commit, true, RaftOperationStatus.Success, 5, HLCTimestamp.Zero),
            promise));

        Assert.Equal(KeyValueResponseType.MustRetry, resp.Type);
        // The promise was resolved (not leaked) with the same response the handler returned.
        Assert.True(promise.Task.IsCompletedSuccessfully);
    }

    /// <summary>
    /// R4 (B3): the prepare write-intent lease is dispatch-relative (now + timeout), not tx-start-relative.
    /// A long-running transaction reaching prepare an hour after it started must still get a future lease
    /// so the entry stays pinned across the detached phase-two window — not one born already expired.
    /// </summary>
    [Fact]
    public async Task Prepare_SetsDispatchRelativeIntentExpiry_NotTxStartRelative()
    {
        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        RaftManager raft = CreateRaft("prepare-expiry");
        KeyValueContext context = DirectContext(raft, Config(), logger);

        const string key = "phasetwo/expiry";
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp oldTxId = now - 3_600_000;  // the transaction started an hour ago
        HLCTimestamp commitId = now;

        KeyValueEntry entry = new()
        {
            Value = "v"u8.ToArray(),
            Revision = 1,
            State = KeyValueState.Set,
            LastUsed = oldTxId,
            LastModified = oldTxId,
            MvccEntries = new() { [oldTxId] = new KeyValueMvccEntry { Value = "v2"u8.ToArray(), Revision = 1, State = KeyValueState.Set, LastModified = commitId } }
        };
        context.Store.Insert(key, entry);

        KeyValueRequest prepare = new(
            KeyValueRequestType.TryPrepareMutations, oldTxId, commitId, key, null, null, -1,
            KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, 0, null);

        TryPrepareMutationsHandler handler = new(context);
        KeyValueResponse resp = await handler.Execute(prepare);

        Assert.Equal(KeyValueResponseType.Prepared, resp.Type);
        Assert.NotNull(entry.WriteIntent);
        // Dispatch-relative: the lease expires in the future, well beyond the tx-start-relative deadline.
        Assert.True(entry.WriteIntent!.Expires - now > TimeSpan.Zero);
        Assert.True(entry.WriteIntent.Expires - (oldTxId + 15000) > TimeSpan.Zero);
    }

    /// <summary>
    /// R5 (B4): a successful commit completion whose participant state is missing, with no local proof
    /// (no recent-committed decision, no completion receipt), must not fabricate a Committed — it returns
    /// retryable so the coordinator re-drives.
    /// </summary>
    [Fact]
    public void CommitCompletion_MissingStateNoProof_ReturnsRetryable()
    {
        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        RaftManager raft = CreateRaft("missing-noproof");
        KeyValueContext context = DirectContext(raft, Config(), logger);

        const string key = "phasetwo/missing-noproof";
        HLCTimestamp txId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        // Entry present but no write intent (state gone), and no receipt / recent decision recorded.
        context.Store.Insert(key, new KeyValueEntry { Value = "v"u8.ToArray(), Revision = 5, State = KeyValueState.Set });

        KeyValueProposal proposal = new(
            KeyValueRequestType.TrySet, key, "v"u8.ToArray(), 5, false,
            HLCTimestamp.Zero, txId, txId, KeyValueState.Set, KeyValueDurability.Persistent);
        int phaseTwoId = context.NextPhaseTwoId();
        context.PendingPhaseTwos[phaseTwoId] = PendingPhaseTwo.ForCommit(
            txId, key, KeyValueDurability.Persistent, proposal, txId, txId, 1, null);

        CompletePhaseTwoHandler handler = new(context);
        KeyValueResponse resp = handler.Execute(KeyValueRequest.ForCompletePhaseTwo(
            new PhaseTwoCompletionData(phaseTwoId, PhaseTwoOpKind.Commit, true, RaftOperationStatus.Success, 5, HLCTimestamp.Zero),
            null));

        Assert.Equal(KeyValueResponseType.MustRetry, resp.Type);
    }

    /// <summary>
    /// R5 (B4): a successful commit completion with missing state but a durable completion receipt (proof
    /// the commit applied here, e.g. via the replication apply) returns Committed and records the decision.
    /// </summary>
    [Fact]
    public void CommitCompletion_MissingStateWithReceipt_ReturnsCommitted()
    {
        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        RaftManager raft = CreateRaft("missing-receipt");
        KeyValueContext context = DirectContext(raft, Config(), logger);

        const string key = "phasetwo/missing-receipt";
        HLCTimestamp txId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        context.Store.Insert(key, new KeyValueEntry { Value = "v"u8.ToArray(), Revision = 5, State = KeyValueState.Set });
        context.CompletionReceiptStore.Record(txId, key, null, KeyValueDurability.Persistent);

        KeyValueProposal proposal = new(
            KeyValueRequestType.TrySet, key, "v"u8.ToArray(), 5, false,
            HLCTimestamp.Zero, txId, txId, KeyValueState.Set, KeyValueDurability.Persistent);
        int phaseTwoId = context.NextPhaseTwoId();
        context.PendingPhaseTwos[phaseTwoId] = PendingPhaseTwo.ForCommit(
            txId, key, KeyValueDurability.Persistent, proposal, txId, txId, 1, null);

        CompletePhaseTwoHandler handler = new(context);
        KeyValueResponse resp = handler.Execute(KeyValueRequest.ForCompletePhaseTwo(
            new PhaseTwoCompletionData(phaseTwoId, PhaseTwoOpKind.Commit, true, RaftOperationStatus.Success, 5, HLCTimestamp.Zero),
            null));

        Assert.Equal(KeyValueResponseType.Committed, resp.Type);
        Assert.True(context.WasCommittedHere(txId));
    }

    /// <summary>
    /// R7 (B5): the dispatch deadline is a future absolute monotonic tick for a positive timeout, and the
    /// NoDeadline sentinel when the timeout is disabled — so the worker can bound dispatch-to-resolution.
    /// </summary>
    [Fact]
    public void DeadlineFrom_ComputesFutureDeadline_OrNoDeadlineWhenDisabled()
    {
        long before = Environment.TickCount64;
        long deadline = KeyValuePhaseTwoRequest.DeadlineFrom(5000);
        Assert.True(deadline >= before + 5000);
        Assert.True(deadline <= Environment.TickCount64 + 5000);

        Assert.Equal(KeyValuePhaseTwoRequest.NoDeadline, KeyValuePhaseTwoRequest.DeadlineFrom(0));
        Assert.Equal(KeyValuePhaseTwoRequest.NoDeadline, KeyValuePhaseTwoRequest.DeadlineFrom(-1));
    }

    /// <summary>
    /// R7 (B5): a request already past its dispatch deadline is not skipped — the worker still makes one
    /// minimal Raft attempt (the gate hook is reached) bounded by the leftover budget, so a cancelled
    /// commit can still land late for the idempotent retry. The doomed attempt resolves retryably.
    /// </summary>
    [Fact]
    public async Task Worker_ExpiredDeadline_StillAttemptsRaft_ResolvesRetryable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        RaftManager raft = CreateRaft("phasetwo-expired");

        using IDisposable actorSystemLifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
        (IActorRef<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest> worker,
            IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> replyActor) = SpawnWorkerAndSink(actorSystem, raft, logger);

        KeyValuePhaseTwoActor workerInstance = (worker.Runner.Actor as KeyValuePhaseTwoActor)!;
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        workerInstance.BeforeRaftCallHook = () =>
        {
            entered.TrySetResult();
            return Task.CompletedTask;
        };

        TaskCompletionSource<KeyValueResponse?> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        long expiredDeadline = Environment.TickCount64 - 1000;  // already past its window
        worker.Send(KeyValuePhaseTwoRequest.ForCommit(
            1, 1, raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId()),
            expiredDeadline, replyActor, promise));

        KeyValueResponse? response = await promise.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);
        Assert.NotNull(response);
        // The doomed minimal attempt resolves as a failure, never a fabricated commit.
        Assert.NotEqual(KeyValueResponseType.Committed, response!.Type);

        // The Raft-call path was still reached (a minimal attempt), not skipped.
        Assert.True(entered.Task.IsCompleted);
    }

    /// <summary>
    /// R7/R6 (B6): the periodic collect sweeps an orphaned phase-two entry whose worker never returned a
    /// completion (past its deadline + grace), resolving the caller's retained promise retryably so the
    /// coordinator's ask cannot hang forever. An entry still within its deadline is left untouched.
    /// </summary>
    [Fact]
    public async Task Sweep_ResolvesOrphanedPhaseTwoPastDeadline_LeavesInFlightAlone()
    {
        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        RaftManager raft = CreateRaft("phasetwo-sweep");
        KeyValueContext context = DirectContext(raft, Config(), logger);

        HLCTimestamp txId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        KeyValueProposal proposal = new(
            KeyValueRequestType.TrySet, "sweep/key", null, 1, false,
            HLCTimestamp.Zero, txId, txId, KeyValueState.Set, KeyValueDurability.Persistent);

        // Orphaned: well past its deadline + grace — must be swept.
        TaskCompletionSource<KeyValueResponse?> orphanPromise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int orphanId = context.NextPhaseTwoId();
        PendingPhaseTwo orphan = PendingPhaseTwo.ForCommit(txId, "sweep/key", KeyValueDurability.Persistent, proposal, txId, txId, 1, null);
        orphan.Promise = orphanPromise;
        orphan.DeadlineTicks = Environment.TickCount64 - 60_000;
        context.PendingPhaseTwos[orphanId] = orphan;

        // In flight: deadline in the future — must be left alone.
        TaskCompletionSource<KeyValueResponse?> livePromise = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int liveId = context.NextPhaseTwoId();
        PendingPhaseTwo live = PendingPhaseTwo.ForCommit(txId, "sweep/live", KeyValueDurability.Persistent, proposal, txId, txId, 1, null);
        live.Promise = livePromise;
        live.DeadlineTicks = Environment.TickCount64 + 60_000;
        context.PendingPhaseTwos[liveId] = live;

        // The periodic collect runs the phase-two sweep.
        new TryCollectHandler(context).Execute();

        Assert.False(context.PendingPhaseTwos.ContainsKey(orphanId));
        KeyValueResponse? resolved = await orphanPromise.Task;
        Assert.Equal(KeyValueResponseType.MustRetry, resolved!.Type);

        Assert.True(context.PendingPhaseTwos.ContainsKey(liveId));
        Assert.False(livePromise.Task.IsCompleted);
    }

    /// <summary>
    /// R10 (S6): a completion whose op kind does not match the parked entry is a misrouted/malformed
    /// internal completion — it surfaces a protocol error and does not consume the pending entry (its
    /// real completion still owns it) or apply under the wrong operation.
    /// </summary>
    [Fact]
    public void Completion_OpKindMismatch_ReturnsErrored_WithoutConsumingPending()
    {
        ILogger<IKahuna> logger = loggerFactory.CreateLogger<IKahuna>();
        RaftManager raft = CreateRaft("phasetwo-kindmismatch");
        KeyValueContext context = DirectContext(raft, Config(), logger);

        HLCTimestamp txId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        int phaseTwoId = context.NextPhaseTwoId();
        // Parked as a Commit.
        context.PendingPhaseTwos[phaseTwoId] = PendingPhaseTwo.ForRollback(
            txId, "kind/key", KeyValueDurability.Persistent, txId, txId, 1);

        CompletePhaseTwoHandler handler = new(context);
        // Deliver a Commit completion for a Rollback pending — a correlation mismatch.
        KeyValueResponse resp = handler.Execute(KeyValueRequest.ForCompletePhaseTwo(
            new PhaseTwoCompletionData(phaseTwoId, PhaseTwoOpKind.Commit, true, RaftOperationStatus.Success, 5, HLCTimestamp.Zero),
            null));

        Assert.Equal(KeyValueResponseType.Errored, resp.Type);
        // The pending entry is not consumed by the misrouted completion.
        Assert.True(context.PendingPhaseTwos.ContainsKey(phaseTwoId));
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
            actorSystem.Spawn<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest>("phasetwo-worker", raft, logger);

        // A minimal participant actor acts as the completion sink: it receives the CompletePhaseTwo
        // message and resolves the promise. It exercises no store/backend state on this path.
        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> replyActor =
            actorSystem.Spawn<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                "phasetwo-reply", null!, null!, new MemoryPersistenceBackend(), raft,
                new KeySpaceRegistry(), new RangeMapStore(raft, null, null, logger), config, logger);

        return (worker, replyActor);
    }
}
