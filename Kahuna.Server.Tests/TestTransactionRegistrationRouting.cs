using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// End-to-end coverage of the register-remote operation-registration path: a transaction-scoped
/// operation registers on the coordinator node (routed by the coordinator key) regardless of which
/// node receives the call, so a retry carrying the same operation id replays the cached write response
/// instead of applying the mutation a second time, and a reused id with a different declaration is
/// rejected.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestTransactionRegistrationRouting
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestTransactionRegistrationRouting(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private sealed record Node(RaftManager Raft, KahunaManager Kahuna);

    [Fact]
    public async Task RegistrationRoutesToCoordinator_DedupsRetry_RejectsConflict()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (KeyValueResponseType startType, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);
            Assert.Equal(KeyValueResponseType.Set, startType);
            Assert.False(handle.IsEmpty);

            TransactionOperationId op1 = TransactionOperationId.NewRandom();
            byte[] digestA = [1, 2, 3];
            byte[] digestB = [9, 9, 9];

            // First registration from an arbitrary entry node → New.
            (OperationRegistrationOutcome outcome, _, _, _) =
                await nodes[1].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, op1, OperationKind.Set, digestA, ct);
            Assert.Equal(OperationRegistrationOutcome.New, outcome);

            // Complete it with a confirmed effect and a cached write response.
            HLCTimestamp cachedTs = new(1, 500, 0);
            await nodes[2].Kahuna.LocateAndCompleteOperation(
                handle.CoordinatorKey, handle.TransactionId, op1,
                modifiedKey: "k1", pointLockKey: "k1", readKey: null, readExists: false, readRevision: 0,
                KeyValueDurability.Persistent, KeyValueResponseType.Set, cachedRevision: 7, cachedTs, ct);

            // Retry with the same id (from yet another node) → cached response, no re-apply.
            (OperationRegistrationOutcome replayOutcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp) =
                await nodes[0].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, op1, OperationKind.Set, digestA, ct);
            Assert.Equal(OperationRegistrationOutcome.AlreadyCompleted, replayOutcome);
            Assert.Equal(KeyValueResponseType.Set, cachedType);
            Assert.Equal(7, cachedRevision);
            Assert.Equal(cachedTs, cachedTimestamp);

            // Same id, different declaration → rejected as a conflict.
            (OperationRegistrationOutcome conflictOutcome, _, _, _) =
                await nodes[1].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, op1, OperationKind.Set, digestB, ct);
            Assert.Equal(OperationRegistrationOutcome.RejectedDuplicate, conflictOutcome);

            // A distinct operation id registers fresh.
            (OperationRegistrationOutcome op2Outcome, _, _, _) =
                await nodes[2].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, TransactionOperationId.NewRandom(), OperationKind.Delete, null, ct);
            Assert.Equal(OperationRegistrationOutcome.New, op2Outcome);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    [Fact]
    public async Task Registration_ForUnknownTransaction_IsRejected()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            // No StartTransaction: the coordinator holds no session for this id.
            (OperationRegistrationOutcome outcome, _, _, _) = await nodes[0].Kahuna.LocateAndBeginOperation(
                Guid.NewGuid().ToString("N"), new HLCTimestamp(1, 42, 0), TransactionOperationId.NewRandom(), OperationKind.Set, null, ct);

            Assert.Equal(OperationRegistrationOutcome.RejectedSessionClosed, outcome);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    [Fact]
    public async Task TransactionalSet_RetryWithSameOperationId_ReplaysCachedResponse()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

            TransactionOperationId op = TransactionOperationId.NewRandom();
            byte[] value = "v1"u8.ToArray();

            // First application (retry the same op id until it stops being transient; a transient
            // outcome cancels the registration so the same id re-applies cleanly).
            (KeyValueResponseType type, long revision, HLCTimestamp lastModified) = await SetWithRetry(nodes[1], handle, op, value, ct);
            Assert.Equal(KeyValueResponseType.Set, type);

            // Replay with the same op id and identical payload → the cached response, byte-for-byte,
            // with no second application (revision does not advance).
            (KeyValueResponseType replayType, long replayRevision, HLCTimestamp replayModified) =
                await nodes[2].Kahuna.LocateAndTrySetKeyValue(
                    handle.TransactionId, "wk1", value, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent,
                    ct, 0, handle.CoordinatorKey, op);

            Assert.Equal(type, replayType);
            Assert.Equal(revision, replayRevision);
            Assert.Equal(lastModified, replayModified);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    [Fact]
    public async Task TransactionalDelete_RetryWithSameOperationId_ReplaysCachedResponse()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

            // Seed a value in the transaction, then delete it under its own operation id.
            (KeyValueResponseType setType, _, _) = await SetWithRetry(nodes[1], handle, TransactionOperationId.NewRandom(), "v1"u8.ToArray(), ct);
            Assert.Equal(KeyValueResponseType.Set, setType);

            TransactionOperationId delOp = TransactionOperationId.NewRandom();
            (KeyValueResponseType delType, long delRevision, HLCTimestamp delModified) = await DeleteWithRetry(nodes[2], handle, delOp, ct);
            Assert.Equal(KeyValueResponseType.Deleted, delType);

            (KeyValueResponseType replayType, long replayRevision, HLCTimestamp replayModified) =
                await nodes[0].Kahuna.LocateAndTryDeleteKeyValue(handle.TransactionId, "wk1", KeyValueDurability.Persistent, ct, handle.CoordinatorKey, delOp);

            Assert.Equal(delType, replayType);
            Assert.Equal(delRevision, replayRevision);
            Assert.Equal(delModified, replayModified);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    private static async Task<(KeyValueResponseType, long, HLCTimestamp)> DeleteWithRetry(Node node, TransactionHandle handle, TransactionOperationId op, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 10_000;
        while (true)
        {
            (KeyValueResponseType type, long revision, HLCTimestamp lastModified) =
                await node.Kahuna.LocateAndTryDeleteKeyValue(handle.TransactionId, "wk1", KeyValueDurability.Persistent, ct, handle.CoordinatorKey, op);

            if (type != KeyValueResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return (type, revision, lastModified);

            await Task.Delay(50, ct);
        }
    }

    private static async Task<(KeyValueResponseType, long, HLCTimestamp)> SetWithRetry(Node node, TransactionHandle handle, TransactionOperationId op, byte[] value, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 10_000;
        while (true)
        {
            (KeyValueResponseType type, long revision, HLCTimestamp lastModified) =
                await node.Kahuna.LocateAndTrySetKeyValue(
                    handle.TransactionId, "wk1", value, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent,
                    ct, 0, handle.CoordinatorKey, op);

            if (type != KeyValueResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return (type, revision, lastModified);

            await Task.Delay(50, ct);
        }
    }

    // ── harness ──────────────────────────────────────────────────────────────────────────

    private (RaftManager, KahunaManager) BuildNode(
        int nodeId, int port, string[] peers,
        MemoryInterNodeCommmunication interNode, InMemoryCommunication comm)
    {
        IWAL wal = new InMemoryWAL(raftLogger);
        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration config = new()
        {
            NodeName = "kahuna" + nodeId,
            NodeId = nodeId,
            Host = "localhost",
            Port = port,
            InitialPartitions = 2,
            StartElectionTimeout = 50,
            EndElectionTimeout = 150,
            ElectionTimeoutSeed = 94000 + nodeId,
            CompactEveryOperations = 1000,
            CompactNumberEntries = 50,
            EnableQuiescence = false
        };

        RaftManager raft = new(
            config,
            new StaticDiscovery([new(peers[0]), new(peers[1])]),
            wal, comm, new HybridLogicalClock(), raftLogger);

        KahunaConfiguration kahunaConfig = new()
        {
            HttpsCertificate = "",
            HttpsCertificatePassword = "",
            LocksWorkers = 8,
            KeyValueWorkers = 8,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            StoragePath = "/tmp",
            StorageRevision = Guid.NewGuid().ToString(),
            DefaultTransactionTimeout = 5000,
            ScriptCacheExpiration = TimeSpan.FromMinutes(1),
        };

        KahunaManager kahuna = new(actorSystem, raft, kahunaConfig, interNode, kahunaLogger);

        raft.OnLogRestored += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError += kahuna.OnReplicationError;
        raft.OnLeaderChanged += kahuna.OnLeaderChanged;

        return (raft, kahuna);
    }

    private async Task<Node[]> Assemble()
    {
        MemoryInterNodeCommmunication interNode = new();
        InMemoryCommunication comm = new();

        string[] p1 = ["localhost:8011", "localhost:8012"];
        string[] p2 = ["localhost:8010", "localhost:8012"];
        string[] p3 = ["localhost:8010", "localhost:8011"];

        (RaftManager r1, KahunaManager k1) = BuildNode(1, 8010, p1, interNode, comm);
        (RaftManager r2, KahunaManager k2) = BuildNode(2, 8011, p2, interNode, comm);
        (RaftManager r3, KahunaManager k3) = BuildNode(3, 8012, p3, interNode, comm);

        interNode.SetNodes(new() { { "localhost:8010", k1 }, { "localhost:8011", k2 }, { "localhost:8012", k3 } });
        comm.SetNodes(new() { { "localhost:8010", r1 }, { "localhost:8011", r2 }, { "localhost:8012", r3 } });

        await Task.WhenAll(r1.JoinCluster(), r2.JoinCluster(), r3.JoinCluster());

        for (int partition = 1; partition <= 2; partition++)
            await WaitForAnyLeader(partition, r1, r2, r3);

        return [new(r1, k1), new(r2, k2), new(r3, k3)];
    }

    private static async Task WaitForAnyLeader(int partition, params RaftManager[] rafts)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            foreach (RaftManager raft in rafts)
                if (await raft.AmILeader(partition, ct))
                    return;

            await Task.Delay(50, ct);
        }
    }

    private static async Task LeaveAll(Node[] nodes)
    {
        foreach (Node node in nodes)
        {
            try { await node.Raft.LeaveCluster(dispose: true); }
            catch (ObjectDisposedException) { }
        }
    }
}
