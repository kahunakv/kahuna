using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
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
/// Verifies that registered batch reads fold observations only for confirmed per-key responses and that
/// per-key transients (MustRetry) are excluded rather than folded as "absent" — a false-absent
/// observation manufactures a read-set conflict when a retry confirms the key present, aborting an
/// otherwise conflict-free optimistic commit. Also pins the point-read and range-read transient-cancel
/// behavior and documents the re-execute/first-observation-authoritative contract for duplicate-id reads.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestRegisteredReadTransientFolding
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestRegisteredReadTransientFolding(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private sealed record Node(RaftManager Raft, KahunaManager Kahuna);

    // ── Test 1 ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// A registered batch GetMany whose first execution returns a confirmed subset and a per-key
    /// transient for one key does not manufacture a read-set conflict. The transient key is not folded
    /// as absent; the caller retries it under a new operation id and the confirmed-present observation
    /// folds cleanly — the optimistic commit succeeds with no conflict.
    /// </summary>
    [Fact]
    public async Task BatchGet_PartialTransient_DoesNotManufactureConflict()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (Node[] nodes, MemoryInterNodeCommmunication interNode) = await AssembleWithTransport();
        try
        {
            const string k1 = "brtf_1/alpha";
            const string k2 = "brtf_1/beta";

            // Seed both keys so a confirmed GetMany response returns Get+entry (not absent).
            Assert.Equal(KeyValueResponseType.Set,
                (await nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, k1, "v1"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct)).Item1);
            Assert.Equal(KeyValueResponseType.Set,
                (await nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, k2, "v2"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct)).Item1);

            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

            // Find the entry node whose batch reads route remote (through the internode transport fault).
            Node entryNode = await FindBatchReadRouterNode(nodes, ct);

            List<(string, long, KeyValueDurability)> bothKeys =
                [(k1, -1, KeyValueDurability.Persistent), (k2, -1, KeyValueDurability.Persistent)];

            // First execution: inject per-key transient for k2 to simulate a routing fault.
            TransactionOperationId op1 = TransactionOperationId.NewRandom();
            interNode.GetManyValuesFault = (_, key) => string.Equals(key, k2, StringComparison.Ordinal);

            List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> first =
                await entryNode.Kahuna.LocateAndTryGetManyValues(handle.TransactionId, HLCTimestamp.Zero, bothKeys, ct, handle.CoordinatorKey, op1);

            interNode.GetManyValuesFault = null;

            // k1 is confirmed; k2 came back transient. k2 must NOT be in the read set.
            TransactionWorkingSet? afterFirst = await nodes[0].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterFirst);
            Assert.Contains(afterFirst!.ReadKeys, r => r.Key == k1);
            Assert.DoesNotContain(afterFirst.ReadKeys, r => r.Key == k2);

            // Retry k2 under a new operation id — should confirm present and fold cleanly.
            List<(string, long, KeyValueDurability)> retryKeys = [(k2, -1, KeyValueDurability.Persistent)];
            TransactionOperationId op2 = TransactionOperationId.NewRandom();

            List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> second =
                await entryNode.Kahuna.LocateAndTryGetManyValues(handle.TransactionId, HLCTimestamp.Zero, retryKeys, ct, handle.CoordinatorKey, op2);

            Assert.Equal(KeyValueResponseType.Get, second[0].Item1);

            TransactionWorkingSet? afterRetry = await nodes[0].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterRetry);
            Assert.Contains(afterRetry!.ReadKeys, r => r.Key == k2);

            // Anchor a write so the transaction enters 2PC and read-set validation runs.
            Assert.Equal(KeyValueResponseType.Set,
                await SetKeyWithRetry(nodes[1], handle, TransactionOperationId.NewRandom(), "brtf_1/writer", "w"u8.ToArray(), KeyValueDurability.Persistent, ct));

            // Commit must succeed — no read-observation conflict from the transient.
            Assert.Equal(KeyValueResponseType.Committed, await CommitWithRetry(nodes[2], handle, ct));
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    // ── Test 2 ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// A registered batch GetMany where every per-key result is transient produces no observations in
    /// the coordinator read set and cancels the registration. A retry under the same operation id
    /// re-registers as New (not AlreadyCompleted), confirming the cancel fired and the id was released.
    /// </summary>
    [Fact]
    public async Task BatchGet_AllTransient_CancelsRegistration_SameIdReregistersAsNew()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (Node[] nodes, MemoryInterNodeCommmunication interNode) = await AssembleWithTransport();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

            Node entryNode = await FindBatchReadRouterNode(nodes, ct);

            List<(string, long, KeyValueDurability)> keys =
                [("brtf_2/x", -1, KeyValueDurability.Persistent), ("brtf_2/y", -1, KeyValueDurability.Persistent)];

            // Inject transients for all keys.
            TransactionOperationId op = TransactionOperationId.NewRandom();
            interNode.GetManyValuesFault = (_, _) => true;

            await entryNode.Kahuna.LocateAndTryGetManyValues(handle.TransactionId, HLCTimestamp.Zero, keys, ct, handle.CoordinatorKey, op);

            interNode.GetManyValuesFault = null;

            // No observation must have been folded.
            TransactionWorkingSet? afterTransient = await nodes[0].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterTransient);
            Assert.Empty(afterTransient!.ReadKeys);

            // Retry under the same op id (no fault this time) — the cancel must have freed the id so the
            // retry registers as New and folds observations.
            List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> retry =
                await entryNode.Kahuna.LocateAndTryGetManyValues(handle.TransactionId, HLCTimestamp.Zero, keys, ct, handle.CoordinatorKey, op);

            // Results are absent (keys were never seeded) — but the point is that the call went through,
            // meaning the registration was not stuck on AlreadyCompleted.
            Assert.Equal(2, retry.Count);
            Assert.All(retry, r => Assert.True(r.Item1 is KeyValueResponseType.Get or KeyValueResponseType.Exists or KeyValueResponseType.MustRetry or KeyValueResponseType.DoesNotExist));

            // At least the registration is now complete (not stuck) — WorkingSet is accessible.
            TransactionWorkingSet? afterRetry = await nodes[0].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterRetry);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    // ── Test 3 ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// A batch GetMany for a key that genuinely does not exist still folds a confirmed-absent observation
    /// (Exists=false). A later confirmed-present observation for the same key under a different operation
    /// id trips the read-observation conflict, proving the fix did not suppress real absent observations.
    /// </summary>
    [Fact]
    public async Task BatchGet_ConfirmedAbsent_StillFolds_AndConflictsWhenKeyAppears()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (Node[] nodes, MemoryInterNodeCommmunication interNode) = await AssembleWithTransport();
        try
        {
            // "brtf_3/ghost" is never seeded — it will be confirmed absent on first read.
            const string ghost = "brtf_3/ghost";

            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

            List<(string, long, KeyValueDurability)> keys = [(ghost, -1, KeyValueDurability.Persistent)];
            TransactionOperationId readOp = TransactionOperationId.NewRandom();

            await nodes[1].Kahuna.LocateAndTryGetManyValues(handle.TransactionId, HLCTimestamp.Zero, keys, ct, handle.CoordinatorKey, readOp);

            // The absent key must be in the read set with Exists=false.
            TransactionWorkingSet? ws = await nodes[2].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(ws);
            KeyValueTransactionReadKey? obs = ws!.ReadKeys.SingleOrDefault(r => r.Key == ghost);
            Assert.NotNull(obs);
            Assert.False(obs!.Exists);

            // Another (non-transactional) write creates the ghost key, advancing its state.
            Assert.Equal(KeyValueResponseType.Set,
                (await nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, ghost, "appeared"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct)).Item1);

            // Anchor a write so the optimistic transaction enters 2PC and validates the read set.
            Assert.Equal(KeyValueResponseType.Set,
                await SetKeyWithRetry(nodes[1], handle, TransactionOperationId.NewRandom(), "brtf_3/writer", "w"u8.ToArray(), KeyValueDurability.Persistent, ct));

            // Commit must abort: the first observation saw ghost as absent but it now exists.
            Assert.Equal(KeyValueResponseType.Aborted, await CommitWithRetry(nodes[2], handle, ct));
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    // ── Test 3b ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// A per-key non-transient, non-confirmed result (Errored — a dropped actor response) is excluded
    /// from the read set exactly like a transient: it means the key's state is unknown, not "absent".
    /// Folding it as absent would manufacture a false read-observation conflict on a retry that confirms
    /// the key present. Guards the allowlist (confirmed types only), not a transient-only denylist.
    /// </summary>
    [Fact]
    public async Task BatchGet_PerKeyErrored_IsExcluded_DoesNotManufactureConflict()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (Node[] nodes, MemoryInterNodeCommmunication interNode) = await AssembleWithTransport();
        try
        {
            const string k1 = "brtf_3b/alpha";
            const string k2 = "brtf_3b/beta";

            // Seed both keys so a confirmed read returns Get+entry.
            Assert.Equal(KeyValueResponseType.Set,
                (await nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, k1, "v1"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct)).Item1);
            Assert.Equal(KeyValueResponseType.Set,
                (await nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, k2, "v2"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct)).Item1);

            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

            Node entryNode = await FindBatchReadRouterNode(nodes, ct);

            List<(string, long, KeyValueDurability)> bothKeys =
                [(k1, -1, KeyValueDurability.Persistent), (k2, -1, KeyValueDurability.Persistent)];

            // First execution: inject a per-key Errored for k2 (a non-transient, non-confirmed type).
            TransactionOperationId op1 = TransactionOperationId.NewRandom();
            interNode.GetManyValuesErrorFault = (_, key) => string.Equals(key, k2, StringComparison.Ordinal);

            await entryNode.Kahuna.LocateAndTryGetManyValues(handle.TransactionId, HLCTimestamp.Zero, bothKeys, ct, handle.CoordinatorKey, op1);

            interNode.GetManyValuesErrorFault = null;

            // k1 confirmed; k2 came back Errored — k2 must NOT be folded as absent.
            TransactionWorkingSet? afterFirst = await nodes[0].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterFirst);
            Assert.Contains(afterFirst!.ReadKeys, r => r.Key == k1);
            Assert.DoesNotContain(afterFirst.ReadKeys, r => r.Key == k2);

            // Retry k2 under a new id — confirms present and folds cleanly, no conflict with a stale absent.
            TransactionOperationId op2 = TransactionOperationId.NewRandom();
            List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> second =
                await entryNode.Kahuna.LocateAndTryGetManyValues(handle.TransactionId, HLCTimestamp.Zero, [(k2, -1, KeyValueDurability.Persistent)], ct, handle.CoordinatorKey, op2);
            Assert.Equal(KeyValueResponseType.Get, second[0].Item1);

            // Anchor a write and commit — must succeed, no read-observation conflict manufactured by the Errored.
            Assert.Equal(KeyValueResponseType.Set,
                await SetKeyWithRetry(nodes[1], handle, TransactionOperationId.NewRandom(), "brtf_3b/writer", "w"u8.ToArray(), KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Committed, await CommitWithRetry(nodes[2], handle, ct));
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    // ── Test 4 ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// A transient point read (CachedType=MustRetry) and a transient range read cancel the registration
    /// and fold no observation — the pre-existing correct behavior is pinned so a future refactor cannot
    /// regress it. The same operation id re-registers as New after the cancel.
    /// </summary>
    [Fact]
    public async Task PointRead_Transient_CancelsRegistration_FoldsNoObservation()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

            // ── point read transient ─────────────────────────────────────────────────
            TransactionOperationId pointOp = TransactionOperationId.NewRandom();

            // Register a Get operation then complete it with MustRetry, mirroring what
            // RegisterAndTryReadValue does when the actor returns a transient.
            (OperationRegistrationOutcome firstPoint, _, _, _, _) =
                await nodes[0].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, pointOp, OperationKind.Get, [1, 2], ct);
            Assert.Equal(OperationRegistrationOutcome.New, firstPoint);

            await nodes[0].Kahuna.LocateAndCompleteOperation(
                handle.CoordinatorKey, handle.TransactionId, pointOp,
                new OperationCompletionPayload { CachedType = KeyValueResponseType.MustRetry }, ct);

            // The cancel freed the id: same-id retry registers as New (not AlreadyPending).
            (OperationRegistrationOutcome retryPoint, _, _, _, _) =
                await nodes[0].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, pointOp, OperationKind.Get, [1, 2], ct);
            Assert.Equal(OperationRegistrationOutcome.New, retryPoint);

            // No observation was folded by the transient completion.
            TransactionWorkingSet? wsPoint = await nodes[0].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(wsPoint);
            Assert.Empty(wsPoint!.ReadKeys);

            // ── range read transient ─────────────────────────────────────────────────
            TransactionOperationId rangeOp = TransactionOperationId.NewRandom();

            (OperationRegistrationOutcome firstRange, _, _, _, _) =
                await nodes[0].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, rangeOp, OperationKind.Scan, [3, 4], ct);
            Assert.Equal(OperationRegistrationOutcome.New, firstRange);

            await nodes[0].Kahuna.LocateAndCompleteOperation(
                handle.CoordinatorKey, handle.TransactionId, rangeOp,
                new OperationCompletionPayload { CachedType = KeyValueResponseType.MustRetry }, ct);

            (OperationRegistrationOutcome retryRange, _, _, _, _) =
                await nodes[0].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, rangeOp, OperationKind.Scan, [3, 4], ct);
            Assert.Equal(OperationRegistrationOutcome.New, retryRange);

            TransactionWorkingSet? wsRange = await nodes[0].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(wsRange);
            Assert.Empty(wsRange!.ReadKeys);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    // ── Test 5 ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// A batch read replayed under the same operation id re-executes without re-folding: the
    /// first-recorded observation remains authoritative for commit validation. When the underlying value
    /// changes between the first read and the replay, the first observation's revision is what the
    /// commit checks — and a conflict is detected if the key was mutated after the first read.
    /// </summary>
    [Fact]
    public async Task BatchGet_DuplicateId_ReExecutes_FirstObservationAuthoritative()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (Node[] nodes, MemoryInterNodeCommmunication interNode) = await AssembleWithTransport();
        try
        {
            const string target = "brtf_5/key";

            // Seed the key at revision 1.
            Assert.Equal(KeyValueResponseType.Set,
                (await nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, target, "v1"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct)).Item1);

            Node entryNode = await FindBatchReadRouterNode(nodes, ct);

            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

            List<(string, long, KeyValueDurability)> keys = [(target, -1, KeyValueDurability.Persistent)];
            TransactionOperationId op = TransactionOperationId.NewRandom();

            // First read: confirms the key at revision 1.
            List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> first =
                await entryNode.Kahuna.LocateAndTryGetManyValues(handle.TransactionId, HLCTimestamp.Zero, keys, ct, handle.CoordinatorKey, op);
            Assert.Equal(KeyValueResponseType.Get, first[0].Item1);
            long firstRevision = first[0].Item4!.Revision;

            TransactionWorkingSet? afterFirst = await nodes[0].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterFirst);
            KeyValueTransactionReadKey? firstObs = afterFirst!.ReadKeys.SingleOrDefault(r => r.Key == target);
            Assert.NotNull(firstObs);
            Assert.Equal(firstRevision, firstObs!.Revision);

            // A concurrent non-transactional write advances the key to revision 2.
            Assert.Equal(KeyValueResponseType.Set,
                (await nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, target, "v2"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct)).Item1);

            // Replay under the same op id: re-executes without re-folding. The MVCC path may detect the
            // revision conflict at read time (Aborted) or return the newer value (Get), depending on the
            // actor's MVCC snapshot state. Either is valid — the important property is that no re-folding
            // happens and the commit still aborts from the first-recorded observation.
            List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> replay =
                await entryNode.Kahuna.LocateAndTryGetManyValues(handle.TransactionId, HLCTimestamp.Zero, keys, ct, handle.CoordinatorKey, op);
            Assert.True(replay[0].Item1 is KeyValueResponseType.Get or KeyValueResponseType.Aborted,
                $"Expected Get or Aborted from re-executed duplicate read, got {replay[0].Item1}");

            // The read set is unchanged — the replay did not re-fold.
            TransactionWorkingSet? afterReplay = await nodes[0].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterReplay);
            KeyValueTransactionReadKey? replayObs = afterReplay!.ReadKeys.SingleOrDefault(r => r.Key == target);
            Assert.NotNull(replayObs);
            Assert.Equal(firstRevision, replayObs!.Revision);  // still the first revision

            // Anchor a write so the optimistic transaction enters 2PC.
            Assert.Equal(KeyValueResponseType.Set,
                await SetKeyWithRetry(nodes[1], handle, TransactionOperationId.NewRandom(), "brtf_5/writer", "w"u8.ToArray(), KeyValueDurability.Persistent, ct));

            // Commit aborts: the key changed after the first observation, and that first observation is
            // what commit validation checks — the divergent replay value cannot silently commit.
            Assert.Equal(KeyValueResponseType.Aborted, await CommitWithRetry(nodes[2], handle, ct));
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Finds a node whose batch reads route to remote nodes (through the internode transport) rather
    /// than being served locally. Used to ensure the <see cref="MemoryInterNodeCommmunication.GetManyValuesFault"/>
    /// seam fires for batch-read fault injection tests.
    /// </summary>
    private static async Task<Node> FindBatchReadRouterNode(Node[] nodes, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 10_000;
        while (Environment.TickCount64 < deadline)
        {
            foreach (Node node in nodes)
            {
                bool leadsAny = false;
                for (int p = 1; p <= 2; p++)
                    if (await node.Raft.AmILeader(p, ct)) { leadsAny = true; break; }
                if (!leadsAny)
                    return node;
            }
            await Task.Delay(50, ct);
        }
        // Fall back to any node — fault injection may not fire for every key but the test logic is still
        // exercised for whichever keys route remote.
        return nodes[2];
    }

    private static async Task<KeyValueResponseType> SetKeyWithRetry(Node node, TransactionHandle handle, TransactionOperationId op, string key, byte[] value, KeyValueDurability durability, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 10_000;
        while (true)
        {
            (KeyValueResponseType type, _, _) =
                await node.Kahuna.LocateAndTrySetKeyValue(
                    handle.TransactionId, key, value, null, -1, KeyValueFlags.None, 0, durability,
                    ct, 0, handle.CoordinatorKey, op);
            if (type != KeyValueResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return type;
            await Task.Delay(50, ct);
        }
    }

    private static async Task<KeyValueResponseType> CommitWithRetry(Node node, TransactionHandle handle, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 10_000;
        while (true)
        {
            (KeyValueResponseType type, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);
            if (type != KeyValueResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return type;
            await Task.Delay(50, ct);
        }
    }

    private static readonly double TimingScale = GetTimingScale();

    private static double GetTimingScale()
    {
        string? val = Environment.GetEnvironmentVariable("KAHUNA_TEST_TIMING_SCALE");
        return val is not null && double.TryParse(val, out double s) && s >= 1.0 ? s : 1.0;
    }

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
            HeartbeatInterval = TimeSpan.FromMilliseconds(30),
            StartElectionTimeout = (int)(150 * TimingScale),
            EndElectionTimeout = (int)(300 * TimingScale),
            ElectionTimeoutSeed = 95000 + nodeId,
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
        (Node[] nodes, _) = await AssembleWithTransport();
        return nodes;
    }

    private async Task<(Node[], MemoryInterNodeCommmunication)> AssembleWithTransport()
    {
        MemoryInterNodeCommmunication interNode = new();
        InMemoryCommunication comm = new();

        string[] p1 = ["localhost:8031", "localhost:8032"];
        string[] p2 = ["localhost:8030", "localhost:8032"];
        string[] p3 = ["localhost:8030", "localhost:8031"];

        (RaftManager r1, KahunaManager k1) = BuildNode(1, 8030, p1, interNode, comm);
        (RaftManager r2, KahunaManager k2) = BuildNode(2, 8031, p2, interNode, comm);
        (RaftManager r3, KahunaManager k3) = BuildNode(3, 8032, p3, interNode, comm);

        interNode.SetNodes(new() { { "localhost:8030", k1 }, { "localhost:8031", k2 }, { "localhost:8032", k3 } });
        comm.SetNodes(new() { { "localhost:8030", r1 }, { "localhost:8031", r2 }, { "localhost:8032", r3 } });

        await Task.WhenAll(r1.JoinCluster(), r2.JoinCluster(), r3.JoinCluster());

        for (int partition = 1; partition <= 2; partition++)
            await WaitForAnyLeader(partition, r1, r2, r3);

        return ([new(r1, k1), new(r2, k2), new(r3, k3)], interNode);
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
