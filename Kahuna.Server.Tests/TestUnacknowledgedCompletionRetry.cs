using System.Diagnostics;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A registered operation whose completion the coordinator does not acknowledge keeps its record pending, and every
/// commit and rollback of the transaction waits for that record. A caller that stops retrying (its deadline ended,
/// another statement failed) never sends the same-id retry that would complete it, so the participant sends the
/// completion again itself, detached from the caller. These cases refuse the first completion of one operation on
/// one embedded node, make no retry, and finalize: the finalize ends at once, with the effect folded (a commit
/// keeps it, a rollback releases it).
/// </summary>
public sealed class TestUnacknowledgedCompletionRetry
{
    private const string Prefix = "unack";

    // Well under the finalize drain deadline (the session timeout, 5 s by default): a finalize that waits on a
    // record nobody completes takes the whole deadline and answers MustRetry.
    private const int FinalizeBoundMs = 3_000;

    private readonly ILoggerFactory loggerFactory;

    public TestUnacknowledgedCompletionRetry(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    public enum Shape
    {
        Write,
        PointLock,
        RangeLock,
        Read,
        BucketScan
    }

    /// <summary>Refuses the first completion routed for one operation id and lets every later one land.</summary>
    private sealed class OneShotRefusal(TransactionOperationId target)
    {
        private int fired;

        public int Fired => Volatile.Read(ref fired);

        public bool Refuse(HLCTimestamp _, TransactionOperationId operationId) =>
            operationId == target && Interlocked.CompareExchange(ref fired, 1, 0) == 0;
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
    {
        EmbeddedKahunaOptions options = new()
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        };

        EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync(Prefix + "/k00", ct);
        return node;
    }

    private static KahunaManager Manager(EmbeddedKahunaNode node) => (KahunaManager)node.Kahuna;

    private static OperationRegistrar Registrar(EmbeddedKahunaNode node) => Manager(node).KeyValues.Registrar;

    private static async Task Seed(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType set, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, System.Text.Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, set);
    }

    private static async Task<TransactionHandle> StartTx(IKahuna kahuna, CancellationToken ct, KeyValueTransactionLocking locking = KeyValueTransactionLocking.Pessimistic)
    {
        (KeyValueResponseType type, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(new() { Locking = locking }, ct);
        Assert.Equal(KeyValueResponseType.Set, type);
        Assert.False(handle.IsEmpty);
        return handle;
    }

    private static async Task<TransactionWorkingSet> WorkingSet(IKahuna kahuna, TransactionHandle handle, CancellationToken ct)
    {
        TransactionWorkingSet? ws = await kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
        Assert.NotNull(ws);
        return ws!;
    }

    private static async Task<T> RetryWhileMustRetry<T>(Func<Task<T>> operation, Func<T, KeyValueResponseType> status, CancellationToken ct, int timeoutMs = 5_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        T result = await operation();

        while (status(result) == KeyValueResponseType.MustRetry && Environment.TickCount64 < deadline)
        {
            await Task.Delay(50, ct);
            result = await operation();
        }

        return result;
    }

    private static async Task WaitUntil(Func<bool> condition, CancellationToken ct, int timeoutMs = 5_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;

        while (!condition())
        {
            Assert.True(Environment.TickCount64 < deadline, "the condition did not hold in time");
            await Task.Delay(20, ct);
        }
    }

    /// <summary>Runs one registered operation of <paramref name="shape"/> and returns its answer.</summary>
    private static async Task<KeyValueResponseType> RunShape(IKahuna kahuna, Shape shape, TransactionHandle handle, string key, TransactionOperationId op, CancellationToken ct)
    {
        switch (shape)
        {
            case Shape.Write:
            {
                (KeyValueResponseType type, _, _) = await kahuna.LocateAndTrySetKeyValue(
                    handle.TransactionId, key, "v2"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
                    coordinatorKey: handle.CoordinatorKey, operationId: op);
                return type;
            }

            case Shape.PointLock:
            {
                (KeyValueResponseType type, _, _, _) = await kahuna.LocateAndTryAcquireExclusiveLock(
                    handle.TransactionId, key, 30_000, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, op);
                return type;
            }

            case Shape.RangeLock:
            {
                (KeyValueResponseType type, _) = await kahuna.LocateAndTryAcquireRangeLock(
                    handle.TransactionId, BucketOf(key), null, true, null, false, 30_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct,
                    handle.CoordinatorKey, op);
                return type;
            }

            case Shape.Read:
            {
                (KeyValueResponseType type, _) = await kahuna.LocateAndTryGetValue(
                    handle.TransactionId, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, op);
                return type;
            }

            default:
            {
                KeyValueGetByBucketResult result = await kahuna.LocateAndGetByBucket(
                    handle.TransactionId, BucketOf(key), HLCTimestamp.Zero, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, op);
                return result.Type;
            }
        }
    }

    private static string BucketOf(string key) => key[..key.LastIndexOf('/')];

    /// <summary>
    /// After the rollback, a second transaction gets what the first one held: the lock of a write or a point lock,
    /// the exclusive range of a range lock. A read and a scan hold nothing, and a plain read still sees the seed.
    /// </summary>
    private static async Task AssertNothingHeld(IKahuna kahuna, Shape shape, string key, CancellationToken ct)
    {
        TransactionHandle second = await StartTx(kahuna, ct);

        KeyValueResponseType expected = shape is Shape.Write ? KeyValueResponseType.Set
            : shape is Shape.Read or Shape.BucketScan ? KeyValueResponseType.Get
            : KeyValueResponseType.Locked;

        KeyValueResponseType got = await RetryWhileMustRetry(
            () => RunShape(kahuna, shape, second, key, TransactionOperationId.NewRandom(), ct), r => r, ct, timeoutMs: 2_000);

        Assert.Equal(expected, got);
        Assert.Equal(KeyValueResponseType.RolledBack, await kahuna.LocateAndRollbackTransaction(second, ct));

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.Equal("v1"u8.ToArray(), entry!.Value);
    }

    // ── The caller stops, then finalizes ──────────────────────────────────────────────────────────

    /// <summary>
    /// The first completion of the operation is refused, so it answers <c>MustRetry</c>; the caller makes no retry
    /// and rolls back. The detached retry completes the record, so one rollback ends at once with
    /// <c>RolledBack</c>, and nothing the operation took stays behind.
    /// </summary>
    [Theory]
    [InlineData(Shape.Write)]
    [InlineData(Shape.PointLock)]
    [InlineData(Shape.RangeLock)]
    [InlineData(Shape.Read)]
    [InlineData(Shape.BucketScan)]
    public async Task CompletionRefusedOnce_CallerStops_RollbackEndsAtOnce_AndNothingStaysHeld(Shape shape)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string key = $"{Prefix}-{shape}-{Guid.NewGuid():N}/k";
        await Seed(node.Kahuna, key, "v1", ct);

        TransactionHandle handle = await StartTx(node.Kahuna, ct, shape is Shape.Read or Shape.BucketScan ? KeyValueTransactionLocking.Optimistic : KeyValueTransactionLocking.Pessimistic);
        TransactionOperationId op = TransactionOperationId.NewRandom();
        OneShotRefusal refusal = new(op);
        Registrar(node).TestCompletionRefusal = refusal.Refuse;

        long drivenBefore = DurableTransactionMetrics.UnacknowledgedCompletionCounts.Driven;

        Assert.Equal(KeyValueResponseType.MustRetry, await RunShape(node.Kahuna, shape, handle, key, op, ct));
        Assert.Equal(1, refusal.Fired);

        Stopwatch clock = Stopwatch.StartNew();
        KeyValueResponseType rolledBack = await node.Kahuna.LocateAndRollbackTransaction(handle, ct);

        Assert.Equal(KeyValueResponseType.RolledBack, rolledBack);
        Assert.True(clock.ElapsedMilliseconds < FinalizeBoundMs, $"the rollback waited on the finalize drain: {clock.ElapsedMilliseconds} ms");

        // The rollback can end as soon as the coordinator folds the completion, before the detached retry counts it.
        // The retry counts how it ended before it leaves the count of live retries.
        await WaitUntil(() => Registrar(node).ActiveRedrives == 0, ct);
        Assert.True(DurableTransactionMetrics.UnacknowledgedCompletionCounts.Driven > drivenBefore);
        Assert.Equal(0, Registrar(node).ParticipantOperationCache.Count);

        Registrar(node).TestCompletionRefusal = null;
        await AssertNothingHeld(node.Kahuna, shape, key, ct);
    }

    /// <summary>
    /// The same refused write, and the caller commits without a retry: the commit includes the write that the
    /// detached retry folded, so the committed value is the write's. A commit never ends without the effect: it
    /// either folds it or answers <c>MustRetry</c> until it can.
    /// </summary>
    [Fact]
    public async Task WriteCompletionRefusedOnce_CallerStops_CommitIncludesTheWrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string key = $"{Prefix}-commit-{Guid.NewGuid():N}/k";
        await Seed(node.Kahuna, key, "v1", ct);

        TransactionHandle handle = await StartTx(node.Kahuna, ct);
        TransactionOperationId op = TransactionOperationId.NewRandom();
        OneShotRefusal refusal = new(op);
        Registrar(node).TestCompletionRefusal = refusal.Refuse;

        Assert.Equal(KeyValueResponseType.MustRetry, await RunShape(node.Kahuna, Shape.Write, handle, key, op, ct));

        Stopwatch clock = Stopwatch.StartNew();
        TransactionCommitAnswer committed = await RetryWhileMustRetry(
            () => node.Kahuna.LocateAndCommitTransaction(handle, ct), r => r.Type, ct);

        Assert.Equal(KeyValueResponseType.Committed, committed.Type);
        Assert.True(clock.ElapsedMilliseconds < FinalizeBoundMs, $"the commit waited on the finalize drain: {clock.ElapsedMilliseconds} ms");

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await RetryWhileMustRetry(
            async () => await node.Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
            r => r.Item1, ct);

        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.Equal("v2"u8.ToArray(), entry!.Value);
    }

    /// <summary>
    /// A same-id retry of the caller and the detached retry send the same kept completion at the same time. The
    /// detached copy is held inside the routing while the caller's retry folds the write; the held copy then lands
    /// on a completed record and changes nothing. The caller gets the write's own answer, the working set holds the
    /// write once with no broken staging, and the commit installs it.
    /// </summary>
    [Fact]
    public async Task SameIdRetryBesideTheDetachedRetry_FoldsOnce_AndAnswersTheWrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string key = $"{Prefix}-beside-{Guid.NewGuid():N}/k";
        await Seed(node.Kahuna, key, "v1", ct);

        TransactionHandle handle = await StartTx(node.Kahuna, ct);
        TransactionOperationId op = TransactionOperationId.NewRandom();

        int calls = 0;
        TaskCompletionSource detachedEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using ManualResetEventSlim releaseDetached = new(false);

        // Call 1 is the caller's first completion: refused. Call 2 is the detached retry, since the test has not
        // retried yet: held until the caller's retry (call 3) folded the write.
        Registrar(node).TestCompletionRefusal = (_, operationId) =>
        {
            if (operationId != op)
                return false;

            int call = Interlocked.Increment(ref calls);
            if (call == 1)
                return true;

            if (call == 2)
            {
                detachedEntered.TrySetResult();
                releaseDetached.Wait(TimeSpan.FromSeconds(10));
            }

            return false;
        };

        (KeyValueResponseType first, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            handle.TransactionId, key, "v2"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: op);
        Assert.Equal(KeyValueResponseType.MustRetry, first);

        await detachedEntered.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);

        (KeyValueResponseType retried, long revision, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            handle.TransactionId, key, "v2"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: op);

        Assert.Equal(KeyValueResponseType.Set, retried);
        Assert.Equal(3, Volatile.Read(ref calls));

        releaseDetached.Set();
        await WaitUntil(() => Registrar(node).ActiveRedrives == 0, ct);

        TransactionWorkingSet ws = await WorkingSet(node.Kahuna, handle, ct);
        Assert.Equal(0, ws.PendingOperationCount);
        Assert.Single(ws.ModifiedKeys, m => m.Key == key);

        TransactionContext context = Manager(node).TransactionCoordinator.sessions[handle.TransactionId];
        Assert.Null(context.StagedChainBreak);
        Assert.Equal(revision, context.StagedMutations![key].Revision);

        TransactionCommitAnswer committed = await RetryWhileMustRetry(
            () => node.Kahuna.LocateAndCommitTransaction(handle, ct), r => r.Type, ct);
        Assert.Equal(KeyValueResponseType.Committed, committed.Type);
    }

    /// <summary>
    /// The work of a registered range lock throws after the registration (the caller's cancellation inside the
    /// locator), and the release of the registration is refused once. The detached retry sends the release again;
    /// it applies once the rollback closes the session, so the rollback ends at once.
    /// </summary>
    [Fact]
    public async Task ReleaseOfAThrownOperationRefusedOnce_IsSentAgain_AndTheRollbackEndsAtOnce()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string key = $"{Prefix}-thrown-{Guid.NewGuid():N}/k";
        await Seed(node.Kahuna, key, "v1", ct);

        TransactionHandle handle = await StartTx(node.Kahuna, ct);
        TransactionOperationId op = TransactionOperationId.NewRandom();
        OneShotRefusal refusal = new(op);
        Registrar(node).TestCompletionRefusal = refusal.Refuse;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Manager(node).RegisterAndAcquireRangeLockWithHook(
            handle.TransactionId, handle.CoordinatorKey, op, BucketOf(key), null, true, null, false,
            30_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive,
            afterSnapshot: () => Task.FromException(new OperationCanceledException()),
            cancellationToken: ct));

        Assert.Equal(1, refusal.Fired);

        // The detached copies of the release wait for the close: while the session takes operations, the record
        // stays pending, so a new registration of the same id could not be cancelled by a late copy.
        await Task.Delay(400, ct);
        Assert.Equal(1, (await WorkingSet(node.Kahuna, handle, ct)).PendingOperationCount);
        Assert.Equal(1, Registrar(node).ActiveRedrives);

        Stopwatch clock = Stopwatch.StartNew();
        Assert.Equal(KeyValueResponseType.RolledBack, await node.Kahuna.LocateAndRollbackTransaction(handle, ct));
        Assert.True(clock.ElapsedMilliseconds < FinalizeBoundMs, $"the rollback waited on the finalize drain: {clock.ElapsedMilliseconds} ms");

        await WaitUntil(() => Registrar(node).ActiveRedrives == 0, ct);
    }

    // ── The release that waits for the close ──────────────────────────────────────────────────────

    /// <summary>
    /// A release in the form the detached retry sends is not applied while the session takes operations: it
    /// answers <c>MustRetry</c> and the record stays pending. Once a rollback closes the session, the same release
    /// cancels the record, and the rollback that waits on it ends.
    /// </summary>
    [Fact]
    public async Task GuardedRelease_WaitsForTheClose_ThenEndsTheRollbackDrain()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;

        TransactionHandle handle = await StartTx(kahuna, ct);
        TransactionOperationId op = TransactionOperationId.NewRandom();

        (OperationRegistrationOutcome begun, _, _, _, _, _) = await kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, op, OperationKind.Set, [1, 2, 3], ct);
        Assert.Equal(OperationRegistrationOutcome.New, begun);

        OperationCompletionPayload guarded = new() { CachedType = KeyValueResponseType.MustRetry, ReleaseOnceClosed = true };

        (KeyValueResponseType early, _) = await kahuna.LocateAndCompleteOperation(handle.CoordinatorKey, handle.TransactionId, op, guarded, ct);
        Assert.Equal(KeyValueResponseType.MustRetry, early);
        Assert.Equal(1, (await WorkingSet(kahuna, handle, ct)).PendingOperationCount);

        TransactionContext context = Manager(node).TransactionCoordinator.sessions[handle.TransactionId];
        Task<KeyValueResponseType> rollback = kahuna.LocateAndRollbackTransaction(handle, ct);
        await WaitUntil(() => context.Lifecycle != SessionLifecycle.AcceptingOperations, ct);
        Assert.False(rollback.IsCompleted);

        (KeyValueResponseType late, _) = await kahuna.LocateAndCompleteOperation(handle.CoordinatorKey, handle.TransactionId, op, guarded, ct);
        Assert.Equal(KeyValueResponseType.Set, late);

        Assert.Equal(KeyValueResponseType.RolledBack, await rollback.WaitAsync(TimeSpan.FromSeconds(FinalizeBoundMs / 1000), ct));
    }

    /// <summary>
    /// A copy of a release can arrive after an earlier copy cancelled the record and the caller registered the
    /// same id again. The guarded copy leaves that new registration pending, so its completion folds the effect.
    /// A plain copy would cancel it, and the effect would be discarded.
    /// </summary>
    [Fact]
    public async Task GuardedRelease_LeavesANewRegistrationOfTheSameIdPending_SoItsEffectFolds()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        IKahuna kahuna = node.Kahuna;

        TransactionHandle handle = await StartTx(kahuna, ct);
        TransactionOperationId op = TransactionOperationId.NewRandom();
        byte[] digest = [4, 5, 6];

        (OperationRegistrationOutcome first, _, _, _, _, _) = await kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, op, OperationKind.Set, digest, ct);
        Assert.Equal(OperationRegistrationOutcome.New, first);

        (KeyValueResponseType released, _) = await kahuna.LocateAndCompleteOperation(
            handle.CoordinatorKey, handle.TransactionId, op, new OperationCompletionPayload { CachedType = KeyValueResponseType.MustRetry }, ct);
        Assert.Equal(KeyValueResponseType.Set, released);

        (OperationRegistrationOutcome second, _, _, _, _, _) = await kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, op, OperationKind.Set, digest, ct);
        Assert.Equal(OperationRegistrationOutcome.New, second);

        (KeyValueResponseType lateCopy, _) = await kahuna.LocateAndCompleteOperation(
            handle.CoordinatorKey, handle.TransactionId, op, new OperationCompletionPayload { CachedType = KeyValueResponseType.MustRetry, ReleaseOnceClosed = true }, ct);
        Assert.Equal(KeyValueResponseType.MustRetry, lateCopy);
        Assert.Equal(1, (await WorkingSet(kahuna, handle, ct)).PendingOperationCount);

        (KeyValueResponseType folded, _) = await kahuna.LocateAndCompleteOperation(
            handle.CoordinatorKey, handle.TransactionId, op,
            new OperationCompletionPayload
            {
                ModifiedKey = Prefix + "/guarded",
                Durability = KeyValueDurability.Persistent,
                CachedType = KeyValueResponseType.Set,
                CachedRevision = 1
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, folded);

        TransactionWorkingSet ws = await WorkingSet(kahuna, handle, ct);
        Assert.Equal(0, ws.PendingOperationCount);
        Assert.Contains(ws.ModifiedKeys, m => m.Key == Prefix + "/guarded");
    }

    /// <summary>
    /// A release that reaches a node holding no session for the transaction is not acknowledged on a node that is a
    /// member of a cluster: the session can live on another node that the owner probe missed, and an
    /// acknowledgement would leave its record pending with nothing kept to complete it.
    /// </summary>
    [Fact]
    public async Task Release_ForASessionThisNodeDoesNotHold_IsNotAcknowledged()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        HLCTimestamp unknown = Manager(node).Raft.HybridLogicalClock.TrySendOrLocalEvent(Manager(node).Raft.GetLocalNodeId());

        (KeyValueResponseType outcome, _) = await node.Kahuna.LocateAndCompleteOperation(
            Prefix + "/coordinator", unknown, TransactionOperationId.NewRandom(),
            new OperationCompletionPayload { CachedType = KeyValueResponseType.MustRetry }, ct);

        Assert.Equal(KeyValueResponseType.MustRetry, outcome);
    }

    /// <summary>
    /// A same-id retry of the caller completes the kept write while the detached retry waits between attempts. The
    /// detached retry then stops without another send. Its sends run with a clean execution context, so the flag
    /// that marks the caller's own calls tells the two apart.
    /// </summary>
    [Fact]
    public async Task SameIdRetryCompletesTheWriteFirst_TheDetachedRetryStopsWithoutAnotherSend()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string key = $"{Prefix}-first-{Guid.NewGuid():N}/k";
        await Seed(node.Kahuna, key, "v1", ct);

        TransactionHandle handle = await StartTx(node.Kahuna, ct);
        TransactionOperationId op = TransactionOperationId.NewRandom();

        AsyncLocal<bool> callerRetry = new();
        int refused = 0;
        int detachedSendsAfterSettle = 0;
        bool settled = false;

        // The caller's first completion is refused, and so is every send of the detached retry until the caller's
        // own retry completed the write. A send after that is counted.
        Registrar(node).TestCompletionRefusal = (_, operationId) =>
        {
            if (operationId != op || callerRetry.Value)
                return false;

            if (Volatile.Read(ref settled))
            {
                Interlocked.Increment(ref detachedSendsAfterSettle);
                return false;
            }

            Interlocked.Increment(ref refused);
            return true;
        };

        Assert.Equal(KeyValueResponseType.MustRetry, await RunShape(node.Kahuna, Shape.Write, handle, key, op, ct));

        // The first completion and at least one send of the detached retry were refused.
        await WaitUntil(() => Volatile.Read(ref refused) >= 2, ct);

        callerRetry.Value = true;
        (KeyValueResponseType retried, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            handle.TransactionId, key, "v2"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: op);
        callerRetry.Value = false;
        Volatile.Write(ref settled, true);

        Assert.Equal(KeyValueResponseType.Set, retried);

        await WaitUntil(() => Registrar(node).ActiveRedrives == 0, ct);
        Assert.Equal(0, Volatile.Read(ref detachedSendsAfterSettle));
    }

    // ── Diagnostics and stop conditions ────────────────────────────────────────────────────────────

    /// <summary>
    /// The detached retry stops once its cache entry is settled. A removal settles the entry, and so does a newer
    /// result for the same operation. An eviction does not: the retry is then the only completer left.
    /// </summary>
    [Fact]
    public void CacheEntry_IsSettledByARemovalOrANewerResult_NotByAnEviction()
    {
        ParticipantOperationCache cache = new();
        HLCTimestamp tx = new(1, 100, 0);

        TransactionOperationId removedOp = TransactionOperationId.NewRandom();
        ParticipantOperationCache.Entry removed = cache.Store(tx, removedOp, KeyValueResponseType.Set, new OperationCompletionPayload());
        Assert.False(removed.IsSettled);
        cache.Remove(tx, removedOp);
        Assert.True(removed.IsSettled);

        TransactionOperationId replacedOp = TransactionOperationId.NewRandom();
        ParticipantOperationCache.Entry older = cache.Store(tx, replacedOp, KeyValueResponseType.Set, new OperationCompletionPayload());
        ParticipantOperationCache.Entry newer = cache.Store(tx, replacedOp, KeyValueResponseType.Set, new OperationCompletionPayload());
        Assert.True(older.IsSettled);
        Assert.False(newer.IsSettled);

        // A removal by entry leaves a newer entry of the same operation in place.
        cache.Remove(tx, replacedOp, older);
        Assert.True(cache.TryGet(tx, replacedOp, out _, out _));
        Assert.False(newer.IsSettled);

        TransactionOperationId evictedOp = TransactionOperationId.NewRandom();
        ParticipantOperationCache.Entry evicted = cache.Store(tx, evictedOp, KeyValueResponseType.Set, new OperationCompletionPayload());

        for (int i = 0; i < ParticipantOperationCache.MaxEntries; i++)
            cache.Store(tx, TransactionOperationId.NewRandom(), KeyValueResponseType.Set, new OperationCompletionPayload());

        Assert.False(cache.TryGet(tx, evictedOp, out _, out _));
        Assert.False(evicted.IsSettled);
    }

    /// <summary>
    /// A finalize drain that waits on a record nobody completes is counted after the report threshold. The
    /// detached retry is turned off here, so the record stays pending until the drain deadline.
    /// </summary>
    [Fact]
    public async Task SlowDrain_IsCounted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        string key = $"{Prefix}-slow-{Guid.NewGuid():N}/k";
        await Seed(node.Kahuna, key, "v1", ct);

        TransactionHandle handle = await StartTx(node.Kahuna, ct);
        TransactionOperationId op = TransactionOperationId.NewRandom();
        Registrar(node).TestCompletionRefusal = new OneShotRefusal(op).Refuse;
        Registrar(node).TestDisableRedrive = true;

        Assert.Equal(KeyValueResponseType.MustRetry, await RunShape(node.Kahuna, Shape.Write, handle, key, op, ct));

        long before = DurableTransactionMetrics.SlowFinalizeDrainsCount;

        // The drain deadline is the session timeout (5 s by default): the rollback answers MustRetry after it.
        Assert.Equal(KeyValueResponseType.MustRetry, await node.Kahuna.LocateAndRollbackTransaction(handle, ct));
        Assert.True(DurableTransactionMetrics.SlowFinalizeDrainsCount > before);
    }

    /// <summary>
    /// A detached retry whose completion is refused every time ends at the node shutdown, and is counted as given
    /// up.
    /// </summary>
    [Fact]
    public async Task DetachedRetry_EndsAtShutdown()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);
        OperationRegistrar registrar = Registrar(node);

        try
        {
            string key = $"{Prefix}-shutdown-{Guid.NewGuid():N}/k";
            await Seed(node.Kahuna, key, "v1", ct);

            TransactionHandle handle = await StartTx(node.Kahuna, ct);
            TransactionOperationId op = TransactionOperationId.NewRandom();
            registrar.TestCompletionRefusal = (_, operationId) => operationId == op;

            long givenUpBefore = DurableTransactionMetrics.UnacknowledgedCompletionCounts.GivenUp;

            Assert.Equal(KeyValueResponseType.MustRetry, await RunShape(node.Kahuna, Shape.Write, handle, key, op, ct));
            Assert.Equal(1, registrar.ActiveRedrives);

            Manager(node).KeyValues.SignalShutdown();

            await WaitUntil(() => registrar.ActiveRedrives == 0, ct);
            Assert.True(DurableTransactionMetrics.UnacknowledgedCompletionCounts.GivenUp > givenUpBefore);
        }
        finally
        {
            await node.DisposeAsync();
        }
    }
}
