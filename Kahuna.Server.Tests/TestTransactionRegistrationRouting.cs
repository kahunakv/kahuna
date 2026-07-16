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
            (OperationRegistrationOutcome outcome, _, _, _, _) =
                await nodes[1].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, op1, OperationKind.Set, digestA, ct);
            Assert.Equal(OperationRegistrationOutcome.New, outcome);

            // Complete it with a confirmed persistent effect and a cached write response. The completion
            // returns the record anchor assigned from this first persistent modified key.
            HLCTimestamp cachedTs = new(1, 500, 0);
            (KeyValueResponseType completeOutcome, string? completeAnchor) = await nodes[2].Kahuna.LocateAndCompleteOperation(
                handle.CoordinatorKey, handle.TransactionId, op1,
                new OperationCompletionPayload
                {
                    ModifiedKey = "k1",
                    AcquiredPointLock = "k1",
                    Durability = KeyValueDurability.Persistent,
                    CachedType = KeyValueResponseType.Set,
                    CachedRevision = 7,
                    CachedTimestamp = cachedTs
                },
                ct);
            Assert.Equal(KeyValueResponseType.Set, completeOutcome);
            Assert.Equal("k1", completeAnchor);

            // Retry with the same id (from yet another node) → cached response, no re-apply, and the same
            // canonical anchor is recovered even though this retry never re-ran the mutation.
            (OperationRegistrationOutcome replayOutcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? replayAnchor) =
                await nodes[0].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, op1, OperationKind.Set, digestA, ct);
            Assert.Equal(OperationRegistrationOutcome.AlreadyCompleted, replayOutcome);
            Assert.Equal(KeyValueResponseType.Set, cachedType);
            Assert.Equal(7, cachedRevision);
            Assert.Equal(cachedTs, cachedTimestamp);
            Assert.Equal("k1", replayAnchor);

            // Same id, different declaration → rejected as a conflict.
            (OperationRegistrationOutcome conflictOutcome, _, _, _, _) =
                await nodes[1].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, op1, OperationKind.Set, digestB, ct);
            Assert.Equal(OperationRegistrationOutcome.RejectedDuplicate, conflictOutcome);

            // A distinct operation id registers fresh.
            (OperationRegistrationOutcome op2Outcome, _, _, _, _) =
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
            (OperationRegistrationOutcome outcome, _, _, _, _) = await nodes[0].Kahuna.LocateAndBeginOperation(
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

    [Fact]
    public async Task TransactionalGet_UnderOperationId_ReadsOwnWrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

            byte[] value = "gv"u8.ToArray();
            (KeyValueResponseType setType, _, _) = await SetWithRetry(nodes[1], handle, TransactionOperationId.NewRandom(), value, ct);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // A registered read of the just-written key returns it (read-your-own-writes) and records the observation.
            TransactionOperationId getOp = TransactionOperationId.NewRandom();
            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await GetWithRetry(nodes[2], handle, getOp, ct);
            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.NotNull(entry);
            Assert.Equal(value, entry!.Value);

            // A repeat read under the same op id is idempotent (re-reads, same value).
            (KeyValueResponseType replayType, ReadOnlyKeyValueEntry? replayEntry) =
                await nodes[0].Kahuna.LocateAndTryGetValue(handle.TransactionId, "wk1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, getOp);
            Assert.Equal(KeyValueResponseType.Get, replayType);
            Assert.Equal(value, replayEntry!.Value);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    [Fact]
    public async Task TransactionalPointLock_AcquireAndRelease_DedupUnderSameOperationId()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            TransactionOperationId acquireOp = TransactionOperationId.NewRandom();
            KeyValueResponseType acquired = await LockWithRetry(() =>
                nodes[1].Kahuna.LocateAndTryAcquireExclusiveLock(handle.TransactionId, "lk1", 10_000, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, acquireOp), ct);
            Assert.Equal(KeyValueResponseType.Locked, acquired);

            // Replay the acquire under the same op id → reported as already held by this transaction.
            (KeyValueResponseType replayAcquire, _, _, _) =
                await nodes[2].Kahuna.LocateAndTryAcquireExclusiveLock(handle.TransactionId, "lk1", 10_000, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, acquireOp);
            Assert.Equal(KeyValueResponseType.Locked, replayAcquire);

            TransactionOperationId releaseOp = TransactionOperationId.NewRandom();
            (KeyValueResponseType released, _) =
                await nodes[0].Kahuna.LocateAndTryReleaseExclusiveLock(handle.TransactionId, "lk1", KeyValueDurability.Persistent, ct, handle.CoordinatorKey, releaseOp);
            Assert.Equal(KeyValueResponseType.Unlocked, released);

            // Replay the release under the same op id → idempotent Unlocked.
            (KeyValueResponseType replayRelease, _) =
                await nodes[1].Kahuna.LocateAndTryReleaseExclusiveLock(handle.TransactionId, "lk1", KeyValueDurability.Persistent, ct, handle.CoordinatorKey, releaseOp);
            Assert.Equal(KeyValueResponseType.Unlocked, replayRelease);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    [Fact]
    public async Task Commit_ConsumesServerRecordedWorkingSet_WithEmptyClientLists()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

            byte[] value = "committed-value"u8.ToArray();
            (KeyValueResponseType setType, _, _) = await SetWithRetry(nodes[1], handle, TransactionOperationId.NewRandom(), value, ct);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // Commit with EMPTY client lists: the write must still commit because the coordinator owns
            // the modified-key set recorded during the transaction.
            KeyValueResponseType commit = await CommitWithRetry(nodes[2], handle, ct);
            Assert.Equal(KeyValueResponseType.Committed, commit);

            // The committed value is visible to a non-transactional read.
            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) =
                await nodes[0].Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, "wk1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.Equal(value, entry!.Value);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    [Fact]
    public async Task WorkingSetQuery_AndCloseSnapshot_ReflectServerRecordedEffects_AndFenceNewOps()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            // Record a write and a point lock under the transaction.
            (KeyValueResponseType setType, _, _) = await SetWithRetry(nodes[1], handle, TransactionOperationId.NewRandom(), "v1"u8.ToArray(), ct);
            Assert.Equal(KeyValueResponseType.Set, setType);
            KeyValueResponseType lockType = await LockWithRetry(() =>
                nodes[1].Kahuna.LocateAndTryAcquireExclusiveLock(handle.TransactionId, "lk1", 10_000, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, TransactionOperationId.NewRandom()), ct);
            Assert.Equal(KeyValueResponseType.Locked, lockType);

            // Live working-set query reflects the confirmed effects.
            TransactionWorkingSet? live = await nodes[2].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(live);
            Assert.Contains(live!.ModifiedKeys, m => m.Key == "wk1");
            Assert.Contains(live.AcquiredLocks, m => m.Key == "lk1");

            // Close-and-snapshot returns the frozen set and is idempotent.
            (KeyValueResponseType closeType, TransactionWorkingSet? frozen) = await nodes[0].Kahuna.LocateAndCloseTransaction(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.Equal(KeyValueResponseType.Set, closeType);
            Assert.NotNull(frozen);
            Assert.Contains(frozen!.ModifiedKeys, m => m.Key == "wk1");

            (KeyValueResponseType closeAgain, TransactionWorkingSet? frozenAgain) = await nodes[1].Kahuna.LocateAndCloseTransaction(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.Equal(KeyValueResponseType.Set, closeAgain);
            Assert.NotNull(frozenAgain);

            // After close, a new transaction-scoped operation is fenced out.
            (KeyValueResponseType afterClose, _, _) = await nodes[2].Kahuna.LocateAndTrySetKeyValue(
                handle.TransactionId, "wk2", "late"u8.ToArray(), null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent,
                ct, 0, handle.CoordinatorKey, TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Aborted, afterClose);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// Close freezes the working set and returns its snapshot, then a later commit finalizes the frozen
    /// transaction — the required Close→Commit flow. Close must not make the session terminal (which would
    /// reject the commit as Aborted); it stays finalizable so the commit consumes the same frozen set.
    /// </summary>
    [Fact]
    public async Task CloseThenCommit_CommitsTheFrozenWorkingSet()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            byte[] value = "closed-then-committed"u8.ToArray();
            (KeyValueResponseType setType, _, _) = await SetWithRetry(nodes[1], handle, TransactionOperationId.NewRandom(), value, ct);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // Freeze and snapshot before committing (the cache-publish-before-commit flow).
            (KeyValueResponseType closeType, TransactionWorkingSet? frozen) =
                await nodes[0].Kahuna.LocateAndCloseTransaction(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.Equal(KeyValueResponseType.Set, closeType);
            Assert.Contains(frozen!.ModifiedKeys, m => m.Key == "wk1");

            // The frozen session is still finalizable: commit finalizes it (previously returned Aborted).
            Assert.Equal(KeyValueResponseType.Committed, await CommitWithRetry(nodes[2], handle, ct));

            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) =
                await nodes[0].Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, "wk1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.Equal(value, entry!.Value);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// Close followed by rollback rolls back the frozen transaction, leaving no committed state.
    /// </summary>
    [Fact]
    public async Task CloseThenRollback_RollsBackTheFrozenWorkingSet()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            (KeyValueResponseType setType, _, _) = await SetWithRetry(nodes[1], handle, TransactionOperationId.NewRandom(), "discarded"u8.ToArray(), ct);
            Assert.Equal(KeyValueResponseType.Set, setType);

            (KeyValueResponseType closeType, _) =
                await nodes[0].Kahuna.LocateAndCloseTransaction(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.Equal(KeyValueResponseType.Set, closeType);

            KeyValueResponseType rollback = await RollbackWithRetry(nodes[2], handle, ct);
            Assert.Equal(KeyValueResponseType.RolledBack, rollback);

            (KeyValueResponseType getType, _) =
                await nodes[0].Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, "wk1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.NotEqual(KeyValueResponseType.Get, getType);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    [Fact]
    public async Task PrefixLock_RoutesAndDedups_AndWorkingSetReflectsHeldPrefixLock()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            const string prefix = "pfx:";
            TransactionOperationId acquireOp = TransactionOperationId.NewRandom();
            KeyValueResponseType acquired = await PrefixLockWithRetry(() =>
                nodes[1].Kahuna.LocateAndTryAcquireExclusivePrefixLock(handle.TransactionId, prefix, 10_000, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, acquireOp), ct);
            Assert.Equal(KeyValueResponseType.Locked, acquired);

            // Replay the acquire under the same op id → reported as already held by this transaction.
            Assert.Equal(KeyValueResponseType.Locked,
                await nodes[2].Kahuna.LocateAndTryAcquireExclusivePrefixLock(handle.TransactionId, prefix, 10_000, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, acquireOp));

            // The coordinator working set reflects the held prefix lock.
            TransactionWorkingSet? live = await nodes[2].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(live);
            Assert.Contains(live!.AcquiredPrefixLocks, m => m.Key == prefix);

            TransactionOperationId releaseOp = TransactionOperationId.NewRandom();
            Assert.Equal(KeyValueResponseType.Unlocked,
                await nodes[0].Kahuna.LocateAndTryReleaseExclusivePrefixLock(handle.TransactionId, prefix, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, releaseOp));

            // Replay the release under the same op id → idempotent Unlocked.
            Assert.Equal(KeyValueResponseType.Unlocked,
                await nodes[1].Kahuna.LocateAndTryReleaseExclusivePrefixLock(handle.TransactionId, prefix, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, releaseOp));

            // After a confirmed release the descriptor is dropped from the coordinator-owned lock set.
            TransactionWorkingSet? afterRelease = await nodes[0].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterRelease);
            Assert.DoesNotContain(afterRelease!.AcquiredPrefixLocks, m => m.Key == prefix);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    [Fact]
    public async Task RangeLock_AcquireUpgradeRelease_RoutesAndDedups_AndWorkingSetReflectsRange()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            const string prefix = "rng";
            const string startKey = "a";
            const string endKey = "z";

            // Shared acquire.
            TransactionOperationId sharedOp = TransactionOperationId.NewRandom();
            KeyValueResponseType shared = await RangeLockWithRetry(() =>
                nodes[1].Kahuna.LocateAndTryAcquireRangeLock(handle.TransactionId, prefix, startKey, true, endKey, false, 10_000, KeyValueDurability.Persistent, RangeLockMode.Shared, ct, handle.CoordinatorKey, sharedOp), ct);
            Assert.Equal(KeyValueResponseType.Locked, shared);

            // Replay under the same op id → idempotent, still held by this transaction.
            (KeyValueResponseType replayShared, _) =
                await nodes[2].Kahuna.LocateAndTryAcquireRangeLock(handle.TransactionId, prefix, startKey, true, endKey, false, 10_000, KeyValueDurability.Persistent, RangeLockMode.Shared, ct, handle.CoordinatorKey, sharedOp);
            Assert.Equal(KeyValueResponseType.Locked, replayShared);

            TransactionWorkingSet? afterShared = await nodes[2].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterShared);
            KeyValueTransactionRangeLock? sharedDesc = afterShared!.AcquiredRangeLocks.SingleOrDefault(r => r.Prefix == prefix && r.StartKey == startKey && r.EndKey == endKey);
            Assert.NotNull(sharedDesc);
            Assert.Equal(RangeLockMode.Shared, sharedDesc!.Mode);

            // Shared→exclusive upgrade of the same bounds (a distinct op id, distinct digest).
            TransactionOperationId upgradeOp = TransactionOperationId.NewRandom();
            KeyValueResponseType upgraded = await RangeLockWithRetry(() =>
                nodes[0].Kahuna.LocateAndTryAcquireRangeLock(handle.TransactionId, prefix, startKey, true, endKey, false, 10_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, handle.CoordinatorKey, upgradeOp), ct);
            Assert.Equal(KeyValueResponseType.Locked, upgraded);

            TransactionWorkingSet? afterUpgrade = await nodes[1].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterUpgrade);
            // The upgrade replaces the mode of the matching descriptor — exactly one range, now exclusive.
            List<KeyValueTransactionRangeLock> matching = afterUpgrade!.AcquiredRangeLocks.Where(r => r.Prefix == prefix && r.StartKey == startKey && r.EndKey == endKey).ToList();
            Assert.Single(matching);
            Assert.Equal(RangeLockMode.Exclusive, matching[0].Mode);

            // Release drops the descriptor.
            TransactionOperationId releaseOp = TransactionOperationId.NewRandom();
            Assert.Equal(KeyValueResponseType.Unlocked,
                await nodes[2].Kahuna.LocateAndTryReleaseExclusiveRangeLock(handle.TransactionId, prefix, startKey, true, endKey, false, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, releaseOp));

            // Replay the release → idempotent Unlocked.
            Assert.Equal(KeyValueResponseType.Unlocked,
                await nodes[0].Kahuna.LocateAndTryReleaseExclusiveRangeLock(handle.TransactionId, prefix, startKey, true, endKey, false, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, releaseOp));

            TransactionWorkingSet? afterRelease = await nodes[1].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterRelease);
            Assert.DoesNotContain(afterRelease!.AcquiredRangeLocks, r => r.Prefix == prefix && r.StartKey == startKey && r.EndKey == endKey);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    [Fact]
    public async Task Scan_RegistersAndRecordsReturnedItemsAsReadObservations()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            const string bucket = "scanbucket";
            string k1 = bucket + "/one";
            string k2 = bucket + "/two";

            // Seed two committed keys under the bucket (non-transactional).
            (KeyValueResponseType s1, _, _) = await nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, k1, "v1"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            (KeyValueResponseType s2, _, _) = await nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, k2, "v2"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, s1);
            Assert.Equal(KeyValueResponseType.Set, s2);

            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

            TransactionOperationId scanOp = TransactionOperationId.NewRandom();
            KeyValueGetByBucketResult result = await nodes[1].Kahuna.LocateAndGetByBucket(handle.TransactionId, bucket, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, scanOp);
            Assert.Equal(KeyValueResponseType.Get, result.Type);
            Assert.Equal(2, result.Items.Count);

            // Every returned item is recorded in the coordinator read set with point-read-set semantics.
            TransactionWorkingSet? live = await nodes[2].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(live);
            Assert.Contains(live!.ReadKeys, r => r.Key == k1);
            Assert.Contains(live.ReadKeys, r => r.Key == k2);

            // Replaying the scan under the same op id re-runs the read without changing the recorded set.
            KeyValueGetByBucketResult replay = await nodes[2].Kahuna.LocateAndGetByBucket(handle.TransactionId, bucket, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, scanOp);
            Assert.Equal(KeyValueResponseType.Get, replay.Type);
            Assert.Equal(2, replay.Items.Count);

            TransactionWorkingSet? afterReplay = await nodes[0].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterReplay);
            Assert.Equal(2, afterReplay!.ReadKeys.Count);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// Pins the register-remote forward: a transaction-scoped operation entered on a node that is NOT
    /// the coordinator-partition leader is dispatched over the inter-node transport to the coordinator,
    /// which records its effect. The coordinator-partition leader for the same transaction handles the
    /// registration locally with no transport hop. This proves the forward branch fires on the
    /// coordinator≠entry path rather than every registration happening to be local.
    /// </summary>
    [Fact]
    public async Task RegisterRemote_FromNonCoordinatorNode_ForwardsOverTransport_AndCoordinatorRecordsEffect()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (Node[] nodes, MemoryInterNodeCommmunication interNode) = await AssembleWithTransport();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            // Exactly one node leads the coordinator partition (Locate(coordinatorKey)); route a fresh
            // registration through every node and classify each by whether it hopped the transport.
            Node? coordinatorLeader = null;
            Node? remoteEntry = null;
            TransactionOperationId remoteOp = default;
            byte[] digest = [1, 2, 3];

            foreach (Node node in nodes)
            {
                int before = interNode.BeginOperationCallCount;
                TransactionOperationId opId = TransactionOperationId.NewRandom();
                (OperationRegistrationOutcome outcome, _, _, _, _) =
                    await node.Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, opId, OperationKind.Set, digest, ct);
                Assert.Equal(OperationRegistrationOutcome.New, outcome);

                if (interNode.BeginOperationCallCount == before)
                    coordinatorLeader = node;      // handled locally: this node leads the coordinator partition
                else
                {
                    remoteEntry = node;            // forwarded inter-node: this node is not the leader
                    remoteOp = opId;
                }
            }

            // The forward branch and the local branch were both exercised.
            Assert.NotNull(coordinatorLeader);
            Assert.NotNull(remoteEntry);

            // Completing the forwarded op from the same non-coordinator node hops the transport again,
            // carrying the confirmed effect to the coordinator.
            int beforeComplete = interNode.CompleteOperationCallCount;
            await remoteEntry!.Kahuna.LocateAndCompleteOperation(
                handle.CoordinatorKey, handle.TransactionId, remoteOp,
                new OperationCompletionPayload
                {
                    ModifiedKey = "rk1",
                    Durability = KeyValueDurability.Persistent,
                    CachedType = KeyValueResponseType.Set,
                    CachedRevision = 1,
                    CachedTimestamp = new HLCTimestamp(1, 10, 0)
                },
                ct);
            Assert.True(interNode.CompleteOperationCallCount > beforeComplete);

            // The effect that arrived over the inter-node transport is present in the coordinator's set,
            // observed via the coordinator-partition leader itself (a local read, no further hop).
            TransactionWorkingSet? ws = await coordinatorLeader!.Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(ws);
            Assert.Contains(ws!.ModifiedKeys, m => m.Key == "rk1");
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A transaction-scoped write whose actor applies but whose completion is lost leaves the coordinator
    /// record pending. A same-id retry must recover the confirmed effect from the participant cache —
    /// re-driving the completion, not reapplying the write — instead of spinning on <c>MustRetry</c>
    /// forever. Exercised over the real transport by failing the first completion of the write.
    /// </summary>
    [Fact]
    public async Task RegisteredWrite_LostCompletionThenRetry_RecoversWithoutSecondApply()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (Node[] nodes, MemoryInterNodeCommmunication interNode) = await AssembleWithTransport();
        try
        {
            // Classify a node whose register-remote calls hop the transport to the coordinator (so the
            // write's completion routes through the fault-injecting transport) and the coordinator-partition
            // leader (for a local, hop-free working-set read).
            //
            // A coordinator-partition leadership change discards the in-memory interactive session — it is not
            // reconstructed on a new leader, by design — and surfaces as RejectedSessionClosed. The aggressive
            // election timeouts in this cluster make that possible while the probe loop hops across nodes, so
            // establish the session and classify the nodes under a bounded retry, restarting with a fresh
            // transaction if the session is lost mid-classification.
            TransactionHandle handle = default;
            Node? coordinatorLeader = null;
            Node? remoteEntry = null;
            byte[] probe = [4, 5, 6];

            for (int attempt = 1; ; attempt++)
            {
                (_, handle) =
                    await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

                coordinatorLeader = null;
                remoteEntry = null;
                bool sessionLost = false;

                foreach (Node node in nodes)
                {
                    int before = interNode.BeginOperationCallCount;
                    (OperationRegistrationOutcome probeOutcome, _, _, _, _) =
                        await node.Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, TransactionOperationId.NewRandom(), OperationKind.Set, probe, ct);

                    if (probeOutcome == OperationRegistrationOutcome.RejectedSessionClosed)
                    {
                        sessionLost = true;
                        break;
                    }

                    Assert.Equal(OperationRegistrationOutcome.New, probeOutcome);

                    if (interNode.BeginOperationCallCount == before)
                        coordinatorLeader = node;
                    else
                        remoteEntry = node;
                }

                if (!sessionLost && coordinatorLeader is not null && remoteEntry is not null)
                    break;

                Assert.True(attempt < 8, "coordinator-partition leadership never held a session long enough to classify the nodes");
            }

            Assert.NotNull(coordinatorLeader);
            Assert.NotNull(remoteEntry);

            // Fail the first completion of this specific write so its coordinator record stays pending.
            TransactionOperationId writeOp = TransactionOperationId.NewRandom();
            bool faulted = false;
            interNode.CompleteOperationFault = (_, opId) =>
            {
                if (opId == writeOp && !faulted)
                {
                    faulted = true;
                    return true;
                }

                return false;
            };

            int completesBefore = interNode.CompleteOperationCallCount;

            // First attempt applies the write, its completion is lost; the retry recovers the confirmed
            // response from the participant cache without re-running the actor.
            (KeyValueResponseType type, long revision, _) =
                await SetWithRetry(remoteEntry!, handle, writeOp, [1, 1, 1], ct);

            Assert.True(faulted, "the injected completion fault should have fired");
            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.True(revision >= 0);

            // Completion was attempted twice (the faulted attempt plus the successful recovery), proving the
            // retry re-drove the idempotent completion rather than staying stuck on MustRetry.
            Assert.True(interNode.CompleteOperationCallCount - completesBefore >= 2);

            // The effect folded exactly once at the coordinator: wk1 is recorded and anchors the record.
            TransactionWorkingSet? ws =
                await coordinatorLeader!.Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(ws);
            Assert.Equal("wk1", ws!.RecordAnchorKey);
            Assert.Contains(ws.ModifiedKeys, m => m.Key == "wk1");
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// Cancelling a pending operation (after a transient/no-effect result) releases its id: a same-id retry
    /// carrying the identical declaration re-registers as <c>New</c> and can complete with a real effect,
    /// rather than wedging on <c>MustRetry</c> forever because a lingering cancelled record maps to
    /// <c>AlreadyPending</c>.
    /// </summary>
    [Fact]
    public async Task CancelledOperation_SameIdReregistersAsNew_AndCompletes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            TransactionOperationId op = TransactionOperationId.NewRandom();
            byte[] digest = [7, 7, 7];

            // Register the operation, then cancel it via a transient (MustRetry) completion — a no-effect
            // result the node-local completion path turns into a cancellation.
            (OperationRegistrationOutcome first, _, _, _, _) =
                await nodes[0].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, op, OperationKind.Set, digest, ct);
            Assert.Equal(OperationRegistrationOutcome.New, first);

            await nodes[0].Kahuna.LocateAndCompleteOperation(
                handle.CoordinatorKey, handle.TransactionId, op,
                new OperationCompletionPayload { CachedType = KeyValueResponseType.MustRetry }, ct);

            // The same id with the identical declaration must re-register as New (pre-fix it returned
            // AlreadyPending forever), then complete with a confirmed effect.
            (OperationRegistrationOutcome second, _, _, _, _) =
                await nodes[0].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, op, OperationKind.Set, digest, ct);
            Assert.Equal(OperationRegistrationOutcome.New, second);

            (KeyValueResponseType anchorOutcome, string? anchor) = await nodes[0].Kahuna.LocateAndCompleteOperation(
                handle.CoordinatorKey, handle.TransactionId, op,
                new OperationCompletionPayload
                {
                    ModifiedKey = "ck1",
                    Durability = KeyValueDurability.Persistent,
                    CachedType = KeyValueResponseType.Set,
                    CachedRevision = 3,
                    CachedTimestamp = new HLCTimestamp(1, 20, 0)
                }, ct);
            Assert.Equal(KeyValueResponseType.Set, anchorOutcome);
            Assert.Equal("ck1", anchor);

            TransactionWorkingSet? ws =
                await nodes[0].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(ws);
            Assert.Equal("ck1", ws!.RecordAnchorKey);
            Assert.Contains(ws.ModifiedKeys, m => m.Key == "ck1");
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A transaction-scoped request that carries only one of {coordinator key, operation id} is malformed:
    /// it must return <c>InvalidInput</c> rather than silently degrade to the unregistered path and mutate a
    /// participant outside the finalize fence. A request with neither (a script/non-transactional call) still
    /// takes the legacy path — that case is exercised throughout the rest of the suite.
    /// </summary>
    [Fact]
    public async Task MalformedTransactionIdentity_IsRejectedAsInvalidInput()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            HLCTimestamp txId = handle.TransactionId;
            TransactionOperationId op = TransactionOperationId.NewRandom();

            // Coordinator key but no operation id → malformed.
            (KeyValueResponseType setNoOp, _, _) = await nodes[0].Kahuna.LocateAndTrySetKeyValue(
                txId, "mk1", [1], null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct, 0, handle.CoordinatorKey, default);
            Assert.Equal(KeyValueResponseType.InvalidInput, setNoOp);

            // Operation id but no coordinator key → malformed.
            (KeyValueResponseType setNoCoord, _, _) = await nodes[0].Kahuna.LocateAndTrySetKeyValue(
                txId, "mk1", [1], null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct, 0, "", op);
            Assert.Equal(KeyValueResponseType.InvalidInput, setNoCoord);

            // Delete and point-lock acquire reject partial identity the same way.
            (KeyValueResponseType delNoOp, _, _) = await nodes[0].Kahuna.LocateAndTryDeleteKeyValue(
                txId, "mk1", KeyValueDurability.Persistent, ct, handle.CoordinatorKey, default);
            Assert.Equal(KeyValueResponseType.InvalidInput, delNoOp);

            (KeyValueResponseType lockNoOp, _, _, _) = await nodes[0].Kahuna.LocateAndTryAcquireExclusiveLock(
                txId, "mk1", 5000, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, default);
            Assert.Equal(KeyValueResponseType.InvalidInput, lockNoOp);

            // None of the rejected requests registered or folded an effect: no anchor was assigned.
            TransactionWorkingSet? ws =
                await nodes[0].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, txId, ct);
            Assert.NotNull(ws);
            Assert.Null(ws!.RecordAnchorKey);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// Commit returns the coordinator's canonical record anchor from the frozen finalize snapshot — the
    /// anchor travels back on the commit response itself, not via a separate pre-commit working-set query,
    /// so there is no window in which a concurrent operation assigns the anchor after it is read but before
    /// the working set freezes.
    /// </summary>
    [Fact]
    public async Task Commit_ReturnsRecordAnchorFromFrozenFinalizeResult()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            // A confirmed persistent write assigns the record anchor on the coordinator.
            (KeyValueResponseType setType, _, _) =
                await SetWithRetry(nodes[1], handle, TransactionOperationId.NewRandom(), "v"u8.ToArray(), ct);
            Assert.Equal(KeyValueResponseType.Set, setType);

            // The commit itself carries the anchor back, captured under the finalize fence.
            (KeyValueResponseType commitType, string? anchor) = await CommitReturningAnchor(nodes[2], handle, ct);
            Assert.Equal(KeyValueResponseType.Committed, commitType);
            Assert.Equal("wk1", anchor);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A pinned read snapshot and write-skew validation are contradictory — a fixed past timestamp cannot
    /// observe writes that land after it, so the two together cannot honor the guarantee. StartTransaction
    /// rejects the combination up front with <c>InvalidInput</c> and hands back no usable session id.
    /// </summary>
    [Fact]
    public async Task StartTransaction_SnapshotReadWithTrackAndValidate_IsRejected()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (KeyValueResponseType type, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new()
                {
                    Locking = KeyValueTransactionLocking.Optimistic,
                    ReadValidation = ReadValidation.TrackAndValidate,
                    ReadTimestamp = new HLCTimestamp(1, 1000, 0)
                }, ct);

            Assert.Equal(KeyValueResponseType.InvalidInput, type);
            Assert.Equal(HLCTimestamp.Zero, handle.TransactionId);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A pinned read snapshot without write-skew validation is a legitimate combination (a plain snapshot
    /// read) — the Begin-time guard is specific to the contradictory pairing and must not reject it.
    /// </summary>
    [Fact]
    public async Task StartTransaction_SnapshotReadWithoutValidation_IsAccepted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (KeyValueResponseType type, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new()
                {
                    Locking = KeyValueTransactionLocking.Optimistic,
                    ReadValidation = ReadValidation.None,
                    ReadTimestamp = new HLCTimestamp(1, 1000, 0)
                }, ct);

            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.NotEqual(HLCTimestamp.Zero, handle.TransactionId);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A snapshot read (a non-null read timestamp) is pinned to a past state and owns no live transactional
    /// MVCC, so it must contribute no read dependency to the coordinator read set — otherwise commit-time
    /// validation would probe a key the transaction never depended on at the current timestamp. A latest
    /// read (a null/zero read timestamp) still records its observation.
    /// </summary>
    [Fact]
    public async Task SnapshotRead_RecordsNoReadObservation_LatestReadDoes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

            // A snapshot read at a pinned (non-null) timestamp completes but records no read dependency.
            await nodes[0].Kahuna.LocateAndTryGetValue(
                handle.TransactionId, "snapKey", -1, handle.TransactionId, KeyValueDurability.Persistent,
                ct, handle.CoordinatorKey, TransactionOperationId.NewRandom());

            // A latest read (zero timestamp = read-your-writes / newest) records its observation.
            await nodes[0].Kahuna.LocateAndTryGetValue(
                handle.TransactionId, "latestKey", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent,
                ct, handle.CoordinatorKey, TransactionOperationId.NewRandom());

            TransactionWorkingSet? ws =
                await nodes[0].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(ws);
            Assert.DoesNotContain(ws!.ReadKeys, r => r.Key == "snapKey");
            Assert.Contains(ws.ReadKeys, r => r.Key == "latestKey");
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// When a close cannot drain in time (an operation registered before the close never completes), the
    /// close must return <c>MustRetry</c> with no partial snapshot <em>and leave the session closed</em>:
    /// a new operation attempted after finalization began is rejected, never admitted by a reopened
    /// session. A later close continues waiting on the same pre-close pending set.
    /// </summary>
    [Fact]
    public async Task CloseTimeout_KeepsSessionClosed_RejectsNewRegistration()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            // A short transaction timeout bounds the close drain deadline.
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(
                    new() { Locking = KeyValueTransactionLocking.Pessimistic, Timeout = 500 }, ct);

            // Register an operation but never complete it, so the close drain cannot finish.
            TransactionOperationId stuck = TransactionOperationId.NewRandom();
            (OperationRegistrationOutcome stuckOutcome, _, _, _, _) =
                await nodes[0].Kahuna.LocateAndBeginOperation(
                    handle.CoordinatorKey, handle.TransactionId, stuck, OperationKind.Set, [1, 2, 3], ct);
            Assert.Equal(OperationRegistrationOutcome.New, stuckOutcome);

            // The close hits its drain deadline and asks the caller to retry — with no partial snapshot.
            (KeyValueResponseType closeType, TransactionWorkingSet? ws) =
                await nodes[0].Kahuna.LocateAndCloseTransaction(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.Equal(KeyValueResponseType.MustRetry, closeType);
            Assert.Null(ws);

            // The session stayed closed: a brand-new operation is rejected, not admitted by a reopened session.
            TransactionOperationId fresh = TransactionOperationId.NewRandom();
            (OperationRegistrationOutcome afterClose, _, _, _, _) =
                await nodes[0].Kahuna.LocateAndBeginOperation(
                    handle.CoordinatorKey, handle.TransactionId, fresh, OperationKind.Set, [4, 5, 6], ct);
            Assert.Equal(OperationRegistrationOutcome.RejectedSessionClosed, afterClose);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A lock acquire that first completed with a definitive failure (the key is held by another
    /// transaction, so <c>AlreadyLocked</c>) must replay that exact failure on a same-operation-id retry —
    /// never hardcode <c>Locked</c>. Returning success would tell the caller it holds a lock it does not,
    /// and no lock effect was ever recorded in the coordinator working set.
    /// </summary>
    [Fact]
    public async Task RegisteredLockAcquire_CachedFailureReplaysFailureNotSuccess()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            // Transaction A takes and holds the exclusive lock on the contended key.
            (_, TransactionHandle handleA) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);
            (KeyValueResponseType aType, _, _, _) =
                await AcquireLockWithRetry(nodes[0], handleA, TransactionOperationId.NewRandom(), "lk1", ct);
            Assert.Equal(KeyValueResponseType.Locked, aType);

            // Transaction B's first attempt on the same key is denied — this failure is cached under op B.
            (_, TransactionHandle handleB) =
                await nodes[1].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);
            TransactionOperationId opB = TransactionOperationId.NewRandom();
            (KeyValueResponseType bType, _, _, HLCTimestamp bHolder) =
                await AcquireLockWithRetry(nodes[1], handleB, opB, "lk1", ct);
            Assert.Equal(KeyValueResponseType.AlreadyLocked, bType);
            Assert.Equal(handleA.TransactionId, bHolder);

            // A retry under the same operation id replays the exact cached failure, not a bogus success.
            (KeyValueResponseType retryType, _, _, _) =
                await nodes[1].Kahuna.LocateAndTryAcquireExclusiveLock(
                    handleB.TransactionId, "lk1", 5000, KeyValueDurability.Persistent, ct, handleB.CoordinatorKey, opB);
            Assert.Equal(KeyValueResponseType.AlreadyLocked, retryType);

            // The denied acquire recorded no lock effect: B's working set holds no point lock.
            TransactionWorkingSet? ws =
                await nodes[1].Kahuna.LocateAndGetTransactionWorkingSet(handleB.CoordinatorKey, handleB.TransactionId, ct);
            Assert.NotNull(ws);
            Assert.DoesNotContain(ws!.AcquiredLocks, l => l.Key == "lk1");
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// Completing an operation whose session no longer exists (reaped/aborted, or never started) must not
    /// be silently acknowledged as success — otherwise a participant that already applied the operation
    /// would report a write that never entered any coordinator working set. The completion surfaces as a
    /// failure the participant turns into a retry, which then observes the closed/absent session and aborts.
    /// </summary>
    [Fact]
    public async Task CompleteOperation_OnMissingSession_IsNotSilentlyAcknowledged()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            string coordinatorKey = Guid.NewGuid().ToString("N");

            await Assert.ThrowsAnyAsync<Exception>(() =>
                nodes[0].Kahuna.LocateAndCompleteOperation(
                    coordinatorKey, new HLCTimestamp(1, 77, 0), TransactionOperationId.NewRandom(),
                    new OperationCompletionPayload
                    {
                        ModifiedKey = "gone",
                        Durability = KeyValueDurability.Persistent,
                        CachedType = KeyValueResponseType.Set
                    },
                    ct));
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// The record anchor is the first confirmed <b>persistent</b> modified key, assigned exactly once and
    /// immutable: a later persistent write does not move it, though it is still recorded as a modified key.
    /// </summary>
    [Fact]
    public async Task RecordAnchor_IsFirstPersistentModifiedKey_AndImmutable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            byte[] value = "v"u8.ToArray();

            // First persistent write assigns the anchor.
            Assert.Equal(KeyValueResponseType.Set,
                await SetKeyWithRetry(nodes[1], handle, TransactionOperationId.NewRandom(), "anchor/a", value, KeyValueDurability.Persistent, ct));

            TransactionWorkingSet? afterFirst = await nodes[2].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterFirst);
            Assert.Equal("anchor/a", afterFirst!.RecordAnchorKey);

            // A later persistent write does not move the anchor, but is still recorded as a modified key.
            Assert.Equal(KeyValueResponseType.Set,
                await SetKeyWithRetry(nodes[0], handle, TransactionOperationId.NewRandom(), "anchor/b", value, KeyValueDurability.Persistent, ct));

            TransactionWorkingSet? afterSecond = await nodes[1].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterSecond);
            Assert.Equal("anchor/a", afterSecond!.RecordAnchorKey);
            Assert.Contains(afterSecond.ModifiedKeys, m => m.Key == "anchor/b");
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// An ephemeral modification is recorded as a modified key but never becomes the anchor: a Durable
    /// record cannot live on an ephemeral key, so a transaction with only ephemeral writes has no anchor.
    /// </summary>
    [Fact]
    public async Task RecordAnchor_NotAssignedForEphemeralModifications()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            Assert.Equal(KeyValueResponseType.Set,
                await SetKeyWithRetry(nodes[1], handle, TransactionOperationId.NewRandom(), "eph/a", "v"u8.ToArray(), KeyValueDurability.Ephemeral, ct));

            TransactionWorkingSet? ws = await nodes[2].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(ws);
            Assert.Null(ws!.RecordAnchorKey);
            Assert.Contains(ws.ModifiedKeys, m => m.Key == "eph/a");
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A batch delete registers as a single coordinator operation whose confirmed persistent keys fold in
    /// canonical request order, so the anchor is the first key in the submitted list — not the arrival order
    /// of the per-partition fan-out, and not the sorted order.
    /// </summary>
    [Fact]
    public async Task RecordAnchor_BatchDelete_UsesFirstPersistentKeyInRequestOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            // Seed three committed persistent keys in a prior transaction so the batch delete confirms each.
            (_, TransactionHandle seed) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);
            byte[] value = "v"u8.ToArray();
            foreach (string key in new[] { "batch/m", "batch/a", "batch/z" })
                Assert.Equal(KeyValueResponseType.Set,
                    await SetKeyWithRetry(nodes[1], seed, TransactionOperationId.NewRandom(), key, value, KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Committed, await CommitWithRetry(nodes[0], seed, ct));

            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            // Submit deliberately un-sorted request order: first item "batch/m" is neither the sorted-first
            // key ("batch/a") nor guaranteed to be the earliest fan-out completion.
            List<KahunaDeleteKeyValueResponseItem> responses = await DeleteManyWithRetry(
                nodes[2], handle, TransactionOperationId.NewRandom(),
                ["batch/m", "batch/a", "batch/z"], KeyValueDurability.Persistent, ct);

            foreach (KahunaDeleteKeyValueResponseItem r in responses)
                Assert.Equal(KeyValueResponseType.Deleted, r.Type);

            TransactionWorkingSet? ws = await nodes[1].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(ws);
            Assert.Equal("batch/m", ws!.RecordAnchorKey);
            Assert.Contains(ws.ModifiedKeys, m => m.Key == "batch/m");
            Assert.Contains(ws.ModifiedKeys, m => m.Key == "batch/a");
            Assert.Contains(ws.ModifiedKeys, m => m.Key == "batch/z");
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A batch delete replayed under the same operation id is re-driven (deletes are idempotent) and recovers
    /// the same anchor without double-recording: the coordinator completion is a no-op the second time.
    /// </summary>
    [Fact]
    public async Task RecordAnchor_BatchDelete_RetrySameOperationId_RecoversSameAnchor()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle seed) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);
            byte[] value = "v"u8.ToArray();
            foreach (string key in new[] { "dup/m", "dup/a" })
                Assert.Equal(KeyValueResponseType.Set,
                    await SetKeyWithRetry(nodes[1], seed, TransactionOperationId.NewRandom(), key, value, KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Committed, await CommitWithRetry(nodes[0], seed, ct));

            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            TransactionOperationId op = TransactionOperationId.NewRandom();
            await DeleteManyWithRetry(nodes[2], handle, op, ["dup/m", "dup/a"], KeyValueDurability.Persistent, ct);

            TransactionWorkingSet? afterFirst = await nodes[1].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.Equal("dup/m", afterFirst!.RecordAnchorKey);
            int modifiedCount = afterFirst.ModifiedKeys.Count;

            // Replay the identical batch under the same operation id.
            await DeleteManyWithRetry(nodes[2], handle, op, ["dup/m", "dup/a"], KeyValueDurability.Persistent, ct);

            TransactionWorkingSet? afterReplay = await nodes[0].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.Equal("dup/m", afterReplay!.RecordAnchorKey);
            Assert.Equal(modifiedCount, afterReplay.ModifiedKeys.Count);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A batch set registers as a single coordinator operation whose confirmed persistent keys fold in
    /// canonical request order, so the anchor is the first key in the submitted list — not the arrival order
    /// of the per-partition fan-out, and not the sorted order.
    /// </summary>
    [Fact]
    public async Task RecordAnchor_BatchSet_UsesFirstPersistentKeyInRequestOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            // Submit deliberately un-sorted request order: first item "bset/m" is neither the sorted-first
            // key ("bset/a") nor guaranteed to be the earliest per-partition fan-out completion.
            List<KahunaSetKeyValueResponseItem> responses = await SetManyWithRetry(
                nodes[2], handle, TransactionOperationId.NewRandom(),
                SetItems(handle, KeyValueFlags.None, KeyValueDurability.Persistent, "bset/m", "bset/a", "bset/z"), ct);

            foreach (KahunaSetKeyValueResponseItem r in responses)
                Assert.Equal(KeyValueResponseType.Set, r.Type);

            TransactionWorkingSet? ws = await nodes[1].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(ws);
            Assert.Equal("bset/m", ws!.RecordAnchorKey);
            Assert.Contains(ws.ModifiedKeys, m => m.Key == "bset/m");
            Assert.Contains(ws.ModifiedKeys, m => m.Key == "bset/a");
            Assert.Contains(ws.ModifiedKeys, m => m.Key == "bset/z");
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A batch set replayed under the same operation id is re-driven and recovers the same anchor without
    /// double-recording: the coordinator completion is a no-op the second time.
    /// </summary>
    [Fact]
    public async Task RecordAnchor_BatchSet_RetrySameOperationId_RecoversSameAnchor()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            TransactionOperationId op = TransactionOperationId.NewRandom();
            await SetManyWithRetry(nodes[2], handle, op,
                SetItems(handle, KeyValueFlags.None, KeyValueDurability.Persistent, "rset/m", "rset/a"), ct);

            TransactionWorkingSet? afterFirst = await nodes[1].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.Equal("rset/m", afterFirst!.RecordAnchorKey);
            int modifiedCount = afterFirst.ModifiedKeys.Count;

            // Replay the identical batch under the same operation id.
            await SetManyWithRetry(nodes[2], handle, op,
                SetItems(handle, KeyValueFlags.None, KeyValueDurability.Persistent, "rset/m", "rset/a"), ct);

            TransactionWorkingSet? afterReplay = await nodes[0].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.Equal("rset/m", afterReplay!.RecordAnchorKey);
            Assert.Equal(modifiedCount, afterReplay.ModifiedKeys.Count);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// Only genuinely confirmed writes fold into the coordinator record: a conditional set that fails
    /// (a <c>SetIfNotExists</c> on an already-present key returns <c>NotSet</c>) contributes no modified key
    /// and never anchors, yet the batch still completes — the failed item does not cancel the registration.
    /// The anchor is therefore the first <em>confirmed</em> key in request order, skipping the earlier item
    /// whose conditional set was rejected.
    /// </summary>
    [Fact]
    public async Task BatchSet_FoldsOnlyConfirmedWrites_SkipsNotSet_AndAnchorsFirstConfirmedInRequestOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            // Seed one committed persistent key so a SetIfNotExists on it later fails while the others succeed.
            (_, TransactionHandle seed) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);
            Assert.Equal(KeyValueResponseType.Set,
                await SetKeyWithRetry(nodes[1], seed, TransactionOperationId.NewRandom(), "mixset/exists", "v"u8.ToArray(), KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Committed, await CommitWithRetry(nodes[0], seed, ct));

            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            // Request order puts the pre-existing (will-fail) key first, then two fresh keys in non-sorted
            // order, so the anchor must be the first CONFIRMED key ("mixset/znew"), not the sorted-first
            // ("mixset/anew") and not the earlier rejected item.
            List<KahunaSetKeyValueResponseItem> responses = await SetManyWithRetry(
                nodes[2], handle, TransactionOperationId.NewRandom(),
                SetItems(handle, KeyValueFlags.SetIfNotExists, KeyValueDurability.Persistent, "mixset/exists", "mixset/znew", "mixset/anew"), ct);

            Dictionary<string, KeyValueResponseType> byKey = responses.ToDictionary(r => r.Key, r => r.Type, StringComparer.Ordinal);
            Assert.Equal(KeyValueResponseType.NotSet, byKey["mixset/exists"]);
            Assert.Equal(KeyValueResponseType.Set, byKey["mixset/znew"]);
            Assert.Equal(KeyValueResponseType.Set, byKey["mixset/anew"]);

            TransactionWorkingSet? ws = await nodes[1].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(ws);
            Assert.Equal("mixset/znew", ws!.RecordAnchorKey);
            Assert.Contains(ws.ModifiedKeys, m => m.Key == "mixset/znew");
            Assert.Contains(ws.ModifiedKeys, m => m.Key == "mixset/anew");
            Assert.DoesNotContain(ws.ModifiedKeys, m => m.Key == "mixset/exists");
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A batch set carrying only one of {coordinator key, operation id} is malformed: it must return
    /// <c>InvalidInput</c> for every item rather than silently degrade to the unregistered fan-out and mutate
    /// participants outside the finalize fence. No effect is folded and no anchor is assigned.
    /// </summary>
    [Fact]
    public async Task BatchSet_MalformedIdentity_IsRejectedAsInvalidInput()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            // Coordinator key but no operation id → malformed.
            List<KahunaSetKeyValueResponseItem> noOp = await nodes[0].Kahuna.LocateAndTrySetManyKeyValue(
                SetItems(handle, KeyValueFlags.None, KeyValueDurability.Persistent, "bad/1", "bad/2"), ct, handle.CoordinatorKey, default);
            Assert.All(noOp, r => Assert.Equal(KeyValueResponseType.InvalidInput, r.Type));

            // Operation id but no coordinator key → malformed.
            List<KahunaSetKeyValueResponseItem> noCoord = await nodes[0].Kahuna.LocateAndTrySetManyKeyValue(
                SetItems(handle, KeyValueFlags.None, KeyValueDurability.Persistent, "bad/1", "bad/2"), ct, "", TransactionOperationId.NewRandom());
            Assert.All(noCoord, r => Assert.Equal(KeyValueResponseType.InvalidInput, r.Type));

            // Neither rejected request registered or folded an effect: no anchor was assigned.
            TransactionWorkingSet? ws = await nodes[0].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(ws);
            Assert.Null(ws!.RecordAnchorKey);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A registered batch read (GetMany) folds one observation per returned key into the coordinator read
    /// set, and a replay under the same operation id re-runs the read without folding the observations a
    /// second time (the coordinator completion is a no-op on the already-completed id).
    /// </summary>
    [Fact]
    public async Task RegisteredBatchGet_FoldsOneObservationPerKey_AndReplayDoesNotDoubleCount()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            const string gk1 = "bget/one";
            const string gk2 = "bget/two";
            Assert.Equal(KeyValueResponseType.Set, (await nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, gk1, "v1"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct)).Item1);
            Assert.Equal(KeyValueResponseType.Set, (await nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, gk2, "v2"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct)).Item1);

            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

            List<(string, long, KeyValueDurability)> keys =
                [(gk1, -1, KeyValueDurability.Persistent), (gk2, -1, KeyValueDurability.Persistent)];

            TransactionOperationId op = TransactionOperationId.NewRandom();
            List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> results =
                await nodes[1].Kahuna.LocateAndTryGetManyValues(handle.TransactionId, HLCTimestamp.Zero, keys, ct, handle.CoordinatorKey, op);
            Assert.All(results, r => Assert.Equal(KeyValueResponseType.Get, r.Item1));

            TransactionWorkingSet? live = await nodes[2].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(live);
            Assert.Contains(live!.ReadKeys, r => r.Key == gk1);
            Assert.Contains(live.ReadKeys, r => r.Key == gk2);
            int readCount = live.ReadKeys.Count;

            // Replay under the same op id: re-runs the read, records nothing new.
            List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> replay =
                await nodes[2].Kahuna.LocateAndTryGetManyValues(handle.TransactionId, HLCTimestamp.Zero, keys, ct, handle.CoordinatorKey, op);
            Assert.All(replay, r => Assert.Equal(KeyValueResponseType.Get, r.Item1));

            TransactionWorkingSet? afterReplay = await nodes[0].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterReplay);
            Assert.Equal(readCount, afterReplay!.ReadKeys.Count);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A registered batch existence check (ExistsMany) folds one observation per probed key into the
    /// coordinator read set, so a concurrent write to a probed key can later fail an optimistic commit.
    /// </summary>
    [Fact]
    public async Task RegisteredBatchExists_FoldsOneObservationPerKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            const string ek1 = "bexists/one";
            const string ek2 = "bexists/two";
            Assert.Equal(KeyValueResponseType.Set, (await nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, ek1, "v1"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct)).Item1);
            Assert.Equal(KeyValueResponseType.Set, (await nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, ek2, "v2"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct)).Item1);

            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

            List<(string, long, KeyValueDurability)> keys =
                [(ek1, -1, KeyValueDurability.Persistent), (ek2, -1, KeyValueDurability.Persistent)];

            List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> results =
                await nodes[1].Kahuna.LocateAndTryExistsManyValues(handle.TransactionId, HLCTimestamp.Zero, keys, ct, handle.CoordinatorKey, TransactionOperationId.NewRandom());
            Assert.All(results, r => Assert.Equal(KeyValueResponseType.Exists, r.Item1));

            TransactionWorkingSet? live = await nodes[2].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(live);
            Assert.Contains(live!.ReadKeys, r => r.Key == ek1);
            Assert.Contains(live.ReadKeys, r => r.Key == ek2);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A batch read with an empty coordinator key takes the legacy (unregistered) path: the values still
    /// come back, but no read observation is folded into any coordinator working set — the dominant
    /// non-optimistic path is unchanged.
    /// </summary>
    [Fact]
    public async Task LegacyBatchRead_EmptyCoordinatorKey_FoldsNoObservation()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            const string lk1 = "blegacy/one";
            Assert.Equal(KeyValueResponseType.Set, (await nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, lk1, "v1"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct)).Item1);

            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

            List<(string, long, KeyValueDurability)> keys = [(lk1, -1, KeyValueDurability.Persistent)];

            // No coordinator key/op → legacy fan-out, identical results, no registration.
            List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> results =
                await nodes[1].Kahuna.LocateAndTryGetManyValues(handle.TransactionId, HLCTimestamp.Zero, keys, ct);
            Assert.All(results, r => Assert.Equal(KeyValueResponseType.Get, r.Item1));

            TransactionWorkingSet? live = await nodes[2].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(live);
            Assert.DoesNotContain(live!.ReadKeys, r => r.Key == lk1);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A registered streaming scan folds an observation for every returned key, exercised across a page
    /// boundary (small page size forces multiple pages, each registered under its own derived operation id).
    /// </summary>
    [Fact]
    public async Task RegisteredScanRange_FoldsObservationsAcrossPageBoundary()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            const string prefix = "rscan";
            string[] seeded = [$"{prefix}/0", $"{prefix}/1", $"{prefix}/2"];
            foreach (string key in seeded)
                Assert.Equal(KeyValueResponseType.Set, (await nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, "v"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct)).Item1);

            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

            List<string> seen = [];
            await foreach ((string key, ReadOnlyKeyValueEntry _) in nodes[1].Kahuna.LocateAndScanRange(
                handle.TransactionId, prefix, null, true, null, false,
                pageSize: 1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
                handle.CoordinatorKey, TransactionOperationId.NewRandom()))
                seen.Add(key);

            Assert.Equal(3, seen.Count);

            TransactionWorkingSet? live = await nodes[2].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(live);
            foreach (string key in seeded)
                Assert.Contains(live!.ReadKeys, r => r.Key == key);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A snapshot streaming scan (pinned read timestamp) registers each page for finalize fencing and
    /// idempotent replay but records no read dependency — a fixed past timestamp owns no live transactional
    /// MVCC to validate at commit.
    /// </summary>
    [Fact]
    public async Task SnapshotScanRange_RegistersButRecordsNoObservation()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            const string prefix = "sscan";
            foreach (string key in new[] { $"{prefix}/0", $"{prefix}/1" })
                Assert.Equal(KeyValueResponseType.Set, (await nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, "v"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct)).Item1);

            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

            // A pinned (non-null) read timestamp → snapshot scan.
            await foreach ((string _, ReadOnlyKeyValueEntry _) in nodes[1].Kahuna.LocateAndScanRange(
                handle.TransactionId, prefix, null, true, null, false,
                pageSize: 1, handle.TransactionId, KeyValueDurability.Persistent, ct,
                handle.CoordinatorKey, TransactionOperationId.NewRandom()))
            {
            }

            TransactionWorkingSet? live = await nodes[2].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(live);
            Assert.DoesNotContain(live!.ReadKeys, r => r.Key is not null && r.Key.StartsWith(prefix, StringComparison.Ordinal));
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// End-to-end read-set validation over a registered scan: an optimistic transaction that streamed a
    /// multi-page scan (folding every observed key) and then wrote a key commits <c>Aborted</c> once another
    /// transaction commits a write to a key it observed, and the same flow with no conflicting write commits.
    /// The writer key is what carries the transaction into two-phase commit, where its scanned read set is
    /// validated.
    /// </summary>
    [Fact]
    public async Task RegisteredScanRange_OptimisticCommit_AbortsOnConflict_CommitsOtherwise()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            const string prefix = "rconf";
            string[] seeded = [$"{prefix}/0", $"{prefix}/1", $"{prefix}/2"];
            foreach (string key in seeded)
                Assert.Equal(KeyValueResponseType.Set, (await nodes[0].Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, "v"u8.ToArray(), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct)).Item1);

            // Transaction A streams the scan (folding observations) and writes an unrelated key so it enters
            // two-phase commit and validates its scanned read set.
            (_, TransactionHandle handleA) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);
            await ScanAll(nodes[1], handleA, prefix, ct);
            Assert.Equal(KeyValueResponseType.Set,
                await SetKeyWithRetry(nodes[1], handleA, TransactionOperationId.NewRandom(), $"{prefix}/writerA", "w"u8.ToArray(), KeyValueDurability.Persistent, ct));

            // Transaction B commits a write to one key A observed, advancing its revision.
            (_, TransactionHandle handleB) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);
            Assert.Equal(KeyValueResponseType.Set,
                await SetKeyWithRetry(nodes[1], handleB, TransactionOperationId.NewRandom(), $"{prefix}/1", "changed"u8.ToArray(), KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Committed, await CommitWithRetry(nodes[2], handleB, ct));

            // A's commit fails read-set validation over the scanned observation whose revision B advanced.
            (KeyValueResponseType commitA, _) = await CommitReturningAnchor(nodes[2], handleA, ct);
            Assert.Equal(KeyValueResponseType.Aborted, commitA);

            // Control: a fresh optimistic scan + write with no conflicting write commits.
            (_, TransactionHandle handleC) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);
            await ScanAll(nodes[1], handleC, prefix, ct);
            Assert.Equal(KeyValueResponseType.Set,
                await SetKeyWithRetry(nodes[1], handleC, TransactionOperationId.NewRandom(), $"{prefix}/writerC", "w"u8.ToArray(), KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Committed, await CommitWithRetry(nodes[2], handleC, ct));
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A range lock a live session holds is kept alive by the coordinator's server-driven renewal, so it
    /// outlives its short original acquire TTL without any client re-acquire: after the TTL would have lapsed, a
    /// conflicting acquire from another transaction is still blocked.
    /// </summary>
    [Fact]
    public async Task SessionRenewedRangeLock_OutlivesItsAcquireTtl()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            const string prefix = "rlrenew";
            const int shortTtlMs = 800;

            (_, TransactionHandle handleA) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);
            KeyValueResponseType acquired = await RangeLockWithRetry(() =>
                nodes[1].Kahuna.LocateAndTryAcquireRangeLock(handleA.TransactionId, prefix, "a", true, "z", false, shortTtlMs, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, handleA.CoordinatorKey, TransactionOperationId.NewRandom()), ct);
            Assert.Equal(KeyValueResponseType.Locked, acquired);

            // Server-driven renewal — every node renews its own hosted sessions, exactly as the reaper does.
            foreach (Node node in nodes)
                await node.Kahuna.RenewSessionRangeLocks();

            // Advance well past the original short TTL; only server renewal can be keeping the lock alive.
            await Task.Delay(shortTtlMs + 700, ct);

            // A different transaction's conflicting exclusive acquire over the same range is still blocked.
            (_, TransactionHandle handleB) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);
            KeyValueResponseType conflict = await RangeLockWithRetry(() =>
                nodes[2].Kahuna.LocateAndTryAcquireRangeLock(handleB.TransactionId, prefix, "a", true, "z", false, 10_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, handleB.CoordinatorKey, TransactionOperationId.NewRandom()), ct);
            Assert.Equal(KeyValueResponseType.AlreadyLocked, conflict);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// Control for <see cref="SessionRenewedRangeLock_OutlivesItsAcquireTtl"/>: without any renewal, a range
    /// lock genuinely lapses at its short acquire TTL, so a conflicting acquire then succeeds. This proves the
    /// TTL is real and that renewal — not a long implicit lease — is what keeps the lock alive in the positive
    /// test.
    /// </summary>
    [Fact]
    public async Task RangeLock_WithoutRenewal_LapsesAtItsAcquireTtl()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            const string prefix = "rllapse";
            const int shortTtlMs = 800;

            (_, TransactionHandle handleA) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);
            KeyValueResponseType acquired = await RangeLockWithRetry(() =>
                nodes[1].Kahuna.LocateAndTryAcquireRangeLock(handleA.TransactionId, prefix, "a", true, "z", false, shortTtlMs, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, handleA.CoordinatorKey, TransactionOperationId.NewRandom()), ct);
            Assert.Equal(KeyValueResponseType.Locked, acquired);

            // No renewal: advance past the TTL and the lock lapses.
            await Task.Delay(shortTtlMs + 700, ct);

            (_, TransactionHandle handleB) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);
            KeyValueResponseType afterLapse = await RangeLockWithRetry(() =>
                nodes[2].Kahuna.LocateAndTryAcquireRangeLock(handleB.TransactionId, prefix, "a", true, "z", false, 10_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, handleB.CoordinatorKey, TransactionOperationId.NewRandom()), ct);
            Assert.Equal(KeyValueResponseType.Locked, afterLapse);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// An abandoned session's range lock still lapses: once the reaper reclaims the session (past its deadline)
    /// its locks are released, so renewal cannot keep an abandoned transaction's locks alive forever. Here the
    /// reaper is driven directly and the session is confirmed gone, after which a conflicting acquire succeeds.
    /// </summary>
    [Fact]
    public async Task ReapedSession_ReleasesItsRangeLock_EvenAfterRenewal()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            const string prefix = "rlreap";

            // A short transaction timeout so the reaper's deadline (Timeout + grace) is reachable quickly.
            (_, TransactionHandle handleA) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic, Timeout = 1 }, ct);
            KeyValueResponseType acquired = await RangeLockWithRetry(() =>
                nodes[1].Kahuna.LocateAndTryAcquireRangeLock(handleA.TransactionId, prefix, "a", true, "z", false, 60_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, handleA.CoordinatorKey, TransactionOperationId.NewRandom()), ct);
            Assert.Equal(KeyValueResponseType.Locked, acquired);

            // Renewal keeps live sessions alive, but must not resurrect one past its reap deadline. Wait out the
            // deadline (Timeout + ReapGraceMs), then renew (should skip it) and reap (should reclaim it).
            await Task.Delay(TransactionCoordinator.ReapGraceMs + 1500, ct);
            foreach (Node node in nodes)
            {
                await node.Kahuna.RenewSessionRangeLocks();
                await node.Kahuna.ReapAbandonedSessions();
            }

            // The reaper released the abandoned session's range lock: a conflicting acquire now succeeds.
            (_, TransactionHandle handleB) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);
            KeyValueResponseType afterReap = await RangeLockWithRetry(() =>
                nodes[2].Kahuna.LocateAndTryAcquireRangeLock(handleB.TransactionId, prefix, "a", true, "z", false, 10_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, handleB.CoordinatorKey, TransactionOperationId.NewRandom()), ct);
            Assert.Equal(KeyValueResponseType.Locked, afterReap);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    private static async Task ScanAll(Node node, TransactionHandle handle, string prefix, CancellationToken ct)
    {
        await foreach ((string _, ReadOnlyKeyValueEntry _) in node.Kahuna.LocateAndScanRange(
            handle.TransactionId, prefix, null, true, null, false,
            pageSize: 1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            handle.CoordinatorKey, TransactionOperationId.NewRandom()))
        {
        }
    }

    /// <summary>
    /// An optimistic transaction whose only writes go through batch set-many / delete-many — with no explicit
    /// <c>AcquireMany</c> — folds the implicit point lock of each confirmed write into the coordinator's held-lock
    /// set and commits. Before batch writes folded their implicit locks, such a transaction reached
    /// <c>PrepareMutations</c> with an empty lock set and aborted every time.
    /// </summary>
    [Fact]
    public async Task OptimisticBatchWrites_CommitWithoutExplicitLocks_FoldingImplicitPointLocks()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            // Seed two committed keys so the optimistic transaction can also batch-delete.
            (_, TransactionHandle seed) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);
            foreach (string key in new[] { "cbatch/x", "cbatch/y" })
                Assert.Equal(KeyValueResponseType.Set,
                    await SetKeyWithRetry(nodes[1], seed, TransactionOperationId.NewRandom(), key, "v"u8.ToArray(), KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Committed, await CommitWithRetry(nodes[0], seed, ct));

            // Optimistic transaction: batch set + batch delete, and no explicit lock acquire anywhere.
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

            List<KahunaSetKeyValueResponseItem> setResponses = await SetManyWithRetry(
                nodes[1], handle, TransactionOperationId.NewRandom(),
                SetItems(handle, KeyValueFlags.None, KeyValueDurability.Persistent, "cbatch/a", "cbatch/b"), ct);
            Assert.All(setResponses, r => Assert.Equal(KeyValueResponseType.Set, r.Type));

            List<KahunaDeleteKeyValueResponseItem> delResponses = await DeleteManyWithRetry(
                nodes[2], handle, TransactionOperationId.NewRandom(), ["cbatch/x", "cbatch/y"], KeyValueDurability.Persistent, ct);
            Assert.All(delResponses, r => Assert.Equal(KeyValueResponseType.Deleted, r.Type));

            // Every confirmed batch write folded its implicit point lock into the coordinator working set.
            TransactionWorkingSet? ws = await nodes[0].Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(ws);
            foreach (string key in new[] { "cbatch/a", "cbatch/b", "cbatch/x", "cbatch/y" })
                Assert.Contains(ws!.AcquiredLocks, l => l.Key == key);

            // Commit succeeds off the server-owned working set — no explicit locks, no client lists.
            Assert.Equal(KeyValueResponseType.Committed, await CommitWithRetry(nodes[0], handle, ct));

            // The effects are durable: the set keys exist, the deleted keys are gone.
            (KeyValueResponseType getA, _) = await nodes[1].Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, "cbatch/a", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, getA);
            (KeyValueResponseType getX, _) = await nodes[1].Kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, "cbatch/x", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.NotEqual(KeyValueResponseType.Get, getX);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// Folding a batch write's implicit point lock does not weaken write-write detection: two optimistic
    /// transactions that each batch-write the same key cannot both stage the write, and cannot both commit it.
    /// The first writer stages its intent; the second's overlapping batch write is rejected by the write-intent
    /// conflict, so exactly one holds the write — no lost update.
    /// </summary>
    [Fact]
    public async Task OptimisticBatchWrites_ConcurrentOverlap_CannotBothWin()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            const string shared = "cww/shared";
            (_, TransactionHandle txA) = await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);
            (_, TransactionHandle txB) = await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Optimistic }, ct);

            // The first batch write stages its intent with no contention and wins the key; the second's
            // overlapping batch write hits that intent and is rejected — it never stages a Set, so the two
            // writers cannot both write the shared key (no lost update).
            List<KahunaSetKeyValueResponseItem> a = await nodes[1].Kahuna.LocateAndTrySetManyKeyValue(
                SetItems(txA, KeyValueFlags.None, KeyValueDurability.Persistent, shared), ct, txA.CoordinatorKey, TransactionOperationId.NewRandom());
            List<KahunaSetKeyValueResponseItem> b = await nodes[1].Kahuna.LocateAndTrySetManyKeyValue(
                SetItems(txB, KeyValueFlags.None, KeyValueDurability.Persistent, shared), ct, txB.CoordinatorKey, TransactionOperationId.NewRandom());

            Assert.Equal(KeyValueResponseType.Set, a[0].Type);
            Assert.NotEqual(KeyValueResponseType.Set, b[0].Type);
        }
        finally { await LeaveAll(nodes); }
    }

    private static async Task<List<KahunaSetKeyValueResponseItem>> SetManyWithRetry(
        Node node, TransactionHandle handle, TransactionOperationId op, List<KahunaSetKeyValueRequestItem> items, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 10_000;
        while (true)
        {
            List<KahunaSetKeyValueResponseItem> responses =
                await node.Kahuna.LocateAndTrySetManyKeyValue(items, ct, handle.CoordinatorKey, op);

            if (!responses.Any(r => r.Type == KeyValueResponseType.MustRetry) || Environment.TickCount64 >= deadline)
                return responses;

            await Task.Delay(50, ct);
        }
    }

    private static List<KahunaSetKeyValueRequestItem> SetItems(
        TransactionHandle handle, KeyValueFlags flags, KeyValueDurability durability, params string[] keys)
    {
        return [.. keys.Select(k => new KahunaSetKeyValueRequestItem
        {
            TransactionId = handle.TransactionId,
            Key = k,
            Value = "v"u8.ToArray(),
            CompareValue = null,
            CompareRevision = -1,
            Flags = flags,
            ExpiresMs = 0,
            Durability = durability
        })];
    }

    private static async Task<List<KahunaDeleteKeyValueResponseItem>> DeleteManyWithRetry(
        Node node, TransactionHandle handle, TransactionOperationId op, string[] keys, KeyValueDurability durability, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 10_000;
        while (true)
        {
            List<KahunaDeleteKeyValueRequestItem> items = [.. keys.Select(k => new KahunaDeleteKeyValueRequestItem
            {
                TransactionId = handle.TransactionId,
                Key = k,
                Durability = durability
            })];

            List<KahunaDeleteKeyValueResponseItem> responses =
                await node.Kahuna.LocateAndTryDeleteManyKeyValue(items, ct, handle.CoordinatorKey, op);

            if (!responses.Any(r => r.Type == KeyValueResponseType.MustRetry) || Environment.TickCount64 >= deadline)
                return responses;

            await Task.Delay(50, ct);
        }
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

    private static async Task<KeyValueResponseType> PrefixLockWithRetry(Func<Task<KeyValueResponseType>> op, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 10_000;
        while (true)
        {
            KeyValueResponseType type = await op();
            if (type != KeyValueResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return type;
            await Task.Delay(50, ct);
        }
    }

    private static async Task<KeyValueResponseType> RangeLockWithRetry(Func<Task<(KeyValueResponseType, HLCTimestamp)>> op, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 10_000;
        while (true)
        {
            (KeyValueResponseType type, _) = await op();
            if (type != KeyValueResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return type;
            await Task.Delay(50, ct);
        }
    }

    private static async Task<KeyValueResponseType> CommitWithRetry(Node node, TransactionHandle handle, CancellationToken ct)
    {
        (KeyValueResponseType type, _) = await CommitReturningAnchor(node, handle, ct);
        return type;
    }

    private static async Task<(KeyValueResponseType, string?)> CommitReturningAnchor(Node node, TransactionHandle handle, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 10_000;
        while (true)
        {
            (KeyValueResponseType type, string? anchor) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);
            if (type != KeyValueResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return (type, anchor);
            await Task.Delay(50, ct);
        }
    }

    private static async Task<KeyValueResponseType> RollbackWithRetry(Node node, TransactionHandle handle, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 10_000;
        while (true)
        {
            KeyValueResponseType type = await node.Kahuna.LocateAndRollbackTransaction(handle, ct);
            if (type != KeyValueResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return type;
            await Task.Delay(50, ct);
        }
    }

    private static async Task<KeyValueResponseType> LockWithRetry(Func<Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)>> op, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 10_000;
        while (true)
        {
            (KeyValueResponseType type, _, _, _) = await op();
            if (type != KeyValueResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return type;
            await Task.Delay(50, ct);
        }
    }

    private static async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> GetWithRetry(Node node, TransactionHandle handle, TransactionOperationId op, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 10_000;
        while (true)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) =
                await node.Kahuna.LocateAndTryGetValue(handle.TransactionId, "wk1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, op);

            if (type != KeyValueResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return (type, entry);

            await Task.Delay(50, ct);
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

    private static async Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp)> AcquireLockWithRetry(
        Node node, TransactionHandle handle, TransactionOperationId op, string key, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 10_000;
        while (true)
        {
            (KeyValueResponseType type, string resultKey, KeyValueDurability durability, HLCTimestamp holder) =
                await node.Kahuna.LocateAndTryAcquireExclusiveLock(
                    handle.TransactionId, key, 5000, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, op);

            if (type != KeyValueResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return (type, resultKey, durability, holder);

            await Task.Delay(50, ct);
        }
    }

    // ── harness ──────────────────────────────────────────────────────────────────────────

    // Widen the fixed election timers on loaded runners exactly as the shared cluster harness does, so
    // KAHUNA_TEST_TIMING_SCALE=3 in CI buys the same election slack and keeps leadership (and its in-memory
    // sessions) stable through a test.
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
            // Keep the leader's heartbeat well inside the election window (Kommander's own guideline is
            // HeartbeatInterval <= StartElectionTimeout / 5). The default 500 ms heartbeat is far larger than
            // the fast election timers used here, so during any idle gap between a session's operations a
            // follower's election timer fires before the next heartbeat arrives — a spurious re-election that
            // discards the coordinator-partition leader's in-memory interactive sessions. A 30 ms heartbeat
            // keeps followers fed between operations, and scaling the election window by the test timing scale
            // gives loaded CI runners the same slack the shared cluster harness relies on.
            HeartbeatInterval = TimeSpan.FromMilliseconds(30),
            StartElectionTimeout = (int)(150 * TimingScale),
            EndElectionTimeout = (int)(300 * TimingScale),
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
        (Node[] nodes, _) = await AssembleWithTransport();
        return nodes;
    }

    private async Task<(Node[], MemoryInterNodeCommmunication)> AssembleWithTransport()
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

    /// <summary>
    /// A non-throwing "not delivered" outcome during completion — simulating a coordinator-partition
    /// leadership flip between routing and landing — must retain the participant cache entry. A same-id
    /// retry drives the completion through TryRecoverRegisteredOperation, folds the effect exactly once,
    /// and the finalize/commit path sees the write in the working set (no stranded pending op).
    /// </summary>
    [Fact]
    public async Task CompletionNotDeliveredNonThrowing_ParticipantCacheRetained_SameIdRetrySucceeds()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (Node[] nodes, MemoryInterNodeCommmunication interNode) = await AssembleWithTransport();
        try
        {
            // Locate a remote-entry node (its completion hops the transport) and the coordinator leader.
            TransactionHandle handle = default;
            Node? coordinatorLeader = null;
            Node? remoteEntry = null;
            byte[] probe = [9, 9, 9];

            for (int attempt = 1; ; attempt++)
            {
                (_, handle) =
                    await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

                coordinatorLeader = null;
                remoteEntry = null;
                bool sessionLost = false;

                foreach (Node node in nodes)
                {
                    int before = interNode.BeginOperationCallCount;
                    (OperationRegistrationOutcome probeOutcome, _, _, _, _) =
                        await node.Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, TransactionOperationId.NewRandom(), OperationKind.Set, probe, ct);

                    if (probeOutcome == OperationRegistrationOutcome.RejectedSessionClosed)
                    {
                        sessionLost = true;
                        break;
                    }

                    Assert.Equal(OperationRegistrationOutcome.New, probeOutcome);
                    if (interNode.BeginOperationCallCount == before)
                        coordinatorLeader = node;
                    else
                        remoteEntry = node;
                }

                if (!sessionLost && coordinatorLeader is not null && remoteEntry is not null)
                    break;

                Assert.True(attempt < 8, "coordinator-partition leadership did not stabilise in time to classify nodes");
            }

            Assert.NotNull(coordinatorLeader);
            Assert.NotNull(remoteEntry);

            // Fail the first completion with a non-throwing MustRetry (not an exception) — the path
            // that previously returned a silent false-ack and dropped the cache entry prematurely.
            TransactionOperationId writeOp = TransactionOperationId.NewRandom();
            bool redirected = false;
            interNode.CompleteOperationRedirectFault = (_, opId) =>
            {
                if (opId == writeOp && !redirected)
                {
                    redirected = true;
                    return true;
                }
                return false;
            };

            int completesBefore = interNode.CompleteOperationCallCount;

            // The write applies, its first completion is non-throwingly not delivered; the retry must
            // recover the confirmed response from the participant cache without re-running the actor.
            (KeyValueResponseType type, long revision, _) =
                await SetWithRetry(remoteEntry!, handle, writeOp, [2, 2, 2], ct);

            Assert.True(redirected, "the injected redirect fault should have fired");
            Assert.Equal(KeyValueResponseType.Set, type);
            Assert.True(revision >= 0);

            // At least two completion attempts: the non-delivered one plus the successful recovery.
            Assert.True(interNode.CompleteOperationCallCount - completesBefore >= 2);

            // Effect folded exactly once: wk1 is in the working set with the correct anchor.
            TransactionWorkingSet? ws =
                await coordinatorLeader!.Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(ws);
            Assert.Equal("wk1", ws!.RecordAnchorKey);
            Assert.Contains(ws.ModifiedKeys, m => m.Key == "wk1");
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A successful completion with no persistent anchor must return <c>(Set, null)</c>: the
    /// outcome is Set (acknowledged), the anchor is null (anchorless transaction), and the
    /// participant cache entry is removed — not misread as a transient non-delivery.
    /// </summary>
    [Fact]
    public async Task AnchorlessCompletion_ReturnsSetWithNullAnchor_RemovesCacheEntry()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            TransactionOperationId op = TransactionOperationId.NewRandom();
            byte[] digest = [0xAA];

            (OperationRegistrationOutcome regOutcome, _, _, _, _) =
                await nodes[0].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, op, OperationKind.Set, digest, ct);
            Assert.Equal(OperationRegistrationOutcome.New, regOutcome);

            // Complete with an ephemeral write — no persistent key, so no record anchor.
            (KeyValueResponseType outcome, string? anchor) = await nodes[0].Kahuna.LocateAndCompleteOperation(
                handle.CoordinatorKey, handle.TransactionId, op,
                new OperationCompletionPayload
                {
                    ModifiedKey = "ephem/k",
                    Durability = KeyValueDurability.Ephemeral,
                    CachedType = KeyValueResponseType.Set,
                    CachedRevision = 1,
                    CachedTimestamp = new HLCTimestamp(1, 1, 0)
                },
                ct);

            // Set outcome (acknowledged) and null anchor (no persistent key) — not a transient non-delivery.
            Assert.Equal(KeyValueResponseType.Set, outcome);
            Assert.Null(anchor);

            // Same operation id is now AlreadyCompleted (cache entry was removed; coordinator saw the fold).
            (OperationRegistrationOutcome replayOutcome, _, _, _, _) =
                await nodes[0].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, op, OperationKind.Set, digest, ct);
            Assert.Equal(OperationRegistrationOutcome.AlreadyCompleted, replayOutcome);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// A <c>WaitingForReplication</c> completion with no effect must cancel the coordinator
    /// registration so a same-id retry re-registers as <c>New</c> and can actually apply the
    /// mutation — not replay a terminal transient that blocks finalize forever.
    /// A <c>WaitingForReplication</c> completion that <b>did</b> produce an effect must keep the
    /// cached-response replay path: a same-id retry sees <c>AlreadyCompleted</c> and does not
    /// re-apply.
    /// </summary>
    [Fact]
    public async Task WaitingForReplication_NoEffect_CancelsAndAllowsReregistration_WithEffect_TakesCachedReplayPath()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            byte[] digest = [0x11];

            // — Branch A: WaitingForReplication + no effect — should cancel and allow re-registration.
            TransactionOperationId opNoEffect = TransactionOperationId.NewRandom();

            (OperationRegistrationOutcome regNoEffect, _, _, _, _) =
                await nodes[0].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, opNoEffect, OperationKind.Set, digest, ct);
            Assert.Equal(OperationRegistrationOutcome.New, regNoEffect);

            await nodes[0].Kahuna.LocateAndCompleteOperation(
                handle.CoordinatorKey, handle.TransactionId, opNoEffect,
                new OperationCompletionPayload
                {
                    // No ModifiedKey, no locks, no reads — empty effect.
                    CachedType = KeyValueResponseType.WaitingForReplication,
                    CachedRevision = 0,
                    CachedTimestamp = HLCTimestamp.Zero
                },
                ct);

            // Should have been cancelled — re-registration with the same id must come back as New.
            (OperationRegistrationOutcome reregOutcome, _, _, _, _) =
                await nodes[0].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, opNoEffect, OperationKind.Set, digest, ct);
            Assert.Equal(OperationRegistrationOutcome.New, reregOutcome);

            // — Branch B: WaitingForReplication + has effect — should keep the cached replay path.
            TransactionOperationId opWithEffect = TransactionOperationId.NewRandom();
            byte[] digestB = [0x22];

            (OperationRegistrationOutcome regWithEffect, _, _, _, _) =
                await nodes[0].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, opWithEffect, OperationKind.Set, digestB, ct);
            Assert.Equal(OperationRegistrationOutcome.New, regWithEffect);

            await nodes[0].Kahuna.LocateAndCompleteOperation(
                handle.CoordinatorKey, handle.TransactionId, opWithEffect,
                new OperationCompletionPayload
                {
                    ModifiedKey = "repl/k",
                    Durability = KeyValueDurability.Ephemeral,
                    CachedType = KeyValueResponseType.WaitingForReplication,
                    CachedRevision = 5,
                    CachedTimestamp = new HLCTimestamp(1, 10, 0)
                },
                ct);

            // Effect was registered — same id must come back as AlreadyCompleted (cached-response replay).
            (OperationRegistrationOutcome cachedOutcome, KeyValueResponseType cachedType, long cachedRevision, _, _) =
                await nodes[0].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, opWithEffect, OperationKind.Set, digestB, ct);
            Assert.Equal(OperationRegistrationOutcome.AlreadyCompleted, cachedOutcome);
            Assert.Equal(KeyValueResponseType.WaitingForReplication, cachedType);
            Assert.Equal(5, cachedRevision);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// The inbound handler must re-check local leadership before folding. When the node is not
    /// the coordinator-partition leader, <c>CompleteOperationInbound</c> returns <c>MustRetry</c>
    /// without touching the coordinator — preventing a false acknowledgement from a demoted node.
    /// When the node is the leader the call folds and returns <c>Set</c>.
    /// </summary>
    [Fact]
    public async Task CompleteOperationInbound_WhenNotLeader_ReturnsMustRetry_WhenLeader_ReturnsSet()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Node[] nodes = await Assemble();
        try
        {
            (_, TransactionHandle handle) =
                await nodes[0].Kahuna.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);

            TransactionOperationId op = TransactionOperationId.NewRandom();
            byte[] digest = [0x77];

            // Register the operation on the coordinator.
            (OperationRegistrationOutcome regOutcome, _, _, _, _) =
                await nodes[0].Kahuna.LocateAndBeginOperation(handle.CoordinatorKey, handle.TransactionId, op, OperationKind.Set, digest, ct);
            Assert.Equal(OperationRegistrationOutcome.New, regOutcome);

            OperationCompletionPayload payload = new()
            {
                ModifiedKey = "inbound/k",
                Durability = KeyValueDurability.Persistent,
                CachedType = KeyValueResponseType.Set,
                CachedRevision = 3,
                CachedTimestamp = new HLCTimestamp(1, 99, 0)
            };

            // Find a follower node: one that is not the leader for any partition.
            Node? followerNode = null;
            foreach (Node node in nodes)
            {
                bool isLeaderAny = false;
                for (int partition = 1; partition <= 2; partition++)
                    if (await node.Raft.AmILeader(partition, ct))
                        isLeaderAny = true;
                if (!isLeaderAny)
                {
                    followerNode = node;
                    break;
                }
            }

            if (followerNode is not null)
            {
                // A pure follower rejects the inbound fold with MustRetry.
                (KeyValueResponseType followerOutcome, string? followerAnchor) =
                    await followerNode.Kahuna.CompleteOperationInbound(handle.CoordinatorKey, handle.TransactionId, op, payload);
                Assert.Equal(KeyValueResponseType.MustRetry, followerOutcome);
                Assert.Null(followerAnchor);

                // Operation must still be completable — no false fold happened.
                // Locate the actual coordinator leader and fold there.
                foreach (Node node in nodes)
                {
                    (KeyValueResponseType tryOutcome, _) =
                        await node.Kahuna.CompleteOperationInbound(handle.CoordinatorKey, handle.TransactionId, op, payload);
                    if (tryOutcome == KeyValueResponseType.Set)
                    {
                        // Confirm the working set recorded the effect.
                        TransactionWorkingSet? ws =
                            await node.Kahuna.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
                        Assert.NotNull(ws);
                        Assert.Contains(ws!.ModifiedKeys, m => m.Key == "inbound/k");
                        return;
                    }
                }
                Assert.Fail("No node accepted CompleteOperationInbound as leader");
            }
            else
            {
                // With only two partitions and three nodes, there must be at least one follower.
                Assert.Fail("Expected at least one follower node in the three-node cluster");
            }
        }
        finally
        {
            await LeaveAll(nodes);
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
