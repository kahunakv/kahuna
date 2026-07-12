
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Adversarial coverage for transaction-scoped (register-remote) range locks under range-map changes and
/// partial failure. These exercise the interaction between the coordinator-owned working set and the
/// locator's per-range fan-out + generation fence: a split committing inside the acquire window, a split
/// after a confirmed acquire (release must use current routing), several distinct ranges on one
/// transaction, a partial-acquire rollback that must not leak a descriptor into the working set, and a
/// renewal that races a finalize.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestRangeLockRegistrationAdversarial : BaseCluster
{
    private const string Space = "adv:rl";
    private const string Split = Space + "/m";

    private const int LeftPid  = RangeMapStore.FirstDataPartitionId;     // P2
    private const int RightPid = RangeMapStore.FirstDataPartitionId + 1; // P3

    private readonly ILogger<IRaft>   raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestRangeLockRegistrationAdversarial(ITestOutputHelper outputHelper)
    {
        ILoggerFactory lf = LoggerFactory.Create(b =>
            b.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));
        raftLogger   = lf.CreateLogger<IRaft>();
        kahunaLogger = lf.CreateLogger<IKahuna>();
    }

    // ── Split during acquire ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// A split that commits inside the acquire window trips the generation fence: the registered acquire
    /// returns <c>MustRetry</c> and the operation is cancelled, so the coordinator working set records no
    /// range descriptor. A follow-up acquire on the now-split map (fresh op id) succeeds and records
    /// exactly one logical range using the current routing.
    /// </summary>
    [Fact]
    public async Task SplitDuringAcquire_FencesAndRecordsNoRange_ThenRetrySucceedsWithCurrentRouting()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft, KahunaManager)[] nodes = await Assemble();
        try
        {
            await SeedSingle(nodes, ct);
            KahunaManager node = nodes[0].Item2;
            (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            TransactionHandle handle = await StartTx(node, ct);

            TransactionOperationId acquireOp = TransactionOperationId.NewRandom();
            (KeyValueResponseType fenced, _) = await node.RegisterAndAcquireRangeLockWithHook(
                handle.TransactionId, handle.CoordinatorKey, acquireOp, Space, null, true, null, false,
                30_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive,
                afterSnapshot: async () =>
                {
                    await SplitToTwo(metaLeader, nodes);
                    foreach ((IRaft _, KahunaManager k) in nodes)
                        await WaitUntilAsync(() => k.RangeMapStore.Current.FindAll(Space).Count == 2);
                },
                cancellationToken: ct);

            Assert.Equal(KeyValueResponseType.MustRetry, fenced);

            // The fenced acquire recorded nothing — a MustRetry cancels the operation.
            TransactionWorkingSet? afterFence = await node.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterFence);
            Assert.Empty(afterFence!.AcquiredRangeLocks);

            // Retry with a fresh op id on the now-split map succeeds and records one logical range.
            TransactionOperationId retryOp = TransactionOperationId.NewRandom();
            KeyValueResponseType retry = await RangeAcquireWithRetry(() =>
                node.LocateAndTryAcquireRangeLock(handle.TransactionId, Space, null, true, null, false, 30_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, handle.CoordinatorKey, retryOp), ct);
            Assert.Equal(KeyValueResponseType.Locked, retry);

            TransactionWorkingSet? afterRetry = await node.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterRetry);
            Assert.Single(afterRetry!.AcquiredRangeLocks);
        }
        finally
        {
            await LeaveAll(nodes, ct);
        }
    }

    // ── Split after confirmed acquire, before finalize ─────────────────────────────────────────

    /// <summary>
    /// A range lock confirmed against a single descriptor stays recorded as its logical bounds. When the
    /// map splits before finalize, the registered release must fan out over the current descriptor set and
    /// still drop the descriptor from the working set.
    /// </summary>
    [Fact]
    public async Task SplitAfterConfirmedAcquire_ReleaseUsesCurrentRouting_AndDropsDescriptor()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft, KahunaManager)[] nodes = await Assemble();
        try
        {
            await SeedSingle(nodes, ct);
            KahunaManager node = nodes[0].Item2;
            (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            TransactionHandle handle = await StartTx(node, ct);

            // Acquire the full range against the single descriptor.
            TransactionOperationId acquireOp = TransactionOperationId.NewRandom();
            KeyValueResponseType acquired = await RangeAcquireWithRetry(() =>
                node.LocateAndTryAcquireRangeLock(handle.TransactionId, Space, null, true, null, false, 30_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, handle.CoordinatorKey, acquireOp), ct);
            Assert.Equal(KeyValueResponseType.Locked, acquired);

            TransactionWorkingSet? held = await node.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(held);
            Assert.Single(held!.AcquiredRangeLocks);

            // Split after the acquire — the descriptor set now has two ranges.
            await SplitToTwo(metaLeader, nodes);
            foreach ((IRaft _, KahunaManager k) in nodes)
                await WaitUntilAsync(() => k.RangeMapStore.Current.FindAll(Space).Count == 2);

            // Release fans out over the CURRENT (post-split) routing and still succeeds.
            TransactionOperationId releaseOp = TransactionOperationId.NewRandom();
            KeyValueResponseType released =
                await node.LocateAndTryReleaseExclusiveRangeLock(handle.TransactionId, Space, null, true, null, false, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, releaseOp);
            Assert.Equal(KeyValueResponseType.Unlocked, released);

            TransactionWorkingSet? afterRelease = await node.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterRelease);
            Assert.Empty(afterRelease!.AcquiredRangeLocks);
        }
        finally
        {
            await LeaveAll(nodes, ct);
        }
    }

    // ── Multiple distinct ranges for one transaction ───────────────────────────────────────────

    /// <summary>
    /// Two non-overlapping ranges acquired by one transaction are both recorded as distinct descriptors;
    /// releasing each drops only the matching descriptor.
    /// </summary>
    [Fact]
    public async Task MultipleDistinctRanges_AreRecordedAndReleasedIndependently()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft, KahunaManager)[] nodes = await Assemble();
        try
        {
            await SeedSplit(nodes, ct);
            KahunaManager node = nodes[0].Item2;
            TransactionHandle handle = await StartTx(node, ct);

            // Left half [−∞, Split) and right half [Split, +∞) — distinct descriptors.
            TransactionOperationId leftOp = TransactionOperationId.NewRandom();
            KeyValueResponseType left = await RangeAcquireWithRetry(() =>
                node.LocateAndTryAcquireRangeLock(handle.TransactionId, Space, null, true, Split, false, 30_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, handle.CoordinatorKey, leftOp), ct);
            Assert.Equal(KeyValueResponseType.Locked, left);

            TransactionOperationId rightOp = TransactionOperationId.NewRandom();
            KeyValueResponseType right = await RangeAcquireWithRetry(() =>
                node.LocateAndTryAcquireRangeLock(handle.TransactionId, Space, Split, true, null, false, 30_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, handle.CoordinatorKey, rightOp), ct);
            Assert.Equal(KeyValueResponseType.Locked, right);

            TransactionWorkingSet? both = await node.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(both);
            Assert.Equal(2, both!.AcquiredRangeLocks.Count);
            Assert.Contains(both.AcquiredRangeLocks, r => r.EndKey == Split);
            Assert.Contains(both.AcquiredRangeLocks, r => r.StartKey == Split);

            // Release the left range only — the right descriptor remains.
            TransactionOperationId releaseLeftOp = TransactionOperationId.NewRandom();
            Assert.Equal(KeyValueResponseType.Unlocked,
                await node.LocateAndTryReleaseExclusiveRangeLock(handle.TransactionId, Space, null, true, Split, false, KeyValueDurability.Persistent, ct, handle.CoordinatorKey, releaseLeftOp));

            TransactionWorkingSet? afterLeft = await node.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterLeft);
            Assert.Single(afterLeft!.AcquiredRangeLocks);
            Assert.Contains(afterLeft.AcquiredRangeLocks, r => r.StartKey == Split);
        }
        finally
        {
            await LeaveAll(nodes, ct);
        }
    }

    // ── Partial acquire rollback ───────────────────────────────────────────────────────────────

    /// <summary>
    /// A full-range acquire whose second sub-lock conflicts must roll the first sub-lock back and record
    /// no descriptor in the working set — no partial lock leaks. The rolled-back left half is then
    /// acquirable on its own, proving the first sub-lock was actually released.
    /// </summary>
    [Fact]
    public async Task PartialAcquireRollback_LeavesNoPartialDescriptor_AndReleasesTheAcquiredSubLock()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft, KahunaManager)[] nodes = await Assemble();
        try
        {
            await SeedSplit(nodes, ct);
            KahunaManager node = nodes[0].Item2;

            // A different (non-transactional) holder takes the RIGHT half exclusively so our full-range
            // acquire fails on the second sub-lock.
            HLCTimestamp otherTx = nodes[0].Item1.HybridLogicalClock.TrySendOrLocalEvent(nodes[0].Item1.GetLocalNodeId());
            (KeyValueResponseType blocker, _) = await node.LocateAndTryAcquireRangeLock(
                otherTx, Space, Split, true, null, false, 30_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct);
            Assert.Equal(KeyValueResponseType.Locked, blocker);

            TransactionHandle handle = await StartTx(node, ct);

            // Full-range acquire: left half (P2) grabbed, right half (P3) conflicts → rollback.
            TransactionOperationId fullOp = TransactionOperationId.NewRandom();
            (KeyValueResponseType full, _) = await node.LocateAndTryAcquireRangeLock(
                handle.TransactionId, Space, null, true, null, false, 30_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, handle.CoordinatorKey, fullOp);
            Assert.Equal(KeyValueResponseType.AlreadyLocked, full);

            // No partial descriptor recorded.
            TransactionWorkingSet? afterFail = await node.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterFail);
            Assert.Empty(afterFail!.AcquiredRangeLocks);

            // The left sub-lock was rolled back: our transaction can now take the left half alone.
            TransactionOperationId leftOp = TransactionOperationId.NewRandom();
            KeyValueResponseType left = await RangeAcquireWithRetry(() =>
                node.LocateAndTryAcquireRangeLock(handle.TransactionId, Space, null, true, Split, false, 30_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, handle.CoordinatorKey, leftOp), ct);
            Assert.Equal(KeyValueResponseType.Locked, left);

            TransactionWorkingSet? afterLeft = await node.LocateAndGetTransactionWorkingSet(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.NotNull(afterLeft);
            Assert.Single(afterLeft!.AcquiredRangeLocks);
        }
        finally
        {
            await LeaveAll(nodes, ct);
        }
    }

    // ── Renewal racing finalize ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Once the session is closed (finalizing), a renewal of a held range under a new op id is fenced out
    /// with <c>Aborted</c> and cannot mutate the frozen working set; the frozen snapshot still holds
    /// exactly the one range confirmed before close.
    /// </summary>
    [Fact]
    public async Task RenewalAfterClose_IsFenced_AndFrozenSetUnchanged()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft, KahunaManager)[] nodes = await Assemble();
        try
        {
            await SeedSingle(nodes, ct);
            KahunaManager node = nodes[0].Item2;
            TransactionHandle handle = await StartTx(node, ct);

            TransactionOperationId acquireOp = TransactionOperationId.NewRandom();
            KeyValueResponseType acquired = await RangeAcquireWithRetry(() =>
                node.LocateAndTryAcquireRangeLock(handle.TransactionId, Space, null, true, null, false, 30_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, handle.CoordinatorKey, acquireOp), ct);
            Assert.Equal(KeyValueResponseType.Locked, acquired);

            // Close (finalize) — the frozen snapshot captures the one held range.
            (KeyValueResponseType closeType, TransactionWorkingSet? frozen) =
                await node.LocateAndCloseTransaction(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.Equal(KeyValueResponseType.Set, closeType);
            Assert.NotNull(frozen);
            Assert.Single(frozen!.AcquiredRangeLocks);

            // A renewal (re-acquire same bounds) under a new op id after close is fenced out.
            TransactionOperationId renewOp = TransactionOperationId.NewRandom();
            (KeyValueResponseType renew, _) = await node.LocateAndTryAcquireRangeLock(
                handle.TransactionId, Space, null, true, null, false, 30_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, handle.CoordinatorKey, renewOp);
            Assert.Equal(KeyValueResponseType.Aborted, renew);

            // The frozen snapshot is unchanged — still exactly one range.
            (KeyValueResponseType closeAgain, TransactionWorkingSet? frozenAgain) =
                await node.LocateAndCloseTransaction(handle.CoordinatorKey, handle.TransactionId, ct);
            Assert.Equal(KeyValueResponseType.Set, closeAgain);
            Assert.NotNull(frozenAgain);
            Assert.Single(frozenAgain!.AcquiredRangeLocks);
        }
        finally
        {
            await LeaveAll(nodes, ct);
        }
    }

    // ── Finalize releases confirmed range locks from server effects ─────────────────────────────

    /// <summary>
    /// Committing a transaction that holds a range lock releases it as part of finalize — driven purely
    /// from the coordinator-owned working set, with no client-supplied lock list — so another transaction
    /// can immediately acquire the same exclusive range. Proves the server owns range-lock cleanup.
    /// </summary>
    [Fact]
    public async Task RangeLockReleasedOnCommit_AllowsAnotherTransactionToAcquire()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft, KahunaManager)[] nodes = await Assemble();
        try
        {
            await SeedSingle(nodes, ct);
            KahunaManager node = nodes[0].Item2;

            TransactionHandle first = await StartTx(node, ct);
            TransactionOperationId acquireOp = TransactionOperationId.NewRandom();
            KeyValueResponseType acquired = await RangeAcquireWithRetry(() =>
                node.LocateAndTryAcquireRangeLock(first.TransactionId, Space, null, true, null, false, 30_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, first.CoordinatorKey, acquireOp), ct);
            Assert.Equal(KeyValueResponseType.Locked, acquired);

            // Commit with EMPTY client working-set lists: the range lock is only in the server-owned set,
            // so a Committed here that also frees the range proves finalize releases from server effects.
            (KeyValueResponseType commitType, _) =
                await node.LocateAndCommitTransaction(first, [], [], [], ct);
            Assert.Equal(KeyValueResponseType.Committed, commitType);

            // A second transaction can now take the same exclusive range — the first tx's lock is gone.
            TransactionHandle second = await StartTx(node, ct);
            TransactionOperationId secondOp = TransactionOperationId.NewRandom();
            KeyValueResponseType reacquired = await RangeAcquireWithRetry(() =>
                node.LocateAndTryAcquireRangeLock(second.TransactionId, Space, null, true, null, false, 30_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, second.CoordinatorKey, secondOp), ct);
            Assert.Equal(KeyValueResponseType.Locked, reacquired);
        }
        finally
        {
            await LeaveAll(nodes, ct);
        }
    }

    /// <summary>
    /// Rolling back a transaction that holds a range lock releases it from the server-owned working set —
    /// again with no client-supplied lock list — so a following transaction can acquire the same range.
    /// </summary>
    [Fact]
    public async Task RangeLockReleasedOnRollback_AllowsAnotherTransactionToAcquire()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (IRaft, KahunaManager)[] nodes = await Assemble();
        try
        {
            await SeedSingle(nodes, ct);
            KahunaManager node = nodes[0].Item2;

            TransactionHandle first = await StartTx(node, ct);
            TransactionOperationId acquireOp = TransactionOperationId.NewRandom();
            KeyValueResponseType acquired = await RangeAcquireWithRetry(() =>
                node.LocateAndTryAcquireRangeLock(first.TransactionId, Space, null, true, null, false, 30_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, first.CoordinatorKey, acquireOp), ct);
            Assert.Equal(KeyValueResponseType.Locked, acquired);

            KeyValueResponseType rollbackType =
                await node.LocateAndRollbackTransaction(first, [], [], ct);
            Assert.Equal(KeyValueResponseType.RolledBack, rollbackType);

            TransactionHandle second = await StartTx(node, ct);
            TransactionOperationId secondOp = TransactionOperationId.NewRandom();
            KeyValueResponseType reacquired = await RangeAcquireWithRetry(() =>
                node.LocateAndTryAcquireRangeLock(second.TransactionId, Space, null, true, null, false, 30_000, KeyValueDurability.Persistent, RangeLockMode.Exclusive, ct, second.CoordinatorKey, secondOp), ct);
            Assert.Equal(KeyValueResponseType.Locked, reacquired);
        }
        finally
        {
            await LeaveAll(nodes, ct);
        }
    }

    // ── harness ────────────────────────────────────────────────────────────────────────────────

    private async Task<(IRaft, KahunaManager)[]> Assemble()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            kahuna.RegisterKeyRange(Space);

        return nodes;
    }

    private async Task SeedSingle((IRaft, KahunaManager)[] nodes, CancellationToken ct)
    {
        (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
        bool seeded = await metaLeader.RangeMapStore.MutateAsync(_ =>
            [new RangeDescriptor { KeySpace = Space, StartKey = null, EndKey = null, PartitionId = LeftPid, Generation = 1 }], ct);
        Assert.True(seeded);

        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            await WaitUntilAsync(() => kahuna.RangeMapStore.Current.FindAll(Space).Count == 1);
    }

    private async Task SeedSplit((IRaft, KahunaManager)[] nodes, CancellationToken ct)
    {
        (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
        bool seeded = await metaLeader.RangeMapStore.MutateAsync(_ =>
        [
            new RangeDescriptor { KeySpace = Space, StartKey = null,  EndKey = Split, PartitionId = LeftPid,  Generation = 1 },
            new RangeDescriptor { KeySpace = Space, StartKey = Split, EndKey = null,  PartitionId = RightPid, Generation = 1 }
        ], ct);
        Assert.True(seeded);

        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            await WaitUntilAsync(() => kahuna.RangeMapStore.Current.FindAll(Space).Count == 2);
    }

    private async Task SplitToTwo(KahunaManager metaLeader, (IRaft, KahunaManager)[] nodes)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        bool split = await metaLeader.RangeMapStore.MutateAsync(_ =>
        [
            new RangeDescriptor { KeySpace = Space, StartKey = null,  EndKey = Split, PartitionId = LeftPid,  Generation = 2 },
            new RangeDescriptor { KeySpace = Space, StartKey = Split, EndKey = null,  PartitionId = RightPid, Generation = 2 }
        ], ct);
        Assert.True(split);
    }

    private static async Task<TransactionHandle> StartTx(KahunaManager node, CancellationToken ct)
    {
        (_, TransactionHandle handle) =
            await node.LocateAndStartTransaction(new() { Locking = KeyValueTransactionLocking.Pessimistic }, ct);
        Assert.False(handle.IsEmpty);
        return handle;
    }

    private static async Task<(IRaft Raft, KahunaManager Kahuna)> LeaderOf(int partition, (IRaft, KahunaManager)[] nodes)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            foreach ((IRaft raft, KahunaManager kahuna) in nodes)
                if (await raft.AmILeader(partition, ct))
                    return (raft, kahuna);
            await Task.Delay(50, ct);
        }
    }

    private static async Task<KeyValueResponseType> RangeAcquireWithRetry(Func<Task<(KeyValueResponseType, HLCTimestamp)>> op, CancellationToken ct)
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

    private static async Task LeaveAll((IRaft, KahunaManager)[] nodes, CancellationToken ct)
    {
        foreach ((IRaft raft, KahunaManager _) in nodes)
        {
            try { await raft.LeaveCluster(true, ct); }
            catch (ObjectDisposedException) { }
        }
    }
}
