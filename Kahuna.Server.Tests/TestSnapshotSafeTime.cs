
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Kahuna.Server.KeyValues.Ranges;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for the safe-time wait on prepared writes.
///
/// A snapshot read at T must not miss a write whose commit ts is ≤ T.
/// Both point reads and range scans use the same WaitingForReplication mechanism:
/// the handler returns WaitingForReplication and the retry loop (TryGetValue /
/// LocateAndScanRange) backs off until the intent resolves or expires —
/// the wait is transparent to callers.
///
/// Every scenario here stages its write in an ephemeral 2PC transaction, so its whole state lives in the
/// partition leader's actor and is not replicated. Each therefore runs under
/// <see cref="BaseCluster.RunUnderStableLeadership"/>, which retries it if the leader moved mid-scenario and
/// discarded that state — a failure with leadership unchanged still fails the test.
/// </summary>
public sealed class TestSnapshotSafeTime : BaseCluster
{
    private readonly ILogger<IRaft>   raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestSnapshotSafeTime(ITestOutputHelper outputHelper)
    {
        ILoggerFactory lf = TestLogFactory.Create(outputHelper);
        raftLogger   = lf.CreateLogger<IRaft>();
        kahunaLogger = lf.CreateLogger<IKahuna>();
    }

    // ── PreparedWrite_BelowSnapshot_Blocks_ThenSeesAfterCommit ───────────────

    /// <summary>
    /// A snapshot read at T blocks transparently (WaitingForReplication retry loop) when
    /// a foreign prepared write intent has CommitTimestamp ≤ T. After a concurrent commit
    /// resolves the intent the same read returns the newly committed version — the wait is
    /// invisible to the caller.
    ///
    /// CommitTimestamp = mvccEntry.LastModified (stamped at TrySet time inside the actor).
    /// T is captured after a 10 ms wall-clock advance so T.L > CommitTimestamp.L.
    /// The committer fires 50 ms after prepare, well within the exponential back-off window.
    /// </summary>
    [Fact]
    public async Task PreparedWrite_BelowSnapshot_Blocks_ThenSeesAfterCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        try
        {
            await RunUnderStableLeadership(node1, 3, async () =>
            {
                string key  = "sst:a:" + Guid.NewGuid().ToString("N")[..8];
                byte[] valA = "before"u8.ToArray();
                byte[] valB = "after"u8.ToArray();

                // Commit valA to establish a base committed revision.
                (KeyValueResponseType setA, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, valA, null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Ephemeral, ct);
                Assert.Equal(KeyValueResponseType.Set, setA);

                // Open a 2PC transaction and stage valB.
                (KeyValueResponseType startType, TransactionHandle txHandle) = await kahuna1.LocateAndStartTransaction(
                    new() { CoordinatorKey = Guid.NewGuid().ToString(), Locking = KeyValueTransactionLocking.Pessimistic }, ct);
                HLCTimestamp txId = txHandle.TransactionId;
                Assert.Equal(KeyValueResponseType.Set, startType);

                (KeyValueResponseType setB, _, HLCTimestamp stagedAt) = await kahuna2.LocateAndTrySetKeyValue(
                    txId, key, valB, null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Ephemeral, ct);
                Assert.Equal(KeyValueResponseType.Set, setB);

                // Prepare: parks the write intent with CommitTimestamp = mvccEntry.LastModified.
                // The commit id must order after every write the prepare validates against. Deriving it from a bare
                // local event is not enough: the writes were stamped by the partition leader, and two HLCs produced in
                // the same millisecond with the same counter are ordered by node id — a lower-numbered node would then
                // hand the prepare a commit id sorting *before* the value being committed, which prepare correctly
                // rejects. Folding the staged write's timestamp in through the ordinary HLC receive rule yields an id
                // strictly after it, and after the base revision the same actor stamped earlier.
                HLCTimestamp commitId = node1.HybridLogicalClock.ReceiveEvent(node1.GetLocalNodeId(), stagedAt);
                (KeyValueResponseType prep, HLCTimestamp ticket, _, _) = await kahuna3.LocateAndTryPrepareMutations(
                    txId, commitId, key, KeyValueDurability.Ephemeral, ct);
                Assert.Equal(KeyValueResponseType.Prepared, prep);

                // Advance wall clock past CommitTimestamp, then capture T > CommitTimestamp.
                await Task.Delay(10, ct);
                HLCTimestamp T = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());

                // Commit the parked intent after 50 ms — inside the read's back-off window.
                Task commitTask = Task.Run(async () =>
                {
                    await Task.Delay(50, ct);
                    await kahuna1.LocateAndTryCommitMutations(txId, key, ticket, KeyValueDurability.Ephemeral, ct);
                }, ct);

                // Read at T: intent is live with CommitTimestamp ≤ T → handler returns
                // WaitingForReplication; the retry loop backs off until the concurrent commit
                // resolves the intent, then returns Get + valB. Transparent to the caller.
                (KeyValueResponseType r1, ReadOnlyKeyValueEntry? snap) = await kahuna2.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, T, KeyValueDurability.Ephemeral, ct);

                await commitTask;

                Assert.Equal(KeyValueResponseType.Get, r1);
                Assert.NotNull(snap);
                Assert.Equal("after", Encoding.UTF8.GetString(snap.Value!));
            });
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    // ── PreparedWrite_AboveSnapshot_DoesNotBlock ─────────────────────────────

    /// <summary>
    /// When the prepared intent's CommitTimestamp is provably > T the snapshot read does not
    /// enter the wait loop — it falls through and serves the committed revision at-or-before T.
    ///
    /// CommitTimestamp = HLC at TrySet time, strictly > valA's LastModified on the same
    /// partition actor (HLC monotonicity). T = valA.LastModified guarantees CommitTimestamp > T.
    /// </summary>
    [Fact]
    public async Task PreparedWrite_AboveSnapshot_DoesNotBlock()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        try
        {
            await RunUnderStableLeadership(node1, 3, async () =>
            {
                string key  = "sst:b:" + Guid.NewGuid().ToString("N")[..8];
                byte[] valA = "before"u8.ToArray();
                byte[] valB = "after"u8.ToArray();

                // Commit valA and read it back to obtain entry.LastModified as snapshotT.
                (KeyValueResponseType setA, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, valA, null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Ephemeral, ct);
                Assert.Equal(KeyValueResponseType.Set, setA);

                (KeyValueResponseType getA, ReadOnlyKeyValueEntry? entryA) = await kahuna1.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, ct);
                Assert.Equal(KeyValueResponseType.Get, getA);
                Assert.NotNull(entryA);
                HLCTimestamp snapshotT = entryA.LastModified; // valA's commit ts

                // Stage valB; the actor HLC is strictly > snapshotT so CommitTimestamp > snapshotT.
                (KeyValueResponseType startType, TransactionHandle txHandle) = await kahuna1.LocateAndStartTransaction(
                    new() { CoordinatorKey = Guid.NewGuid().ToString(), Locking = KeyValueTransactionLocking.Pessimistic }, ct);
                HLCTimestamp txId = txHandle.TransactionId;
                Assert.Equal(KeyValueResponseType.Set, startType);

                (KeyValueResponseType setB, _, HLCTimestamp stagedAt) = await kahuna2.LocateAndTrySetKeyValue(
                    txId, key, valB, null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Ephemeral, ct);
                Assert.Equal(KeyValueResponseType.Set, setB);

                // Order the commit id after the staged write through the HLC receive rule — see the note in
                // PreparedWrite_BelowSnapshot_Blocks_ThenSeesAfterCommit on why a bare local event is not enough.
                HLCTimestamp commitId = node1.HybridLogicalClock.ReceiveEvent(node1.GetLocalNodeId(), stagedAt);
                (KeyValueResponseType prep, HLCTimestamp ticket, _, _) = await kahuna3.LocateAndTryPrepareMutations(
                    txId, commitId, key, KeyValueDurability.Ephemeral, ct);
                Assert.Equal(KeyValueResponseType.Prepared, prep);

                // Read at snapshotT: CommitTimestamp > snapshotT → handler falls through
                // immediately (no WaitingForReplication). entry.LastModified = snapshotT ≤ T →
                // snapshot branch is false → serve current committed valA.
                (KeyValueResponseType r1, ReadOnlyKeyValueEntry? snap) = await kahuna1.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, snapshotT, KeyValueDurability.Ephemeral, ct);
                Assert.Equal(KeyValueResponseType.Get, r1);
                Assert.NotNull(snap);
                Assert.Equal("before", Encoding.UTF8.GetString(snap.Value!));

                // Clean up the parked intent.
                await kahuna2.LocateAndTryRollbackMutations(txId, key, ticket, KeyValueDurability.Ephemeral, ct);
            });
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    // ── ExpiredIntent_ClearedByReader_ServesCommittedState ───────────────────

    /// <summary>
    /// An unprepared (CommitTimestamp == Zero) exclusive lock with a short TTL causes the
    /// retry loop to back off. Once the TTL lapses the actor clears the intent as housekeeping
    /// and returns Get — the wait resolves transparently; the caller never sees WaitingForReplication.
    ///
    /// T is captured after the lock transaction began, so the lock's owner is a transaction the
    /// snapshot cannot rule out — that ordering is what makes the read wait at all. A snapshot
    /// below the owner's transaction id skips the intent outright and never reaches the
    /// housekeeping path this test exists for.
    /// </summary>
    [Fact]
    public async Task ExpiredIntent_ClearedByReader_ServesCommittedState()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        try
        {
            await RunUnderStableLeadership(node1, 3, async () =>
            {
                string key  = "sst:c:" + Guid.NewGuid().ToString("N")[..8];
                byte[] valA = "committed"u8.ToArray();

                // Write valA as the base committed revision.
                (KeyValueResponseType setA, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, valA, null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Ephemeral, ct);
                Assert.Equal(KeyValueResponseType.Set, setA);

                // Acquire a short-lived exclusive lock (200 ms TTL). CommitTimestamp == Zero
                // (plain lock, not a 2PC prepared intent).
                (KeyValueResponseType startType, TransactionHandle lockTxHandle) = await kahuna1.LocateAndStartTransaction(
                    new() { CoordinatorKey = Guid.NewGuid().ToString(), Locking = KeyValueTransactionLocking.Pessimistic }, ct);
                HLCTimestamp lockTxId = lockTxHandle.TransactionId;
                Assert.Equal(KeyValueResponseType.Set, startType);

                (KeyValueResponseType lockResult, _, _, _) = await kahuna2.LocateAndTryAcquireExclusiveLock(
                    lockTxId, key, 200, KeyValueDurability.Ephemeral, ct);
                Assert.Equal(KeyValueResponseType.Locked, lockResult);

                // Capture T strictly above the lock owner's transaction id, so the snapshot cannot
                // prove the owner's writes land outside it and the safe-time wait engages.
                await Task.Delay(10, ct);
                HLCTimestamp T = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());

                // Read at T: handler loops (WaitingForReplication) until the 200 ms lock
                // expires, at which point the actor clears it and returns Get + valA.
                // The TTL (200 ms) is shorter than the first back-off plateau so the loop
                // resolves within ~300 ms total.
                (KeyValueResponseType r1, ReadOnlyKeyValueEntry? snap) = await kahuna3.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, T, KeyValueDurability.Ephemeral, ct);
                Assert.Equal(KeyValueResponseType.Get, r1);
                Assert.NotNull(snap);
                Assert.Equal("committed", Encoding.UTF8.GetString(snap.Value!));
            });
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    // ── UnpreparedIntent_WriterBegunAfterSnapshot_PointReadServesImmediately ─────────

    /// <summary>
    /// A live unprepared intent whose owning transaction began after the snapshot must not make
    /// the snapshot read wait: every write that transaction can ever commit is stamped above its
    /// transaction id, so nothing it does can land at or before the snapshot. The lock's TTL here
    /// is far longer than the asserted response time, so a read that waits on the intent fails
    /// this test rather than passing slowly.
    ///
    /// This ordering is exactly a range split's bulk copy under sustained writes: the copy mints
    /// its snapshot, and every write in flight after that moment belongs to a transaction begun
    /// after the snapshot. A read that waits on those intents starves — each retry meets the
    /// writer's newest in-flight intent — and the split then never lands on a busy range.
    /// </summary>
    [Fact]
    public async Task UnpreparedIntent_WriterBegunAfterSnapshot_PointReadServesImmediately()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        try
        {
            await RunUnderStableLeadership(node1, 3, async () =>
            {
                string key  = "sst:d:" + Guid.NewGuid().ToString("N")[..8];
                byte[] valA = "before"u8.ToArray();

                (KeyValueResponseType setA, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, key, valA, null, -1, KeyValueFlags.Set, 0,
                    KeyValueDurability.Ephemeral, ct);
                Assert.Equal(KeyValueResponseType.Set, setA);

                // The committed write was stamped by its partition leader's clock, which is not
                // node1's; a 10 ms wall-clock advance puts T above that stamp so the base revision
                // is visible at T. The delay after T then puts the transaction id strictly above T.
                await Task.Delay(10, ct);
                HLCTimestamp T = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());
                await Task.Delay(10, ct);

                (KeyValueResponseType startType, TransactionHandle txHandle) = await kahuna1.LocateAndStartTransaction(
                    new() { CoordinatorKey = Guid.NewGuid().ToString(), Locking = KeyValueTransactionLocking.Pessimistic }, ct);
                HLCTimestamp txId = txHandle.TransactionId;
                Assert.Equal(KeyValueResponseType.Set, startType);
                Assert.True(txId.CompareTo(T) > 0, "the writer must begin after the snapshot");

                // A 10 s TTL: a safe-time wait on this intent cannot resolve inside the assertion
                // window below, so the old blocking behavior fails loudly instead of passing late.
                (KeyValueResponseType lockResult, _, _, _) = await kahuna2.LocateAndTryAcquireExclusiveLock(
                    txId, key, 10_000, KeyValueDurability.Ephemeral, ct);
                Assert.Equal(KeyValueResponseType.Locked, lockResult);

                System.Diagnostics.Stopwatch elapsed = System.Diagnostics.Stopwatch.StartNew();

                (KeyValueResponseType r1, ReadOnlyKeyValueEntry? snap) = await kahuna3.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, T, KeyValueDurability.Ephemeral, ct);

                elapsed.Stop();

                Assert.Equal(KeyValueResponseType.Get, r1);
                Assert.NotNull(snap);
                Assert.Equal("before", Encoding.UTF8.GetString(snap.Value!));
                Assert.True(elapsed.ElapsedMilliseconds < 5_000,
                    $"the read waited {elapsed.ElapsedMilliseconds} ms on an intent that provably commits above its snapshot");

                await kahuna1.LocateAndTryReleaseExclusiveLock(txId, key, KeyValueDurability.Ephemeral, ct);
            });
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    // ── UnpreparedIntent_WriterBegunAfterSnapshot_ScanServesThePage ─────────────────

    /// <summary>
    /// The scan-path counterpart: a snapshot range scan whose window holds a foreign staged write
    /// (unprepared, CommitTimestamp == Zero) from a transaction begun after the snapshot must serve
    /// the page with the committed values rather than answer MustRetry/WaitingForReplication.
    /// Runs both durabilities because they take different scan paths — ephemeral resolves keys
    /// inline in TryGetByRangeHandler, persistent through the RangeScanContinuation merge — and
    /// each applies the safe-time rule at its own site.
    /// </summary>
    [Theory]
    [InlineData(KeyValueDurability.Ephemeral)]
    [InlineData(KeyValueDurability.Persistent)]
    public async Task UnpreparedIntent_WriterBegunAfterSnapshot_ScanServesThePage(KeyValueDurability durability)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        try
        {
            await RunUnderStableLeadership(node1, 3, async () =>
            {
                // '/'-bucketed keys under a slash-free scan prefix: a write routes by the bucket
                // before the '/', and a scan routes by the prefix string itself, so the two agree
                // only when the prefix omits the trailing slash. This keeps every key of the scan
                // on one partition leader — required for the ephemeral arm, whose data is resident
                // only in that leader's actor.
                string prefix = "sste" + Guid.NewGuid().ToString("N")[..8];

                for (int i = 0; i < 3; i++)
                {
                    (KeyValueResponseType set, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                        HLCTimestamp.Zero, $"{prefix}/k{i}", Encoding.UTF8.GetBytes($"v{i}"), null, -1,
                        KeyValueFlags.Set, 0, durability, ct);
                    Assert.Equal(KeyValueResponseType.Set, set);
                }

                // The seeds were stamped by their partition leaders' clocks; advance the wall
                // clock so T sorts above every seed, then again so the writer's transaction id
                // sorts above T.
                await Task.Delay(10, ct);
                HLCTimestamp T = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());
                await Task.Delay(10, ct);

                (KeyValueResponseType startType, TransactionHandle txHandle) = await kahuna1.LocateAndStartTransaction(
                    new() { CoordinatorKey = Guid.NewGuid().ToString(), Locking = KeyValueTransactionLocking.Pessimistic }, ct);
                HLCTimestamp txId = txHandle.TransactionId;
                Assert.Equal(KeyValueResponseType.Set, startType);
                Assert.True(txId.CompareTo(T) > 0, "the writer must begin after the snapshot");

                // Stage a write on the middle key — a live unprepared intent inside the window,
                // exactly what a split's bulk copy meets under a hammering writer.
                (KeyValueResponseType staged, _, _) = await kahuna2.LocateAndTrySetKeyValue(
                    txId, $"{prefix}/k1", "staged"u8.ToArray(), null, -1,
                    KeyValueFlags.Set, 0, durability, ct);
                Assert.Equal(KeyValueResponseType.Set, staged);

                KeyValueGetByRangeResult page = await kahuna3.LocateAndGetByRange(
                    HLCTimestamp.Zero, prefix, null, true, null, false, 100, T, durability, ct);

                Assert.Equal(KeyValueResponseType.Get, page.Type);
                Assert.Equal(3, page.Items.Count);

                for (int i = 0; i < 3; i++)
                {
                    Assert.Equal($"{prefix}/k{i}", page.Items[i].Item1);
                    Assert.Equal($"v{i}", Encoding.UTF8.GetString(page.Items[i].Item2.Value!));
                }

                await kahuna1.LocateAndTryReleaseExclusiveLock(txId, $"{prefix}/k1", durability, ct);
            });
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

}
