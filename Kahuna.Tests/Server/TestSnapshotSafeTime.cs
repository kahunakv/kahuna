
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Kahuna.Server.KeyValues.Ranges;

namespace Kahuna.Tests.Server;

/// <summary>
/// Tests for the safe-time wait on prepared writes.
///
/// A snapshot read at T must not miss a write whose commit ts is ≤ T.
/// Both point reads and range scans use the same WaitingForReplication mechanism:
/// the handler returns WaitingForReplication and the retry loop (TryGetValue /
/// LocateAndScanRange) backs off until the intent resolves or expires —
/// the wait is transparent to callers.
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
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        try
        {
            string key  = "sst:a:" + Guid.NewGuid().ToString("N")[..8];
            byte[] valA = "before"u8.ToArray();
            byte[] valB = "after"u8.ToArray();

            // Commit valA to establish a base committed revision.
            (KeyValueResponseType setA, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, valA, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, setA);

            // Open a 2PC transaction and stage valB.
            (KeyValueResponseType startType, HLCTimestamp txId) = await kahuna1.LocateAndStartTransaction(
                new() { UniqueId = Guid.NewGuid().ToString(), Locking = KeyValueTransactionLocking.Pessimistic }, ct);
            Assert.Equal(KeyValueResponseType.Set, startType);

            (KeyValueResponseType setB, _, _) = await kahuna2.LocateAndTrySetKeyValue(
                txId, key, valB, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, setB);

            // Prepare: parks the write intent with CommitTimestamp = mvccEntry.LastModified.
            HLCTimestamp commitId = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());
            (KeyValueResponseType prep, HLCTimestamp ticket, _, _) = await kahuna3.LocateAndTryPrepareMutations(
                txId, commitId, key, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Prepared, prep);

            // Advance wall clock past CommitTimestamp, then capture T > CommitTimestamp.
            await Task.Delay(10, ct);
            HLCTimestamp T = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());

            // Commit the parked intent after 50 ms — inside the read's back-off window.
            Task commitTask = Task.Run(async () =>
            {
                await Task.Delay(50, ct);
                await kahuna1.LocateAndTryCommitMutations(txId, key, ticket, KeyValueDurability.Persistent, ct);
            }, ct);

            // Read at T: intent is live with CommitTimestamp ≤ T → handler returns
            // WaitingForReplication; the retry loop backs off until the concurrent commit
            // resolves the intent, then returns Get + valB. Transparent to the caller.
            (KeyValueResponseType r1, ReadOnlyKeyValueEntry? snap) = await kahuna2.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, T, KeyValueDurability.Persistent, ct);

            await commitTask;

            Assert.Equal(KeyValueResponseType.Get, r1);
            Assert.NotNull(snap);
            Assert.Equal("after", Encoding.UTF8.GetString(snap.Value!));
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
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        try
        {
            string key  = "sst:b:" + Guid.NewGuid().ToString("N")[..8];
            byte[] valA = "before"u8.ToArray();
            byte[] valB = "after"u8.ToArray();

            // Commit valA and read it back to obtain entry.LastModified as snapshotT.
            (KeyValueResponseType setA, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, valA, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, setA);

            (KeyValueResponseType getA, ReadOnlyKeyValueEntry? entryA) = await kahuna1.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, getA);
            Assert.NotNull(entryA);
            HLCTimestamp snapshotT = entryA.LastModified; // valA's commit ts

            // Stage valB; the actor HLC is strictly > snapshotT so CommitTimestamp > snapshotT.
            (KeyValueResponseType startType, HLCTimestamp txId) = await kahuna1.LocateAndStartTransaction(
                new() { UniqueId = Guid.NewGuid().ToString(), Locking = KeyValueTransactionLocking.Pessimistic }, ct);
            Assert.Equal(KeyValueResponseType.Set, startType);

            (KeyValueResponseType setB, _, _) = await kahuna2.LocateAndTrySetKeyValue(
                txId, key, valB, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, setB);

            HLCTimestamp commitId = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());
            (KeyValueResponseType prep, HLCTimestamp ticket, _, _) = await kahuna3.LocateAndTryPrepareMutations(
                txId, commitId, key, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Prepared, prep);

            // Read at snapshotT: CommitTimestamp > snapshotT → handler falls through
            // immediately (no WaitingForReplication). entry.LastModified = snapshotT ≤ T →
            // snapshot branch is false → serve current committed valA.
            (KeyValueResponseType r1, ReadOnlyKeyValueEntry? snap) = await kahuna1.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, snapshotT, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, r1);
            Assert.NotNull(snap);
            Assert.Equal("before", Encoding.UTF8.GetString(snap.Value!));

            // Clean up the parked intent.
            await kahuna2.LocateAndTryRollbackMutations(txId, key, ticket, KeyValueDurability.Persistent, ct);
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
    /// </summary>
    [Fact]
    public async Task ExpiredIntent_ClearedByReader_ServesCommittedState()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        try
        {
            string key  = "sst:c:" + Guid.NewGuid().ToString("N")[..8];
            byte[] valA = "committed"u8.ToArray();

            // Write valA and capture its committed LastModified as T.
            (KeyValueResponseType setA, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, valA, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, setA);

            (KeyValueResponseType getA, ReadOnlyKeyValueEntry? entryA) = await kahuna1.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, getA);
            Assert.NotNull(entryA);
            HLCTimestamp T = entryA.LastModified;

            // Acquire a short-lived exclusive lock (200 ms TTL). CommitTimestamp == Zero
            // (plain lock, not a 2PC prepared intent) → handler returns WaitingForReplication
            // until the lock expires; the retry loop then clears it and returns Get.
            (KeyValueResponseType startType, HLCTimestamp lockTxId) = await kahuna1.LocateAndStartTransaction(
                new() { UniqueId = Guid.NewGuid().ToString(), Locking = KeyValueTransactionLocking.Pessimistic }, ct);
            Assert.Equal(KeyValueResponseType.Set, startType);

            (KeyValueResponseType lockResult, _, _) = await kahuna2.LocateAndTryAcquireExclusiveLock(
                lockTxId, key, 200, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Locked, lockResult);

            // Read at T: handler loops (WaitingForReplication) until the 200 ms lock
            // expires, at which point the actor clears it and returns Get + valA.
            // The TTL (200 ms) is shorter than the first back-off plateau so the loop
            // resolves within ~300 ms total.
            (KeyValueResponseType r1, ReadOnlyKeyValueEntry? snap) = await kahuna3.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, T, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, r1);
            Assert.NotNull(snap);
            Assert.Equal("committed", Encoding.UTF8.GetString(snap.Value!));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    // ── TwoKeyTx_CrossPartition_SeenAllOrNothing ────────────────────────────

    /// <summary>
    /// A two-key 2PC transaction whose prepared intents land on different partitions
    /// (different Raft actors) is seen all-or-nothing by concurrent snapshot reads at T.
    ///
    /// Both keys are prepared with CommitTimestamp ≤ T. Snapshot reads on each key
    /// return WaitingForReplication and back off until a concurrent committer resolves
    /// both intents ~50 ms in. Both reads return Get + new values — no partial view.
    ///
    /// Cross-partition placement is enforced by generating random keys until two land
    /// on different hash-pool partitions (DataPartitionRouter.Locate logic).
    /// </summary>
    [Fact]
    public async Task TwoKeyTx_CrossPartition_SeenAllOrNothing()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        const int poolSize = 4; // matches AssembleThreNodeCluster("memory", 4, ...)

        // Find two keys guaranteed to land on different partitions.
        string key1 = "sst:d:" + Guid.NewGuid().ToString("N")[..8];
        string key2 = "sst:d:" + Guid.NewGuid().ToString("N")[..8];
        while (true)
        {
            int p1 = 1 + (int)HashUtils.InversePrefixedHash(key1, '/', poolSize);
            int p2 = 1 + (int)HashUtils.InversePrefixedHash(key2, '/', poolSize);
            if (p1 != p2)
                break;
            key2 = "sst:d:" + Guid.NewGuid().ToString("N")[..8];
        }

        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster("memory", poolSize, raftLogger, kahunaLogger);

        try
        {
            byte[] valA = "before"u8.ToArray();
            byte[] valB = "after"u8.ToArray();

            // Commit valA for both keys.
            (KeyValueResponseType setA1, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key1, valA, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, setA1);

            (KeyValueResponseType setA2, _, _) = await kahuna1.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key2, valA, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, setA2);

            // Open a 2PC transaction and stage valB on both keys.
            (KeyValueResponseType startType, HLCTimestamp txId) = await kahuna1.LocateAndStartTransaction(
                new() { UniqueId = Guid.NewGuid().ToString(), Locking = KeyValueTransactionLocking.Pessimistic }, ct);
            Assert.Equal(KeyValueResponseType.Set, startType);

            (KeyValueResponseType setB1, _, _) = await kahuna2.LocateAndTrySetKeyValue(
                txId, key1, valB, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, setB1);

            (KeyValueResponseType setB2, _, _) = await kahuna2.LocateAndTrySetKeyValue(
                txId, key2, valB, null, -1, KeyValueFlags.Set, 0,
                KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Set, setB2);

            // Prepare both intents — CommitTimestamp is stamped at TrySet time (mvccEntry.LastModified).
            HLCTimestamp commitId = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());
            (KeyValueResponseType prep1, HLCTimestamp ticket1, _, _) = await kahuna3.LocateAndTryPrepareMutations(
                txId, commitId, key1, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Prepared, prep1);

            (KeyValueResponseType prep2, HLCTimestamp ticket2, _, _) = await kahuna3.LocateAndTryPrepareMutations(
                txId, commitId, key2, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Prepared, prep2);

            // Advance wall clock past both CommitTimestamps, then capture T > both.
            await Task.Delay(10, ct);
            HLCTimestamp T = node1.HybridLogicalClock.TrySendOrLocalEvent(node1.GetLocalNodeId());

            // Commit both prepared intents after 50 ms — inside the reads' back-off window.
            Task commitTask = Task.Run(async () =>
            {
                await Task.Delay(50, ct);
                await kahuna1.LocateAndTryCommitMutations(txId, key1, ticket1, KeyValueDurability.Persistent, ct);
                await kahuna1.LocateAndTryCommitMutations(txId, key2, ticket2, KeyValueDurability.Persistent, ct);
            }, ct);

            // Both reads at T: each blocks (WaitingForReplication back-off) until its intent
            // resolves, then returns Get + valB. Run concurrently to stress the all-or-nothing
            // invariant — neither read should see "before" while the other sees "after".
            Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> readTask1 = kahuna2.LocateAndTryGetValue(
                HLCTimestamp.Zero, key1, -1, T, KeyValueDurability.Persistent, ct);
            Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> readTask2 = kahuna3.LocateAndTryGetValue(
                HLCTimestamp.Zero, key2, -1, T, KeyValueDurability.Persistent, ct);

            (KeyValueResponseType r1, ReadOnlyKeyValueEntry? snap1) = await readTask1;
            (KeyValueResponseType r2, ReadOnlyKeyValueEntry? snap2) = await readTask2;
            await commitTask;

            Assert.Equal(KeyValueResponseType.Get, r1);
            Assert.NotNull(snap1);
            Assert.Equal("after", Encoding.UTF8.GetString(snap1.Value!));

            Assert.Equal(KeyValueResponseType.Get, r2);
            Assert.NotNull(snap2);
            Assert.Equal("after", Encoding.UTF8.GetString(snap2.Value!));
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
