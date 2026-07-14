
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Acceptance tests for a real coordinator process loss: a durable decision plus the participant completion
/// receipts that authorize its acknowledgements are written through the persistent WAL and the on-disk decision
/// snapshot, so a node disposed and reconstructed over the same storage/WAL paths reloads the outstanding
/// decision and the per-partition-leader recovery driver completes it. Also covers concurrent durable decisions
/// anchored in distinct data partitions advancing independently, and that a best-effort transaction writes no
/// decision to recover.
///
/// <para>Persistence is what a restart proves, so these run on a single persistent node whose dispose reloads
/// from disk; the multi-node replication of the initial record is covered separately. The headline test relies
/// on WAL-tail replay for the receipt that resolves a committed participant, so it deliberately does not force a
/// backend checkpoint that could compact the receipt-bearing log entry — receipt durability across a compaction
/// event is a separate state-transfer concern.</para>
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDurablePersistentRestart
{
    private static readonly TimeSpan NoPurge = TimeSpan.FromHours(1);

    private static EmbeddedKahunaOptions PersistentOptions(string storagePath, string walPath, int partitions = 1) => new()
    {
        Storage         = "sqlite",
        StoragePath     = storagePath,
        StorageRevision = "durable-restart",
        WalStorage      = "sqlite",
        WalPath         = walPath,
        WalRevision     = "durable-restart-wal",
        WalSyncWrites   = false,
        InitialPartitions = partitions,
    };

    private static KahunaManager Manager(EmbeddedKahunaNode node) => (KahunaManager)node.Kahuna;

    private static CoordinatorDecisionStore Store(EmbeddedKahunaNode node) => Manager(node).CoordinatorDecisionStore;

    private static Task Recover(EmbeddedKahunaNode node, TimeSpan ttl) =>
        Manager(node).RecoverOutstandingDecisions(ttl, TestContext.Current.CancellationToken);

    private static CoordinatorParticipant Participant(string key, bool acked, bool released) =>
        new(key, KeyValueDurability.Persistent, HLCTimestamp.Zero, acked, released);

    private static async Task<(KeyValueResponseType, string?)> Read(EmbeddedKahunaNode node, string key)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (KeyValueResponseType r, ReadOnlyKeyValueEntry? e) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        return (r, e?.Value is null ? null : System.Text.Encoding.UTF8.GetString(e.Value));
    }

    private static async Task WaitUntil(Func<Task<bool>> predicate, int timeoutMs = 15_000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (await predicate())
                return;
            await Task.Delay(50, ct);
        }
        throw new TimeoutException("Timed out waiting for condition.");
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
    }

    // ── A durable decision + its receipts survive a coordinator process loss; recovery then completes it ───

    [Fact]
    public async Task DurableDecision_SurvivesCoordinatorProcessLoss_RecoveryCompletes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string storagePath = Path.Combine(Path.GetTempPath(), "kahuna-durrestart-" + Guid.NewGuid().ToString("N"));
        string walPath = Path.Combine(Path.GetTempPath(), "kahuna-durrestart-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(storagePath);
        Directory.CreateDirectory(walPath);

        try
        {
            const string anchor = "durrestart:anchor";
            const string other  = "durrestart:other";
            byte[] value = "committed"u8.ToArray();
            HLCTimestamp txId;

            await using (EmbeddedKahunaNode first = new(PersistentOptions(storagePath, walPath)))
            {
                await first.StartAsync(ct);

                int nodeId = first.Raft.GetLocalNodeId();
                txId = first.Raft.HybridLogicalClock.TrySendOrLocalEvent(nodeId);
                HLCTimestamp commitId = first.Raft.HybridLogicalClock.TrySendOrLocalEvent(nodeId);

                // Commit `other` through 2PC so a durable completion receipt is written — the proof a recovery
                // re-drive uses to resolve the participant as committed after the coordinator is gone.
                (KeyValueResponseType setType, _, _) = await first.Kahuna.TrySetKeyValue(
                    txId, other, value, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent);
                Assert.Equal(KeyValueResponseType.Set, setType);
                (KeyValueResponseType prepType, HLCTimestamp ticket, _, _) = await first.Kahuna.TryPrepareMutations(
                    txId, commitId, other, KeyValueDurability.Persistent, recordAnchorKey: anchor);
                Assert.Equal(KeyValueResponseType.Prepared, prepType);
                (KeyValueResponseType commitType, _) = await first.Kahuna.TryCommitMutations(
                    txId, other, ticket, KeyValueDurability.Persistent);
                Assert.Equal(KeyValueResponseType.Committed, commitType);

                // The coordinator committed the anchor and installed the decision, but died before acknowledging
                // `other`: the record is outstanding with the anchor acked and `other` still pending.
                CoordinatorDecisionRecord seed = new(
                    txId, "lost-coord", anchor, commitId, CoordinatorDecisionStatus.CommitDecided,
                    [Participant(anchor, acked: true, released: false), Participant(other, acked: false, released: false)],
                    [], txId, HLCTimestamp.Zero);
                await first.Kahuna.ImportCoordinatorDecisions([seed]);

                Assert.True(Store(first).TryGet(txId, out _));
            }

            // Reconstruct the lost process over the same durable WAL + storage + decision snapshot.
            await using EmbeddedKahunaNode second = new(PersistentOptions(storagePath, walPath));
            await second.StartAsync(ct);

            // The decision record reloaded from the on-disk snapshot — a fresh node with no persistence would
            // hold nothing to recover.
            Assert.True(Store(second).TryGet(txId, out _), "decision record did not survive the restart");

            // The committed participant value and its receipt come back via WAL-tail replay.
            await WaitUntil(async () => (await Read(second, other)).Item1 == KeyValueResponseType.Get);
            Assert.Equal("committed", (await Read(second, other)).Item2);

            // Recovery re-drives the outstanding participant to acknowledged and marks the decision Completed.
            await WaitUntil(async () =>
            {
                await Recover(second, NoPurge);
                return Store(second).TryGet(txId, out CoordinatorDecisionRecord r) &&
                       r.Status == CoordinatorDecisionStatus.Completed;
            });

            Assert.True(Store(second).TryGet(txId, out CoordinatorDecisionRecord rec));
            Assert.Equal(CoordinatorDecisionStatus.Completed, rec.Status);
            Assert.True(rec.AllParticipantsAcked);
            Assert.All(rec.Participants, p => Assert.True(p.ReceiptReleased));
            Assert.Equal("committed", (await Read(second, other)).Item2);
        }
        finally
        {
            TryDeleteDirectory(storagePath);
            TryDeleteDirectory(walPath);
        }
    }

    // ── Two durable decisions anchored in distinct data partitions each complete in one recovery sweep ─────

    [Fact]
    public async Task ConcurrentDurableDecisions_TwoPartitions_ProgressIndependently()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string storagePath = Path.Combine(Path.GetTempPath(), "kahuna-durrestart-" + Guid.NewGuid().ToString("N"));
        string walPath = Path.Combine(Path.GetTempPath(), "kahuna-durrestart-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(storagePath);
        Directory.CreateDirectory(walPath);

        try
        {
            await using EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath, partitions: 2));
            await node.StartAsync(ct);

            // Pick two anchor keys that route to two different data partitions, so the two decisions replicate
            // and are driven on independent partition streams.
            (string anchorA, string anchorB) = TwoAnchorsInDistinctPartitions(node);

            HLCTimestamp txA = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
            HLCTimestamp txB = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

            CoordinatorDecisionRecord seedA = new(
                txA, "coord-a", anchorA, txA, CoordinatorDecisionStatus.CommitDecided,
                [Participant(anchorA, acked: true, released: false)], [], txA, HLCTimestamp.Zero);
            CoordinatorDecisionRecord seedB = new(
                txB, "coord-b", anchorB, txB, CoordinatorDecisionStatus.CommitDecided,
                [Participant(anchorB, acked: true, released: false)], [], txB, HLCTimestamp.Zero);
            await node.Kahuna.ImportCoordinatorDecisions([seedA, seedB]);

            await Recover(node, NoPurge);

            Assert.True(Store(node).TryGet(txA, out CoordinatorDecisionRecord recA));
            Assert.True(Store(node).TryGet(txB, out CoordinatorDecisionRecord recB));
            Assert.Equal(CoordinatorDecisionStatus.Completed, recA.Status);
            Assert.Equal(CoordinatorDecisionStatus.Completed, recB.Status);
            Assert.NotEqual(
                Manager(node).GetDataPartitionForKey(anchorA),
                Manager(node).GetDataPartitionForKey(anchorB));
        }
        finally
        {
            TryDeleteDirectory(storagePath);
            TryDeleteDirectory(walPath);
        }
    }

    private static (string, string) TwoAnchorsInDistinctPartitions(EmbeddedKahunaNode node)
    {
        string anchorA = "durrestart-p:aaa";
        int partitionA = Manager(node).GetDataPartitionForKey(anchorA);
        for (int i = 0; i < 4096; i++)
        {
            string candidate = "durrestart-p:b" + i;
            if (Manager(node).GetDataPartitionForKey(candidate) != partitionA)
                return (anchorA, candidate);
        }
        throw new InvalidOperationException("Could not find two anchor keys in distinct data partitions.");
    }

    // ── A best-effort transaction writes no decision to recover, before and after a restart ────────────────

    [Fact]
    public async Task BestEffortTransaction_WritesNoDecisionToRecover()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string storagePath = Path.Combine(Path.GetTempPath(), "kahuna-durrestart-" + Guid.NewGuid().ToString("N"));
        string walPath = Path.Combine(Path.GetTempPath(), "kahuna-durrestart-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(storagePath);
        Directory.CreateDirectory(walPath);

        try
        {
            const string keyA = "besteffort:aaa";
            const string keyB = "besteffort:bbb";

            await using (EmbeddedKahunaNode first = new(PersistentOptions(storagePath, walPath)))
            {
                await first.StartAsync(ct);

                (KeyValueResponseType startType, TransactionHandle handle) = await first.Kahuna.LocateAndStartTransaction(
                    new KeyValueTransactionOptions
                    {
                        CoordinatorKey     = "besteffort-coord",
                        Locking            = KeyValueTransactionLocking.Pessimistic,
                        DecisionDurability = DecisionDurability.BestEffort,
                        AsyncRelease       = false,
                        Timeout            = 10_000,
                    }, ct);
                Assert.Equal(KeyValueResponseType.Set, startType);

                foreach (string key in new[] { keyA, keyB })
                {
                    (KeyValueResponseType s, _, _) = await first.Kahuna.LocateAndTrySetKeyValue(
                        handle.TransactionId, key, "v"u8.ToArray(), null, -1, KeyValueFlags.None, 0,
                        KeyValueDurability.Persistent, ct, coordinatorKey: handle.CoordinatorKey,
                        operationId: TransactionOperationId.NewRandom());
                    Assert.Equal(KeyValueResponseType.Set, s);
                }

                (KeyValueResponseType commitResult, _) = await first.Kahuna.LocateAndCommitTransaction(handle, ct);
                Assert.Equal(KeyValueResponseType.Committed, commitResult);

                // A best-effort commit takes the prior in-memory path — no durable decision is written.
                Assert.Equal(0, Store(first).Count);
            }

            // Nothing to reload or recover after a restart.
            await using EmbeddedKahunaNode second = new(PersistentOptions(storagePath, walPath));
            await second.StartAsync(ct);

            await WaitUntil(async () => (await Read(second, keyA)).Item1 == KeyValueResponseType.Get);
            Assert.Equal(0, Store(second).Count);
        }
        finally
        {
            TryDeleteDirectory(storagePath);
            TryDeleteDirectory(walPath);
        }
    }

    // ── A durable transaction with an ephemeral modified key is rejected before prepare ────────────────────

    [Fact]
    public async Task DurableTransaction_WithEphemeralModifiedKey_RejectedBeforePrepare()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string storagePath = Path.Combine(Path.GetTempPath(), "kahuna-durrestart-" + Guid.NewGuid().ToString("N"));
        string walPath = Path.Combine(Path.GetTempPath(), "kahuna-durrestart-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(storagePath);
        Directory.CreateDirectory(walPath);

        try
        {
            await using EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath));
            await node.StartAsync(ct);

            (KeyValueResponseType startType, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
                new KeyValueTransactionOptions
                {
                    CoordinatorKey     = "durable-eph-coord",
                    Locking            = KeyValueTransactionLocking.Pessimistic,
                    DecisionDurability = DecisionDurability.Durable,
                    AsyncRelease       = false,
                    Timeout            = 10_000,
                }, ct);
            Assert.Equal(KeyValueResponseType.Set, startType);
            HLCTimestamp txId = handle.TransactionId;

            (KeyValueResponseType sp, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                txId, "durable-eph:persistent", "x"u8.ToArray(), null, -1, KeyValueFlags.None, 0,
                KeyValueDurability.Persistent, ct, coordinatorKey: handle.CoordinatorKey,
                operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Set, sp);
            (KeyValueResponseType se, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                txId, "durable-eph:ephemeral", "x"u8.ToArray(), null, -1, KeyValueFlags.None, 0,
                KeyValueDurability.Ephemeral, ct, coordinatorKey: handle.CoordinatorKey,
                operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Set, se);

            (KeyValueResponseType commitResult, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);
            Assert.Equal(KeyValueResponseType.Aborted, commitResult);

            // Rejected before prepare: no decision record was written for it.
            Assert.False(Store(node).TryGet(txId, out _));
        }
        finally
        {
            TryDeleteDirectory(storagePath);
            TryDeleteDirectory(walPath);
        }
    }
}
