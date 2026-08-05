using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A committed transaction must always answer Committed — even when an earlier, discarded finalize attempt
/// failed validation. The one-phase fast path validates the read set BEFORE proposing anything durable; when
/// its write-skew probe finds a concurrent intent on a read key it records a conflict abort and falls back to
/// the standard flow. The standard flow re-validates after prepare, and if the conflicting intent's owner
/// resolved in that window, validation passes and the transaction durably commits — its write materializes and
/// is visible to every reader. Reporting the stale conflict abort from the abandoned first attempt would tell
/// the client a committed write had no effect: an aborted-read (G1a) anomaly for any reader that observes it,
/// and a dirty update once someone read-modify-writes on top.
/// </summary>
public sealed class TestOnePhaseFallbackCommitAnswer
{
    private readonly ILoggerFactory loggerFactory;

    public TestOnePhaseFallbackCommitAnswer(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    /// <summary>
    /// One-shot synchronization point on the write path: when armed, the first batch carrying a
    /// transaction-record entry (the fallback flow's record init) signals arrival and parks until released,
    /// giving the test a deterministic window between the two validation passes to resolve the conflicting
    /// transaction. Everything else passes straight through.
    /// </summary>
    private sealed class ParkingExecutor : IPartitionBatchExecutor
    {
        private readonly IPartitionBatchExecutor inner;
        private TaskCompletionSource? arrived;
        private TaskCompletionSource? release;

        public ParkingExecutor(IPartitionBatchExecutor inner) => this.inner = inner;

        public (Task Arrived, TaskCompletionSource Release) ArmOneShot()
        {
            arrived = new(TaskCreationOptions.RunContinuationsAsynchronously);
            release = new(TaskCreationOptions.RunContinuationsAsynchronously);
            return (arrived.Task, release);
        }

        public async Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            TaskCompletionSource? a = arrived;
            TaskCompletionSource? r = release;
            if (a is not null && r is not null && entries.Any(static e => e.Type == ReplicationTypes.TransactionRecord))
            {
                arrived = null;
                release = null;
                a.TrySetResult();
                await r.Task;
            }

            return await inner.ReplicateAsync(partitionId, entries, cancellationToken);
        }
    }

    [Fact]
    public async Task CommitThatFellBackAfterAResolvedConflict_AnswersCommitted_NotTheStaleAbort()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const string writeKey = "fallback/write/k1";

        ParkingExecutor? parking = null;
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 2,
            WriteIOThreads = 2,
            PartitionExecutorPoolSize = 4,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            // Synchronous settlement materializes the committed value before the commit returns, so
            // post-commit visibility is deterministic to assert.
            DurableDeferredSettlement = false,
            WriteBatchExecutorDecorator = inner => parking = new ParkingExecutor(inner)
        }, loggerFactory);
        await using EmbeddedKahunaNode ownedNode = node;
        await node.StartAsync(ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        await node.WaitForLeaderForKeyAsync(writeKey, ct);

        // Pick a read key on a different partition than the write key, so the conflicting transaction's
        // rollback never queues behind the parked record batch of the same partition.
        string readKey = "abcdefghijklmnopqrstuvwxyz".Select(static c => $"{c}{c}{c}/fallback-read/k1")
            .First(k => kahuna.GetDataPartitionForKey(k) != kahuna.GetDataPartitionForKey(writeKey));
        await node.WaitForLeaderForKeyAsync(readKey, ct);

        (KeyValueResponseType seeded, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, readKey, "base"u8.ToArray(), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Set, seeded);

        // Transaction A: optimistic (read-set validated at commit), reads readKey, writes writeKey.
        (KeyValueResponseType startedA, TransactionHandle handleA) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                Locking = KeyValueTransactionLocking.Optimistic,
                DecisionDurability = DecisionDurability.Durable,
                Timeout = 10_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startedA);

        (KeyValueResponseType readA, _) = await node.Kahuna.LocateAndTryGetValue(
            handleA.TransactionId, readKey, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct,
            handleA.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Get, readA);

        (KeyValueResponseType setA, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            handleA.TransactionId, writeKey, "value-a"u8.ToArray(), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct, 0, handleA.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, setA);

        // Transaction B: places a write intent on A's read key, so A's one-phase pre-propose validation
        // finds a concurrent writer and falls back to the standard flow.
        (KeyValueResponseType startedB, TransactionHandle handleB) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                Locking = KeyValueTransactionLocking.Pessimistic,
                DecisionDurability = DecisionDurability.Durable,
                Timeout = 10_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startedB);

        (KeyValueResponseType setB, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            handleB.TransactionId, readKey, "value-b"u8.ToArray(), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct, 0, handleB.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, setB);

        // Commit A. The one-phase probe sees B's intent on readKey (conflict → fallback, nothing durable
        // yet); the fallback's record init is the first record batch and parks at the executor.
        (Task arrived, TaskCompletionSource release) = parking!.ArmOneShot();
        Task<(KeyValueResponseType, string?)> commitA = kahuna.LocateAndCommitTransaction(handleA, ct);

        await arrived.WaitAsync(TimeSpan.FromSeconds(30), ct);

        // The conflicting writer resolves inside the fallback window — exactly the interleaving sustained
        // load produces — so the standard flow's re-validation passes and the transaction commits.
        KeyValueResponseType rolledBackB = await kahuna.LocateAndRollbackTransaction(handleB, ct);
        Assert.Equal(KeyValueResponseType.RolledBack, rolledBackB);

        release.TrySetResult();

        (KeyValueResponseType commitAnswer, _) = await commitA;

        // The canonical record is durably Commit and the write is visible — the answer must say so.
        TransactionRecord? record = kahuna.DurableTransactionRecordStore.Get(handleA.TransactionId, 1);
        Assert.NotNull(record);
        Assert.Equal(TransactionDecision.Commit, record!.Decision);
        Assert.Equal(KeyValueResponseType.Committed, commitAnswer);

        // A duplicate commit replays the same truthful outcome.
        (KeyValueResponseType replay, _) = await kahuna.LocateAndCommitTransaction(handleA, ct);
        Assert.Equal(KeyValueResponseType.Committed, replay);

        (KeyValueResponseType readW, ReadOnlyKeyValueEntry? entryW) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, writeKey, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, readW);
        Assert.NotNull(entryW?.Value);
        Assert.True(entryW!.Value.AsSpan().SequenceEqual("value-a"u8), "the committed write must be visible");

        // And the rolled-back writer's value never surfaced on the read key.
        (KeyValueResponseType readR, ReadOnlyKeyValueEntry? entryR) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, readKey, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, readR);
        Assert.NotNull(entryR?.Value);
        Assert.True(entryR!.Value.AsSpan().SequenceEqual("base"u8), "the rolled-back write must not be visible");
    }
}
