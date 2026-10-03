using System.Diagnostics;
using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A durable entry (transaction record, prepared intent) is applied once, by the ordered consumer apply that Raft
/// drives through <c>OnReplicationReceived</c>; the write scheduler's completion for a locally proposed entry
/// waits on the apply ledger for the result that apply records. Node disposal drains the write aggregator so
/// in-flight batches settle before Raft and the actors go away — and that drain is only meaningful while the
/// replication callback is still attached. Detaching it first left every batch in flight at dispose waiting for
/// an apply that could never arrive, so the completion parked until the drain deadline (5 s) cancelled it, and
/// a process that disposes many short-lived nodes (a test suite) paid the whole drain budget on a few percent
/// of its disposes.
/// </summary>
public sealed class TestEmbeddedDisposeDrainsInFlightDurableWrites
{
    private readonly ILoggerFactory loggerFactory;

    private readonly ITestOutputHelper output;

    public TestEmbeddedDisposeDrainsInFlightDurableWrites(ITestOutputHelper outputHelper)
    {
        output = outputHelper;
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    /// <summary>
    /// Holds the first durable batch it sees after being armed until the test releases it, so the batch is
    /// provably in flight when <see cref="EmbeddedKahunaNode.DisposeAsync"/> begins.
    /// </summary>
    private sealed class HoldingExecutor(IPartitionBatchExecutor real) : IPartitionBatchExecutor
    {
        private int armed;

        public readonly TaskCompletionSource Held = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public readonly TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Arm() => Volatile.Write(ref armed, 1);

        public async Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            bool durable = false;
            foreach (RaftProposalEntry entry in entries)
            {
                if (entry.Type == ReplicationTypes.PreparedIntent || entry.Type == ReplicationTypes.TransactionRecord)
                    durable = true;
            }

            if (durable && Interlocked.CompareExchange(ref armed, 2, 1) == 1)
            {
                Held.TrySetResult();
                await Release.Task.WaitAsync(TimeSpan.FromSeconds(10), cancellationToken);
            }

            return await real.ReplicateAsync(partitionId, entries, cancellationToken);
        }
    }

    [Fact]
    public async Task ADurableBatchInFlightAtDispose_SettlesThroughTheOrderedApply_InsteadOfWaitingOutTheDrainDeadline()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        HoldingExecutor? holder = null;

        EmbeddedKahunaOptions options = new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            WriteBatchExecutorDecorator = real => holder = new HoldingExecutor(real)
        };

        EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("dispose-drain/key", ct);
        Assert.NotNull(holder);

        (KeyValueResponseType startType, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = "dispose-drain/tx",
                Locking = KeyValueTransactionLocking.Pessimistic,
                DecisionDurability = DecisionDurability.Durable,
                Timeout = 10_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        (KeyValueResponseType writeType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            handle.TransactionId, "dispose-drain/key", Encoding.UTF8.GetBytes("v"), null, -1,
            KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
            coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, writeType);

        // The commit proposes the transaction's durable entries (its prepared intent and record) through the
        // aggregator; the holder parks that batch inside the executor, where the aggregator counts it as in
        // flight.
        holder.Arm();

        Task<(KeyValueResponseType, string?)> commit = node.Kahuna.LocateAndCommitTransaction(handle, ct);

        await holder.Held.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);

        // Dispose with the batch in flight, then let the batch reach Raft. The dispose must observe the batch's
        // ordered apply and finish promptly; before the callbacks were kept attached through the drain it waited
        // the full 5 s drain budget and answered the producer "unobserved".
        Stopwatch disposeClock = Stopwatch.StartNew();
        Task dispose = node.DisposeAsync().AsTask();
        holder.Release.TrySetResult();

        await dispose.WaitAsync(TimeSpan.FromSeconds(20), ct);
        disposeClock.Stop();
        output.WriteLine($"dispose took {disposeClock.ElapsedMilliseconds} ms; commit task completed={commit.IsCompleted}");

        Assert.True(
            disposeClock.ElapsedMilliseconds < 3000,
            $"dispose took {disposeClock.ElapsedMilliseconds} ms: the in-flight durable batch waited out the drain deadline instead of settling through its ordered apply");

        // The producer's completion resolved as well, with a definite answer, since its apply was observed.
        (KeyValueResponseType commitType, _) = await commit.WaitAsync(TimeSpan.FromSeconds(10), ct);
        Assert.Equal(KeyValueResponseType.Committed, commitType);
    }
}
