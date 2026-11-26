using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

// The write scheduler's counters are on a static meter that every node of the process shares, so a test that reads an
// exact total runs alone.
[CollectionDefinition("WriteSchedulerMetrics", DisableParallelization = true)]
public sealed class WriteSchedulerMetricsCollection { }

/// <summary>
/// What the write scheduler counts for a run of merged settles: one entry of the stage <c>settle</c> for the run, one
/// sample of the settle runs for the batch, and each member on the counter of merged settles.
/// </summary>
[Collection("WriteSchedulerMetrics")]
public sealed class TestMergedSettleMetrics
{
    /// <summary>Commits every entry of a batch; holds the first batch until the test releases it.</summary>
    private sealed class HoldingExecutor : IPartitionBatchExecutor
    {
        public readonly TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int calls;

        public int Calls => Volatile.Read(ref calls);

        public async Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref calls) == 1)
                await Release.Task.WaitAsync(cancellationToken);

            List<RaftEntryResult> results = new(entries.Count);
            for (int i = 0; i < entries.Count; i++)
                results.Add(new RaftEntryResult(RaftOperationStatus.Success, 100 + i, HLCTimestamp.Zero));

            return new RaftBatchReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, results);
        }
    }

    private static DurableProposalSubmission Settle(long transaction)
    {
        HLCTimestamp id = new(0, transaction, 0);
        byte[] delta = PreparedIntentStore.SerializeDelta([
            new ResolveIntentCommand(id, 1, "metric/" + transaction, Commit: true),
            new RemoveIntentCommand(id, 1, "metric/" + transaction)]);

        return new DurableProposalSubmission(
            9,
            [new RaftProposalEntry(ReplicationTypes.PreparedIntent, delta, AutoCommit: true, ExpectedGeneration: 0)],
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously),
            WriteAdmissionClass.Terminal,
            WriteSubmissionStage.Settle);
    }

    [Fact]
    public async Task ARunOfMergedSettles_CountsOneSettleEntry_AndEachMember()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        using MetricCapture metrics = new("stage", "kahuna.kv.write.stage_entries", "kahuna.kv.write.settles_merged", "kahuna.kv.write.settle_runs");
        using ActorSystem actorSystem = new();
        HoldingExecutor executor = new();

        PartitionWriteAggregator aggregator = new(actorSystem, executor, new PartitionWriteAggregatorOptions
        {
            MaxBatchItems = 64,
            LingerMs = 0,
            // The batch completion is a priority message, so the lane can read it before the queued submissions. The
            // hold after the completion keeps them together until the lane has read them all.
            PostCompletionHoldMs = 100,
            MaxQueueDelayMs = 10_000,
            MergeAdjacentSettles = true,
            MergedSettleAwaiter = static (_, _, _) => Task.FromResult(DurableCompletionAnswer.Acknowledged)
        }, new RangeMapWriteFenceStub(), NullLogger<IKahuna>.Instance);

        try
        {
            // The first settle dispatches alone and holds the lane; the next four queue behind it and leave together.
            DurableProposalSubmission first = Settle(1);
            Assert.True(aggregator.TryEnqueue(first));
            long deadline = Environment.TickCount64 + 5_000;
            while (executor.Calls == 0 && Environment.TickCount64 < deadline)
                await Task.Delay(5, ct);

            DurableProposalSubmission[] run = [Settle(2), Settle(3), Settle(4), Settle(5)];
            foreach (DurableProposalSubmission settle in run)
                Assert.True(aggregator.TryEnqueue(settle));

            executor.Release.SetResult();

            Assert.True(await first.Committed.WaitAsync(TimeSpan.FromSeconds(5), ct));
            foreach (DurableProposalSubmission settle in run)
                Assert.True(await settle.Committed.WaitAsync(TimeSpan.FromSeconds(5), ct));

            // Two entries of the stage settle: the first settle, and the merged entry of the four others.
            Assert.Equal(2, metrics.Total("kahuna.kv.write.stage_entries", "settle"));
            Assert.Equal(4, metrics.Total("kahuna.kv.write.settles_merged"));
            Assert.Equal([1d, 1d], metrics.Samples("kahuna.kv.write.settle_runs"));
        }
        finally
        {
            aggregator.Stop();
            await actorSystem.GracefulShutdownAll(TimeSpan.FromSeconds(5));
        }
    }

    private sealed class RangeMapWriteFenceStub : IWriteRangeFence
    {
        public bool IsStale(string key, long admittedGeneration, int admittedPartitionId) => false;

        public bool IsQuiesced(string key) => false;
    }
}
