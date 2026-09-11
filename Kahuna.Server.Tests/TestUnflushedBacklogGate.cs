using System.Collections.Concurrent;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Persistence;
using Kahuna.Server.Replication;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Every admitted write is replicated and then held in memory on every replica until the background
/// flush lands it. Nothing bounded that population: a flusher slower than ingest let it grow by the
/// ingest rate until the heap limit killed the replica. The aggregator now consults the node's
/// unflushed-backlog probe before admitting an ordinary write and refuses retryably while the
/// backlog is over budget; terminal work (decisions/settlement) still passes so prepared
/// transactions can finish and release what they hold.
/// </summary>
public sealed class TestUnflushedBacklogGate
{
    private sealed class StubFence : IWriteRangeFence
    {
        public bool IsStale(string key, long admittedGeneration, int admittedPartitionId) => false;
    }

    private sealed class SucceedingExecutor : IPartitionBatchExecutor
    {
        public int Calls;

        public Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Calls);
            List<RaftEntryResult> results = new(entries.Count);
            for (int i = 0; i < entries.Count; i++)
                results.Add(new RaftEntryResult(RaftOperationStatus.Success, i, HLCTimestamp.Zero));
            return Task.FromResult(new RaftBatchReplicationResult(true, RaftOperationStatus.Success, HLCTimestamp.Zero, results));
        }
    }

    private sealed class Submission(int partitionId, int proposalId, WriteAdmissionClass admissionClass) : IProposalSubmission
    {
        public readonly TaskCompletionSource<bool> Done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int ProposalId { get; } = proposalId;
        public int PartitionId { get; } = partitionId;
        public WriteAdmissionClass AdmissionClass { get; } = admissionClass;
        public WriteSubmissionStage Stage => WriteSubmissionStage.Other;
        public int ByteLength => 8;
        public IReadOnlyList<RaftProposalEntry> Entries { get; } = [new RaftProposalEntry(ReplicationTypes.KeyValues, new byte[8], AutoCommit: true, ExpectedGeneration: 0)];
        public long EnqueueTicks { get; set; }
        public bool IsStale(IWriteRangeFence fence) => false;
        public void Complete(IReadOnlyList<long>? entryLogIndices) => Done.TrySetResult(true);
        public void Release(bool transient) => Done.TrySetResult(false);
    }

    [Fact]
    public async Task OrdinaryWritesAreRefusedWhileOverBudget_TerminalPasses_AdmissionResumesWhenItClears()
    {
        bool overBudget = false;
        SucceedingExecutor executor = new();
        ActorSystem actorSystem = new();

        PartitionWriteAggregator aggregator = new(
            actorSystem,
            executor,
            new PartitionWriteAggregatorOptions { LingerMs = 0, UnflushedBacklogGate = () => overBudget },
            new StubFence(),
            NullLogger<IKahuna>.Instance);

        try
        {
            Submission before = new(1, 1, WriteAdmissionClass.Ordinary);
            Assert.True(aggregator.TryEnqueue(before));
            Assert.True(await before.Done.Task.WaitAsync(TimeSpan.FromSeconds(5)));

            overBudget = true;

            Submission refused = new(1, 2, WriteAdmissionClass.Ordinary);
            Assert.False(aggregator.TryEnqueue(refused));
            Assert.False(refused.Done.Task.IsCompleted);
            Assert.Equal(0, aggregator.ReservedItems(1));

            Submission terminal = new(1, 3, WriteAdmissionClass.Terminal);
            Assert.True(aggregator.TryEnqueue(terminal));
            Assert.True(await terminal.Done.Task.WaitAsync(TimeSpan.FromSeconds(5)));

            overBudget = false;

            Submission after = new(1, 4, WriteAdmissionClass.Ordinary);
            Assert.True(aggregator.TryEnqueue(after));
            Assert.True(await after.Done.Task.WaitAsync(TimeSpan.FromSeconds(5)));

            Assert.Equal(3, executor.Calls);
        }
        finally
        {
            aggregator.Stop();
            await actorSystem.GracefulShutdownAll(TimeSpan.FromSeconds(5));
            actorSystem.Dispose();
        }
    }

    [Fact]
    public void BacklogMonitor_ReportsInboxPlusQueuesAndAppliesBothBounds()
    {
        KahunaConfiguration config = ConfigurationValidator.Validate(new()
        {
            Storage = "memory",
            PersistenceMaxUnflushedItems = 5,
            PersistenceMaxUnflushedBytes = 0
        });

        Assert.Equal(5, config.PersistenceMaxUnflushedItems);
        Assert.Equal(0, config.PersistenceMaxUnflushedBytes);

        KahunaConfiguration defaults = ConfigurationValidator.Validate(new());
        Assert.Equal(1_000_000, defaults.PersistenceMaxUnflushedItems);
        Assert.Equal(512L * 1024 * 1024, defaults.PersistenceMaxUnflushedBytes);
        Assert.Equal(TimeSpan.FromMilliseconds(250), defaults.PersistentRevisionCleanupTimeBudget);

        KahunaConfiguration clamped = ConfigurationValidator.Validate(new()
        {
            PersistenceMaxUnflushedItems = -1,
            PersistenceMaxUnflushedBytes = -1,
            PersistentRevisionCleanupTimeBudget = TimeSpan.Zero
        });
        Assert.Equal(0, clamped.PersistenceMaxUnflushedItems);
        Assert.Equal(0, clamped.PersistenceMaxUnflushedBytes);
        Assert.Equal(TimeSpan.FromMilliseconds(250), clamped.PersistentRevisionCleanupTimeBudget);
    }
}
