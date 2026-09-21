using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Replication;
using Kommander.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The range-map store's answer when a proposal ends without a verdict. Kommander reports
/// <see cref="RaftOperationStatus.ProposalOutcomeUnknown"/> when the meta leader steps down with the
/// entry in flight; the entry may still commit through the next leader's barrier, so the store must
/// settle the outcome against the committed map instead of reporting a definite failure.
/// </summary>
public sealed class TestRangeMapStoreProposeOutcome
{
    private static readonly TimeSpan SettleBudget = TimeSpan.FromMilliseconds(300);

    private static RangeMapStore NewStore(ProposeOutcomeStubRaft raft) =>
        new(raft, null, null, NullLogger<IKahuna>.Instance, checkpointEveryMutations: 0, indeterminateOutcomeBudget: SettleBudget);

    private static RangeDescriptor FullRange(string keySpace, int partitionId) => new()
    {
        KeySpace = keySpace,
        StartKey = null,
        EndKey = null,
        PartitionId = partitionId,
        Generation = 1
    };

    /// <summary>Applies <paramref name="payload"/> the way the leader echo / follower apply path does.</summary>
    private static void ApplyCommitted(RangeMapStore store, byte[] payload, long logId)
    {
        Assert.True(store.Replicate(RangeMapStore.MetaPartitionId, new RaftLog
        {
            Id = logId,
            Term = 2,
            Type = RaftLogType.Committed,
            LogType = ReplicationTypes.RangeMap,
            LogData = payload
        }));
    }

    [Fact]
    public async Task UnknownOutcome_EntryCommittedByNextLeader_ReportsCommittedWithoutReproposing()
    {
        ProposeOutcomeStubRaft raft = new();
        raft.ProposeStatuses.Enqueue(RaftOperationStatus.ProposalOutcomeUnknown);
        raft.ConfirmVerdicts.Enqueue(true);

        using RangeMapStore store = NewStore(raft);

        // The inherited entry commits through the next leader's barrier and reaches this node
        // through the apply path before the confirmed read answers.
        raft.BeforeConfirm = _ =>
        {
            ApplyCommitted(store, raft.ProposedPayloads[0], logId: 1);
            return ValueTask.CompletedTask;
        };

        int transforms = 0;

        bool committed = await store.MutateAsync(current =>
        {
            transforms++;
            return [.. current, FullRange("ks0", 2)];
        }, TestContext.Current.CancellationToken);

        Assert.True(committed);
        Assert.Equal(1, transforms);
        Assert.Equal(1, raft.ProposeCalls);
        Assert.Equal(1, raft.ConfirmCalls);
        Assert.Equal(2, store.Current.Find("ks0", "x")!.PartitionId);
    }

    [Fact]
    public async Task UnknownOutcome_EntryDropped_ReproposesAgainstFreshMap()
    {
        ProposeOutcomeStubRaft raft = new();
        raft.ProposeStatuses.Enqueue(RaftOperationStatus.ProposalOutcomeUnknown);
        raft.ProposeStatuses.Enqueue(RaftOperationStatus.Success);
        raft.ConfirmVerdicts.Enqueue(true);

        using RangeMapStore store = NewStore(raft);

        // Between the dropped proposal and the confirmed read, another leader committed a
        // different mutation: the re-run transform must see it.
        byte[] foreignCommit = SerializeMap(FullRange("other", 5));
        raft.BeforeConfirm = _ =>
        {
            ApplyCommitted(store, foreignCommit, logId: 1);
            return ValueTask.CompletedTask;
        };

        List<int> observedCounts = [];

        bool committed = await store.MutateAsync(current =>
        {
            observedCounts.Add(current.Count);
            return [.. current, FullRange("ks0", 2)];
        }, TestContext.Current.CancellationToken);

        Assert.True(committed);
        Assert.Equal([0, 1], observedCounts);
        Assert.Equal(2, raft.ProposeCalls);
        Assert.Equal(2, store.Current.Descriptors.Count);
        Assert.Equal(2, store.Current.Find("ks0", "x")!.PartitionId);
        Assert.Equal(5, store.Current.Find("other", "x")!.PartitionId);
    }

    [Fact]
    public async Task UnknownOutcome_PartitionNeverSettles_ReportsUnconfirmedAndLeavesMapUntouched()
    {
        ProposeOutcomeStubRaft raft = new();
        raft.ProposeStatuses.Enqueue(RaftOperationStatus.ProposalOutcomeUnknown);
        raft.ConfirmVerdicts.Enqueue(false);

        using RangeMapStore store = NewStore(raft);
        long versionBefore = store.MapVersion;

        long started = Environment.TickCount64;

        bool committed = await store.MutateAsync(
            current => [.. current, FullRange("ks0", 2)],
            TestContext.Current.CancellationToken);

        long elapsed = Environment.TickCount64 - started;

        Assert.False(committed);
        Assert.Equal(1, raft.ProposeCalls);
        Assert.True(raft.ConfirmCalls > 1, "the settle wait polls the confirmed read until the budget runs out");
        Assert.True(elapsed >= SettleBudget.TotalMilliseconds - 20, $"gave up after {elapsed} ms, before the settle budget");
        Assert.Empty(store.Current.Descriptors);
        Assert.Equal(versionBefore, store.MapVersion);
    }

    [Fact]
    public async Task UnknownOutcome_DroppedOnEveryAttempt_GivesUpAfterBoundedProposals()
    {
        ProposeOutcomeStubRaft raft = new();
        raft.ProposeStatuses.Enqueue(RaftOperationStatus.ProposalOutcomeUnknown);
        raft.ConfirmVerdicts.Enqueue(true);

        using RangeMapStore store = NewStore(raft);

        bool committed = await store.MutateAsync(
            current => [.. current, FullRange("ks0", 2)],
            TestContext.Current.CancellationToken);

        Assert.False(committed);
        Assert.Equal(RangeMapStore.MaxProposeAttempts, raft.ProposeCalls);
        Assert.Empty(store.Current.Descriptors);
    }

    [Fact]
    public async Task ProposalTimeout_IsSettledLikeAnUnknownOutcome()
    {
        ProposeOutcomeStubRaft raft = new();
        raft.ProposeStatuses.Enqueue(RaftOperationStatus.ProposalTimeout);
        raft.ConfirmVerdicts.Enqueue(true);

        using RangeMapStore store = NewStore(raft);

        raft.BeforeConfirm = _ =>
        {
            ApplyCommitted(store, raft.ProposedPayloads[0], logId: 1);
            return ValueTask.CompletedTask;
        };

        bool committed = await store.MutateAsync(
            current => [.. current, FullRange("ks0", 2)],
            TestContext.Current.CancellationToken);

        Assert.True(committed);
        Assert.Equal(1, raft.ProposeCalls);
        Assert.Single(store.Current.Descriptors);
    }

    [Fact]
    public async Task NotLeader_IsADefiniteRejection_NoSettleWait()
    {
        ProposeOutcomeStubRaft raft = new();
        raft.ProposeStatuses.Enqueue(RaftOperationStatus.NodeIsNotLeader);

        using RangeMapStore store = NewStore(raft);

        bool committed = await store.MutateAsync(
            current => [.. current, FullRange("ks0", 2)],
            TestContext.Current.CancellationToken);

        Assert.False(committed);
        Assert.Equal(1, raft.ProposeCalls);
        Assert.Equal(0, raft.ConfirmCalls);
        Assert.Equal(0, raft.WaitForLeaderCalls);
        Assert.Empty(store.Current.Descriptors);
    }

    /// <summary>
    /// Serializes a map through the store's own codec by proposing it on a throwaway store whose
    /// propose succeeds, then reading back the payload it handed to Raft.
    /// </summary>
    private static byte[] SerializeMap(params RangeDescriptor[] descriptors)
    {
        ProposeOutcomeStubRaft encoder = new();
        encoder.ProposeStatuses.Enqueue(RaftOperationStatus.Success);

        using RangeMapStore store = new(encoder, null, null, NullLogger<IKahuna>.Instance, checkpointEveryMutations: 0);
        Assert.True(store.MutateAsync(_ => descriptors).GetAwaiter().GetResult());

        return encoder.ProposedPayloads[0];
    }
}
