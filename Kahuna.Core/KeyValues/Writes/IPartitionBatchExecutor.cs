using Kommander;
using Kommander.Data;
using Kahuna.Server.Replication;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// The Raft round trip the aggregator performs for one partition batch, behind an interface so tests can
/// record calls, gate one partition while another proceeds, and return chosen statuses without a real cluster.
/// The entries are already-typed <see cref="RaftProposalEntry"/> records, so one batch may carry different log
/// types (heterogeneous proposal); the executor only replicates them.
/// </summary>
internal interface IPartitionBatchExecutor
{
    /// <summary>Replicates the batch and returns Kommander's index-aligned per-entry result, so the caller can
    /// complete or release each submission by its <b>own</b> entries' outcomes rather than one batch-level
    /// boolean. <paramref name="cancellationToken"/> carries the scheduler's execution deadline and shutdown
    /// signal; a cancelled call surfaces as a retryable outcome.</summary>
    Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken);
}

/// <summary>
/// Production executor: one auto-commit <see cref="IRaft.ReplicateEntries"/> proposal carrying the batched
/// log records — one proposal, one AppendEntries round trip, one group-committed WAL flush. The entries are
/// typed by their producers (a uniform key/value batch, or a mix once other producers share the scheduler), so
/// the observable outcome matches the prior single-type <c>ReplicateLogs</c> call while the heterogeneous entry
/// surface lets unrelated record types share the same proposal. Every entry is autoCommit with
/// ExpectedGeneration 0 — the key-range fence already ran per item on the owning actor, and Kommander's per-entry
/// generation fence is a different (physical partition) namespace Kahuna never syncs.
/// </summary>
internal sealed class RaftPartitionBatchExecutor : IPartitionBatchExecutor
{
    private readonly IRaft raft;

    public RaftPartitionBatchExecutor(IRaft raft) => this.raft = raft;

    public async Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
    {
        RaftProposalEntry[] array = entries as RaftProposalEntry[] ?? [.. entries];

        // Return the full per-entry result; the aggregator maps each submission's contiguous entry slice to its
        // own outcome. The scheduler's execution-deadline/shutdown token bounds the round trip.
        return await raft.ReplicateEntries(partitionId, array, cancellationToken).ConfigureAwait(false);
    }
}
