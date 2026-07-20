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
    Task<RaftReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries);
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

    public async Task<RaftReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries)
    {
        RaftProposalEntry[] array = entries as RaftProposalEntry[] ?? [.. entries];

        RaftBatchReplicationResult batch = await raft.ReplicateEntries(partitionId, array).ConfigureAwait(false);

        // The aggregator consumes one batch-level outcome (Success/Status). LogIndex/TicketId are unused here;
        // surface the proposal's shared ticket and the first appended entry's index for diagnostics/correlation.
        long logIndex = batch.Entries.Count > 0 ? batch.Entries[0].LogIndex : -1;
        return new RaftReplicationResult(batch.Success, batch.Status, batch.TicketId, logIndex);
    }
}
