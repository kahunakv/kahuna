using Kommander;
using Kommander.Data;
using Kahuna.Server.Replication;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// The Raft round trip the aggregator performs for one partition batch, behind an interface so tests can
/// record calls, gate one partition while another proceeds, and return chosen statuses without a real cluster.
/// </summary>
internal interface IPartitionBatchExecutor
{
    Task<RaftReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<byte[]> logs);
}

/// <summary>
/// Production executor: one auto-commit <see cref="IRaft.ReplicateEntries"/> proposal carrying the batched
/// key/value log records — one proposal, one AppendEntries round trip, one group-committed WAL flush. The
/// batch is still uniform (every entry is a key/value record, autoCommit, ExpectedGeneration 0), so the
/// observable outcome matches the prior single-type <c>ReplicateLogs</c> call; using the heterogeneous
/// entry surface here is what later lets unrelated record types share the same proposal.
/// ExpectedGeneration is 0 — the key-range fence already ran per item on the owning actor, and Kommander's
/// per-entry generation fence is a different (physical partition) namespace Kahuna never syncs.
/// </summary>
internal sealed class RaftPartitionBatchExecutor : IPartitionBatchExecutor
{
    private readonly IRaft raft;

    public RaftPartitionBatchExecutor(IRaft raft) => this.raft = raft;

    public async Task<RaftReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<byte[]> logs)
    {
        RaftProposalEntry[] entries = new RaftProposalEntry[logs.Count];
        for (int i = 0; i < logs.Count; i++)
            entries[i] = new RaftProposalEntry(ReplicationTypes.KeyValues, logs[i], AutoCommit: true, ExpectedGeneration: 0);

        RaftBatchReplicationResult batch = await raft.ReplicateEntries(partitionId, entries).ConfigureAwait(false);

        // The aggregator consumes one batch-level outcome (Success/Status). LogIndex/TicketId are unused here;
        // surface the proposal's shared ticket and the first appended entry's index for diagnostics/correlation.
        long logIndex = batch.Entries.Count > 0 ? batch.Entries[0].LogIndex : -1;
        return new RaftReplicationResult(batch.Success, batch.Status, batch.TicketId, logIndex);
    }
}
