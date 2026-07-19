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
/// Production executor: one auto-commit <see cref="IRaft.ReplicateLogs"/> of the batched key/value log
/// records. expectedGeneration is 0 — the key-range fence already ran per item on the owning actor, and
/// Kommander's expectedGeneration is a different (physical partition) namespace Kahuna never syncs.
/// </summary>
internal sealed class RaftPartitionBatchExecutor : IPartitionBatchExecutor
{
    private readonly IRaft raft;

    public RaftPartitionBatchExecutor(IRaft raft) => this.raft = raft;

    public Task<RaftReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<byte[]> logs) =>
        raft.ReplicateLogs(partitionId, ReplicationTypes.KeyValues, logs, autoCommit: true, expectedGeneration: 0);
}
