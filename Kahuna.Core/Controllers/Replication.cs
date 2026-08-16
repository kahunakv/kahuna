
using Kommander.Data;
using Kahuna.Server.Replication;

namespace Kahuna;

/// <summary>
/// Raft apply callbacks: routes each committed log entry to the subsystem that owns its type.
/// </summary>
public sealed partial class KahunaManager
{
    // Both apply callbacks run once per committed log entry; delegating the inner task directly
    // (no async wrapper) avoids a state-machine allocation per entry, and the unowned-type arm
    // reuses one cached task.
    private static readonly Task<bool> TrueTask = Task.FromResult(true);

    public Task<bool> OnLogRestored(int partitionId, RaftLog log)
    {
        return ReplicationLogRouter.OwnerOf(log.LogType) switch
        {
            ReplicationLogOwner.KeyValues => keyValues.OnLogRestored(partitionId, log),
            ReplicationLogOwner.Locks => locks.OnLogRestored(partitionId, log),
            _ => TrueTask
        };
    }

    public Task<bool> OnReplicationReceived(int partitionId, RaftLog log)
    {
        return ReplicationLogRouter.OwnerOf(log.LogType) switch
        {
            ReplicationLogOwner.KeyValues => keyValues.OnReplicationReceived(partitionId, log),
            ReplicationLogOwner.Locks => locks.OnReplicationReceived(partitionId, log),
            _ => TrueTask
        };
    }

    public void OnReplicationError(int partitionId, RaftLog log)
    {
        locks.OnReplicationError(log);
        keyValues.OnReplicationError(log);
    }

    public Task<bool> OnLeaderChanged(int partitionId, string node)
    {
        // Sequence blocks are reserved against a specific record revision on a specific partition.
        // Once that partition changes hands, surrender the blocks rather than keep draining them.
        sequencer.OnLeaderChanged(partitionId);

        return keyValues.OnLeaderChanged(partitionId, node);
    }
}
