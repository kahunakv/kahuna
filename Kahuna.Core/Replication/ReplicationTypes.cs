
namespace Kahuna.Server.Replication;

/// <summary>
/// Provides string constants representing different types of replication logs
/// utilized within the system. These types are used to categorize and process
/// specific replication scenarios, such as locks and key-values replication.
/// </summary>
public static class ReplicationTypes
{
    public const string Locks = "lock";

    public const string KeyValues = "kv";

    /// <summary>
    /// The range-descriptor map snapshot, replicated on the meta partition
    /// (<see cref="Kahuna.Server.KeyValues.Ranges.RangeMapStore.MetaPartitionId"/>).
    /// </summary>
    public const string RangeMap = "rangemap";

    /// <summary>
    /// The snapshot-floor hold registry, replicated on the meta partition
    /// (<see cref="Kahuna.Server.KeyValues.Ranges.RangeMapStore.MetaPartitionId"/>).
    /// </summary>
    public const string SnapshotFloor = "snapshotfloor";

    /// <summary>
    /// A durable coordinator decision record delta, replicated on the data partition that currently owns
    /// the transaction's record anchor key (never the meta partition).
    /// </summary>
    public const string CoordinatorDecision = "coorddecision";

    /// <summary>
    /// A batch of persistent-participant completion receipts handed to a destination data partition during
    /// a range split or merge, replicated on that partition so every replica of the moved range holds them
    /// (a re-commit routed to a new leader after cutover still resolves <c>Committed</c>). In steady state a
    /// receipt rides its key-value commit and needs no dedicated entry; this type carries only the split/merge
    /// handoff, which is otherwise not present in the destination partition's log.
    /// </summary>
    public const string CompletionReceipt = "receipt";

    /// <summary>
    /// A canonical transaction-record CAS transition (initialize/commit/abort), replicated on the data partition
    /// that owns the transaction's record anchor key (never the meta partition). The durable-intent 2PC model's
    /// terminal-decision authority.
    /// </summary>
    public const string TransactionRecord = "txnrecord";

    /// <summary>
    /// A durable prepared-intent transition (prepare/resolve/remove), replicated on each participant's data
    /// partition. The durable-intent 2PC model's per-key pending-mutation authority.
    /// </summary>
    public const string PreparedIntent = "preparedintent";
}