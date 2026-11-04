
namespace Kahuna.Server.Persistence;

/// <summary>
/// What a node knows about the integrity of its own key-value and lock store, for the readiness probe
/// and for operators. The two faults are terminal for the process: the operator repairs or replaces the
/// store and restarts the node.
/// </summary>
/// <param name="StoreFailStopReason">
/// The storage engine's own message when the store is fail-stopped on damage (a <c>Corruption</c> status
/// on a read, a write or the open), or null while it is healthy. While set, the node answers
/// <c>StorageCorrupt</c> to every read and write of the partitions it hosts and withholds its candidacy
/// for them.
/// </param>
/// <param name="StoreReplacedNotice">
/// Set when the store was found empty at start while the node's Raft log holds history: the store
/// directory was lost or replaced. The node's partitions are rebuilt from their peers by snapshot, or
/// stay quarantined where no peer exists. Null otherwise.
/// </param>
public sealed record KahunaStorageHealth(string? StoreFailStopReason, string? StoreReplacedNotice)
{
    /// <summary>The report of a node whose store is intact as far as it can tell.</summary>
    public static KahunaStorageHealth Healthy { get; } = new(null, null);

    /// <summary>True when neither fault is set.</summary>
    public bool IsHealthy => StoreFailStopReason is null && StoreReplacedNotice is null;

    /// <summary>The fault to report, the fail-stop first; null when healthy.</summary>
    public string? Fault => StoreFailStopReason ?? StoreReplacedNotice;
}
