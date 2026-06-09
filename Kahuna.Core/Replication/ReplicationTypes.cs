
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
}