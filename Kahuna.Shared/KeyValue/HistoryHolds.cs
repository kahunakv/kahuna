using Kommander.Time;

namespace Kahuna.Shared.KeyValue;

/// <summary>What a <see cref="HistoryHoldRequest"/> asks the history hold registry to do.</summary>
public enum HistoryHoldOperation
{
    /// <summary>Registers (or moves forward) one hold per named partition for a holder, from a timestamp.</summary>
    Acquire = 0,

    /// <summary>Renews the lease of the named holds, and can move their timestamp forward.</summary>
    Renew = 1,

    /// <summary>Releases the named holds.</summary>
    Release = 2,

    /// <summary>Lists the registered holds, of one holder or of every holder.</summary>
    List = 3
}

/// <summary>
/// A request to the history hold registry. A history hold keeps the WAL of one data partition retained from a
/// timestamp onward on every replica, so a watch can resume from that timestamp on any of them. A hold makes a resume
/// likely, not certain: a replica that compacted before it applied the hold answers <c>HistoryUnavailable</c>.
/// </summary>
/// <param name="Operation">What to do.</param>
/// <param name="HolderId">Acquire: the holder. List: limits the list to this holder (null lists every hold).</param>
/// <param name="Partitions">Acquire: the data partitions to hold, one hold each.</param>
/// <param name="Timestamp">Acquire: the timestamp to hold from. Renew: the timestamp to move the holds to, never
/// below their current one; zero keeps it.</param>
/// <param name="LeaseMs">Acquire and renew: the lease, in milliseconds from now.</param>
/// <param name="HoldIds">Renew and release: the holds.</param>
public sealed record HistoryHoldRequest(
    HistoryHoldOperation Operation,
    string? HolderId = null,
    IReadOnlyList<int>? Partitions = null,
    HLCTimestamp Timestamp = default,
    int LeaseMs = 0,
    IReadOnlyList<string>? HoldIds = null);

/// <summary>One registered history hold.</summary>
public sealed record HistoryHoldInfo(
    string HoldId,
    string HolderId,
    int PartitionId,
    HLCTimestamp Timestamp,
    HLCTimestamp LeaseExpiry);

/// <summary>
/// The registry's answer. On success the type is <see cref="KeyValueResponseType.Set"/> (acquire, renew),
/// <see cref="KeyValueResponseType.Deleted"/> (release) or <see cref="KeyValueResponseType.Get"/> (list), and
/// <see cref="Holds"/> lists the holds the request touched or found. Refusals:
/// <see cref="KeyValueResponseType.InvalidInput"/> (a malformed request, a lease above the bound, or a timestamp below
/// a hold's current one), <see cref="KeyValueResponseType.DoesNotExist"/> (a named hold was released or purged: its
/// history must be presumed reclaimed), <see cref="KeyValueResponseType.HistoryHoldLimitReached"/> and
/// <see cref="KeyValueResponseType.MustRetry"/> (no confirmed meta-partition leader, or the replication failed).
/// </summary>
public readonly record struct HistoryHoldAnswer(KeyValueResponseType Type, IReadOnlyList<HistoryHoldInfo> Holds)
{
    public static HistoryHoldAnswer Refused(KeyValueResponseType type) => new(type, []);
}
