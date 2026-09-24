
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The committed base a granted point lock protects: the key's committed revision, or its absence, on the
/// actor at the moment of the grant.
///
/// <para>A pessimistic writer computes its value from what it saw under the lock, so the grant is an
/// observation of the key in the same sense a transactional read is, and the coordinator folds it into the
/// read set. When the transaction later writes the key, that observation becomes the write's validated base,
/// and the commit-time base check refuses the write if another transaction committed over it. That can only
/// happen when the exclusion was lost: the lock lives in one leader's actor memory, a leader change does not
/// carry it over, and the new leader grants the key to the next writer. Without the observation the write is
/// blind, and once the competitor settled it stages a revision above the new head, which the same-revision
/// rule cannot see.</para>
/// </summary>
internal static class PointLockBase
{
    /// <summary>No base was observed: the lock was not granted, or the answer came from a path that carries
    /// none (a replayed completion, a transport refusal).</summary>
    public const long None = long.MinValue;

    /// <summary>The revision a read observation records for an absent key (never written, deleted, or expired),
    /// so a lock over an absent key validates exactly as a read that found nothing.</summary>
    public const long Absent = -1;

    /// <summary>The base the granted lock protects, read from the entry the actor holds at grant time.</summary>
    public static long Observe(KeyValueEntry entry, HLCTimestamp currentTime)
    {
        if (entry.State != KeyValueState.Set)
            return Absent;

        if (entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero)
            return Absent;

        return entry.Revision;
    }

    /// <summary>The read-set observation for a granted lock, or null when no base was observed.</summary>
    public static KeyValueTransactionReadKey? ToObservation(string key, KeyValueDurability durability, long baseRevision)
    {
        if (baseRevision == None)
            return null;

        bool exists = baseRevision >= 0;

        return new()
        {
            Key = key,
            Durability = durability,
            Exists = exists,
            Revision = exists ? baseRevision : Absent
        };
    }
}
