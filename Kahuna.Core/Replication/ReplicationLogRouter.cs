
namespace Kahuna.Server.Replication;

/// <summary>The subsystem that owns a replicated log type on this node.</summary>
internal enum ReplicationLogOwner
{
    /// <summary>Not a Kahuna log type — a Kommander system entry, or one this version does not know.</summary>
    None,

    /// <summary>The key-value layer, including the range map, snapshot floor and 2PC record types.</summary>
    KeyValues,

    /// <summary>The distributed-lock layer.</summary>
    Locks
}

/// <summary>
/// Single source of truth for which subsystem owns each <see cref="ReplicationTypes"/> value. The
/// apply callbacks (restore and replication-received) both route through here, so adding a
/// replication type means teaching exactly one place about it.
///
/// <para>
/// Unknown types — and an absent one — deliberately answer <see cref="ReplicationLogOwner.None"/>
/// rather than throwing: a partition's log also carries Kommander's own system entries, which no
/// Kahuna subsystem applies, and <c>RaftLog.LogType</c> is nullable.
/// The trade-off is that a Kahuna type left out of this map would be silently ignored on apply, so
/// the mapping is covered by a test that walks every constant on <see cref="ReplicationTypes"/>.
/// </para>
/// </summary>
internal static class ReplicationLogRouter
{
    internal static ReplicationLogOwner OwnerOf(string? logType) => logType switch
    {
        ReplicationTypes.KeyValues
            or ReplicationTypes.RangeMap
            or ReplicationTypes.SnapshotFloor
            or ReplicationTypes.CoordinatorDecision
            or ReplicationTypes.TransactionRecord
            or ReplicationTypes.PreparedIntent
            or ReplicationTypes.CompletionReceipt => ReplicationLogOwner.KeyValues,
        ReplicationTypes.Locks => ReplicationLogOwner.Locks,
        _ => ReplicationLogOwner.None
    };
}
