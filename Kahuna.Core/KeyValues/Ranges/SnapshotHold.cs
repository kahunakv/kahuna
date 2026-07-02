
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// An immutable record of one client-held MVCC snapshot hold. A hold at <see cref="Timestamp"/>
/// keeps the revision current at that timestamp readable via every read path while the hold is live
/// (i.e. while <see cref="LeaseExpiry"/> has not elapsed on the cluster HLC).
/// </summary>
internal sealed record SnapshotHold(
    string HoldId,
    string HolderId,
    HLCTimestamp Timestamp,
    HLCTimestamp LeaseExpiry
)
{
    /// <summary>True when <paramref name="currentTime"/> is still before this hold's lease expiry.</summary>
    public bool IsLive(HLCTimestamp currentTime) => LeaseExpiry.CompareTo(currentTime) > 0;
}
