
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// An immutable record of one client-held MVCC snapshot hold. A hold at <see cref="Timestamp"/>
/// keeps the revision current at that timestamp readable via every read path while the hold stays
/// registered. The lease (<see cref="LeaseExpiry"/>, compared against the cluster HLC) governs the
/// reported effective floor and when the reaper may purge the hold — not the protection cutoff:
/// reclamation honors a registered hold even after its lease lapses, which is what lets a renew
/// revive a lapsed-but-unpurged hold with its pinned history provably intact.
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
