using Kommander.Time;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// One holder's claim on the history of one data partition: every replica keeps the partition's WAL retained from
/// <see cref="Timestamp"/> onward (mapped to a log index through the commit admission window), so a watch can resume
/// from that timestamp on any replica. The lease (<see cref="LeaseExpiry"/>, compared against the cluster HLC) governs
/// when the reaper may purge the hold, not the retention: a registered hold keeps its pin even after its lease lapses,
/// until a replicated release or purge removes it.
/// </summary>
internal sealed record HistoryHold(
    string HoldId,
    string HolderId,
    int PartitionId,
    HLCTimestamp Timestamp,
    HLCTimestamp LeaseExpiry)
{
    /// <summary>True when <paramref name="currentTime"/> is still before this hold's lease expiry.</summary>
    public bool IsLive(HLCTimestamp currentTime) => LeaseExpiry.CompareTo(currentTime) > 0;
}
