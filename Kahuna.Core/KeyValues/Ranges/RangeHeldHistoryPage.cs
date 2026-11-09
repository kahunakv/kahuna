using Kommander.Time;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// One page of what a moving key range must carry beyond its current rows, read from the source partition's leader:
/// each key's revision at each asked snapshot hold, and the tombstones of keys deleted since the copy could have seen
/// them live (see <see cref="RangeStateTransferService.CarryHeldHistoryAsync"/>).
/// </summary>
/// <param name="Ok">False when leadership could not be confirmed, so the caller re-routes. Never means "nothing to carry".</param>
/// <param name="Served">False when the peer predates this kind of page and carried nothing.</param>
/// <param name="Rows">A checksummed key-value page in the range-copy format; empty when the page carries no rows.</param>
/// <param name="RefusedPins">Asked holds the source cannot answer itself, so the destination cannot be said to answer them.</param>
/// <param name="HasMore">Whether more keys remain past <paramref name="NextCursor"/>.</param>
/// <param name="NextCursor">Resume strictly after this key.</param>
public sealed record RangeHeldHistoryPage(bool Ok, bool Served, byte[] Rows, HLCTimestamp[] RefusedPins, bool HasMore, string? NextCursor)
{
    public static readonly RangeHeldHistoryPage NotLeader = new(false, false, [], [], false, null);
}
