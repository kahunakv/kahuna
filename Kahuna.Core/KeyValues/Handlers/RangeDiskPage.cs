
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// The combined result of a stage-2 range disk read: the snapshot-projected entries plus
/// the raw pagination facts extracted from the unfiltered disk page.
///
/// Keeping raw pagination info and projected rows separate prevents a silent correctness bug
/// where all raw rows on a page are post-snapshot (dropped by projection), causing
/// <c>diskHasMore=false</c> to be derived from an empty projected list — terminating the scan
/// before later pages with snapshot-visible keys are ever fetched.
/// </summary>
internal sealed class RangeDiskPage
{
    /// <summary>Snapshot-projected entries from this page (sentinel excluded).</summary>
    internal readonly List<(string Key, ReadOnlyKeyValueEntry Entry)> Projected;

    /// <summary>True when the raw (unprojected) disk page contained <c>limit + 1</c> rows.</summary>
    internal readonly bool RawHasMore;

    /// <summary>
    /// The key of the <c>(limit+1)</c>-th raw row; the start key for the next disk page.
    /// Non-null if and only if <see cref="RawHasMore"/> is <c>true</c>.
    /// </summary>
    internal readonly string? RawNextCursor;

    internal RangeDiskPage(
        List<(string, ReadOnlyKeyValueEntry)> projected,
        bool rawHasMore,
        string? rawNextCursor)
    {
        Projected = projected;
        RawHasMore = rawHasMore;
        RawNextCursor = rawNextCursor;
    }
}
