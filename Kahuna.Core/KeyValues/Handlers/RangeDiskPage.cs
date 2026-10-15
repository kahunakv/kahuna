
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
    /// <summary>
    /// Snapshot-projected entries from this page, the <c>limit + 1</c> sentinel row included: it is a row of
    /// the range like any other, and merging it lets a full page close without a second disk read.
    /// </summary>
    internal readonly List<(string Key, ReadOnlyKeyValueEntry Entry)> Projected;

    /// <summary>True when the raw (unprojected) disk page contained <c>limit + 1</c> rows.</summary>
    internal readonly bool RawHasMore;

    /// <summary>
    /// The key of the <c>(limit+1)</c>-th raw row; the start key for the next disk page.
    /// Non-null if and only if <see cref="RawHasMore"/> is <c>true</c>.
    /// </summary>
    internal readonly string? RawNextCursor;

    /// <summary>
    /// What the read cost: the revision rows and seeks of the disk walk, and the rows the as-of projection
    /// read and dropped. Filled on the scheduler thread and read on the actor thread after the page is handed
    /// over, never written concurrently.
    /// </summary>
    internal readonly KeyValueScanTally Tally;

    internal RangeDiskPage(
        List<(string, ReadOnlyKeyValueEntry)> projected,
        bool rawHasMore,
        string? rawNextCursor,
        KeyValueScanTally tally = default)
    {
        Projected = projected;
        RawHasMore = rawHasMore;
        RawNextCursor = rawNextCursor;
        Tally = tally;
    }
}
