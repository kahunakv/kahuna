
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

    /// <summary>True when the raw (unprojected) disk page was as long as the page size the backend was asked for.</summary>
    internal readonly bool RawHasMore;

    /// <summary>
    /// The key of the last raw row; the start key for the next disk page, which the merge of this page
    /// consumed and the next page skips. Non-null if and only if <see cref="RawHasMore"/> is <c>true</c>.
    /// </summary>
    internal readonly string? RawNextCursor;

    /// <summary>
    /// What the read cost: the revision rows and seeks of the disk walk, and the rows the as-of projection
    /// read and dropped. Filled by the thread that read the page (a scheduler worker, or the actor thread
    /// for an in-memory backend) and read on the actor thread after the page is handed over, never written
    /// concurrently.
    /// </summary>
    internal readonly KeyValueScanTally Tally;

    /// <summary>
    /// The keys of this page whose as-of image the projection could not take from the whole persisted history:
    /// their head is past the snapshot and a revision below it is still queued for the background writer, so a
    /// history read across the queued revisions would have projected an older row, or no row, in its place. Each
    /// maps to the ceiling it was projected at instead, one below its oldest queued revision
    /// (<see cref="UnflushedHistoryFence.FlushedHistoryCeiling"/>): its row in <see cref="Projected"/>, when the
    /// flushed history holds one at or before the snapshot, is the as-of image only if every queued revision is past
    /// the snapshot. The merge serves such a key from its resident entry, whose archive holds the queued revisions
    /// and so can tell, and answers MustRetry for one that is not resident, since nothing else can answer for it
    /// until the flush lands. Null when the projection fenced nothing.
    /// </summary>
    internal readonly Dictionary<string, long>? HistoryFencedKeys;

    internal RangeDiskPage(
        List<(string, ReadOnlyKeyValueEntry)> projected,
        bool rawHasMore,
        string? rawNextCursor,
        KeyValueScanTally tally = default,
        Dictionary<string, long>? historyFencedKeys = null)
    {
        Projected = projected;
        RawHasMore = rawHasMore;
        RawNextCursor = rawNextCursor;
        Tally = tally;
        HistoryFencedKeys = historyFencedKeys;
    }
}
