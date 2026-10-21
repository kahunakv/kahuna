using Kahuna.Server.Persistence;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Decides whether a snapshot read may take its answer from the persisted revision history.
///
/// <para>The background writer persists a key's revisions some time after they commit. Until it does,
/// the persisted history is missing them: a history read below a ceiling that covers a queued revision
/// answers with an older row (or with nothing for a young key), and that answer changes once the flush
/// lands, so two reads at one timestamp disagree. The unflushed-writes index records the oldest revision
/// of each key that is still queued; the history is complete only below it.</para>
///
/// <para>A queued revision the in-memory archive still holds is harmless: the archive already answered
/// for it (an archive miss means it is newer than the snapshot). Only a queued revision that is in
/// neither place makes the history answer unsafe, and the read must fail closed with MustRetry until
/// the flush confirms. The window is bounded by the flush cadence.</para>
///
/// <para>A fenced read without an archive can still take from the history its newest revision below the
/// oldest queued one (<see cref="HistoryMayLag(UnflushedKeyValueWritesIndex?, string, KeyValueRevisionHistory?, long, out long)"/>
/// reports that ceiling). That row is the as-of answer exactly when every queued revision is past the
/// snapshot, which only the resident archive can tell, since it retains every unflushed revision: the
/// merge that holds both decides, and <see cref="FlushedHistoryCeiling"/> lets it check that the ceiling
/// has not moved since the projection was taken.</para>
/// </summary>
internal static class UnflushedHistoryFence
{
    /// <summary>
    /// The ceiling of a key with no revision-keeping write queued: its persisted history is complete up to
    /// its head.
    /// </summary>
    public const long NoQueuedRevision = long.MaxValue;

    /// <summary>
    /// True when a persisted-history read of <paramref name="key"/> at or below
    /// <paramref name="asOfCeiling"/> could miss a committed revision because the revision is still
    /// queued for the writer and absent from <paramref name="archive"/>. A key whose newest queued write
    /// suppresses history (<c>NoRevision</c>) keeps no per-revision history to protect, so it is not
    /// fenced.
    /// </summary>
    public static bool HistoryMayLag(UnflushedKeyValueWritesIndex? unflushed, string key, KeyValueRevisionHistory? archive, long asOfCeiling)
        => HistoryMayLag(unflushed, key, archive, asOfCeiling, out _);

    /// <summary>
    /// <see cref="HistoryMayLag(UnflushedKeyValueWritesIndex?, string, KeyValueRevisionHistory?, long)"/> that also
    /// reports, in <paramref name="flushedCeiling"/>, the highest revision below which the persisted history of
    /// <paramref name="key"/> is complete: one below its oldest queued revision, or <see cref="NoQueuedRevision"/>
    /// when no revision-keeping write of it is queued. A fenced read may still project the key from the history at
    /// that ceiling; see the class summary for when the projection is the answer.
    /// </summary>
    public static bool HistoryMayLag(
        UnflushedKeyValueWritesIndex? unflushed, string key, KeyValueRevisionHistory? archive, long asOfCeiling, out long flushedCeiling)
    {
        flushedCeiling = NoQueuedRevision;

        if (unflushed is null || unflushed.IsEmpty || asOfCeiling < 0)
            return false;

        if (!unflushed.TryGet(key, out UnflushedKeyValueWrite queued) || queued.NoRevision)
            return false;

        long oldest = queued.OldestRevision;
        flushedCeiling = oldest - 1;

        if (oldest > asOfCeiling)
            return false;

        // Every queued revision up to the ceiling must be answered from memory. More revisions than the
        // archive holds cannot all be in it, which also bounds the walk below by the archive size.
        long queuedBelowCeiling = asOfCeiling - oldest + 1;
        if (archive is null || queuedBelowCeiling > archive.Count)
            return Fenced();

        for (long revision = oldest; revision <= asOfCeiling; revision++)
        {
            if (!archive.ContainsKey(revision))
                return Fenced();
        }

        return false;
    }

    /// <summary>
    /// The highest revision below which the persisted history of <paramref name="key"/> is complete right now: one
    /// below its oldest queued revision, or <see cref="NoQueuedRevision"/> when no revision-keeping write of it is
    /// queued. A projection taken from the history below a fenced key's queued revisions is its as-of answer only
    /// while this is still the ceiling it was taken at: a flush that landed since moved the boundary up, and the
    /// archive may have trimmed a revision the projection never saw.
    /// </summary>
    public static long FlushedHistoryCeiling(UnflushedKeyValueWritesIndex? unflushed, string key)
    {
        if (unflushed is null || unflushed.IsEmpty
            || !unflushed.TryGet(key, out UnflushedKeyValueWrite queued) || queued.NoRevision)
            return NoQueuedRevision;

        return queued.OldestRevision - 1;
    }

    private static bool Fenced()
    {
        KeyValueSnapshotReadMetrics.HistoryReadsFenced.Add(1);
        return true;
    }
}
