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
/// </summary>
internal static class UnflushedHistoryFence
{
    /// <summary>
    /// True when a persisted-history read of <paramref name="key"/> at or below
    /// <paramref name="asOfCeiling"/> could miss a committed revision because the revision is still
    /// queued for the writer and absent from <paramref name="archive"/>. A key whose newest queued write
    /// suppresses history (<c>NoRevision</c>) keeps no per-revision history to protect, so it is not
    /// fenced.
    /// </summary>
    public static bool HistoryMayLag(UnflushedKeyValueWritesIndex? unflushed, string key, KeyValueRevisionHistory? archive, long asOfCeiling)
    {
        if (unflushed is null || unflushed.IsEmpty || asOfCeiling < 0)
            return false;

        if (!unflushed.TryGet(key, out UnflushedKeyValueWrite queued) || queued.NoRevision)
            return false;

        long oldest = queued.OldestRevision;
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

    private static bool Fenced()
    {
        KeyValueSnapshotReadMetrics.HistoryReadsFenced.Add(1);
        return true;
    }
}
