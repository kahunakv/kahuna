namespace Kahuna.Server.KeyValues;

/// <summary>
/// Hard caps for the non-paginated prefix/bucket scan APIs (GetByBucket, ScanByPrefix,
/// ScanByPrefixFromDisk). Unlike GetByRange — which streams results page-by-page through an
/// opaque cursor — these APIs materialize the whole matching set in memory in a single call,
/// so an unbounded prefix over a large key space would allocate without limit on the actor
/// thread. The cap bounds that allocation to a predictable ceiling.
///
/// Consumers that need to walk more than <see cref="MaxPrefixScanResults"/> entries must use
/// the paginated range scan (LocateAndScanRange / GetByRange) instead.
/// </summary>
public static class KeyValueScanLimits
{
    /// <summary>
    /// Maximum number of entries returned by a single non-paginated prefix/bucket scan.
    /// Results beyond this ceiling are silently truncated; use the paginated range scan to
    /// retrieve larger sets.
    /// </summary>
    public const int MaxPrefixScanResults = 4096;

    /// <summary>
    /// Extra <em>inspected</em> entries a scan may walk beyond the entries it needs to collect
    /// before it stops. Result caps bound how many live entries a scan <em>returns</em> but not how
    /// many it <em>inspects</em>: a run of interleaved tombstones/expired rows can otherwise make the
    /// mailbox-thread walk O(resident-range) before it gathers enough live results. Adding this slack
    /// to the collection target bounds that walk — a normal (sparse-tombstone) scan never hits it, and
    /// a pathologically tombstone-dense range truncates early instead of parking the actor.
    ///
    /// The paginated range scan resumes from a cursor at the last inspected key, so truncation there is
    /// invisible to callers (just more pages). The non-paginated bucket/prefix scans already document
    /// "silently truncated; use the paginated range scan for larger sets", so an inspection-bounded
    /// truncation is consistent with their contract; it is logged rather than silent.
    /// </summary>
    public const int MaxScanInspectionSlack = 8192;

    /// <summary>
    /// Largest page a client may ask of the paginated range read (get-by-range and its stream). A page holds
    /// its whole item list in memory and in one response, so a client-chosen limit of <see cref="int.MaxValue"/>
    /// would make one request materialize an entire range. A page cut to this size reports
    /// <c>HasMore</c> with a cursor, which a caller must already follow: the inspection budget ends pages early
    /// the same way.
    /// </summary>
    public const int MaxRangePageSize = MaxPrefixScanResults;

    /// <summary>
    /// Resolves the page size a client asked of the paginated range read. Zero asks for the server's page size,
    /// and a larger request is cut to <see cref="MaxRangePageSize"/>. Returns false for a negative limit, which
    /// names no page size.
    /// </summary>
    public static bool TryBoundRangePageLimit(int requested, out int limit)
    {
        if (requested < 0)
        {
            limit = 0;
            return false;
        }

        limit = requested == 0 || requested > MaxRangePageSize ? MaxRangePageSize : requested;
        return true;
    }
}
