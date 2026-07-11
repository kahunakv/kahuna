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
internal static class KeyValueScanLimits
{
    /// <summary>
    /// Maximum number of entries returned by a single non-paginated prefix/bucket scan.
    /// Results beyond this ceiling are silently truncated; use the paginated range scan to
    /// retrieve larger sets.
    /// </summary>
    internal const int MaxPrefixScanResults = 4096;

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
    internal const int MaxScanInspectionSlack = 8192;
}
