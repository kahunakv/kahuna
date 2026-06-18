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
}
