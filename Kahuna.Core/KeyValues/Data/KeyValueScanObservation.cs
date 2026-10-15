
namespace Kahuna.Server.KeyValues;

/// <summary>
/// What one range-scan page cost the serving node: how many entries it touched, how many of those it
/// left out, how much revision history it read on disk, how many prepared intents it merged and how many
/// times it was retried before it was served. The process meters in <see cref="KeyValueScanMetrics"/>
/// aggregate over every scan in the process; this record belongs to one page, so a consumer can
/// attribute read amplification to the statement that ran the scan.
/// <para>
/// Observations are advisory: they never change paging, retries, visibility or results. All counts are
/// additive and zero when the path that feeds them did not run. A page that carries no observation (a
/// peer on an older build served it) is unknown, never zero.
/// </para>
/// <para>
/// A page retried from the same cursor reports the sum over all of its attempts, not the last attempt only.
/// </para>
/// </summary>
/// <param name="EntriesInspected">Entries the page walk touched, live or dead: resident entries, disk
/// heads and disk rows the as-of projection dropped.</param>
/// <param name="DeadEntriesSkipped">Entries the walk touched and did not return: tombstones, expired
/// entries, keys with no committed version at the read timestamp. On an as-of scan, a key that is both
/// resident and on disk can count once for each source.</param>
/// <param name="RevisionRowsExamined">Revision-history rows read on disk for this page: rows other than
/// the current head that the disk walk stepped over, plus the rows read to resolve each as-of image.
/// The RocksDB backend stores the revision row of every write next to its head, so a key written once
/// costs one row there. Backends that cannot count rows report one per as-of lookup.</param>
/// <param name="RevisionRunSeeks">Seeks that jumped the disk walk over the rest of one key's revision
/// rows. The same seeks feed <c>kahuna.scan.revision_run_seeks_total</c>.</param>
/// <param name="IntentsMerged">Committed prepared intents the overlay applied to the page: an override,
/// an injected key, or a removed key.</param>
/// <param name="PageRetries">Transient retries this page took before it was served.</param>
/// <param name="ServedRemotely">True when a peer served the page (or any part of it).</param>
/// <param name="DiskFallback">True when the page merged resident entries with a disk read, not only
/// the resident walk.</param>
public readonly record struct KeyValueScanObservation(
    long EntriesInspected,
    long DeadEntriesSkipped,
    long RevisionRowsExamined,
    long RevisionRunSeeks,
    long IntentsMerged,
    long PageRetries,
    bool ServedRemotely,
    bool DiskFallback)
{
    /// <summary>Sums two observations field by field; the flags are true when either side is true.</summary>
    public static KeyValueScanObservation operator +(in KeyValueScanObservation a, in KeyValueScanObservation b) =>
        new(a.EntriesInspected + b.EntriesInspected,
            a.DeadEntriesSkipped + b.DeadEntriesSkipped,
            a.RevisionRowsExamined + b.RevisionRowsExamined,
            a.RevisionRunSeeks + b.RevisionRunSeeks,
            a.IntentsMerged + b.IntentsMerged,
            a.PageRetries + b.PageRetries,
            a.ServedRemotely || b.ServedRemotely,
            a.DiskFallback || b.DiskFallback);

    /// <summary>
    /// Sums two optional observations. Null when either side is null: a sum over a part whose cost is
    /// unknown is itself unknown, never a lower bound reported as exact.
    /// </summary>
    internal static KeyValueScanObservation? Combine(KeyValueScanObservation? a, KeyValueScanObservation? b) =>
        a is { } x && b is { } y ? x + y : null;
}

/// <summary>
/// Mutable counters a scan path fills while it runs, then freezes into a
/// <see cref="KeyValueScanObservation"/>. Owned by one thread at a time: the actor turn that runs the page,
/// or the scheduler thread that runs one disk read. It is never shared between threads while it is written.
/// </summary>
internal struct KeyValueScanTally
{
    public long EntriesInspected;
    public long DeadEntriesSkipped;
    public long RevisionRowsExamined;
    public long RevisionRunSeeks;
    public long IntentsMerged;

    public void Add(in KeyValueScanTally other)
    {
        EntriesInspected += other.EntriesInspected;
        DeadEntriesSkipped += other.DeadEntriesSkipped;
        RevisionRowsExamined += other.RevisionRowsExamined;
        RevisionRunSeeks += other.RevisionRunSeeks;
        IntentsMerged += other.IntentsMerged;
    }

    public readonly KeyValueScanObservation ToObservation(bool diskFallback) =>
        new(EntriesInspected, DeadEntriesSkipped, RevisionRowsExamined, RevisionRunSeeks, IntentsMerged,
            PageRetries: 0, ServedRemotely: false, DiskFallback: diskFallback);
}

/// <summary>
/// Accumulates the per-page observations of one streamed range scan
/// (<see cref="IKahuna.LocateAndScanRange"/>). Pass an instance to the scan and read it after the
/// enumeration ends. Not thread-safe: the enumerator writes it, and the caller reads it only after the
/// enumeration completes or stops.
/// </summary>
public sealed class KeyValueScanObservationSink
{
    /// <summary>Pages the scan served to the caller.</summary>
    public long Pages { get; private set; }

    /// <summary>
    /// Pages served without an observation (a peer on an older build served them). Their counts are not
    /// in <see cref="Total"/>, so <see cref="Total"/> is exact only when this is zero.
    /// </summary>
    public long PagesUnobserved { get; private set; }

    /// <summary>
    /// The sum of the observations of every observed page. <see cref="KeyValueScanObservation.PageRetries"/>
    /// counts the retries of every page, observed or not, because the scan loop counts them itself.
    /// </summary>
    public KeyValueScanObservation Total { get; private set; }

    /// <summary>Records one served page and the retries the scan loop took for it.</summary>
    internal void Add(KeyValueScanObservation? observation, long pageRetries)
    {
        Pages++;

        KeyValueScanObservation total = Total;

        if (observation is { } page)
            total += page;
        else
            PagesUnobserved++;

        Total = total with { PageRetries = total.PageRetries + pageRetries };
    }
}
