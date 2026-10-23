
using Kahuna.Server.KeyValues;

namespace Kahuna.Server.Persistence.Backend;

/// <summary>
/// The scan counters a persistence backend records, on Kahuna's own meter, so every backend reports
/// the same <c>kahuna.scan.*</c> instruments the dashboards read.
/// </summary>
public static class PersistenceBackendMetrics
{
    /// <summary>
    /// Counts one seek that jumped a scan over the rest of a key's revision rows instead of stepping
    /// through them (<c>kahuna.scan.revision_run_seeks_total</c>).
    /// </summary>
    public static void RecordRevisionRunSeek() => KeyValueScanMetrics.RevisionRunSeeks.Add(1);

    /// <summary>
    /// Counts one snapshot (as-of) prefix scan: the physical rows it examined, head and revision-history
    /// rows both, and the entries it returned (<c>kahuna.scan.snapshot_prefix_rows_examined_total</c>,
    /// <c>kahuna.scan.snapshot_prefix_entries_returned_total</c>), and whether it stopped early because
    /// its caller had gone (<c>kahuna.scan.abandoned_cancelled_total</c>).
    /// </summary>
    public static void RecordSnapshotPrefixScan(long rowsExamined, long entriesReturned, bool abandoned)
    {
        KeyValueScanMetrics.SnapshotPrefixRowsExamined.Add(rowsExamined);
        KeyValueScanMetrics.SnapshotPrefixEntriesReturned.Add(entriesReturned);
        if (abandoned)
            KeyValueScanMetrics.ScansAbandonedCancelled.Add(1);
    }
}
