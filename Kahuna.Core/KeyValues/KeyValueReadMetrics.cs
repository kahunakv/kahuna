using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for the point-read path, so the service time of a
/// cached-key get can be split into the steps it is made of without a profiler attached.
///
/// <para>A point get served locally passes four steps: the operation registration on the coordinator (a
/// transaction-scoped read only), the quorum-confirmed leadership check, the actor round trip that reads the
/// value, and the completion that folds the read observation into the working set (transaction-scoped only).
/// Each step records one sample per get under the <c>stage</c> tag, and the whole get records one <c>total</c>
/// sample tagged by <c>path</c>: <c>registered</c> for a transaction-scoped read, <c>direct</c> for a read
/// without a session. The leadership check is also tagged by whether the leader's lease answered it
/// synchronously (<c>lease=hit</c>) or the answer was asynchronous (<c>lease=miss</c>): a wait for the
/// leader's applied frontier under a fresh lease, or an executor read-index round when the lease had lapsed.</para>
///
/// <para>The clock is <see cref="Stopwatch"/>, so a sample is wall time on the serving node and includes
/// any wait inside the step (a mailbox queue, a lease miss waiting for its ack round). Forwarded reads
/// record nothing here: their time is spent on the node that serves them, where it is recorded.</para>
/// </summary>
internal static class KeyValueReadMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>
    /// One point get split into its steps, tagged by <c>stage</c>: <c>register</c>, <c>confirm</c>, <c>actor</c>,
    /// <c>complete</c>, and the whole get as <c>total</c>. <c>confirm</c> carries <c>lease=hit|miss</c>;
    /// <c>actor</c> carries <c>path=published|actor</c> (answered from the owning actor's published head record
    /// on the caller's thread, or by a mailbox round trip); <c>total</c> carries <c>path=registered|direct</c>.
    /// The tag set is fixed.
    /// </summary>
    internal static readonly Histogram<double> StageMs =
        Meter.CreateHistogram<double>("kahuna.kv.read.stage", unit: "ms", description: "Point-get service time per step on the serving node, tagged by stage; the total series is tagged by path.");

    /// <summary>
    /// The point gets that took the actor path, tagged by <c>reason</c>: the guard that kept the published head
    /// record from answering (see <see cref="PublishedReadFallback"/>). The value is the time of the actor step,
    /// the same one the <c>actor</c> stage of <see cref="StageMs"/> records under <c>path=actor</c>, so the count
    /// of each reason is its share of the actor path and the mean is what that kind of read costs there. A
    /// series of its own, with one tag: the tag sets of <see cref="StageMs"/> stay as they are for whoever reads
    /// them. The tag set is fixed.
    /// </summary>
    internal static readonly Histogram<double> FallbackMs =
        Meter.CreateHistogram<double>("kahuna.kv.read.fallback", unit: "ms", description: "Point gets that took the actor path instead of the published head record, by the guard that refused the record; the value is the time of the actor step.");

    private static readonly KeyValuePair<string, object?> StageRegister = new("stage", "register");
    private static readonly KeyValuePair<string, object?> StageConfirm = new("stage", "confirm");
    private static readonly KeyValuePair<string, object?> StageActor = new("stage", "actor");
    private static readonly KeyValuePair<string, object?> StageComplete = new("stage", "complete");
    private static readonly KeyValuePair<string, object?> StageTotal = new("stage", "total");

    private static readonly KeyValuePair<string, object?> LeaseHit = new("lease", "hit");
    private static readonly KeyValuePair<string, object?> LeaseMiss = new("lease", "miss");

    private static readonly KeyValuePair<string, object?> PathRegistered = new("path", "registered");
    private static readonly KeyValuePair<string, object?> PathDirect = new("path", "direct");
    private static readonly KeyValuePair<string, object?> PathPublished = new("path", "published");
    private static readonly KeyValuePair<string, object?> PathActor = new("path", "actor");

    // One tag per PublishedReadFallback value, indexed by the value, so a sample allocates nothing.
    private static readonly KeyValuePair<string, object?>[] FallbackReasons = BuildFallbackReasons();

    private static KeyValuePair<string, object?>[] BuildFallbackReasons()
    {
        PublishedReadFallback[] values = Enum.GetValues<PublishedReadFallback>();
        KeyValuePair<string, object?>[] tags = new KeyValuePair<string, object?>[values.Length];

        foreach (PublishedReadFallback value in values)
            tags[(int)value] = new("reason", FallbackReasonName(value));

        return tags;
    }

    /// <summary>The <c>reason</c> tag value of a fallback.</summary>
    internal static string FallbackReasonName(PublishedReadFallback fallback) => fallback switch
    {
        PublishedReadFallback.Disabled => "disabled",
        PublishedReadFallback.ReadKind => "read_kind",
        PublishedReadFallback.SnapshotBound => "snapshot_bound",
        PublishedReadFallback.SnapshotBelowHead => "snapshot_below_head",
        PublishedReadFallback.Absent => "absent",
        PublishedReadFallback.Retracted => "retracted",
        PublishedReadFallback.ReplicationIntent => "replication_intent",
        PublishedReadFallback.ParkedHead => "parked_head",
        PublishedReadFallback.PrefixLock => "prefix_lock",
        PublishedReadFallback.PendingApply => "pending_apply",
        PublishedReadFallback.Unflushed => "unflushed",
        PublishedReadFallback.PreparedIntent => "prepared_intent",
        PublishedReadFallback.WriteIntent => "write_intent",
        PublishedReadFallback.MvccHeld => "mvcc_held",
        PublishedReadFallback.MvccStaged => "mvcc_staged",
        PublishedReadFallback.MvccPinMoved => "mvcc_pin_moved",
        PublishedReadFallback.PinInFlight => "pin_in_flight",
        PublishedReadFallback.IntentWait => "intent_wait",
        PublishedReadFallback.Expired => "expired",
        _ => "none"
    };

    /// <summary>Elapsed milliseconds between two <see cref="Stopwatch.GetTimestamp"/> readings.</summary>
    internal static double ElapsedMs(long start, long end) => (end - start) * 1000.0 / Stopwatch.Frequency;

    /// <summary>The coordinator registration of a transaction-scoped read, local or forwarded.</summary>
    internal static void RecordRegister(long start, long end) => StageMs.Record(ElapsedMs(start, end), StageRegister);

    /// <summary>
    /// The quorum-confirmed leadership check before a local read. <paramref name="leaseHit"/> is true when the
    /// check completed synchronously, which is the leader's lease answering it with the applied frontier already
    /// covering the commit frontier; false covers both asynchronous answers, the frontier wait and the round.
    /// </summary>
    internal static void RecordConfirm(long start, long end, bool leaseHit) =>
        StageMs.Record(ElapsedMs(start, end), StageConfirm, leaseHit ? LeaseHit : LeaseMiss);

    /// <summary>
    /// The step that reads the value: a mailbox round trip (mailbox wait, the turn, and any replication wait),
    /// or, when <paramref name="published"/> is true, the lookup of the owning actor's published head record on
    /// the caller's thread. A round trip that the published record refused is also recorded under
    /// <paramref name="fallback"/>, the guard that refused it.
    /// </summary>
    internal static void RecordActor(long start, long end, bool published, PublishedReadFallback fallback = PublishedReadFallback.None)
    {
        double elapsedMs = ElapsedMs(start, end);

        StageMs.Record(elapsedMs, StageActor, published ? PathPublished : PathActor);

        if (!published && fallback != PublishedReadFallback.None)
            FallbackMs.Record(elapsedMs, FallbackReasons[(int)fallback]);
    }

    /// <summary>The completion that folds a transaction-scoped read into the working set, local or forwarded.</summary>
    internal static void RecordComplete(long start, long end) => StageMs.Record(ElapsedMs(start, end), StageComplete);

    /// <summary>The whole get as seen by the read façade, tagged by whether it was transaction-scoped.</summary>
    internal static void RecordTotal(long start, long end, bool registered) =>
        StageMs.Record(ElapsedMs(start, end), StageTotal, registered ? PathRegistered : PathDirect);
}
