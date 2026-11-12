using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// How a transactional read that asked to wait behind a live foreign write intent finished, and how a set that can
/// no longer commit was answered. One tag set per instrument, all fixed.
/// </summary>
internal static class IntentWaitMetrics
{
    private static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>Reads parked behind a live foreign write intent. A read that parks again after a wake counts once
    /// for each park.</summary>
    internal static readonly Counter<long> Parked =
        Meter.CreateCounter<long>(
            "kahuna.kv.intent_wait.parked",
            description: "Transactional reads parked behind a live write intent of another transaction.");

    /// <summary>
    /// Reads that asked to wait and did not park, or that finished a wait, tagged by <c>exit</c>:
    /// <c>settled</c> (the intent went away and the read was served), <c>limit</c> (the wait limit passed and the read
    /// pinned the committed head below the intent), <c>bound</c> (too many reads already parked on the key or the
    /// actor), <c>wait_die</c> (a younger requester that holds locks), <c>moved</c> (the node no longer leads the key
    /// when the read woke; answered <c>MustRetry</c>).
    /// </summary>
    internal static readonly Counter<long> Exits =
        Meter.CreateCounter<long>(
            "kahuna.kv.intent_wait.exits",
            description: "Intent waits that ended or never started, tagged by exit.");

    /// <summary>Wall time from the first park of a read to its answer, on the serving node.</summary>
    internal static readonly Histogram<double> WaitMs =
        Meter.CreateHistogram<double>(
            "kahuna.kv.intent_wait.duration",
            unit: "ms",
            description: "Time a transactional read waited behind a live write intent of another transaction.");

    /// <summary>Transactional sets answered <c>Aborted</c> at once because they could no longer commit.</summary>
    internal static readonly Counter<long> DoomedSetAborts =
        Meter.CreateCounter<long>(
            "kahuna.kv.intent_wait.doomed_set_aborts",
            description: "Transactional sets refused by a live foreign write intent on a key the transaction already read, answered Aborted at once.");

    private static readonly KeyValuePair<string, object?> ExitSettled = new("exit", "settled");
    private static readonly KeyValuePair<string, object?> ExitLimit = new("exit", "limit");
    private static readonly KeyValuePair<string, object?> ExitBound = new("exit", "bound");
    private static readonly KeyValuePair<string, object?> ExitWaitDie = new("exit", "wait_die");
    private static readonly KeyValuePair<string, object?> ExitMoved = new("exit", "moved");

    internal static void RecordExit(IntentWaitExit exit) => Exits.Add(1, exit switch
    {
        IntentWaitExit.Settled => ExitSettled,
        IntentWaitExit.Limit => ExitLimit,
        IntentWaitExit.Bound => ExitBound,
        IntentWaitExit.WaitDie => ExitWaitDie,
        _ => ExitMoved
    });

    internal static void RecordWait(long startTimestamp) =>
        WaitMs.Record((Stopwatch.GetTimestamp() - startTimestamp) * 1000.0 / Stopwatch.Frequency);
}

/// <summary>How an intent wait ended, or why it did not start (see <see cref="IntentWaitMetrics.Exits"/>).</summary>
internal enum IntentWaitExit
{
    Settled,
    Limit,
    Bound,
    WaitDie,
    Moved
}
