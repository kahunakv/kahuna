using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Kahuna.Communication.External.Grpc.KeyValues;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for the ordered lane of an inbound inter-node stream. The
/// lane runs its handlers one at a time per stream, in arrival order, and each handler holds its turn until its
/// service call has answered. A request on the lane therefore waits for every earlier lane request on the same
/// stream, and that wait is invisible in the handler's own timings. The tag set is fixed: one value per request
/// type that takes a lane turn.
/// </summary>
internal static class InterNodeLaneMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>
    /// Bucket boundaries for <see cref="OrderedLaneWaitMs"/>, in milliseconds. A free lane answers in
    /// microseconds; a lane behind a durable request waits for that request's Raft round, which is tens of
    /// milliseconds, and a queue of them reaches hundreds.
    /// </summary>
    private static readonly InstrumentAdvice<double> OrderedLaneWaitAdvice = new()
    {
        HistogramBucketBoundaries = [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000]
    };

    /// <summary>
    /// Time a lane request waited for the earlier lane requests on its stream, from the moment the read loop gave
    /// it its turn to the moment its predecessor completed, tagged by <c>request</c>. One sample per lane
    /// request, zero when the lane was free. The wait ends before the handler's own service call starts, so the
    /// handler's time is not inside it.
    /// </summary>
    internal static readonly Histogram<double> OrderedLaneWaitMs =
        Meter.CreateHistogram<double>(
            "kahuna.internode.ordered_lane_wait_ms",
            unit: "ms",
            description: "Wait of an inbound inter-node request for the earlier ordered-lane requests on its stream, tagged by request type.",
            tags: null,
            advice: OrderedLaneWaitAdvice);

    private static readonly KeyValuePair<string, object?> RequestSeedRange = new("request", "seed_range");
    private static readonly KeyValuePair<string, object?> RequestRemoveRange = new("request", "remove_range");
    private static readonly KeyValuePair<string, object?> RequestGetRangeLocks = new("request", "get_range_locks");
    private static readonly KeyValuePair<string, object?> RequestImportRangeLocks = new("request", "import_range_locks");
    private static readonly KeyValuePair<string, object?> RequestImportReceipts = new("request", "import_receipts");
    private static readonly KeyValuePair<string, object?> RequestDurableOperation = new("request", "durable_operation");
    private static readonly KeyValuePair<string, object?> RequestDurableBundle = new("request", "durable_bundle");
    private static readonly KeyValuePair<string, object?> RequestDurableOnePhase = new("request", "durable_one_phase");
    private static readonly KeyValuePair<string, object?> RequestDurableDecision = new("request", "durable_decision");
    private static readonly KeyValuePair<string, object?> RequestLookupRecord = new("request", "lookup_record");
    private static readonly KeyValuePair<string, object?> RequestRangePage = new("request", "range_page");
    private static readonly KeyValuePair<string, object?> RequestRangeTransactionState = new("request", "range_transaction_state");
    private static readonly KeyValuePair<string, object?> RequestOther = new("request", "other");

    private static KeyValuePair<string, object?> RequestTag(GrpcServerBatchType type) => type switch
    {
        GrpcServerBatchType.ServerTryEnsureKeyRangeSeeded => RequestSeedRange,
        GrpcServerBatchType.ServerTryEnsureKeyRangeRemoved => RequestRemoveRange,
        GrpcServerBatchType.ServerTryGetRangeLocks => RequestGetRangeLocks,
        GrpcServerBatchType.ServerTryImportRangeLocks => RequestImportRangeLocks,
        GrpcServerBatchType.ServerImportCompletionReceipts => RequestImportReceipts,
        GrpcServerBatchType.ServerDurableOperation => RequestDurableOperation,
        GrpcServerBatchType.ServerDurableBundle => RequestDurableBundle,
        GrpcServerBatchType.ServerDurableOnePhase => RequestDurableOnePhase,
        GrpcServerBatchType.ServerDurableDecision => RequestDurableDecision,
        GrpcServerBatchType.ServerLookupTransactionRecord => RequestLookupRecord,
        GrpcServerBatchType.ServerReplicateKeyValueRangePage => RequestRangePage,
        GrpcServerBatchType.ServerGetRangeTransactionState => RequestRangeTransactionState,
        _ => RequestOther
    };

    /// <summary>Records the wait of one lane request whose turn was given at <paramref name="turnGivenAt"/>
    /// (a <see cref="Stopwatch.GetTimestamp"/> value).</summary>
    internal static void LaneWaited(GrpcServerBatchType type, long turnGivenAt)
    {
        if (OrderedLaneWaitMs.Enabled)
            OrderedLaneWaitMs.Record(Stopwatch.GetElapsedTime(turnGivenAt).TotalMilliseconds, RequestTag(type));
    }
}
