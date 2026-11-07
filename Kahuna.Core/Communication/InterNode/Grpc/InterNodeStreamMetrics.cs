using System.Diagnostics.Metrics;

namespace Kahuna.Server.Communication.Internode.Grpc;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for the shared duplex streams the inter-node
/// batcher keeps to each peer. Every instrument is tagged by <c>peer</c> (the peer's URL, one value per
/// cluster member), so a soak can read a re-open storm against a dead peer from the counters without
/// the log: a steady open rate with a matching failure rate is the storm, and the refusal counter is
/// how much of the offered load the backoff answered without an open.
/// </summary>
internal static class InterNodeStreamMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>
    /// Shared stream sets the batcher opened to a peer: one set is the lock and key-value streams
    /// on every pooled channel to that peer. Steady state opens one set per peer for the process
    /// lifetime; a sustained rate means the peer refuses or drops them.
    /// </summary>
    internal static readonly Counter<long> StreamOpens =
        Meter.CreateCounter<long>(
            "kahuna.internode.stream_opens_total",
            description: "Shared stream sets the inter-node batcher opened to a peer, tagged by peer.");

    /// <summary>
    /// Shared stream sets torn down after a write failed or stalled, a response stream closed or the
    /// peer refused the stream. Each one steps the peer's re-open backoff.
    /// </summary>
    internal static readonly Counter<long> StreamFailures =
        Meter.CreateCounter<long>(
            "kahuna.internode.stream_failures_total",
            description: "Shared stream sets torn down after a failed or stalled write, a closed response stream or a refusal by the peer, tagged by peer.");

    /// <summary>
    /// Forwarded requests answered unavailable without a stream open because the peer's re-open
    /// backoff was in force. Every one of them becomes the operation's own retryable answer.
    /// </summary>
    internal static readonly Counter<long> RequestsRefusedByBackoff =
        Meter.CreateCounter<long>(
            "kahuna.internode.requests_refused_by_backoff_total",
            description: "Forwarded requests refused without a stream open because the peer's re-open backoff was in force, tagged by peer.");
}
