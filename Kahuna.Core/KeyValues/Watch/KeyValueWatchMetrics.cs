using System.Diagnostics.Metrics;

namespace Kahuna.Server.KeyValues.Watch;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for key-value watch streams. A silent drop is the failure
/// mode a watch must never have, so every way a stream can end without its consumer asking is counted by cause.
/// </summary>
internal static class KeyValueWatchMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>Open watches on the node, in-process and remote. Every watch is counted once, however many
    /// partitions it covers.</summary>
    internal static readonly UpDownCounter<long> ActiveWatches =
        Meter.CreateUpDownCounter<long>(
            "kahuna.kv.watch.active",
            description: "Open key-value watches on the node.");

    /// <summary>Open remote watch streams on the node, tagged by transport (grpc, rest). One gRPC stream can carry
    /// many watches.</summary>
    internal static readonly UpDownCounter<long> RemoteStreams =
        Meter.CreateUpDownCounter<long>(
            "kahuna.kv.watch.remote_streams",
            description: "Open remote watch streams on the node, by transport.");

    /// <summary>Watch frames written to remote streams, tagged by transport.</summary>
    internal static readonly Counter<long> RemoteFramesSent =
        Meter.CreateCounter<long>(
            "kahuna.kv.watch.remote_frames_sent_total",
            description: "Watch frames written to remote watch streams, by transport.");

    /// <summary>Events delivered to watch subscriptions, one per matched subscription.</summary>
    internal static readonly Counter<long> EventsDelivered =
        Meter.CreateCounter<long>(
            "kahuna.kv.watch.events_delivered_total",
            description: "Committed key-value mutations delivered to watch subscriptions (one per matched subscription).");

    /// <summary>
    /// Buffered frames dropped when a stream ended without its consumer asking. Every drop is announced to the
    /// consumer by the frame that ended the stream.
    /// </summary>
    internal static readonly Counter<long> FramesDropped =
        Meter.CreateCounter<long>(
            "kahuna.kv.watch.frames_dropped_total",
            description: "Buffered watch frames dropped when a stream ended with Lagged.");

    /// <summary>Streams ended with Lagged, tagged by reason. A sustained overflow rate means consumers read too
    /// slowly for their capacity; any other reason follows a state transfer, an apply failure or a range change.</summary>
    internal static readonly Counter<long> StreamsLagged =
        Meter.CreateCounter<long>(
            "kahuna.kv.watch.streams_lagged_total",
            description: "Watch streams ended with Lagged, by reason.");

    /// <summary>Empty progress-tick entries this node proposed on idle partitions it leads, so their watch
    /// watermark keeps moving.</summary>
    internal static readonly Counter<long> ProgressTicks =
        Meter.CreateCounter<long>(
            "kahuna.kv.watch.progress_ticks_total",
            description: "Progress-tick entries proposed on idle partitions while the commit admission window is on.");

    /// <summary>Watch requests refused, tagged by the refusal.</summary>
    internal static readonly Counter<long> WatchesRefused =
        Meter.CreateCounter<long>(
            "kahuna.kv.watch.refused_total",
            description: "Watch requests refused, by refusal type.");
}
