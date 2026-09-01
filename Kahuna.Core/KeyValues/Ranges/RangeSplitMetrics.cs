using System.Diagnostics.Metrics;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for the range-split and range-merge
/// subsystems. Counters are cumulative and safe to observe from any thread.
///
/// <para>
/// Instrument naming follows the OpenTelemetry semantic conventions (dot-separated lowercase).
/// Prometheus exporters typically translate dots to underscores automatically.
/// </para>
///
/// <para>
/// All instruments are tagged with <c>keyspace</c> where the split or merge event is scoped
/// to a specific key space, allowing per-space dashboards.
/// </para>
/// </summary>
internal static class RangeSplitMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>
    /// Total range splits successfully committed. Incremented once per successful
    /// <c>ExecuteSplitAsync</c> call (covers both the count and the load branch).
    /// </summary>
    internal static readonly Counter<long> Splits =
        Meter.CreateCounter<long>(
            "kahuna.range.splits",
            description: "Total range splits committed (count branch + load branch combined).");

    /// <summary>
    /// Splits refused by the indivisibility guard: no split key could put each child below
    /// the configured write-imbalance threshold (all traffic concentrated on one key).
    /// Also incremented by the load branch when a hot range is too small to divide while its
    /// writes concentrate on a single key — without that case a hot range below the sample
    /// floor would be declined on every pass with no counter moving at all.
    /// A persistently high value means a hot-key workload that splitting cannot relieve.
    /// </summary>
    internal static readonly Counter<long> IndivisibleRefusals =
        Meter.CreateCounter<long>(
            "kahuna.range.split.indivisible_refusals",
            description: "Splits refused because no split key achieves acceptable write-balance (hot-key workload).");

    /// <summary>
    /// Descriptors skipped because they are within the post-split settle window. High values
    /// relative to <see cref="Splits"/> indicate the settle window is working as intended;
    /// a value that never drops to zero may indicate the window is too long.
    /// </summary>
    internal static readonly Counter<long> SettleSkips =
        Meter.CreateCounter<long>(
            "kahuna.range.split.settle_skips",
            description: "Descriptors skipped because they are within the post-split settle window.");

    /// <summary>
    /// Load-triggered splits skipped because no active peer node was visible to host the new
    /// child partition's leader. Indicates load splitting is inert (single-node cluster or all
    /// peers silent). See <c>EnableLeaderBalancer</c> and peer connectivity.
    /// </summary>
    internal static readonly Counter<long> NoReliefSkips =
        Meter.CreateCounter<long>(
            "kahuna.range.split.no_relief_skips",
            description: "Load-splits skipped because no peer node is available to host the child partition.");

    /// <summary>
    /// Moves refused because the moving half still held unsettled durable intents. Unlike
    /// <see cref="IndivisibleRefusals"/> this is a "not right now": the range is being written and
    /// its coordinators have not all decided. A count that keeps climbing while
    /// <see cref="Splits"/> stays flat means the range is too busy to drain inside the move's
    /// settle timeout, so it is being retried on a lengthening backoff and never dividing.
    /// </summary>
    internal static readonly Counter<long> DrainRefusals =
        Meter.CreateCounter<long>(
            "kahuna.range.split.drain_refusals",
            description: "Moves refused because the moving range still held unsettled durable intents.");

    /// <summary>
    /// Merge candidate pairs skipped because at least one partition's write rate meets or
    /// exceeds the split threshold. Merging would immediately re-trigger a split; the pair
    /// is left to cool before the next checker pass.
    /// </summary>
    internal static readonly Counter<long> MergeWarmSkips =
        Meter.CreateCounter<long>(
            "kahuna.range.merge.warm_skips",
            description: "Merge candidates skipped because at least one partition is warm (ops ≥ split threshold).");
}
