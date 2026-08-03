using System.Diagnostics.Metrics;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for the periodic collect/eviction cycle
/// (<see cref="TryCollectHandler"/>), recorded once per cycle on the actor's own mailbox thread.
///
/// <para>The collect cycle runs on the same thread that serves key-value operations, so a long cycle
/// directly parks the actor. That makes <c>kahuna.collect.cycle.duration</c> the instrument to reach
/// for when request latency shows periodic spikes that load alone does not explain: a spike in this
/// histogram that lines up with a latency spike identifies collection as the cause, and one that does
/// not rules it out.</para>
///
/// <para>Tags are bounded, fixed vocabularies — an eviction class or a scan name, never a key,
/// partition id, or transaction id — so no instrument here can grow unbounded time series. Counters
/// are only recorded when non-zero, so an idle store produces cycle counts and durations and nothing
/// else.</para>
/// </summary>
internal static class CollectMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>Collect cycles executed. The denominator for every per-cycle average below.</summary>
    internal static readonly Counter<long> Cycles =
        Meter.CreateCounter<long>(
            "kahuna.collect.cycles",
            description: "Collect/eviction cycles executed.");

    /// <summary>
    /// Wall-clock duration of a collect cycle. Recorded on the mailbox thread, so this time is time the
    /// actor is not serving key-value operations.
    /// </summary>
    internal static readonly Histogram<double> CycleDuration =
        Meter.CreateHistogram<double>(
            "kahuna.collect.cycle.duration",
            unit: "ms",
            description: "Duration of a collect/eviction cycle, during which the actor serves no operations.");

    /// <summary>
    /// Entries evicted, tagged by <c>reason</c>: <c>tombstone</c> (deleted/undefined drained from the
    /// tombstone queue), <c>expiry</c> (TTL elapsed), <c>lru</c> (evicted under entry/byte budget
    /// pressure), or <c>idle</c> (untouched past <c>CacheEntryTtl</c>). A sustained <c>lru</c> rate means
    /// the actor is running against its budget and is re-reading evicted entries from the backend.
    /// </summary>
    internal static readonly Counter<long> Evicted =
        Meter.CreateCounter<long>(
            "kahuna.collect.evicted",
            description: "Entries evicted by a collect cycle, by eviction class.");

    /// <summary>
    /// Entries examined but not necessarily evicted, tagged by <c>scan</c>: <c>expiry</c> or <c>lru</c>.
    /// Inspected far exceeding evicted means cycles are spending their budget walking entries that are
    /// pinned (dirty or intent-held) rather than reclaiming anything.
    /// </summary>
    internal static readonly Counter<long> Inspected =
        Meter.CreateCounter<long>(
            "kahuna.collect.inspected",
            description: "Entries inspected by a collect cycle, by scan.");

    /// <summary>
    /// Cycles that ended with work carried past their budget and self-scheduled a follow-up. A rate
    /// approaching <see cref="Cycles"/> means collection is not keeping up and is running continuously.
    /// </summary>
    internal static readonly Counter<long> Backlogged =
        Meter.CreateCounter<long>(
            "kahuna.collect.backlogged",
            description: "Collect cycles that carried work past their budget and scheduled a follow-up.");

    // Pre-built so the per-cycle recording path allocates nothing. A string tag value is a reference,
    // so these box nothing when passed to Add.
    private static readonly KeyValuePair<string, object?> ReasonTombstone = new("reason", "tombstone");
    private static readonly KeyValuePair<string, object?> ReasonExpiry = new("reason", "expiry");
    private static readonly KeyValuePair<string, object?> ReasonLru = new("reason", "lru");
    private static readonly KeyValuePair<string, object?> ReasonIdle = new("reason", "idle");
    private static readonly KeyValuePair<string, object?> ScanExpiry = new("scan", "expiry");
    private static readonly KeyValuePair<string, object?> ScanLru = new("scan", "lru");

    /// <summary>
    /// Publishes one completed cycle. Called at the end of <see cref="TryCollectHandler.Execute"/> with
    /// the same values it records in its stats struct, so the metrics and that struct cannot drift.
    /// Zero-valued counters are skipped: an idle store runs cycles constantly and recording six
    /// no-op measurements each time would be pure overhead.
    /// </summary>
    internal static void RecordCycle(in CollectCycleStats stats)
    {
        Cycles.Add(1);
        CycleDuration.Record(stats.ElapsedMs);

        if (stats.TombstoneEvicted > 0)
            Evicted.Add(stats.TombstoneEvicted, ReasonTombstone);

        if (stats.ExpiryEvicted > 0)
            Evicted.Add(stats.ExpiryEvicted, ReasonExpiry);

        if (stats.LruEvicted > 0)
            Evicted.Add(stats.LruEvicted, ReasonLru);

        if (stats.IdleEvicted > 0)
            Evicted.Add(stats.IdleEvicted, ReasonIdle);

        if (stats.ExpiryInspected > 0)
            Inspected.Add(stats.ExpiryInspected, ScanExpiry);

        if (stats.LruVisited > 0)
            Inspected.Add(stats.LruVisited, ScanLru);

        if (stats.Backlog)
            Backlogged.Add(1);
    }
}
