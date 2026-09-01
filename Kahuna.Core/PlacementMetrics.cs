using System.Diagnostics.Metrics;

namespace Kahuna.Server;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for per-partition replica placement.
/// These are how a badly placed cluster is told apart from a healthy one: a hot forward path or a
/// high hint-miss rate means keys are being served far from their data. All counters except
/// <see cref="ChainedForwardsRefused"/> are inert under full replication (replication factor 0) —
/// every partition is hosted locally, so nothing resolves a remote target from the placement map.
/// </summary>
internal static class PlacementMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>Partitions this node started hosting (replica gained), per committed map transition.</summary>
    internal static readonly Counter<long> ReplicasGained =
        Meter.CreateCounter<long>("kahuna.placement.replicas_gained", description: "Partitions this node started hosting.");

    /// <summary>Partitions this node stopped hosting (replica lost), per committed map transition.</summary>
    internal static readonly Counter<long> ReplicasLost =
        Meter.CreateCounter<long>("kahuna.placement.replicas_lost", description: "Partitions this node stopped hosting.");

    /// <summary>Operations on non-hosted partitions that resolved a remote node to forward to.</summary>
    internal static readonly Counter<long> ForwardsResolved =
        Meter.CreateCounter<long>("kahuna.placement.forwards_resolved", description: "Non-hosted operations that resolved a forward target.");

    /// <summary>
    /// Operations on non-hosted partitions with no forward target (no hint, no known replica) —
    /// the caller answers MustRetry. A sustained rate means placement views are stale.
    /// </summary>
    internal static readonly Counter<long> ForwardsUnresolved =
        Meter.CreateCounter<long>("kahuna.placement.forwards_unresolved", description: "Non-hosted operations with no forward target.");

    /// <summary>
    /// Forwards refused because the request had already been forwarded here by another node. A
    /// sustained rate means two nodes hold disagreeing leadership or placement views for the same
    /// partition; a brief burst around an election is normal — the caller retries.
    /// </summary>
    internal static readonly Counter<long> ChainedForwardsRefused =
        Meter.CreateCounter<long>("kahuna.placement.chained_forwards_refused", description: "Forwards refused because the request already arrived forwarded.");

    /// <summary>Forward resolutions answered by the gossip-fed leader hint.</summary>
    internal static readonly Counter<long> LeaderHintHits =
        Meter.CreateCounter<long>("kahuna.placement.leader_hint_hits", description: "Forward targets answered by the gossiped leader hint.");

    /// <summary>
    /// Forward resolutions where the hint was absent or useless (unknown, expired, or pointing at
    /// this node) and the replica-set fallback had to answer. The miss rate is
    /// misses / (hits + misses).
    /// </summary>
    internal static readonly Counter<long> LeaderHintMisses =
        Meter.CreateCounter<long>("kahuna.placement.leader_hint_misses", description: "Forward resolutions the leader hint could not answer.");
}
