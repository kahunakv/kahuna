
using Kommander.Time;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Computes and enforces the recoverable HLC window of a resolved backup chain. Both the offline
/// restore path (<see cref="BackupService.RestoreTo"/>) and the join-existing bootstrap path
/// (<see cref="BootstrapHelper"/>) validate a target against these bounds <b>before</b> mutating any
/// backend or WAL, so a target below the base cut or above captured coverage is rejected up front
/// rather than silently seeding a state that was never reconstructed.
/// </summary>
internal static class BackupChainCoverage
{
    /// <summary>
    /// Returns the chain's recoverable window: <c>min</c> is the Full's recorded base cut — the
    /// earliest state the image represents — or <c>null</c> when unknown (a legacy full with no
    /// recorded cut); <c>max</c> is the newest captured entry across the whole chain.
    /// </summary>
    internal static (HLCTimestamp? min, HLCTimestamp max) Compute(IReadOnlyList<BackupManifest> chain)
    {
        HLCTimestamp? min = chain.Count > 0 ? chain[0].BaseCut : null;
        HLCTimestamp seed = min ?? HLCTimestamp.Zero;
        HLCTimestamp max = chain
            .SelectMany(m => m.PartitionRanges)
            .Select(r => r.ToHlc)
            .Aggregate(seed, (acc, hlc) => hlc.CompareTo(acc) > 0 ? hlc : acc);
        return (min, max);
    }

    /// <summary>
    /// Validates <paramref name="requestedTarget"/> against the chain's coverage and returns the
    /// effective target: <see cref="HLCTimestamp.Zero"/> resolves to the validated natural end
    /// (<c>max</c>). Fails closed when the lower bound is unknown or the target is out of range.
    /// </summary>
    /// <exception cref="BackupUnsupportedFormatException">The chain has no provable lower bound.</exception>
    /// <exception cref="BackupDriverException">The target is outside <c>[min, max]</c> (typed <c>TargetOutsideCoverage</c>).</exception>
    internal static HLCTimestamp Resolve(IReadOnlyList<BackupManifest> chain, HLCTimestamp requestedTarget)
    {
        (HLCTimestamp? min, HLCTimestamp max) = Compute(chain);

        if (min is null)
            throw new BackupUnsupportedFormatException(
                "Backup chain has no recorded base cut; its recoverable lower bound is unknown and it " +
                "cannot be safely restored or bootstrapped to a point in time.");

        HLCTimestamp lo = min.Value;
        HLCTimestamp target = requestedTarget == HLCTimestamp.Zero ? max : requestedTarget;

        // Upper bound at millisecond granularity: a wall-clock target is the inclusive END of its
        // millisecond (counter = max), so a target within the newest captured millisecond must not be
        // rejected merely because the newest captured entry sits at a lower counter/node within that
        // same millisecond — the restore filter (commit HLC ≤ target) still includes exactly what was
        // captured. Only a target in a strictly later millisecond is genuinely beyond coverage. The
        // lower bound stays a full-HLC comparison so a target below the base cut is still refused.
        if (target.CompareTo(lo) < 0 || target.L > max.L)
            throw new BackupDriverException(
                $"Target {target} is outside this chain's recoverable coverage [{lo}, {max}].")
                { TargetOutsideCoverage = true };

        return target;
    }

    /// <summary>
    /// Validates that the chain covers every data partition of the cluster it was captured from:
    /// the union of the manifests' covered sets must reach the newest recorded cluster partition
    /// set. Under per-partition replica placement a node captures only the partitions it hosts, so
    /// a single node's chain can be a strict subset of the cluster — restoring it alone would
    /// silently reconstruct a cluster missing the partitions hosted elsewhere. Chains with no
    /// coverage records (written before coverage existed) pass: they could only be produced under
    /// full replication, where one node's capture is the whole cluster by construction.
    /// </summary>
    /// <exception cref="BackupDriverException">
    /// The chain does not cover every cluster partition (typed <c>RestrictedCoverage</c>); the
    /// message names the missing partitions.
    /// </exception>
    internal static void ValidatePartitionCoverage(IReadOnlyList<BackupManifest> chain)
    {
        List<int>? clusterPartitions = null;
        HashSet<int> covered = [];
        bool anyRecorded = false;

        foreach (BackupManifest m in chain)
        {
            // The newest recorded cluster set wins; within a chain the topology-generation link
            // check keeps the partition layout constant anyway, so "newest" is a formality.
            if (m.ClusterPartitions is not null)
                clusterPartitions = m.ClusterPartitions;

            if (m.CoveredPartitions is not null)
            {
                anyRecorded = true;
                covered.UnionWith(m.CoveredPartitions);
            }
        }

        if (!anyRecorded || clusterPartitions is null)
            return;

        List<int> missing = [.. clusterPartitions.Where(p => !covered.Contains(p)).Order()];
        if (missing.Count > 0)
            throw new BackupDriverException(
                $"This backup chain does not cover the whole cluster: partition(s) {string.Join(", ", missing)} " +
                "were hosted on other nodes when it was taken (per-partition replica placement was active), " +
                "so restoring it would silently reconstruct a cluster missing their data. Restore a chain " +
                "that covers every partition, or combine per-node backups once composed restores are supported.")
                { RestrictedCoverage = true };
    }
}
