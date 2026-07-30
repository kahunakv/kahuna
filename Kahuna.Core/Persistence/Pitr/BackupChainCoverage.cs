
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
}
