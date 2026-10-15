
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence.Pitr;
using Kommander.Time;

namespace Kahuna.Server;

/// <summary>
/// Runs the base image step of <c>--pitr-bootstrap-from</c> at startup, before the host builds the
/// persistence backend: it is the only point where the storage path is not yet open. The replication
/// service then reads the returned plan to replay the chain, seed the WAL and join.
/// </summary>
internal static partial class PitrBootstrapStartup
{
    /// <summary>The value of <c>--pitr-bootstrap-from</c> that selects the newest valid chain.</summary>
    internal const string LatestBackup = "latest";

    /// <summary>
    /// Validates the bootstrap flags and puts the base image in place. Returns null when the node starts
    /// without <c>--pitr-bootstrap-from</c>. Throws when the start must stop; the store then holds no
    /// backend data that the step published.
    /// </summary>
    internal static async Task<PitrBootstrapPlan?> PrepareAsync(
        KahunaCommandLineOptions opts,
        KahunaConfiguration configuration,
        bool standalone,
        ILogger logger,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(opts.PitrBootstrapFrom))
            return null;

        if (standalone || !opts.RaftJoinExisting)
            throw new InvalidOperationException(
                "--pitr-bootstrap-from requires --join-existing and --initial-cluster (the node must join an existing cluster after seeding).");

        if (string.IsNullOrWhiteSpace(opts.PitrBackupDir))
            throw new InvalidOperationException(
                "--pitr-bootstrap-from requires --pitr-backup-dir pointing to the backup catalog/artifacts directory.");

        Guid? leafBackupId = ParseLeaf(opts.PitrBootstrapFrom);

        // Shared resolver: bootstrap and the regular restore API (RestoreToAsync) map a millisecond target to
        // the same inclusive end-of-millisecond HLC, so they can never diverge.
        HLCTimestamp targetTime = PitrTargetResolver.FromUnixMilliseconds(opts.PitrTargetTimeMs);

        PitrBootstrapPlan plan;
        try
        {
            plan = await BackupFacade.PrepareBootstrapAsync(
                configuration,
                opts.PitrBackupDir,
                leafBackupId,
                targetTime,
                TimeSpan.FromSeconds(opts.PitrWindowSeconds),
                TimeSpan.FromSeconds(opts.BaseSnapshotIntervalSeconds),
                opts.PitrBootstrapFallbackToJoin,
                logger,
                ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogBaseImageFailed(logger, ex);
            throw;
        }

        switch (plan.Action)
        {
            case PitrBootstrapAction.Seed:
                LogBaseImageReady(logger, plan.LeafBackupId, plan.Reason);
                break;

            case PitrBootstrapAction.Skip:
                LogSeedSkipped(logger, plan.Reason);
                break;

            case PitrBootstrapAction.Fallback:
                LogSeedFallback(logger, plan.Reason);
                break;
        }

        return plan;
    }

    private static Guid? ParseLeaf(string value)
    {
        string trimmed = value.Trim();
        if (string.Equals(trimmed, LatestBackup, StringComparison.OrdinalIgnoreCase))
            return null;

        if (Guid.TryParse(trimmed, out Guid leaf))
            return leaf;

        throw new InvalidOperationException(
            $"--pitr-bootstrap-from must be a backup ID (GUID) or '{LatestBackup}'; got '{value}'.");
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "PITR bootstrap: base image ready for the chain ending at {LeafBackupId} ({Reason}); the chain replay and the WAL seed run before the join.")]
    private static partial void LogBaseImageReady(ILogger logger, Guid? leafBackupId, string reason);

    [LoggerMessage(Level = LogLevel.Warning, Message = "PITR bootstrap: seed skipped because {Reason}. The node joins with its current store.")]
    private static partial void LogSeedSkipped(ILogger logger, string reason);

    [LoggerMessage(Level = LogLevel.Warning, Message = "PITR bootstrap: seed skipped because the backup cannot help: {Reason} The store stays empty and the node joins through the normal catch-up (--pitr-bootstrap-fallback-to-join).")]
    private static partial void LogSeedFallback(ILogger logger, string reason);

    [LoggerMessage(Level = LogLevel.Critical, Message = "PITR bootstrap failed before the store opened; the node will not start. Check --pitr-backup-dir, --pitr-bootstrap-from, --pitr-target-time-ms and --storage-revision, and verify the backup chain is valid and within the retention window.")]
    private static partial void LogBaseImageFailed(ILogger logger, Exception ex);
}
