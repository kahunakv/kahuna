
using Kommander;
using Kommander.System;
using Kommander.Time;
using Kahuna.Server.Diagnostics;
using Kahuna.Server.Persistence.Pitr;

namespace Kahuna.Services;

/// <summary>
/// The ReplicationService class is a background service responsible for managing the replication process
/// within a Kahuna cluster. This class handles interactions between the Raft distributed consensus
/// algorithm and the application's main functionality by subscribing to and responding to Raft events.
/// </summary>
public sealed partial class ReplicationService : BackgroundService //, IDisposable
{
    private readonly IKahuna kahuna;

    private readonly IRaft raft;

    private readonly KahunaCommandLineOptions options;

    private readonly ThreadStatsLogger statsLogger;

    private readonly ILogger<IRaft> logger;

    private readonly string localEndpoint;

    private readonly PitrBootstrapPlan? bootstrapPlan;

    /// <param name="bootstrapPlan">
    /// The outcome of the base image step of a PITR bootstrap, which runs at startup before the persistence
    /// backend opens. Registered only when the node starts with <c>--pitr-bootstrap-from</c>.
    /// </param>
    public ReplicationService(IKahuna kahuna, IRaft raft, KahunaCommandLineOptions options, ILogger<IRaft> logger, PitrBootstrapPlan? bootstrapPlan = null)
    {
        this.kahuna = kahuna;
        this.raft = raft;
        this.options = options;
        this.logger = logger;
        this.bootstrapPlan = bootstrapPlan;
        this.statsLogger = new(logger);
        localEndpoint = raft.GetLocalEndpoint();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (raft.Configuration.Host == "*")
            return;

        raft.OnLogRestored += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError += kahuna.OnReplicationError;
        raft.OnLeaderChanged += kahuna.OnLeaderChanged;
        raft.OnLeadershipLost += kahuna.OnLeadershipLost;
        raft.OnMembershipChanged += OnMembershipChanged;

        // The base image step already ran, before the backend opened, and decided whether a seed is due. The
        // flag combination was validated there as well.
        if (bootstrapPlan is { Action: PitrBootstrapAction.Seed, LeafBackupId: { } leafId })
        {
            // Shared resolver: bootstrap and the regular restore API (RestoreToAsync) map a millisecond
            // target to the same inclusive end-of-millisecond HLC, so they can never diverge.
            HLCTimestamp targetTime = PitrTargetResolver.FromUnixMilliseconds(options.PitrTargetTimeMs);
            TimeSpan baseSnapshotInterval = TimeSpan.FromSeconds(options.BaseSnapshotIntervalSeconds);

            try
            {
                // The retention window was checked before the base image was downloaded. It is not checked
                // again: a long download could close it now, and every restart would then continue the seed
                // into the same refusal. A seed below the leader's compaction floor is still correct; the
                // leader catches the node up by snapshot.
                await kahuna.BootstrapFromPitrBackupAsync(
                    options.PitrBackupDir, leafId, targetTime, raft.WalAdapter, Timeout.InfiniteTimeSpan, baseSnapshotInterval);

                // Recorded only after the seed returned, so a crash before this line continues the seed on the
                // next start instead of skipping it.
                BackupFacade.CompleteBootstrap(bootstrapPlan, logger);
                LogBootstrapComplete(logger, leafId, targetTime);
            }
            catch (Exception ex)
            {
                LogBootstrapFailed(logger, ex);
                throw;
            }
        }

        if (options.RaftJoinExisting)
        {
            List<string> seeds = options.InitialCluster?.ToList() ?? [];
            if (seeds.Count == 0)
                throw new InvalidOperationException(
                    "--join-existing requires --initial-cluster to list at least one seed endpoint.");

            try
            {
                await raft.JoinCluster(seeds, stoppingToken);
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("promotion permanently blocked", StringComparison.OrdinalIgnoreCase))
            {
                LogJoinCompactionFloor(logger, ex);
                throw;
            }
            catch (TimeoutException ex)
            {
                LogJoinTimeout(logger, ex);
                throw;
            }
            catch (Exception ex)
            {
                LogJoinFailed(logger, ex);
                throw;
            }
        }
        else
        {
            await raft.JoinCluster(stoppingToken);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        raft.OnLogRestored -= kahuna.OnLogRestored;
        raft.OnReplicationReceived -= kahuna.OnReplicationReceived;
        raft.OnReplicationError -= kahuna.OnReplicationError;
        raft.OnLeaderChanged -= kahuna.OnLeaderChanged;
        raft.OnMembershipChanged -= OnMembershipChanged;

        if (options.RaftGracefulLeaveOnShutdown)
        {
            using CancellationTokenSource deadline = new(TimeSpan.FromSeconds(10));
            using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);
            try
            {
                // LeaveCluster absorbs InsufficientVoters internally (resp.Terminal path)
                // and returns without throwing, so no special case is needed here.
                await raft.LeaveCluster(dispose: false).WaitAsync(cts.Token);
            }
            catch (OperationCanceledException)
            {
                LogLeaveTimeout(logger);
            }
            catch (Exception ex)
            {
                LogLeaveError(logger, ex);
            }
        }

        await base.StopAsync(cancellationToken);
    }

    private void OnMembershipChanged(ClusterMembership membership)
    {
        long version = membership.MembershipVersion;
        int count = membership.Members.Count;
        ClusterMemberRole role = membership.Members.FirstOrDefault(m => m.Endpoint == localEndpoint)?.Role
            ?? ClusterMemberRole.NotMember;
        LogMembershipChanged(logger, version, count, role);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Membership advanced to version {Version}: {MemberCount} member(s), local role {LocalRole}")]
    private static partial void LogMembershipChanged(ILogger logger, long version, int memberCount, ClusterMemberRole localRole);

    [LoggerMessage(Level = LogLevel.Information, Message = "PITR bootstrap from backup {LeafBackupId} at target time {TargetTime} completed successfully; proceeding to join cluster.")]
    private static partial void LogBootstrapComplete(ILogger logger, Guid leafBackupId, HLCTimestamp targetTime);

    [LoggerMessage(Level = LogLevel.Critical, Message = "PITR bootstrap failed. Check --pitr-backup-dir, --pitr-bootstrap-from, and --pitr-target-time-ms, and verify the backup chain is valid and within the retention window.")]
    private static partial void LogBootstrapFailed(ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Critical, Message = "Seed join permanently blocked — the joining node's start index is below the WAL compaction floor on one or more partitions. " +
        "Mitigation: restore from a PITR backup taken within the retention window so the start index lands above the floor, then retry. " +
        "Upstream snapshot install (the unconditional fix) is not yet implemented.")]
    private static partial void LogJoinCompactionFloor(ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Critical, Message = "Seed join timed out before the node was admitted as a Learner or promoted to Voter. " +
        "Check that the seed endpoints in --initial-cluster are reachable and that the cluster has an elected leader.")]
    private static partial void LogJoinTimeout(ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Critical, Message = "Seed join failed with an unexpected error. The node will not start.")]
    private static partial void LogJoinFailed(ILogger logger, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Graceful leave timed out; proceeding with shutdown")]
    private static partial void LogLeaveTimeout(ILogger logger);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Graceful leave failed; proceeding with shutdown")]
    private static partial void LogLeaveError(ILogger logger, Exception ex);
}
