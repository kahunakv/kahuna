
using Kommander;
using Kommander.System;
using Kommander.Time;
using Kahuna.Server.Diagnostics;

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

    public ReplicationService(IKahuna kahuna, IRaft raft, KahunaCommandLineOptions options, ILogger<IRaft> logger)
    {
        this.kahuna = kahuna;
        this.raft = raft;
        this.options = options;
        this.logger = logger;
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
        raft.OnMembershipChanged += OnMembershipChanged;

        if (options.PitrBootstrapFrom.HasValue)
        {
            if (!options.RaftJoinExisting)
                throw new InvalidOperationException(
                    "--pitr-bootstrap-from requires --join-existing (the node must join an existing cluster after seeding).");

            if (string.IsNullOrWhiteSpace(options.PitrBackupDir))
                throw new InvalidOperationException(
                    "--pitr-bootstrap-from requires --pitr-backup-dir pointing to the backup catalog/artifacts directory.");

            Guid leafId = options.PitrBootstrapFrom.Value;
            HLCTimestamp targetTime = options.PitrTargetTimeMs > 0
                ? new HLCTimestamp(0, options.PitrTargetTimeMs, 0)
                : HLCTimestamp.Zero;
            TimeSpan pitrWindow = TimeSpan.FromSeconds(options.PitrWindowSeconds);
            TimeSpan baseSnapshotInterval = TimeSpan.FromSeconds(options.BaseSnapshotIntervalSeconds);

            try
            {
                await kahuna.BootstrapFromPitrBackupAsync(
                    options.PitrBackupDir, leafId, targetTime, raft.WalAdapter, pitrWindow, baseSnapshotInterval);
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
