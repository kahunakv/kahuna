using CommandLine;
using Kommander;

namespace Kahuna.Server.Tests;

public sealed class TestKommanderConfigurationSurface
{
    private static readonly Dictionary<string, string> RaftToCliOptionProperties = new()
    {
        [nameof(RaftConfiguration.NodeName)] = nameof(KahunaCommandLineOptions.RaftNodeName),
        [nameof(RaftConfiguration.NodeId)] = nameof(KahunaCommandLineOptions.RaftNodeId),
        [nameof(RaftConfiguration.Host)] = nameof(KahunaCommandLineOptions.RaftHost),
        [nameof(RaftConfiguration.Port)] = nameof(KahunaCommandLineOptions.RaftPort),
        [nameof(RaftConfiguration.InitialPartitions)] = nameof(KahunaCommandLineOptions.InitialClusterPartitions),
        [nameof(RaftConfiguration.HttpScheme)] = nameof(KahunaCommandLineOptions.RaftHttpScheme),
        [nameof(RaftConfiguration.HttpAuthBearerToken)] = nameof(KahunaCommandLineOptions.RaftHttpAuthBearerToken),
        [nameof(RaftConfiguration.HttpTimeout)] = nameof(KahunaCommandLineOptions.RaftHttpTimeout),
        [nameof(RaftConfiguration.HttpVersion)] = nameof(KahunaCommandLineOptions.RaftHttpVersion),
        [nameof(RaftConfiguration.HeartbeatInterval)] = nameof(KahunaCommandLineOptions.RaftHeartbeatInterval),
        [nameof(RaftConfiguration.RecentHeartbeat)] = nameof(KahunaCommandLineOptions.RaftRecentHeartbeat),
        [nameof(RaftConfiguration.VotingTimeout)] = nameof(KahunaCommandLineOptions.RaftVotingTimeout),
        [nameof(RaftConfiguration.CheckLeaderInterval)] = nameof(KahunaCommandLineOptions.RaftCheckLeaderInterval),
        [nameof(RaftConfiguration.LeadershipBarrierTimeout)] = nameof(KahunaCommandLineOptions.RaftLeadershipBarrierTimeout),
        [nameof(RaftConfiguration.LeadershipConfirmationTimeout)] = nameof(KahunaCommandLineOptions.RaftLeadershipConfirmationTimeout),
        [nameof(RaftConfiguration.EnableCheckQuorum)] = nameof(KahunaCommandLineOptions.RaftEnableCheckQuorum),
        [nameof(RaftConfiguration.CheckQuorumIntervalMultiplier)] = nameof(KahunaCommandLineOptions.RaftCheckQuorumIntervalMultiplier),
        [nameof(RaftConfiguration.TimerInitialDelay)] = nameof(KahunaCommandLineOptions.RaftTimerInitialDelay),
        [nameof(RaftConfiguration.UpdateNodesInterval)] = nameof(KahunaCommandLineOptions.RaftUpdateNodesInterval),
        [nameof(RaftConfiguration.StartElectionTimeout)] = nameof(KahunaCommandLineOptions.RaftStartElectionTimeout),
        [nameof(RaftConfiguration.EndElectionTimeout)] = nameof(KahunaCommandLineOptions.RaftEndElectionTimeout),
        [nameof(RaftConfiguration.StartElectionTimeoutIncrement)] = nameof(KahunaCommandLineOptions.RaftStartElectionTimeoutIncrement),
        [nameof(RaftConfiguration.EndElectionTimeoutIncrement)] = nameof(KahunaCommandLineOptions.RaftEndElectionTimeoutIncrement),
        [nameof(RaftConfiguration.SlowRaftStateMachineLog)] = nameof(KahunaCommandLineOptions.RaftSlowStateMachineLog),
        [nameof(RaftConfiguration.SlowRaftWALMachineLog)] = nameof(KahunaCommandLineOptions.RaftSlowWalMachineLog),
        [nameof(RaftConfiguration.ReadIOThreads)] = nameof(KahunaCommandLineOptions.ReadIOThreads),
        [nameof(RaftConfiguration.WriteIOThreads)] = nameof(KahunaCommandLineOptions.WriteIOThreads),
        [nameof(RaftConfiguration.CompactEveryOperations)] = nameof(KahunaCommandLineOptions.RaftCompactEveryOperations),
        [nameof(RaftConfiguration.CompactNumberEntries)] = nameof(KahunaCommandLineOptions.RaftCompactNumberEntries),
        [nameof(RaftConfiguration.MaxEntriesPerCompaction)] = nameof(KahunaCommandLineOptions.RaftMaxEntriesPerCompaction),
        [nameof(RaftConfiguration.ElectionTimeoutSeed)] = nameof(KahunaCommandLineOptions.RaftElectionTimeoutSeed),
        [nameof(RaftConfiguration.MaxQueuedClientProposalsPerPartition)] = nameof(KahunaCommandLineOptions.RaftMaxQueuedClientProposals),
        [nameof(RaftConfiguration.MaxWalQueueDepthPerPartition)] = nameof(KahunaCommandLineOptions.RaftMaxWalQueueDepthPerPartition),
        [nameof(RaftConfiguration.MaxGlobalWalQueueDepth)] = nameof(KahunaCommandLineOptions.RaftMaxGlobalWalQueueDepth),
        [nameof(RaftConfiguration.MaxWalBatchSize)] = nameof(KahunaCommandLineOptions.RaftMaxWalBatchSize),
        [nameof(RaftConfiguration.MaxWalGroupBatchPartitions)] = nameof(KahunaCommandLineOptions.RaftMaxWalGroupBatchPartitions),
        [nameof(RaftConfiguration.SqliteWalShardCount)] = nameof(KahunaCommandLineOptions.RaftSqliteWalShardCount),
        [nameof(RaftConfiguration.MaxDrainQuantumControl)] = nameof(KahunaCommandLineOptions.RaftMaxDrainQuantumControl),
        [nameof(RaftConfiguration.MaxDrainQuantumReplication)] = nameof(KahunaCommandLineOptions.RaftMaxDrainQuantumReplication),
        [nameof(RaftConfiguration.MaxDrainQuantumClient)] = nameof(KahunaCommandLineOptions.RaftMaxDrainQuantumClient),
        [nameof(RaftConfiguration.MaxDrainQuantumMaintenance)] = nameof(KahunaCommandLineOptions.RaftMaxDrainQuantumMaintenance),
        [nameof(RaftConfiguration.TransportSecurity)] = nameof(KahunaCommandLineOptions.RaftTransportSecurity),
        [nameof(RaftConfiguration.GrpcScheme)] = nameof(KahunaCommandLineOptions.RaftGrpcScheme),
        [nameof(RaftConfiguration.BackfillThreshold)] = nameof(KahunaCommandLineOptions.RaftBackfillThreshold),
        [nameof(RaftConfiguration.MaxBackfillEntriesPerRound)] = nameof(KahunaCommandLineOptions.RaftMaxBackfillEntriesPerRound),
        [nameof(RaftConfiguration.LearnerPromotionLag)] = nameof(KahunaCommandLineOptions.RaftLearnerPromotionLag),
        [nameof(RaftConfiguration.LearnerPromotionStableWindow)] = nameof(KahunaCommandLineOptions.RaftLearnerPromotionStableWindow),
        [nameof(RaftConfiguration.GossipInterval)] = nameof(KahunaCommandLineOptions.RaftGossipInterval),
        [nameof(RaftConfiguration.GossipFanout)] = nameof(KahunaCommandLineOptions.RaftGossipFanout),
        [nameof(RaftConfiguration.PingTimeout)] = nameof(KahunaCommandLineOptions.RaftPingTimeout),
        [nameof(RaftConfiguration.IndirectPingFanout)] = nameof(KahunaCommandLineOptions.RaftIndirectPingFanout),
        [nameof(RaftConfiguration.SuspicionTimeout)] = nameof(KahunaCommandLineOptions.RaftSuspicionTimeout),
        [nameof(RaftConfiguration.DeadMemberEvictionGrace)] = nameof(KahunaCommandLineOptions.RaftDeadMemberEvictionGrace),
        [nameof(RaftConfiguration.PingInterval)] = nameof(KahunaCommandLineOptions.RaftPingInterval),
        [nameof(RaftConfiguration.GrpcChannelsPerNode)] = nameof(KahunaCommandLineOptions.RaftGrpcChannelsPerNode),
        [nameof(RaftConfiguration.GrpcEnableMultipleHttp2Connections)] = nameof(KahunaCommandLineOptions.RaftGrpcEnableMultipleHttp2Connections),
        [nameof(RaftConfiguration.GrpcEnableSnapshotCompression)] = nameof(KahunaCommandLineOptions.RaftGrpcEnableSnapshotCompression),
        [nameof(RaftConfiguration.SnapshotReceiveSessionTtl)] = nameof(KahunaCommandLineOptions.RaftSnapshotReceiveSessionTtl),
        [nameof(RaftConfiguration.SnapshotMaxPendingSessions)] = nameof(KahunaCommandLineOptions.RaftSnapshotMaxPendingSessions),
        [nameof(RaftConfiguration.SnapshotMaxPendingBytes)] = nameof(KahunaCommandLineOptions.RaftSnapshotMaxPendingBytes),
        [nameof(RaftConfiguration.AllowLegacySnapshotSenders)] = nameof(KahunaCommandLineOptions.RaftAllowLegacySnapshotSenders),
        [nameof(RaftConfiguration.EnableAutoRejoin)] = nameof(KahunaCommandLineOptions.RaftEnableAutoRejoin),
        [nameof(RaftConfiguration.EnableQuiescence)] = nameof(KahunaCommandLineOptions.RaftEnableQuiescence),
        [nameof(RaftConfiguration.QuiesceAfter)] = nameof(KahunaCommandLineOptions.RaftQuiesceAfter),
        [nameof(RaftConfiguration.EnableLeaderBalancer)] = nameof(KahunaCommandLineOptions.RaftEnableLeaderBalancer),
        [nameof(RaftConfiguration.LeaderBalancerReportInterval)] = nameof(KahunaCommandLineOptions.RaftLeaderBalancerReportInterval),
        [nameof(RaftConfiguration.LeaderBalancerInterval)] = nameof(KahunaCommandLineOptions.RaftLeaderBalancerInterval),
        [nameof(RaftConfiguration.LeaderBalancerReportTtl)] = nameof(KahunaCommandLineOptions.RaftLeaderBalancerReportTtl),
        [nameof(RaftConfiguration.CountDeadband)] = nameof(KahunaCommandLineOptions.RaftCountDeadband),
        [nameof(RaftConfiguration.LoadImbalanceThreshold)] = nameof(KahunaCommandLineOptions.RaftLoadImbalanceThreshold),
        [nameof(RaftConfiguration.MinLeaderStabilityMs)] = nameof(KahunaCommandLineOptions.RaftMinLeaderStabilityMs),
        [nameof(RaftConfiguration.MoveCooldown)] = nameof(KahunaCommandLineOptions.RaftMoveCooldown),
        [nameof(RaftConfiguration.MaxMovesPerPass)] = nameof(KahunaCommandLineOptions.RaftMaxMovesPerPass),
        [nameof(RaftConfiguration.MaxConcurrentTransfers)] = nameof(KahunaCommandLineOptions.RaftMaxConcurrentTransfers),
        [nameof(RaftConfiguration.LeaderBalancerOpsWeight)] = nameof(KahunaCommandLineOptions.RaftLeaderBalancerOpsWeight),
        [nameof(RaftConfiguration.LeaderBalancerQueueWeight)] = nameof(KahunaCommandLineOptions.RaftLeaderBalancerQueueWeight),
        [nameof(RaftConfiguration.SuggestionTimeout)] = nameof(KahunaCommandLineOptions.RaftSuggestionTimeout),
        [nameof(RaftConfiguration.GrpcEnableAppendLogsCoalescing)] = nameof(KahunaCommandLineOptions.RaftGrpcEnableAppendLogsCoalescing),
        [nameof(RaftConfiguration.GrpcAppendLogsMaxCoalesceBatch)] = nameof(KahunaCommandLineOptions.RaftGrpcAppendLogsMaxCoalesceBatch),
        [nameof(RaftConfiguration.EnableSharedExecutorPool)] = nameof(KahunaCommandLineOptions.RaftEnableSharedExecutorPool),
        [nameof(RaftConfiguration.PartitionExecutorPoolSize)] = nameof(KahunaCommandLineOptions.RaftExecutorPoolSize),
        [nameof(RaftConfiguration.WalGroupCommitLingerMs)] = nameof(KahunaCommandLineOptions.RaftWalGroupCommitLingerMs),
        [nameof(RaftConfiguration.WalSingleFsyncCommit)] = nameof(KahunaCommandLineOptions.RaftWalSingleFsyncCommit),
        [nameof(RaftConfiguration.ReplicationFactor)] = nameof(KahunaCommandLineOptions.RaftReplicationFactor),
        [nameof(RaftConfiguration.EnablePlacementRebalancer)] = nameof(KahunaCommandLineOptions.RaftEnablePlacementRebalancer),
        [nameof(RaftConfiguration.MaxReplicaMovesPerPass)] = nameof(KahunaCommandLineOptions.RaftMaxReplicaMovesPerPass),
        [nameof(RaftConfiguration.MaxConcurrentReplicaTransfers)] = nameof(KahunaCommandLineOptions.RaftMaxConcurrentReplicaTransfers),
        [nameof(RaftConfiguration.ReplicaCountDeadband)] = nameof(KahunaCommandLineOptions.RaftReplicaCountDeadband),
        [nameof(RaftConfiguration.Zone)] = nameof(KahunaCommandLineOptions.RaftZone)
    };

    /// <summary>
    /// Writable RaftConfiguration members that are code-level hooks rather than operator-settable
    /// options: they carry live objects no CLI value can express and are wired by the host
    /// (KahunaManager assigns the application-durability provider after construction, before the
    /// node joins).
    /// </summary>
    private static readonly HashSet<string> NonCliHookProperties =
    [
        nameof(RaftConfiguration.ApplicationDurabilityProvider)
    ];

    [Fact]
    public void TestServerCliExposesEveryKommanderRaftConfigurationOption()
    {
        string[] writableRaftProperties = typeof(RaftConfiguration)
            .GetProperties()
            .Where(property => property.SetMethod?.IsPublic == true && !NonCliHookProperties.Contains(property.Name))
            .Select(property => property.Name)
            .Order()
            .ToArray();

        Assert.Empty(writableRaftProperties.Except(RaftToCliOptionProperties.Keys));
        Assert.Empty(RaftToCliOptionProperties.Keys.Except(writableRaftProperties));

        HashSet<string> cliProperties = typeof(KahunaCommandLineOptions)
            .GetProperties()
            .Select(property => property.Name)
            .ToHashSet(StringComparer.Ordinal);

        Assert.All(RaftToCliOptionProperties.Values, cliProperty =>
            Assert.Contains(cliProperty, cliProperties));
    }

    /// <summary>
    /// The snapshot-receive bounds are reachable from the command line: a declared property satisfies the surface
    /// check above without proving the flag parses, so each one is driven through the parser with a non-default
    /// value, and the defaults are pinned to the ones Kommander documents.
    /// </summary>
    [Fact]
    public void TestSnapshotReceiveBoundOptionsParseFromTheCommandLine()
    {
        ParserResult<KahunaCommandLineOptions> defaults = Parser.Default.ParseArguments<KahunaCommandLineOptions>([]);

        Assert.Equal(ParserResultType.Parsed, defaults.Tag);
        Assert.Equal(30000, defaults.Value.RaftSnapshotReceiveSessionTtl);
        Assert.Equal(8, defaults.Value.RaftSnapshotMaxPendingSessions);
        Assert.Equal(512L * 1024 * 1024, defaults.Value.RaftSnapshotMaxPendingBytes);
        Assert.False(defaults.Value.RaftAllowLegacySnapshotSenders);

        ParserResult<KahunaCommandLineOptions> overridden = Parser.Default.ParseArguments<KahunaCommandLineOptions>([
            "--raft-snapshot-receive-session-ttl", "45000",
            "--raft-snapshot-max-pending-sessions", "3",
            "--raft-snapshot-max-pending-bytes", "67108864",
            "--raft-allow-legacy-snapshot-senders"
        ]);

        Assert.Equal(ParserResultType.Parsed, overridden.Tag);
        Assert.Equal(45000, overridden.Value.RaftSnapshotReceiveSessionTtl);
        Assert.Equal(3, overridden.Value.RaftSnapshotMaxPendingSessions);
        Assert.Equal(64L * 1024 * 1024, overridden.Value.RaftSnapshotMaxPendingBytes);
        Assert.True(overridden.Value.RaftAllowLegacySnapshotSenders);
    }
}
