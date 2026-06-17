using Kommander;

namespace Kahuna.Tests.Server;

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
        [nameof(RaftConfiguration.EnableQuiescence)] = nameof(KahunaCommandLineOptions.RaftEnableQuiescence),
        [nameof(RaftConfiguration.QuiesceAfter)] = nameof(KahunaCommandLineOptions.RaftQuiesceAfter)
    };

    [Fact]
    public void TestServerCliExposesEveryKommanderRaftConfigurationOption()
    {
        string[] writableRaftProperties = typeof(RaftConfiguration)
            .GetProperties()
            .Where(property => property.SetMethod?.IsPublic == true)
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
}
