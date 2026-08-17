
using Kahuna.Server.Configuration;

using Kommander;

using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Coverage for the replica-placement configuration surface: every placement knob set on
/// <see cref="EmbeddedKahunaOptions"/> must reach the <see cref="RaftConfiguration"/> handed to
/// Kommander, and startup validation must refuse a negative replication factor while warning
/// (not failing) on the questionable-but-workable settings.
/// </summary>
public sealed class TestPlacementConfiguration
{
    [Fact]
    public void EmbeddedRaftConfiguration_CarriesEveryPlacementKnob()
    {
        EmbeddedKahunaOptions options = new()
        {
            ReplicationFactor = 3,
            EnablePlacementRebalancer = true,
            PlacementPassInterval = TimeSpan.FromSeconds(2),
            MaxReplicaMovesPerPass = 6,
            MaxConcurrentReplicaTransfers = 2,
            MaxConcurrentReplicaRepairs = 4,
            ReplicaCountDeadband = 3,
            Zone = "zone-a"
        };

        RaftConfiguration configuration = EmbeddedKahunaNode.CreateRaftConfiguration(options);

        Assert.Equal(3, configuration.ReplicationFactor);
        Assert.True(configuration.EnablePlacementRebalancer);
        Assert.Equal(TimeSpan.FromSeconds(2), configuration.PlacementPassInterval);
        Assert.Equal(6, configuration.MaxReplicaMovesPerPass);
        Assert.Equal(2, configuration.MaxConcurrentReplicaTransfers);
        Assert.Equal(4, configuration.MaxConcurrentReplicaRepairs);
        Assert.Equal(3, configuration.ReplicaCountDeadband);
        Assert.Equal("zone-a", configuration.Zone);
    }

    /// <summary>
    /// Placement passes are scheduled independently of the leader balancer: a node configured only
    /// for placement must still run them, which is what makes a committed replication-factor target
    /// converge. The predicate is Kommander's, but the wiring that reaches it is Kahuna's.
    /// </summary>
    [Fact]
    public void EmbeddedRaftConfiguration_SchedulesPlacementPassesWithoutTheLeaderBalancer()
    {
        RaftConfiguration placed = EmbeddedKahunaNode.CreateRaftConfiguration(new()
        {
            ReplicationFactor = 3
        });

        Assert.False(placed.EnableLeaderBalancer);
        Assert.True(placed.PlacementPassEnabled);
        Assert.True(placed.PlacementPassInterval > TimeSpan.Zero);

        // A per-range override on a cluster whose global factor is 0 also needs passes.
        RaftConfiguration rebalancerOnly = EmbeddedKahunaNode.CreateRaftConfiguration(new()
        {
            EnablePlacementRebalancer = true
        });

        Assert.True(rebalancerOnly.PlacementPassEnabled);

        // Neither: nothing can create placement work, so no timer is warranted.
        Assert.False(EmbeddedKahunaNode.CreateRaftConfiguration(new()).PlacementPassEnabled);
    }

    [Fact]
    public void EmbeddedRaftConfiguration_DefaultsToFullReplication()
    {
        RaftConfiguration configuration = EmbeddedKahunaNode.CreateRaftConfiguration(new());

        Assert.Equal(0, configuration.ReplicationFactor);
        Assert.False(configuration.EnablePlacementRebalancer);
        Assert.Null(configuration.Zone);
    }

    [Fact]
    public void ValidateReplicaPlacement_NegativeFactor_IsRefused()
    {
        CapturingLogger logger = new();

        KahunaServerException exception = Assert.Throws<KahunaServerException>(() =>
            ConfigurationValidator.ValidateReplicaPlacement(-1, seedNodeCount: 3, logger));

        Assert.Contains("-1", exception.Message);
        Assert.Empty(logger.Entries);
    }

    [Fact]
    public void ValidateReplicaPlacement_ZeroFactor_IsSilent()
    {
        CapturingLogger logger = new();

        ConfigurationValidator.ValidateReplicaPlacement(0, seedNodeCount: 1, logger);

        Assert.Empty(logger.Entries);
    }

    [Fact]
    public void ValidateReplicaPlacement_OddFactorWithinSeeds_IsSilent()
    {
        CapturingLogger logger = new();

        ConfigurationValidator.ValidateReplicaPlacement(3, seedNodeCount: 6, logger);

        Assert.Empty(logger.Entries);
    }

    [Fact]
    public void ValidateReplicaPlacement_EvenFactor_Warns()
    {
        CapturingLogger logger = new();

        ConfigurationValidator.ValidateReplicaPlacement(4, seedNodeCount: 6, logger);

        (LogLevel level, string message) = Assert.Single(logger.Entries);
        Assert.Equal(LogLevel.Warning, level);
        Assert.Contains("even", message);
    }

    [Fact]
    public void ValidateReplicaPlacement_FactorAboveSeedCount_Warns()
    {
        CapturingLogger logger = new();

        ConfigurationValidator.ValidateReplicaPlacement(5, seedNodeCount: 3, logger);

        (LogLevel level, string message) = Assert.Single(logger.Entries);
        Assert.Equal(LogLevel.Warning, level);
        Assert.Contains("seed", message);
    }

    [Fact]
    public void ValidateReplicaPlacement_UnknownSeedCount_SkipsTheSeedWarning()
    {
        CapturingLogger logger = new();

        ConfigurationValidator.ValidateReplicaPlacement(5, seedNodeCount: null, logger);

        Assert.Empty(logger.Entries);
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Entries.Add((logLevel, formatter(state, exception)));
    }
}
