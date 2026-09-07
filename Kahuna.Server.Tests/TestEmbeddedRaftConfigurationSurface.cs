using Kommander;

namespace Kahuna.Server.Tests;

public sealed class TestEmbeddedRaftConfigurationSurface
{
    [Fact]
    public void TestWalThroughputKnobsDefaultToKommanderDefaults()
    {
        // The embedded surface preserves Kommander's own defaults for the WAL throughput knobs, NOT the
        // Kahuna.Server defaults — with one deliberate exception: single-fsync is kept OFF here regardless of
        // Kommander's default (Kommander now defaults it ON), because flipping it changes durability/recovery
        // timing for every embedded consumer, so that choice must be explicit rather than inherited.
        RaftConfiguration kommanderDefaults = new();
        EmbeddedKahunaOptions options = new();

        Assert.Equal(kommanderDefaults.MaxWalGroupBatchPartitions, options.RaftMaxWalGroupBatchPartitions);
        Assert.Equal(kommanderDefaults.WalGroupCommitLingerMs, options.RaftWalGroupCommitLingerMs);
        Assert.False(options.RaftWalSingleFsyncCommit);
    }

    [Fact]
    public void TestWalThroughputKnobsThreadThroughRaftConfiguration()
    {
        EmbeddedKahunaOptions options = new()
        {
            RaftMaxWalGroupBatchPartitions = 17,
            RaftWalGroupCommitLingerMs = 3,
            RaftWalSingleFsyncCommit = true
        };

        RaftConfiguration configuration = EmbeddedKahunaNode.CreateRaftConfiguration(options);

        Assert.Equal(17, configuration.MaxWalGroupBatchPartitions);
        Assert.Equal(3, configuration.WalGroupCommitLingerMs);
        Assert.True(configuration.WalSingleFsyncCommit);
    }

    [Fact]
    public void TestDefaultOptionsProduceAValidRaftConfiguration()
    {
        // A default embedded node must construct. The heartbeat de-dup window has to stay strictly
        // below the heartbeat cadence, or Kommander rejects the pair and no embedded node starts.
        EmbeddedKahunaOptions options = new();

        Assert.True(options.RecentHeartbeat < options.HeartbeatInterval);

        RaftConfiguration configuration = EmbeddedKahunaNode.CreateRaftConfiguration(options);

        Assert.Equal(TimeSpan.FromMilliseconds(25), configuration.RecentHeartbeat);
        configuration.Validate();
    }

    [Fact]
    public void TestTheDedupWindowTracksALoweredHeartbeatInterval()
    {
        // A consumer that shortens only the cadence — a fast test node, say — must still get a
        // window below it. A fixed window would sit above the new cadence and fail construction.
        EmbeddedKahunaOptions options = new() { HeartbeatInterval = TimeSpan.FromMilliseconds(20) };

        Assert.Equal(TimeSpan.FromMilliseconds(5), options.RecentHeartbeat);

        EmbeddedKahunaNode.CreateRaftConfiguration(options).Validate();
    }

    [Fact]
    public void TestALoweredHeartbeatIntervalTracksRegardlessOfInitializerOrder()
    {
        // The derivation reads the cadence at get time, so an initializer that assigns the cadence
        // after any other member still gets the matching window.
        EmbeddedKahunaOptions options = new()
        {
            NodeName = "order-check",
            HeartbeatInterval = TimeSpan.FromMilliseconds(40)
        };

        Assert.Equal(TimeSpan.FromMilliseconds(10), options.RecentHeartbeat);
    }

    [Fact]
    public void TestAnExplicitDedupWindowOverridesTheDerivation()
    {
        EmbeddedKahunaOptions options = new()
        {
            HeartbeatInterval = TimeSpan.FromMilliseconds(200),
            RecentHeartbeat = TimeSpan.FromMilliseconds(7)
        };

        Assert.Equal(TimeSpan.FromMilliseconds(7), options.RecentHeartbeat);

        RaftConfiguration configuration = EmbeddedKahunaNode.CreateRaftConfiguration(options);

        Assert.Equal(TimeSpan.FromMilliseconds(7), configuration.RecentHeartbeat);
    }

    [Fact]
    public void TestAnExplicitZeroDedupWindowSurvivesTheDerivation()
    {
        // Zero disables the window in Kommander. The derivation must not treat it as unset.
        EmbeddedKahunaOptions options = new() { RecentHeartbeat = TimeSpan.Zero };

        Assert.Equal(TimeSpan.Zero, options.RecentHeartbeat);
        Assert.Equal(TimeSpan.Zero, EmbeddedKahunaNode.CreateRaftConfiguration(options).RecentHeartbeat);
    }
}
