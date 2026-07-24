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
}
