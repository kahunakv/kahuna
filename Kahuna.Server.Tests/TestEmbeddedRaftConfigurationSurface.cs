using Kommander;

namespace Kahuna.Server.Tests;

public sealed class TestEmbeddedRaftConfigurationSurface
{
    [Fact]
    public void TestWalThroughputKnobsDefaultToKommanderDefaults()
    {
        // The embedded surface preserves Kommander's own defaults, NOT the Kahuna.Server defaults.
        // In particular single-fsync is OFF here (the server defaults it on), because flipping it
        // changes durability/recovery timing for every embedded consumer.
        RaftConfiguration kommanderDefaults = new();
        EmbeddedKahunaOptions options = new();

        Assert.Equal(kommanderDefaults.MaxWalGroupBatchPartitions, options.RaftMaxWalGroupBatchPartitions);
        Assert.Equal(kommanderDefaults.WalGroupCommitLingerMs, options.RaftWalGroupCommitLingerMs);
        Assert.Equal(kommanderDefaults.WalSingleFsyncCommit, options.RaftWalSingleFsyncCommit);
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
