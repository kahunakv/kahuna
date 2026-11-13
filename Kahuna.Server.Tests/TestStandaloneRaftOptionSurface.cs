using CommandLine;
using Kommander;

namespace Kahuna.Server.Tests;

/// <summary>
/// A node started without peers runs the embedded engine, and its Raft configuration is built from the embedded
/// options and not from the command line directly. A flag the translation drops is silently ignored on such a
/// node: it starts, and runs on a default. These tests follow each translated flag to the configuration Kommander
/// receives, and pin the flags that are left on the embedded defaults on purpose.
/// </summary>
public sealed class TestStandaloneRaftOptionSurface
{
    [Fact]
    public void RaftIoFlags_ReachTheConfigurationOfAStandaloneNode()
    {
        KahunaCommandLineOptions commandLine = new()
        {
            ReadIOThreads = 3,
            WriteIOThreads = 5,
            RaftMaxWalGroupBatchPartitions = 7,
            RaftWalGroupCommitLingerMs = 2,
            RaftWalSingleFsyncCommit = false,
            RaftSnapshotStagingDirectory = "/var/lib/kahuna/staging"
        };

        EmbeddedKahunaOptions options = EmbeddedOptionsFactory.CreateEmbeddedOptions(commandLine);

        Assert.Equal(3, options.ReadIOThreads);
        Assert.Equal(5, options.WriteIOThreads);
        Assert.Equal(7, options.RaftMaxWalGroupBatchPartitions);
        Assert.Equal(2, options.RaftWalGroupCommitLingerMs);
        Assert.False(options.RaftWalSingleFsyncCommit);
        Assert.Equal("/var/lib/kahuna/staging", options.RaftSnapshotStagingDirectory);

        RaftConfiguration configuration = EmbeddedKahunaNode.CreateRaftConfiguration(options);

        Assert.Equal(3, configuration.ReadIOThreads);
        Assert.Equal(5, configuration.WriteIOThreads);
        Assert.Equal(7, configuration.MaxWalGroupBatchPartitions);
        Assert.Equal(2, configuration.WalGroupCommitLingerMs);
        Assert.False(configuration.WalSingleFsyncCommit);
        Assert.Equal("/var/lib/kahuna/staging", configuration.SnapshotStagingDirectory);
    }

    /// <summary>
    /// With no flag set, a standalone node gets the values the command line documents as its defaults, the same
    /// ones a clustered node gets.
    /// </summary>
    [Fact]
    public void ADefaultCommandLine_GivesAStandaloneNodeTheDocumentedDefaults()
    {
        KahunaCommandLineOptions commandLine = new();

        RaftConfiguration configuration = EmbeddedKahunaNode.CreateRaftConfiguration(
            EmbeddedOptionsFactory.CreateEmbeddedOptions(commandLine));

        Assert.Equal(commandLine.ReadIOThreads, configuration.ReadIOThreads);
        Assert.Equal(commandLine.WriteIOThreads, configuration.WriteIOThreads);
        Assert.Equal(commandLine.RaftMaxWalGroupBatchPartitions, configuration.MaxWalGroupBatchPartitions);
        Assert.Equal(commandLine.RaftWalGroupCommitLingerMs, configuration.WalGroupCommitLingerMs);
        Assert.Null(commandLine.RaftWalSingleFsyncCommit);
        Assert.True(configuration.WalSingleFsyncCommit);
    }

    /// <summary>
    /// The snapshot bounds and timeouts are not translated. The command line always carries a value for them, so
    /// translating them would replace what the embedded node derives for a node that stages snapshots on disk: a
    /// receive cap sized for a large partition, and a transfer step timeout long enough for its export.
    /// </summary>
    [Fact]
    public void SnapshotBoundsAndTimeouts_StayUnset_SoTheDiskStagedDefaultsApply()
    {
        EmbeddedKahunaOptions options = EmbeddedOptionsFactory.CreateEmbeddedOptions(new KahunaCommandLineOptions
        {
            RaftSnapshotStagingDirectory = "/var/lib/kahuna/staging"
        });

        Assert.Null(options.RaftSnapshotMaxPendingBytes);
        Assert.Null(options.RaftSnapshotMaxPendingSessions);
        Assert.Null(options.RaftSnapshotStagingMemoryBytes);
        Assert.Null(options.RaftSnapshotChunkAckTimeout);
        Assert.Null(options.RaftSnapshotTransferStepTimeout);

        RaftConfiguration configuration = EmbeddedKahunaNode.CreateRaftConfiguration(options);

        Assert.Equal(EmbeddedKahunaNode.DiskStagedSnapshotMaxPendingBytes, configuration.SnapshotMaxPendingBytes);
        Assert.Equal(EmbeddedKahunaNode.DiskStagedSnapshotTransferStepTimeout, configuration.SnapshotTransferStepTimeout);
    }

    /// <summary>
    /// The single-fsync commit is on unless the command line says otherwise, and a value given on the command line
    /// is the value the node runs with. A flag that only acts as a switch could not turn the default off.
    /// </summary>
    [Theory]
    [InlineData(null, true)]
    [InlineData("true", true)]
    [InlineData("false", false)]
    public void SingleFsyncCommit_TakesTheValueGivenOnTheCommandLine(string? value, bool expected)
    {
        string[] args = value is null ? [] : ["--raft-wal-single-fsync-commit", value];

        ParserResult<KahunaCommandLineOptions> result = Parser.Default.ParseArguments<KahunaCommandLineOptions>(args);

        Assert.Equal(ParserResultType.Parsed, result.Tag);

        RaftConfiguration configuration = EmbeddedKahunaNode.CreateRaftConfiguration(
            EmbeddedOptionsFactory.CreateEmbeddedOptions(result.Value));

        Assert.Equal(expected, configuration.WalSingleFsyncCommit);
    }

    /// <summary>
    /// Every flag whose default is on takes a value, so the command line can turn it off. Unset, it stays null
    /// and the node applies the default.
    /// </summary>
    [Theory]
    [InlineData("--raft-enable-shared-executor-pool", nameof(KahunaCommandLineOptions.RaftEnableSharedExecutorPool))]
    [InlineData("--raft-enable-check-quorum", nameof(KahunaCommandLineOptions.RaftEnableCheckQuorum))]
    [InlineData("--raft-wal-single-fsync-commit", nameof(KahunaCommandLineOptions.RaftWalSingleFsyncCommit))]
    [InlineData("--raft-backfill-enabled", nameof(KahunaCommandLineOptions.RaftBackfillEnabled))]
    [InlineData("--raft-enable-auto-rejoin", nameof(KahunaCommandLineOptions.RaftEnableAutoRejoin))]
    [InlineData("--raft-enable-quiescence", nameof(KahunaCommandLineOptions.RaftEnableQuiescence))]
    [InlineData("--raft-fan-out-before-local-write", nameof(KahunaCommandLineOptions.RaftFanOutBeforeLocalWrite))]
    [InlineData("--raft-follower-apply-in-own-turn", nameof(KahunaCommandLineOptions.RaftFollowerApplyInOwnTurn))]
    [InlineData("--fail-fast-on-oom", nameof(KahunaCommandLineOptions.FailFastOnOutOfMemory))]
    public void AFlagThatIsOnByDefault_CanBeTurnedOffOnTheCommandLine(string flag, string property)
    {
        Assert.Null(ReadFlag(property));
        Assert.Equal(false, ReadFlag(property, flag, "false"));
        Assert.Equal(true, ReadFlag(property, flag, "true"));
    }

    private static bool? ReadFlag(string property, params string[] args)
    {
        ParserResult<KahunaCommandLineOptions> result = Parser.Default.ParseArguments<KahunaCommandLineOptions>(args);

        Assert.Equal(ParserResultType.Parsed, result.Tag);

        return (bool?)typeof(KahunaCommandLineOptions).GetProperty(property)!.GetValue(result.Value);
    }
}
