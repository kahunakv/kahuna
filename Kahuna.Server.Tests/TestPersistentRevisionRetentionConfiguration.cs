using CommandLine;
using Kahuna.Server.Configuration;

namespace Kahuna.Server.Tests;

public sealed class TestPersistentRevisionRetentionConfiguration
{
    [Fact]
    public void ValidateDefaultsKeepRetentionDisabled()
    {
        KahunaConfiguration configuration = ConfigurationValidator.Validate(new());

        Assert.Equal(0, configuration.PersistentRevisionRetentionCount);
        Assert.Equal(TimeSpan.Zero, configuration.PersistentRevisionRetentionAge);
        Assert.False(ConfigurationValidator.IsPersistentRevisionRetentionEnabled(configuration));
        Assert.Equal(TimeSpan.FromMinutes(5), configuration.PersistentRevisionCleanupInterval);
        Assert.Equal(1000, configuration.PersistentRevisionCleanupBatchSize);
        Assert.True(configuration.PersistentRevisionCleanupOnWrite);
    }

    [Fact]
    public void ValidateClampNegativeRetentionCountToDisabled()
    {
        KahunaConfiguration configuration = ConfigurationValidator.Validate(new()
        {
            PersistentRevisionRetentionCount = -5
        });

        Assert.Equal(0, configuration.PersistentRevisionRetentionCount);
        Assert.False(ConfigurationValidator.IsPersistentRevisionRetentionEnabled(configuration));
    }

    [Fact]
    public void ValidateClampNegativeRetentionAgeToDisabled()
    {
        KahunaConfiguration configuration = ConfigurationValidator.Validate(new()
        {
            PersistentRevisionRetentionAge = TimeSpan.FromSeconds(-30)
        });

        Assert.Equal(TimeSpan.Zero, configuration.PersistentRevisionRetentionAge);
        Assert.False(ConfigurationValidator.IsPersistentRevisionRetentionEnabled(configuration));
    }

    [Fact]
    public void ValidateCountRetentionEnabledClampsToAtLeastOne()
    {
        KahunaConfiguration configuration = ConfigurationValidator.Validate(new()
        {
            PersistentRevisionRetentionCount = 3
        });

        Assert.Equal(3, configuration.PersistentRevisionRetentionCount);
        Assert.True(ConfigurationValidator.IsPersistentRevisionRetentionEnabled(configuration));
    }

    [Fact]
    public void ValidateAgeRetentionEnabledRequiresPositiveCleanupInterval()
    {
        KahunaConfiguration configuration = ConfigurationValidator.Validate(new()
        {
            PersistentRevisionRetentionAge = TimeSpan.FromHours(1),
            PersistentRevisionCleanupInterval = TimeSpan.Zero
        });

        Assert.Equal(TimeSpan.FromHours(1), configuration.PersistentRevisionRetentionAge);
        Assert.Equal(TimeSpan.FromMinutes(5), configuration.PersistentRevisionCleanupInterval);
    }

    [Fact]
    public void ValidateClampCleanupBatchSizeToAtLeastOne()
    {
        KahunaConfiguration configuration = ConfigurationValidator.Validate(new()
        {
            PersistentRevisionCleanupBatchSize = 0
        });

        Assert.Equal(1, configuration.PersistentRevisionCleanupBatchSize);
    }

    [Fact]
    public void ValidateNegativeCleanupBatchSizeClampsToOne()
    {
        KahunaConfiguration configuration = ConfigurationValidator.Validate(new()
        {
            PersistentRevisionCleanupBatchSize = -100
        });

        Assert.Equal(1, configuration.PersistentRevisionCleanupBatchSize);
    }

    [Fact]
    public void ServerCliAcceptsPersistentRevisionRetentionOptions()
    {
        string[] args =
        [
            "--persistent-revision-retention-count", "3",
            "--persistent-revision-retention-age", "3600",
            "--persistent-revision-cleanup-interval", "120",
            "--persistent-revision-cleanup-batch-size", "500"
        ];

        ParserResult<KahunaCommandLineOptions> result = Parser.Default.ParseArguments<KahunaCommandLineOptions>(args);

        Assert.True(result.Tag == ParserResultType.Parsed);
        KahunaCommandLineOptions options = result.Value;

        Assert.Equal(3, options.PersistentRevisionRetentionCount);
        Assert.Equal(3600, options.PersistentRevisionRetentionAge);
        Assert.Equal(120, options.PersistentRevisionCleanupInterval);
        Assert.Equal(500, options.PersistentRevisionCleanupBatchSize);
        Assert.True(options.GetPersistentRevisionCleanupOnWrite());
    }

    [Fact]
    public void ServerCliDefaultsPersistentRevisionCleanupOnWriteToEnabled()
    {
        ParserResult<KahunaCommandLineOptions> result = Parser.Default.ParseArguments<KahunaCommandLineOptions>([]);

        Assert.True(result.Tag == ParserResultType.Parsed);
        Assert.True(result.Value.GetPersistentRevisionCleanupOnWrite());
    }

    [Fact]
    public void ServerCliDisablePersistentRevisionCleanupOnWriteTurnsOffCleanup()
    {
        ParserResult<KahunaCommandLineOptions> result = Parser.Default.ParseArguments<KahunaCommandLineOptions>([
            "--disable-persistent-revision-cleanup-on-write"
        ]);

        Assert.True(result.Tag == ParserResultType.Parsed);
        Assert.False(result.Value.GetPersistentRevisionCleanupOnWrite());
    }

    [Fact]
    public void ServerCliExplicitEnablePersistentRevisionCleanupOnWrite()
    {
        ParserResult<KahunaCommandLineOptions> result = Parser.Default.ParseArguments<KahunaCommandLineOptions>([
            "--persistent-revision-cleanup-on-write"
        ]);

        Assert.True(result.Tag == ParserResultType.Parsed);
        Assert.True(result.Value.GetPersistentRevisionCleanupOnWrite());
    }

    [Fact]
    public void ServerCliCannotDisableCleanupOnWriteWithFalseSuffix()
    {
        ParserResult<KahunaCommandLineOptions> misparse = Parser.Default.ParseArguments<KahunaCommandLineOptions>([
            "--persistent-revision-cleanup-on-write", "false"
        ]);

        Assert.True(misparse.Tag == ParserResultType.Parsed);
        Assert.True(misparse.Value.GetPersistentRevisionCleanupOnWrite());
    }

    [Fact]
    public void EmbeddedOptionsMapIntoValidatedConfiguration()
    {
        EmbeddedKahunaOptions embedded = new()
        {
            PersistentRevisionRetentionCount = 5,
            PersistentRevisionRetentionAge = TimeSpan.FromMinutes(10),
            PersistentRevisionCleanupInterval = TimeSpan.FromMinutes(2),
            PersistentRevisionCleanupBatchSize = 250,
            PersistentRevisionCleanupOnWrite = false
        };

        KahunaConfiguration configuration = ConfigurationValidator.Validate(new()
        {
            PersistentRevisionRetentionCount = embedded.PersistentRevisionRetentionCount,
            PersistentRevisionRetentionAge = embedded.PersistentRevisionRetentionAge,
            PersistentRevisionCleanupInterval = embedded.PersistentRevisionCleanupInterval,
            PersistentRevisionCleanupBatchSize = embedded.PersistentRevisionCleanupBatchSize,
            PersistentRevisionCleanupOnWrite = embedded.PersistentRevisionCleanupOnWrite
        });

        Assert.Equal(5, configuration.PersistentRevisionRetentionCount);
        Assert.Equal(TimeSpan.FromMinutes(10), configuration.PersistentRevisionRetentionAge);
        Assert.Equal(TimeSpan.FromMinutes(2), configuration.PersistentRevisionCleanupInterval);
        Assert.Equal(250, configuration.PersistentRevisionCleanupBatchSize);
        Assert.False(configuration.PersistentRevisionCleanupOnWrite);
        Assert.True(ConfigurationValidator.IsPersistentRevisionRetentionEnabled(configuration));
    }

    // ── PITR config ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void PitrDefaults_AreApplied()
    {
        KahunaConfiguration configuration = ConfigurationValidator.Validate(new());

        Assert.Equal(TimeSpan.FromHours(1), configuration.PitrWindow);
        Assert.Equal(TimeSpan.FromMinutes(30), configuration.BaseSnapshotInterval);
    }

    [Fact]
    public void PitrWindow_Zero_ClampsToDefault()
    {
        KahunaConfiguration configuration = ConfigurationValidator.Validate(new()
        {
            PitrWindow = TimeSpan.Zero
        });

        Assert.Equal(TimeSpan.FromHours(1), configuration.PitrWindow);
    }

    [Fact]
    public void PitrWindow_Negative_ClampsToDefault()
    {
        KahunaConfiguration configuration = ConfigurationValidator.Validate(new()
        {
            PitrWindow = TimeSpan.FromMinutes(-10)
        });

        Assert.Equal(TimeSpan.FromHours(1), configuration.PitrWindow);
    }

    [Fact]
    public void PitrWindow_AboveMax_ClampsToSixHours()
    {
        KahunaConfiguration configuration = ConfigurationValidator.Validate(new()
        {
            PitrWindow = TimeSpan.FromHours(8)
        });

        Assert.Equal(TimeSpan.FromHours(6), configuration.PitrWindow);
    }

    [Fact]
    public void PitrWindow_ValidValue_PassesThrough()
    {
        KahunaConfiguration configuration = ConfigurationValidator.Validate(new()
        {
            PitrWindow = TimeSpan.FromHours(2)
        });

        Assert.Equal(TimeSpan.FromHours(2), configuration.PitrWindow);
    }

    [Fact]
    public void BaseSnapshotInterval_Zero_ClampsToDefault()
    {
        KahunaConfiguration configuration = ConfigurationValidator.Validate(new()
        {
            BaseSnapshotInterval = TimeSpan.Zero
        });

        Assert.Equal(TimeSpan.FromMinutes(30), configuration.BaseSnapshotInterval);
    }

    [Fact]
    public void BaseSnapshotInterval_ExceedsPitrWindow_ClampsToWindow()
    {
        KahunaConfiguration configuration = ConfigurationValidator.Validate(new()
        {
            PitrWindow = TimeSpan.FromMinutes(20),
            BaseSnapshotInterval = TimeSpan.FromHours(1)
        });

        Assert.Equal(TimeSpan.FromMinutes(20), configuration.BaseSnapshotInterval);
    }

    [Fact]
    public void BaseSnapshotInterval_EqualToPitrWindow_IsValid()
    {
        KahunaConfiguration configuration = ConfigurationValidator.Validate(new()
        {
            PitrWindow = TimeSpan.FromHours(2),
            BaseSnapshotInterval = TimeSpan.FromHours(2)
        });

        Assert.Equal(TimeSpan.FromHours(2), configuration.PitrWindow);
        Assert.Equal(TimeSpan.FromHours(2), configuration.BaseSnapshotInterval);
    }

    [Fact]
    public void EmbeddedPitrOptions_MapIntoValidatedConfiguration()
    {
        EmbeddedKahunaOptions embedded = new()
        {
            PitrWindow = TimeSpan.FromHours(3),
            BaseSnapshotInterval = TimeSpan.FromHours(1)
        };

        KahunaConfiguration configuration = ConfigurationValidator.Validate(new()
        {
            PitrWindow = embedded.PitrWindow,
            BaseSnapshotInterval = embedded.BaseSnapshotInterval
        });

        Assert.Equal(TimeSpan.FromHours(3), configuration.PitrWindow);
        Assert.Equal(TimeSpan.FromHours(1), configuration.BaseSnapshotInterval);
    }

    [Fact]
    public void ServerCliAcceptsPitrOptions()
    {
        string[] args =
        [
            "--pitr-window", "7200",
            "--base-snapshot-interval", "900"
        ];

        ParserResult<KahunaCommandLineOptions> result = Parser.Default.ParseArguments<KahunaCommandLineOptions>(args);

        Assert.True(result.Tag == ParserResultType.Parsed);
        KahunaCommandLineOptions options = result.Value;

        Assert.Equal(7200, options.PitrWindowSeconds);
        Assert.Equal(900, options.BaseSnapshotIntervalSeconds);
    }

    [Fact]
    public void ServerCliPitrDefaults_AreOneHourAndThirtyMinutes()
    {
        ParserResult<KahunaCommandLineOptions> result = Parser.Default.ParseArguments<KahunaCommandLineOptions>([]);

        Assert.True(result.Tag == ParserResultType.Parsed);
        Assert.Equal(3600, result.Value.PitrWindowSeconds);
        Assert.Equal(1800, result.Value.BaseSnapshotIntervalSeconds);
    }
}
