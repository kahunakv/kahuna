using System.Reflection;
using Kahuna.Server.Configuration;

namespace Kahuna.Server.Tests;

/// <summary>
/// Coverage for the configuration a clustered server runs on.
/// <para>
/// The clustered server used to build its <see cref="KahunaConfiguration"/> from a hand-written copy of the
/// embedded mapping. Every knob added to one copy and not the other parsed, bound, and was then ignored on one
/// node shape: on a cluster the backup settings and the durable-record retention budget, on a standalone node
/// the checkpoint interval and the published-head switch. These cases follow each such flag from the command
/// line to the configuration the managers read, and pin that the server adds nothing to the shared mapping
/// except the settings only a server has.
/// </para>
/// </summary>
public sealed class TestServerConfigurationSurface
{
    /// <summary>The settings a server sets on top of the shared mapping. Every other field must come from it.</summary>
    private static readonly HashSet<string> ServerOnlySettings = new(StringComparer.Ordinal)
    {
        nameof(KahunaConfiguration.HttpsCertificate),
        nameof(KahunaConfiguration.HttpsCertificatePassword),
        nameof(KahunaConfiguration.InterNodeGrpcScheme),
        nameof(KahunaConfiguration.AdvertisedClientEndpoint),
        nameof(KahunaConfiguration.AdvertisedClientScheme),
        nameof(KahunaConfiguration.AdvertisePeerEndpoints),
        nameof(KahunaConfiguration.RoutingHintsEnabled),
    };

    /// <summary>Non-default values throughout, so a missed assignment cannot pass by coincidence.</summary>
    private static KahunaCommandLineOptions NonDefaultOptions() => new()
    {
        StorageRevision = "v1",
        DurableRecordRetentionMax = 1_234,
        DurableRecordRetentionMaxBytes = 5_678_901,
        DurableRecordRetentionHeapPressure = 0.5,
        DurableRecordRetentionFloorSeconds = 45,
        DurableMaintenanceIntervalSeconds = 4,
        PitrBackupDir = "/srv/kahuna/backups",
        PitrBackupScratchDir = "/srv/kahuna/scratch",
        PitrBackupClusterId = "cluster-a",
        PitrBackupMacKeyFile = "/etc/kahuna/mac.key",
        PitrRestoreRoot = "/srv/kahuna/restores",
        PitrAllowUnconfinedRemoteRestore = true,
        BackupRetentionMaxChains = 3,
        BackupRetentionMaxAgeSeconds = 7_200,
        BackupRetentionMaxBytes = 9_999,
        BackupGcIntervalSeconds = 600,
        BackupRestoreThrottleMbps = 7,
        BackupCaptureThrottleMbps = 11,
        BackupApplyBarrierTimeoutSeconds = 21,
        CheckpointIntervalSeconds = 17,
        DisablePublishedHeadReads = true,
        RaftGrpcScheme = "http://",
        AdvertisedClientEndpoint = "https://node9:8082",
        AdvertisedClientScheme = "http://",
        DisablePeerEndpointAdvertisement = true,
        DisableRoutingHints = true,
        StorageCompactionRateMbps = 13,
        StorageMaxBackgroundCompactions = 3,
        StorageSharedMemtableBudgetMb = 48,
        StorageSharedCacheMb = 96,
    };

    [Fact]
    public void FlagsTheClusterUsedToIgnore_ReachTheServerConfiguration()
    {
        KahunaConfiguration configuration = ServerConfigurationFactory.Create(NonDefaultOptions());

        Assert.Equal(1_234, configuration.DurableRecordRetentionMax);
        Assert.Equal(5_678_901, configuration.DurableRecordRetentionMaxBytes);
        Assert.Equal(0.5, configuration.DurableRecordRetentionHeapPressure);
        Assert.Equal(TimeSpan.FromSeconds(45), configuration.DurableRecordRetentionFloor);
        Assert.Equal(TimeSpan.FromSeconds(4), configuration.DurableMaintenanceInterval);

        Assert.Equal("/srv/kahuna/backups", configuration.BackupDir);
        Assert.Equal("local", configuration.BackupTarget);
        Assert.Equal("/srv/kahuna/scratch", configuration.BackupScratchDir);
        Assert.Equal("cluster-a", configuration.BackupClusterId);
        Assert.Equal("/etc/kahuna/mac.key", configuration.BackupMacKeyFile);
        Assert.Equal("/srv/kahuna/restores", configuration.RestoreRoot);
        Assert.True(configuration.AllowUnconfinedRemoteRestore);
        Assert.Equal(3, configuration.BackupRetentionMaxChains);
        Assert.Equal(TimeSpan.FromSeconds(7_200), configuration.BackupRetentionMaxAge);
        Assert.Equal(9_999, configuration.BackupRetentionMaxBytes);
        Assert.Equal(TimeSpan.FromSeconds(600), configuration.BackupGcInterval);
        Assert.Equal(7_000_000, configuration.BackupRestoreThrottleBytesPerSec);
        Assert.Equal(11_000_000, configuration.BackupCaptureThrottleBytesPerSec);
        Assert.Equal(TimeSpan.FromSeconds(21), configuration.BackupApplyBarrierTimeout);
    }

    [Fact]
    public void StorageCompactionSettings_ReachBothNodeShapes_AndDefaultToTheBackendsOwn()
    {
        KahunaCommandLineOptions opts = NonDefaultOptions();
        KahunaConfiguration server = ServerConfigurationFactory.Create(opts);
        KahunaConfiguration embedded = EmbeddedKahunaNode.CreateKahunaConfiguration(
            EmbeddedOptionsFactory.CreateEmbeddedOptions(opts), singleProcessRaftGroup: false);

        foreach (KahunaConfiguration configuration in new[] { server, embedded })
        {
            Assert.Equal(13_000_000, configuration.StorageCompactionRateBytesPerSec);
            Assert.Equal(3, configuration.StorageMaxBackgroundCompactions);
        }

        KahunaConfiguration defaults = new();
        Assert.Equal(0, defaults.StorageCompactionRateBytesPerSec);
        Assert.Equal(0, defaults.StorageMaxBackgroundCompactions);
    }

    [Theory]
    [InlineData(-1, 0, "--storage-compaction-rate-mbps")]
    [InlineData(0, -2, "--storage-max-background-compactions")]
    public void NegativeStorageCompactionSettings_AreRefused(int rateMbps, int compactions, string flag)
    {
        KahunaCommandLineOptions opts = NonDefaultOptions();
        opts.StorageCompactionRateMbps = rateMbps;
        opts.StorageMaxBackgroundCompactions = compactions;

        KahunaServerException ex = Assert.Throws<KahunaServerException>(() => ServerConfigurationFactory.Create(opts));
        Assert.Contains(flag, ex.Message);
    }

    [Fact]
    public void TheSharedMemory_ReachesBothNodeShapes_AndDefaultsToNone()
    {
        KahunaCommandLineOptions opts = NonDefaultOptions();
        KahunaConfiguration server = ServerConfigurationFactory.Create(opts);
        KahunaConfiguration embedded = EmbeddedKahunaNode.CreateKahunaConfiguration(
            EmbeddedOptionsFactory.CreateEmbeddedOptions(opts), singleProcessRaftGroup: false);

        foreach (KahunaConfiguration configuration in new[] { server, embedded })
        {
            Assert.Equal(48L * 1024 * 1024, configuration.StorageSharedMemtableBudgetBytes);
            Assert.Equal(96L * 1024 * 1024, configuration.StorageSharedCacheBytes);
            Assert.Equal(48L * 1024 * 1024, configuration.SharedMemory!.MemtableBudgetBytes);
            Assert.Equal(96L * 1024 * 1024, configuration.SharedMemory.CacheBytes);

            // The WAL and the store of a node read it from one configuration; both must get the same object.
            Assert.Same(configuration.SharedMemory, configuration.SharedMemory);
        }

        KahunaCommandLineOptions parsed = CommandLine.Parser.Default
            .ParseArguments<KahunaCommandLineOptions>(["--storage-revision", "v1"]).Value;
        Assert.Equal(0, ServerConfigurationFactory.Create(parsed).StorageSharedMemtableBudgetBytes);
        Assert.Equal(0, ServerConfigurationFactory.Create(parsed).StorageSharedCacheBytes);
        Assert.Null(ServerConfigurationFactory.Create(parsed).SharedMemory);

        // Either size alone makes a shared memory.
        Assert.Equal(64L * 1024 * 1024, new KahunaConfiguration { StorageSharedCacheBytes = 64L * 1024 * 1024 }.SharedMemory!.CacheBytes);
        Assert.Equal(0, new KahunaConfiguration { StorageSharedCacheBytes = 64L * 1024 * 1024 }.SharedMemory!.MemtableBudgetBytes);
        Assert.Null(EmbeddedKahunaNode.CreateKahunaConfiguration(new EmbeddedKahunaOptions(), singleProcessRaftGroup: true).SharedMemory);
    }

    [Theory]
    [InlineData(new[] { "--rocksdb-shared-memtable-budget-mb", "64" }, 64)]
    [InlineData(new[] { "--rocksdb-shared-memtable-budget-mb", "64", "--storage-shared-memtable-budget-mb", "32" }, 32)]
    [InlineData(new[] { "--storage-shared-memtable-budget-mb", "32" }, 32)]
    public void TheFormerRocksDbMemtableBudgetFlag_SetsTheSharedBudget_UnlessTheNewOneIsSet(string[] args, int expectedMb)
    {
        KahunaCommandLineOptions parsed = CommandLine.Parser.Default
            .ParseArguments<KahunaCommandLineOptions>(["--storage-revision", "v1", .. args]).Value;

        Assert.Equal(expectedMb * 1024L * 1024, ServerConfigurationFactory.Create(parsed).StorageSharedMemtableBudgetBytes);
    }

    [Theory]
    [InlineData(new[] { "--rocksdb-shared-memory-budget-mb", "512" }, 512)]
    [InlineData(new[] { "--rocksdb-shared-memory-budget-mb", "512", "--storage-shared-cache-mb", "320" }, 320)]
    [InlineData(new[] { "--storage-shared-cache-mb", "320" }, 320)]
    public void TheFormerRocksDbCacheFlag_SetsTheSharedCache_UnlessTheNewOneIsSet(string[] args, int expectedMb)
    {
        KahunaCommandLineOptions parsed = CommandLine.Parser.Default
            .ParseArguments<KahunaCommandLineOptions>(["--storage-revision", "v1", .. args]).Value;

        Assert.Equal(expectedMb * 1024L * 1024, ServerConfigurationFactory.Create(parsed).StorageSharedCacheBytes);
    }

    [Theory]
    [InlineData(-1, 0, "--storage-shared-memtable-budget-mb")]
    [InlineData(0, -1, "--storage-shared-cache-mb")]
    [InlineData(128, 64, "at most")]
    public void ANegativeSharedMemory_OrABudgetPastTheCache_IsRefused(int budgetMb, int cacheMb, string expected)
    {
        KahunaCommandLineOptions opts = NonDefaultOptions();
        opts.StorageSharedMemtableBudgetMb = budgetMb;
        opts.StorageSharedCacheMb = cacheMb;

        KahunaServerException ex = Assert.Throws<KahunaServerException>(() => ServerConfigurationFactory.Create(opts));
        Assert.Contains(expected, ex.Message);
    }

    /// <summary>
    /// The shared RocksDB cache charges its write-buffer manager's memtables into it, so it takes its size and
    /// its memtable budget from the shared memory and refuses to start without both.
    /// </summary>
    [Fact]
    public void RocksDbSharedMemory_TakesTheSharedCacheAndMemtableBudget_AndRefusesToStartWithoutThem()
    {
        EmbeddedKahunaOptions options = new()
        {
            Storage = "rocksdb",
            WalStorage = "rocksdb",
            RocksDbSharedMemoryEnabled = true,
            StorageSharedCacheMb = 64,
        };

        KahunaServerException ex = Assert.Throws<KahunaServerException>(() => EmbeddedKahunaNode.CreateSharedResources(options));
        Assert.Contains("--storage-shared-memtable-budget-mb", ex.Message);

        options.StorageSharedCacheMb = 0;
        options.StorageSharedMemtableBudgetMb = 16;
        ex = Assert.Throws<KahunaServerException>(() => EmbeddedKahunaNode.CreateSharedResources(options));
        Assert.Contains("--storage-shared-cache-mb", ex.Message);

        options.StorageSharedCacheMb = 64;
        using (Kommander.WAL.RocksDbSharedResources resources = EmbeddedKahunaNode.CreateSharedResources(options)!)
        {
            Assert.Equal(16UL * 1024 * 1024, (ulong)RocksDbSharp.Native.Instance.rocksdb_write_buffer_manager_buffer_size(resources.WriteBufferManagerHandle));
            Assert.Equal(64UL * 1024 * 1024, (ulong)RocksDbSharp.Native.Instance.rocksdb_cache_get_capacity(resources.BlockCache.Handle));
        }

        // Sharing needs both databases on RocksDB; otherwise there is no bundle, budget or not.
        options.WalStorage = "memory";
        Assert.Null(EmbeddedKahunaNode.CreateSharedResources(options));
    }

    [Fact]
    public void ServerOnlySettings_ReachTheServerConfiguration()
    {
        KahunaConfiguration configuration = ServerConfigurationFactory.Create(NonDefaultOptions());

        Assert.Equal("http://", configuration.InterNodeGrpcScheme);
        Assert.Equal("https://node9:8082", configuration.AdvertisedClientEndpoint);
        Assert.Equal("http://", configuration.AdvertisedClientScheme);
        Assert.False(configuration.AdvertisePeerEndpoints);
        Assert.False(configuration.RoutingHintsEnabled);
        Assert.False(configuration.SingleProcessRaftGroup);
    }

    [Fact]
    public void FlagsTheStandaloneNodeUsedToIgnore_ReachBothNodeShapes()
    {
        EmbeddedKahunaOptions embedded = EmbeddedOptionsFactory.CreateEmbeddedOptions(NonDefaultOptions());
        KahunaConfiguration standalone = EmbeddedKahunaNode.CreateKahunaConfiguration(embedded, singleProcessRaftGroup: true);
        KahunaConfiguration server = ServerConfigurationFactory.Create(NonDefaultOptions());

        Assert.Equal(TimeSpan.FromSeconds(17), standalone.CheckpointInterval);
        Assert.False(standalone.PublishedHeadReads);
        Assert.Equal(TimeSpan.FromSeconds(17), server.CheckpointInterval);
        Assert.False(server.PublishedHeadReads);
    }

    [Fact]
    public void RevisionsToCache_IsTheInMemoryRevisionArchive_OnBothNodeShapes()
    {
        KahunaCommandLineOptions opts = NonDefaultOptions();
        opts.RevisionsToKeepCached = 9;

        KahunaConfiguration standalone = EmbeddedKahunaNode.CreateKahunaConfiguration(
            EmbeddedOptionsFactory.CreateEmbeddedOptions(opts), singleProcessRaftGroup: true);
        KahunaConfiguration server = ServerConfigurationFactory.Create(opts);

        Assert.Equal(9, standalone.RevisionRetention);
        Assert.Equal(9, server.RevisionRetention);
    }

    [Fact]
    public void RevisionsToCache_DefaultsToFourOnEveryNodeShape()
    {
        KahunaCommandLineOptions parsed = CommandLine.Parser.Default
            .ParseArguments<KahunaCommandLineOptions>(["--storage-revision", "v1"]).Value;

        Assert.Equal(4, parsed.RevisionsToKeepCached);
        Assert.Equal(4, ServerConfigurationFactory.Create(parsed).RevisionRetention);

        // An embedding host that never sets the option gets the same value as the server.
        Assert.Equal(4, EmbeddedKahunaNode.CreateKahunaConfiguration(new EmbeddedKahunaOptions(), singleProcessRaftGroup: true).RevisionRetention);
        Assert.Equal(4, new KahunaConfiguration().RevisionRetention);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-3)]
    public void RevisionsToCache_BelowOne_IsRefused(int value)
    {
        KahunaCommandLineOptions opts = NonDefaultOptions();
        opts.RevisionsToKeepCached = value;

        KahunaServerException ex = Assert.Throws<KahunaServerException>(() => ServerConfigurationFactory.Create(opts));
        Assert.Contains("--revisions-to-cache", ex.Message);
    }

    /// <summary>
    /// The server configuration is the shared mapping plus the server-only settings, and nothing else: a
    /// server-side override of a shared field would bring back a knob that one node shape ignores.
    /// </summary>
    [Fact]
    public void ServerConfiguration_IsTheEmbeddedMapping_ExceptTheServerOnlySettings()
    {
        KahunaCommandLineOptions opts = NonDefaultOptions();
        KahunaConfiguration server = ServerConfigurationFactory.Create(opts);
        KahunaConfiguration embedded = EmbeddedKahunaNode.CreateKahunaConfiguration(
            EmbeddedOptionsFactory.CreateEmbeddedOptions(opts), singleProcessRaftGroup: false);

        List<string> differences = [];
        foreach (PropertyInfo property in typeof(KahunaConfiguration).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!property.CanRead || property.GetIndexParameters().Length > 0 || ServerOnlySettings.Contains(property.Name))
                continue;

            // Each build gets its own registry instance; the content is checked by the function tests.
            if (property.PropertyType == typeof(Kahuna.Extensibility.KahunaFunctionRegistry))
                continue;

            // Each build gets its own shared memory object; compare its sizes.
            if (property.PropertyType == typeof(Kahuna.Server.Persistence.Providers.SharedStorageMemory))
            {
                if (server.SharedMemory?.MemtableBudgetBytes != embedded.SharedMemory?.MemtableBudgetBytes
                    || server.SharedMemory?.CacheBytes != embedded.SharedMemory?.CacheBytes)
                    differences.Add($"{property.Name}: the shared memory sizes differ");

                continue;
            }

            // The same for the storage engines: compare what is registered, not the instance.
            if (property.PropertyType == typeof(Kahuna.Server.Persistence.Providers.StorageProviders))
            {
                if (!server.StorageProviders.BackendTypes.SequenceEqual(embedded.StorageProviders.BackendTypes)
                    || !server.StorageProviders.WalTypes.SequenceEqual(embedded.StorageProviders.WalTypes))
                    differences.Add($"{property.Name}: the registered storage types differ");

                continue;
            }

            if (!Equals(property.GetValue(server), property.GetValue(embedded)))
                differences.Add($"{property.Name}: server={property.GetValue(server)} embedded={property.GetValue(embedded)}");
        }

        Assert.True(differences.Count == 0, string.Join(Environment.NewLine, differences));
    }
}
