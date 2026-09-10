using Kahuna.Server.Configuration;
using Kommander.WAL;

namespace Kahuna.Server.Tests;

/// <summary>
/// Coverage for the Raft WAL shard column-family tuning surface.
/// <para>
/// The failure this guards against is the one the knobs were added to fix: Kommander gained eight
/// sizing knobs on its RocksDB Raft log and no host above it could set any of them, because both
/// WAL construction sites passed no tuning at all. So each knob is followed the whole way —
/// command line, embedded options, the record Kommander receives, and for two of them the options
/// RocksDB actually opened the database with.
/// </para>
/// <para>
/// The other half is the inverse property, and it matters more than any single mapping: a host that
/// sets nothing must get Kommander's shipped layout field for field. These knobs change write
/// amplification and restart-replay cost for every consumer, so "unset" has to mean untouched.
/// </para>
/// </summary>
public sealed class TestRaftWalShardTuningSurface
{
    /// <summary>Non-default values throughout, so a missed assignment cannot pass by coincidence.</summary>
    private static KahunaCommandLineOptions TunedCommandLine() => new()
    {
        RaftWalShardWriteBufferSizeMb = 32,
        RaftWalShardMinWriteBufferNumberToMerge = 3,
        RaftWalShardMaxWriteBufferNumber = 6,
        RaftWalShardLevel0FileNumCompactionTrigger = 10,
        RaftWalShardLevel0SlowdownWritesTrigger = 30,
        RaftWalShardLevel0StopWritesTrigger = 50,
        RaftWalShardMaxBytesForLevelBaseMb = 2048,
        RaftWalShardUniversalCompaction = true
    };

    [Fact]
    public void UnsetKnobs_ProduceKommandersShippedTuning()
    {
        RocksDbWalTuning built = RaftWalTuningFactory.Build(new EmbeddedKahunaOptions());

        // Record equality, not a field-by-field list: a field added to RocksDbWalTuning that this
        // surface forgets to carry still has to leave the default alone, and only whole-record
        // equality keeps saying so after that field exists.
        Assert.Equal(RocksDbWalTuning.Default, built);
    }

    [Fact]
    public void EveryKnob_ReachesTheTuningKommanderReceives()
    {
        RocksDbWalTuning built = RaftWalTuningFactory.Build(new EmbeddedKahunaOptions
        {
            RaftWalShardWriteBufferSizeMb = 32,
            RaftWalShardMinWriteBufferNumberToMerge = 3,
            RaftWalShardMaxWriteBufferNumber = 6,
            RaftWalShardLevel0FileNumCompactionTrigger = 10,
            RaftWalShardLevel0SlowdownWritesTrigger = 30,
            RaftWalShardLevel0StopWritesTrigger = 50,
            RaftWalShardMaxBytesForLevelBaseMb = 2048,
            RaftWalShardUniversalCompaction = true
        });

        Assert.Equal(32L * 1024 * 1024, built.ShardWriteBufferSizeBytes);
        Assert.Equal(3, built.ShardMinWriteBufferNumberToMerge);
        Assert.Equal(6, built.ShardMaxWriteBufferNumber);
        Assert.Equal(10, built.ShardLevel0FileNumCompactionTrigger);
        Assert.Equal(30, built.ShardLevel0SlowdownWritesTrigger);
        Assert.Equal(50, built.ShardLevel0StopWritesTrigger);
        Assert.Equal(2048L * 1024 * 1024, built.ShardMaxBytesForLevelBase);
        Assert.True(built.ShardUniversalCompaction);
    }

    [Fact]
    public void OneKnobSet_LeavesTheOtherSevenOnKommandersDefaults()
    {
        // The single-override case is the one an operator actually writes, and the one a naive
        // mapping breaks: building the record from the host's values instead of from Default would
        // silently zero the seven knobs nobody mentioned.
        RocksDbWalTuning shipped = RocksDbWalTuning.Default;

        RocksDbWalTuning built = RaftWalTuningFactory.Build(new EmbeddedKahunaOptions
        {
            RaftWalShardMaxBytesForLevelBaseMb = 2048
        });

        Assert.Equal(shipped with { ShardMaxBytesForLevelBase = 2048L * 1024 * 1024 }, built);
    }

    [Fact]
    public void EveryFlag_ReachesTheEmbeddedOptions()
    {
        EmbeddedKahunaOptions options = EmbeddedOptionsFactory.CreateEmbeddedOptions(TunedCommandLine());

        Assert.Equal(32, options.RaftWalShardWriteBufferSizeMb);
        Assert.Equal(3, options.RaftWalShardMinWriteBufferNumberToMerge);
        Assert.Equal(6, options.RaftWalShardMaxWriteBufferNumber);
        Assert.Equal(10, options.RaftWalShardLevel0FileNumCompactionTrigger);
        Assert.Equal(30, options.RaftWalShardLevel0SlowdownWritesTrigger);
        Assert.Equal(50, options.RaftWalShardLevel0StopWritesTrigger);
        Assert.Equal(2048, options.RaftWalShardMaxBytesForLevelBaseMb);
        Assert.True(options.RaftWalShardUniversalCompaction);
    }

    [Fact]
    public void ADefaultCommandLine_ReachesKommandersShippedTuning()
    {
        // Zero is how the command line spells "unset" on these knobs. A default-constructed
        // KahunaCommandLineOptions must therefore still come out equal to the shipped record — the
        // translation is where a sentinel is most easily mistaken for a real value of zero.
        EmbeddedKahunaOptions options = EmbeddedOptionsFactory.CreateEmbeddedOptions(new KahunaCommandLineOptions());

        Assert.Null(options.RaftWalShardWriteBufferSizeMb);
        Assert.Null(options.RaftWalShardMaxBytesForLevelBaseMb);
        Assert.Equal(RocksDbWalTuning.Default, RaftWalTuningFactory.Build(options));
    }

    [Theory]
    [InlineData(nameof(EmbeddedKahunaOptions.RaftWalShardWriteBufferSizeMb))]
    [InlineData(nameof(EmbeddedKahunaOptions.RaftWalShardMinWriteBufferNumberToMerge))]
    [InlineData(nameof(EmbeddedKahunaOptions.RaftWalShardMaxWriteBufferNumber))]
    [InlineData(nameof(EmbeddedKahunaOptions.RaftWalShardLevel0FileNumCompactionTrigger))]
    [InlineData(nameof(EmbeddedKahunaOptions.RaftWalShardLevel0SlowdownWritesTrigger))]
    [InlineData(nameof(EmbeddedKahunaOptions.RaftWalShardLevel0StopWritesTrigger))]
    [InlineData(nameof(EmbeddedKahunaOptions.RaftWalShardMaxBytesForLevelBaseMb))]
    public void ANonPositiveKnob_IsRefusedAndNamed(string option)
    {
        EmbeddedKahunaOptions options = new();
        typeof(EmbeddedKahunaOptions).GetProperty(option)!.SetValue(options, 0);

        KahunaServerException error = Assert.Throws<KahunaServerException>(
            () => RaftWalTuningFactory.Build(options));

        Assert.Contains(option, error.Message);
    }

    [Fact]
    public void WriteBuffersWithoutHeadroomForTheMergeQuorum_AreRefused()
    {
        // Kommander's stall-lock guard: a flush claims the merge quorum of immutable memtables, so
        // the writer needs one mutable memtable above it or every rotation stalls. Equality is the
        // boundary case and is therefore the one asserted.
        EmbeddedKahunaOptions options = new()
        {
            RaftWalShardMinWriteBufferNumberToMerge = 4,
            RaftWalShardMaxWriteBufferNumber = 4
        };

        KahunaServerException error = Assert.Throws<KahunaServerException>(
            () => RaftWalTuningFactory.Build(options));

        Assert.Contains(nameof(EmbeddedKahunaOptions.RaftWalShardMaxWriteBufferNumber), error.Message);
        Assert.Contains(nameof(EmbeddedKahunaOptions.RaftWalShardMinWriteBufferNumberToMerge), error.Message);
    }

    [Fact]
    public void AOneSidedMergeOverrideThatBreaksTheGuard_IsRefused()
    {
        // Only the merge count is given. Against Kommander's default of 4 maximum buffers it leaves
        // no mutable memtable, so the effective pair is invalid even though neither key looks wrong
        // on its own. Checking the effective pair is what catches it.
        EmbeddedKahunaOptions options = new()
        {
            RaftWalShardMinWriteBufferNumberToMerge = RocksDbWalTuning.Default.ShardMaxWriteBufferNumber
        };

        Assert.Throws<KahunaServerException>(() => RaftWalTuningFactory.Build(options));
    }

    [Fact]
    public void UnorderedLevel0Triggers_AreRefusedAndNamed()
    {
        EmbeddedKahunaOptions options = new()
        {
            RaftWalShardLevel0FileNumCompactionTrigger = 40,
            RaftWalShardLevel0SlowdownWritesTrigger = 20,
            RaftWalShardLevel0StopWritesTrigger = 50
        };

        KahunaServerException error = Assert.Throws<KahunaServerException>(
            () => RaftWalTuningFactory.Build(options));

        Assert.Contains(nameof(EmbeddedKahunaOptions.RaftWalShardLevel0SlowdownWritesTrigger), error.Message);
    }

    [Fact]
    public void AOneSidedSlowdownOverrideThatCrossesTheStopTrigger_IsRefused()
    {
        // Slowdown raised past Kommander's default stop trigger of 44: writers would be slowed
        // above the point at which they are already stopped.
        EmbeddedKahunaOptions options = new()
        {
            RaftWalShardLevel0SlowdownWritesTrigger = RocksDbWalTuning.Default.ShardLevel0StopWritesTrigger + 1
        };

        Assert.Throws<KahunaServerException>(() => RaftWalTuningFactory.Build(options));
    }

    [Fact]
    public void ABadKnob_IsRefusedEvenWhenTheWalBackendWouldNeverReadIt()
    {
        // An in-memory WAL never builds a RocksDB database, so nothing downstream would look at
        // these values. Accepting a broken one here would hide the typo until the deployment that
        // switches the WAL to RocksDB.
        EmbeddedKahunaOptions options = new()
        {
            WalStorage = "memory",
            RaftWalShardMaxWriteBufferNumber = 1,
            RaftWalShardMinWriteBufferNumberToMerge = 2
        };

        Assert.Throws<ArgumentException>(() => new EmbeddedKahunaNode(options));
    }

    [Fact]
    public async Task ALargerLevelBase_ReachesTheOptionsRocksDbOpensWith()
    {
        string walPath = Path.Combine(Path.GetTempPath(), "kahuna-waltuning-" + Guid.NewGuid().ToString("N"));

        string log = await OpenNodeAndReadWalLogAsync(walPath, new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "rocksdb",
            WalPath = walPath,
            WalRevision = "v1",
            InitialPartitions = 1,
            RaftWalShardMaxBytesForLevelBaseMb = 2048
        });

        // RocksDB dumps the effective per-column-family options when it opens the database, so this
        // is the engine's own account of what it runs with — not a restatement of what was passed.
        Assert.Contains("max_bytes_for_level_base: 2147483648", log);
    }

    [Fact]
    public async Task UniversalCompaction_ReachesTheOptionsRocksDbOpensWith()
    {
        string walPath = Path.Combine(Path.GetTempPath(), "kahuna-waltuning-" + Guid.NewGuid().ToString("N"));

        string log = await OpenNodeAndReadWalLogAsync(walPath, new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "rocksdb",
            WalPath = walPath,
            WalRevision = "v1",
            InitialPartitions = 1,
            RaftWalShardUniversalCompaction = true
        });

        Assert.Contains("kCompactionStyleUniversal", log);
    }

    /// <summary>
    /// Opens an embedded node on <paramref name="options"/>, closes it, and returns the RocksDB
    /// <c>LOG</c> the Raft WAL wrote. The node is disposed before the log is read so the file is
    /// complete and the temporary directory can be removed.
    /// </summary>
    private static async Task<string> OpenNodeAndReadWalLogAsync(string walPath, EmbeddedKahunaOptions options)
    {
        try
        {
            await using (EmbeddedKahunaNode node = new(options))
                await node.StartAsync(TestContext.Current.CancellationToken);

            return await File.ReadAllTextAsync(
                Path.Combine(walPath, options.WalRevision, "LOG"),
                TestContext.Current.CancellationToken);
        }
        finally
        {
            if (Directory.Exists(walPath))
                Directory.Delete(walPath, recursive: true);
        }
    }
}
