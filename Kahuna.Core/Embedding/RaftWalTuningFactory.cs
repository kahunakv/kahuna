using Kahuna.Server.Configuration;
using Kommander.WAL;

namespace Kahuna;

/// <summary>
/// Turns the eight <c>RaftWalShard*</c> knobs of <see cref="EmbeddedKahunaOptions"/> into the
/// <see cref="RocksDbWalTuning"/> record Kommander's <see cref="RocksDbWAL"/> takes.
///
/// <para>It exists so the two hosts that build a WAL — the embedded node and the
/// <c>Kahuna.Server</c> cluster entry point — share one mapping. Duplicating the eight assignments
/// is how a knob ends up wired on one path and silently left on its default on the other, which is
/// invisible to any test that only exercises the embedded node.</para>
///
/// <para><b>Unset means unchanged.</b> The build starts from <see cref="RocksDbWalTuning.Default"/>
/// and applies only the fields the host actually set, so a host that sets none produces a record
/// equal to <see cref="RocksDbWalTuning.Default"/> — the shipped layout, field for field. The
/// defaults live in Kommander and are never restated here: a copy would be a second source of truth
/// that drifts the next time Kommander retunes one.</para>
///
/// <para>The method is pure and validates before it maps, so every construction path rejects a bad
/// value with the Kahuna option named (see
/// <see cref="ConfigurationValidator.ValidateRaftWalShardTuning"/>).</para>
/// </summary>
public static class RaftWalTuningFactory
{
    /// <summary>
    /// The WAL tuning <paramref name="options"/> describes. Call it wherever a
    /// <see cref="RocksDbWAL"/> is constructed; the result is safe to build more than once because
    /// nothing here mutates <paramref name="options"/>.
    /// </summary>
    /// <exception cref="KahunaServerException">Thrown when a knob is out of range or a cross-field
    /// guard fails.</exception>
    public static RocksDbWalTuning Build(EmbeddedKahunaOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        ConfigurationValidator.ValidateRaftWalShardTuning(options);

        RocksDbWalTuning tuning = RocksDbWalTuning.Default;

        if (options.RaftWalShardWriteBufferSizeMb is int writeBufferMb)
            tuning = tuning with { ShardWriteBufferSizeBytes = (long)writeBufferMb * 1024 * 1024 };

        if (options.RaftWalShardMinWriteBufferNumberToMerge is int minToMerge)
            tuning = tuning with { ShardMinWriteBufferNumberToMerge = minToMerge };

        if (options.RaftWalShardMaxWriteBufferNumber is int maxBuffers)
            tuning = tuning with { ShardMaxWriteBufferNumber = maxBuffers };

        if (options.RaftWalShardLevel0FileNumCompactionTrigger is int compactionTrigger)
            tuning = tuning with { ShardLevel0FileNumCompactionTrigger = compactionTrigger };

        if (options.RaftWalShardLevel0SlowdownWritesTrigger is int slowdownTrigger)
            tuning = tuning with { ShardLevel0SlowdownWritesTrigger = slowdownTrigger };

        if (options.RaftWalShardLevel0StopWritesTrigger is int stopTrigger)
            tuning = tuning with { ShardLevel0StopWritesTrigger = stopTrigger };

        if (options.RaftWalShardMaxBytesForLevelBaseMb is int levelBaseMb)
            tuning = tuning with { ShardMaxBytesForLevelBase = (long)levelBaseMb * 1024 * 1024 };

        if (options.RaftWalShardUniversalCompaction is bool universal)
            tuning = tuning with { ShardUniversalCompaction = universal };

        return tuning;
    }
}
