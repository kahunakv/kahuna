
using Kommander.Time;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Summary of a completed PITR restore operation.
/// </summary>
internal sealed record RestoreResult(
    /// <summary>Number of partitions for which at least one WAL entry was applied.</summary>
    int PartitionsRestored,
    /// <summary>Total WAL entries applied across all partitions.</summary>
    long EntriesApplied,
    /// <summary>
    /// Highest HLC timestamp among all applied entries across all partitions.
    /// Default when nothing was applied.
    /// </summary>
    HLCTimestamp LastAppliedTime,
    /// <summary>
    /// WAL index of the entry that produced <see cref="LastAppliedTime"/>.
    /// Meaningful as a pair with <see cref="LastAppliedTime"/>; the index alone is
    /// partition-scoped and not comparable across partitions.
    /// Zero when nothing was applied.
    /// </summary>
    long LastAppliedIndex);
