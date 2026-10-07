
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Computes the cluster-wide snapshot timestamp T for coordinated backups.
///
/// <para><b>What this protects against:</b> T is placed strictly below the minimum in-flight
/// <c>CommitTimestamp</c> this node can see — prepared actor write intents and the durable prepared
/// intents that can still commit — so a transaction visibly mid-commit when the coordinator runs commits
/// with a shared HLC strictly above T and is excluded as a whole. A durable prepare can apply on this node
/// after T is chosen (its commit timestamp is minted on its coordinator first), so the choice alone is not
/// a proof: <see cref="BackupService"/> verifies T after the capture and chooses again when a transaction
/// at or below it was in flight.</para>
///
/// <para><b>Already-committed cross-shard transactions are not torn.</b> Each participant of a
/// transaction carries the same shared coordinator commit HLC in its committed value's payload
/// (<c>LastModified</c>), even though each shard's Raft WAL entry <c>Time</c> is stamped from that
/// shard's own local clock and the two can straddle T. Backup capture and restore both cut on the
/// shared commit HLC, not the per-shard WAL <c>Time</c> (see <see cref="RestoreEngine"/> and
/// <see cref="BackupDriver"/>), so a transaction whose per-shard WAL <c>Time</c> values straddle T
/// is still included or excluded atomically. In quiesced clusters (no in-flight work) the fallback
/// returns the WAL max committed HLC.</para>
///
/// <para><b>Testability:</b> the cluster-query is injected as a delegate so unit tests can
/// supply a simple stub without a live actor system.  The WAL fallback uses
/// <see cref="BackupDriver.FindLastCommitted"/> directly.</para>
/// </summary>
internal static class SnapshotCoordinator
{
    /// <summary>
    /// Computes the cluster-wide T.
    ///
    /// <list type="bullet">
    ///   <item>Calls <paramref name="queryClusterMinInFlight"/> to obtain the minimum in-flight
    ///   <c>CommitTimestamp</c> M this node sees.  The delegate returns
    ///   <see cref="HLCTimestamp.Zero"/> when nothing is visibly in flight.</item>
    ///   <item>If M is non-zero, returns <c>Predecessor(M)</c> — the HLC tick immediately
    ///   before M.  A transaction still in flight commits with a shared HLC <c>≥ M</c> — strictly
    ///   above T — so the commit-HLC cut applied at capture/restore excludes it as a whole.
    ///   Already-committed transactions are handled by that same commit-HLC cut regardless of how
    ///   their per-shard WAL <c>Time</c> values fall relative to T (see class-level doc).</item>
    ///   <item>If quiesced (M == Zero), falls back to the WAL maximum committed HLC.  No
    ///   straddling is possible when the cluster is fully idle, so no decrement is needed.</item>
    /// </list>
    ///
    /// Returns <see cref="HLCTimestamp.Zero"/> only when the cluster is quiesced AND the WAL
    /// has no committed entries on any partition (e.g. a brand-new empty cluster).
    /// </summary>
    /// <param name="queryClusterMinInFlight">
    /// Async delegate that returns the minimum in-flight <c>CommitTimestamp</c> this node sees across its
    /// actor shards and its durable prepared intents (or Zero).
    /// </param>
    /// <param name="wal">WAL adapter used for the quiesced fallback scan.</param>
    /// <param name="partitions">Partition list — same set passed to <see cref="BackupDriver"/>.</param>
    internal static async Task<HLCTimestamp> ComputeSafeSnapshotTimeAsync(
        Func<Task<HLCTimestamp>> queryClusterMinInFlight,
        IWAL wal,
        IReadOnlyList<RaftPartitionRange> partitions,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        HLCTimestamp minInFlight = await queryClusterMinInFlight();

        if (minInFlight != HLCTimestamp.Zero)
            return Predecessor(minInFlight);

        // Quiesced path: WAL max committed is already a fully-durable boundary — no decrement.
        HLCTimestamp maxCommitted = HLCTimestamp.Zero;
        foreach (RaftPartitionRange partition in partitions)
        {
            ct.ThrowIfCancellationRequested();

            if (partition.State is RaftPartitionState.Draining or RaftPartitionState.Removed)
                continue;

            (_, HLCTimestamp hlc, _) = BackupDriver.FindLastCommitted(wal, partition.PartitionId, ct);
            if (hlc.CompareTo(maxCommitted) > 0)
                maxCommitted = hlc;
        }

        return maxCommitted;
    }

    /// <summary>
    /// Returns the HLC timestamp immediately preceding <paramref name="ts"/> in the total
    /// ordering defined by <c>HLCTimestamp.CompareTo</c> (primary: L/physical ms, secondary: C/counter).
    /// When <c>C &gt; 0</c> decrements the counter; when <c>C == 0</c> wraps to the previous
    /// millisecond with counter <c>uint.MaxValue</c>.  Returns <see cref="HLCTimestamp.Zero"/>
    /// when <paramref name="ts"/> is already at the minimum.
    /// </summary>
    internal static HLCTimestamp Predecessor(HLCTimestamp ts)
    {
        if (ts == HLCTimestamp.Zero)
            return HLCTimestamp.Zero;

        if (ts.C > 0)
            return new HLCTimestamp(ts.N, ts.L, ts.C - 1);

        // C == 0: wrap into the previous millisecond.
        return ts.L > 0
            ? new HLCTimestamp(ts.N, ts.L - 1, uint.MaxValue)
            : HLCTimestamp.Zero;
    }
}
