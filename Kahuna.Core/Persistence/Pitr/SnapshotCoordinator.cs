
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Computes the cluster-wide snapshot timestamp T for coordinated backups.
///
/// <para><b>What this protects against:</b> T is placed strictly below the minimum in-flight
/// (prepared but not yet committed) <c>CommitTimestamp</c> across all actor shards, so any
/// transaction that is actively mid-commit when the coordinator runs will not be partially
/// included — its prepared mutations land at <c>Time ≥ T</c> and are excluded by the segment
/// cap.</para>
///
/// <para><b>Known limitation — already-committed straddles:</b> this mechanism inspects only
/// live <c>WriteIntent</c> state.  A cross-shard transaction that <em>committed before</em> the
/// coordinator ran has no live intent; its per-shard commit HLCs are already in the WAL and may
/// straddle T (shard A at <c>t=240</c>, shard B at <c>t=260</c>, T chosen between them).
/// <c>min(in-flight)</c> does <em>not</em> prevent that tearing.  The complete unconditional fix
/// requires stamping a single shared commit HLC on all participating shards — a transaction-layer
/// change deferred beyond Phase 1.  In quiesced clusters (no in-flight work) the fallback
/// returns the WAL max committed HLC, which is safe because no straddling is possible when the
/// cluster is idle.</para>
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
    ///   <item>Calls <paramref name="queryClusterMinInFlight"/> to obtain the minimum prepared
    ///   <c>CommitTimestamp</c> M across all actor shards.  The delegate returns
    ///   <see cref="HLCTimestamp.Zero"/> when the cluster is quiesced (no in-flight 2PC).</item>
    ///   <item>If M is non-zero, returns <c>Predecessor(M)</c> — the HLC tick immediately
    ///   before M.  Because the segment reader cap is inclusive (<c>Time ≤ T</c>), any entry
    ///   with <c>Time ≥ M</c> (including an in-flight commit landing exactly at M) is excluded.
    ///   Already-committed transactions whose WAL <c>Time</c> values straddle T are NOT
    ///   protected by this path — see class-level doc for the limitation.</item>
    ///   <item>If quiesced (M == Zero), falls back to the WAL maximum committed HLC.  No
    ///   straddling is possible when the cluster is fully idle, so no decrement is needed.</item>
    /// </list>
    ///
    /// Returns <see cref="HLCTimestamp.Zero"/> only when the cluster is quiesced AND the WAL
    /// has no committed entries on any partition (e.g. a brand-new empty cluster).
    /// </summary>
    /// <param name="queryClusterMinInFlight">
    /// Async delegate that fans out <c>GetSafeTimestamp</c> to all actor shards and returns
    /// the cluster-wide minimum in-flight <c>CommitTimestamp</c> (or Zero).
    /// </param>
    /// <param name="wal">WAL adapter used for the quiesced fallback scan.</param>
    /// <param name="partitions">Partition list — same set passed to <see cref="BackupDriver"/>.</param>
    internal static async Task<HLCTimestamp> ComputeSafeSnapshotTimeAsync(
        Func<Task<HLCTimestamp>> queryClusterMinInFlight,
        IWAL wal,
        IReadOnlyList<RaftPartitionRange> partitions)
    {
        HLCTimestamp minInFlight = await queryClusterMinInFlight();

        if (minInFlight != HLCTimestamp.Zero)
            return Predecessor(minInFlight);

        // Quiesced path: WAL max committed is already a fully-durable boundary — no decrement.
        HLCTimestamp maxCommitted = HLCTimestamp.Zero;
        foreach (RaftPartitionRange partition in partitions)
        {
            if (partition.State is RaftPartitionState.Draining or RaftPartitionState.Removed)
                continue;

            (_, HLCTimestamp hlc, _) = BackupDriver.FindLastCommitted(wal, partition.PartitionId);
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
