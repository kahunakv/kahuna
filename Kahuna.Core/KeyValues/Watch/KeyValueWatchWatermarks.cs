using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions;

namespace Kahuna.Server.KeyValues.Watch;

/// <summary>
/// Computes the progress watermark of each data partition on this replica and hands it to the partition's
/// watches. The watermark W promises that no event with a commit timestamp at or below W arrives later on that
/// partition. Runs only while the commit admission window is on (see <see cref="CommitAdmission"/>).
///
/// <para><b>The rule.</b> After this replica applies an entry with time τ, and after the entry's events were
/// published, <c>W = min(τ − lag, lowest pending commit timestamp on the partition − 1 ms)</c>. Every later entry
/// has a time at or above τ and admits only commits above <c>τ − lag</c>; an intent already pending is in the log,
/// and its event comes when it settles, so its commit timestamp caps W until then. Clock skew between nodes changes
/// only how many commits the window refuses, never whether W holds. A stuck pending intent holds W back for as long
/// as it stays pending; the oldest-pending-intent metric shows it.</para>
///
/// <para><b>Below the unbounded commit ceiling, no watermark.</b> An entry admitted with no bound can carry a commit
/// a replay from a lower cursor would skip (see <see cref="PartitionHistoryBounds"/>). A watermark is a resume cursor,
/// so none below the partition's ceiling is sent: every watermark a consumer holds is one it can resume from.</para>
///
/// <para><b>Order.</b> Kommander applies one partition's entries one at a time, so the progress frames and the
/// events of a partition are published from the same thread in log order: a watch never receives a watermark
/// before an event it covers. Frames are sent at most every <see cref="MinProgressIntervalMs"/> per partition;
/// skipping one is safe, because a later frame carries a later watermark.</para>
/// </summary>
internal sealed class KeyValueWatchWatermarks : IDisposable
{
    /// <summary>The shortest gap between two progress frames of one partition.</summary>
    internal const int MinProgressIntervalMs = 100;

    private sealed class PartitionProgress
    {
        // The physical part of the time of the last entry applied, for the idle tick and the lag metric. Written
        // by the partition's apply thread, read by the ticker and the metric.
        public long LastEntryPhysical;

        // When the last progress frame went out, on the monotonic tick clock. Apply thread only.
        public long LastPublishTicks;
    }

    private readonly ConcurrentDictionary<int, PartitionProgress> partitions = new();

    private readonly KeyValueWatchBroker broker;

    private readonly PreparedIntentStore intents;

    private readonly long lagMs;

    private readonly PartitionHistoryBounds? historyBounds;

    // The gauges read this node's state, so they live on a meter owned by the node and disposed with it.
    private readonly Meter gaugeMeter = new("Kahuna", "1.0");

    public KeyValueWatchWatermarks(KeyValueWatchBroker broker, PreparedIntentStore intents, long lagMs, PartitionHistoryBounds? historyBounds = null)
    {
        this.broker = broker;
        this.intents = intents;
        this.lagMs = lagMs;
        this.historyBounds = historyBounds;

        gaugeMeter.CreateObservableGauge(
            "kahuna.kv.watch.watermark_lag_ms",
            ObserveWatermarkLag,
            unit: "ms",
            description: "The largest gap on the node between the time of a partition's last applied entry and the last progress watermark sent on it.");

        gaugeMeter.CreateObservableGauge(
            "kahuna.kv.commit_admission.oldest_pending_intent_age_ms",
            ObserveOldestPendingIntentAge,
            unit: "ms",
            description: "Age of the oldest commit timestamp among the prepared intents still pending on the node. A stuck intent holds the watch watermark of its partition back.");
    }

    /// <summary>The lag bound the watermark is computed with.</summary>
    public long LagMs => lagMs;

    /// <summary>
    /// Records that this replica applied the entry <paramref name="logIndex"/> of <paramref name="partitionId"/> at <paramref name="entryTime"/>,
    /// after the entry's events were published, and sends the partition's watches a new watermark when one is due.
    /// Called from the partition's ordered apply.
    /// </summary>
    public void OnEntryApplied(int partitionId, HLCTimestamp entryTime, long logIndex)
    {
        if (entryTime == HLCTimestamp.Zero)
            return;

        PartitionProgress progress = partitions.GetOrAdd(partitionId, static _ => new PartitionProgress());
        Volatile.Write(ref progress.LastEntryPhysical, entryTime.L);

        if (!broker.HasSubscribers(partitionId))
            return;

        long now = Environment.TickCount64;
        if (now - progress.LastPublishTicks < MinProgressIntervalMs)
            return;

        HLCTimestamp watermark = WatermarkAt(partitionId, entryTime);

        if (historyBounds is not null && watermark < historyBounds.UnboundedCeiling(partitionId))
            return;

        if (broker.PublishProgress(partitionId, watermark, logIndex))
            progress.LastPublishTicks = now;
    }

    /// <summary>The watermark of <paramref name="partitionId"/> right after an entry at <paramref name="entryTime"/>
    /// applied.</summary>
    internal HLCTimestamp WatermarkAt(int partitionId, HLCTimestamp entryTime)
    {
        HLCTimestamp watermark = CommitAdmission.Floor(entryTime, lagMs);

        if (intents.TryGetLowestPendingCommit(partitionId, out HLCTimestamp lowestPending))
        {
            HLCTimestamp belowPending = new(lowestPending.N, lowestPending.L - 1, lowestPending.C);
            if (belowPending < watermark)
                watermark = belowPending;
        }

        return watermark;
    }

    /// <summary>The physical time of the last entry this replica applied on <paramref name="partitionId"/>, or zero
    /// when it applied none since the node started.</summary>
    public long LastEntryPhysical(int partitionId) =>
        partitions.TryGetValue(partitionId, out PartitionProgress? progress) ? Volatile.Read(ref progress.LastEntryPhysical) : 0;

    public void Dispose() => gaugeMeter.Dispose();

    private Measurement<long> ObserveWatermarkLag()
    {
        long largest = 0;

        foreach (KeyValuePair<int, PartitionProgress> entry in partitions)
        {
            if (!broker.TryGetPublishedWatermark(entry.Key, out HLCTimestamp watermark))
                continue;

            long gap = Volatile.Read(ref entry.Value.LastEntryPhysical) - watermark.L;
            if (gap > largest)
                largest = gap;
        }

        return new Measurement<long>(largest);
    }

    private Measurement<long> ObserveOldestPendingIntentAge()
    {
        if (!intents.TryGetLowestPendingCommit(out HLCTimestamp lowest))
            return new Measurement<long>(0);

        return new Measurement<long>(Math.Max(0, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - lowest.L));
    }
}
