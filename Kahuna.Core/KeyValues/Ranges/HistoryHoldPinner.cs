using System.Diagnostics.Metrics;

using Nixie;

using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Watch;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Keeps this replica's WAL retained for the history holds of the partitions it hosts. For each hosted data partition
/// that has a registered hold, the pinner keeps exactly one Kommander retention hold at the lowest index a resume from
/// the lowest held timestamp reads (<see cref="IndexFor"/>): every event with a commit timestamp above a held timestamp
/// W comes from an entry whose time is above <c>W − MaxClockLeadMs</c>, and the replay also reads the entry before the
/// first such entry.
///
/// <para><b>Order.</b> A new handle is acquired before the old one is disposed, so the pin never drops in between.
/// Kommander composes retention holds by minimum, so this pin never weakens a backup's or the PITR horizon's.</para>
///
/// <para><b>Startup.</b> Kommander compacts a partition only after a commit or a follower append persists, and a
/// partition finishes its restore before it takes either. The registry is loaded from its durable copy before Raft
/// starts, so <see cref="PinOnRestore"/>, called from the restore completion, pins a partition that has a hold at
/// index 1 (everything retained) before its first compaction can run. The exact index follows from the next
/// reconcile, which runs off the apply and restore threads.</para>
/// </summary>
internal sealed class HistoryHoldPinner : IDisposable
{
    private readonly record struct Pin(long Index, IDisposable Handle);

    private readonly IRaft raft;

    private readonly HistoryHoldStore store;

    private readonly ILogger<IKahuna> logger;

    private readonly object gate = new();

    // Guarded by gate.
    private readonly Dictionary<int, Pin> pins = [];

    private readonly Meter gaugeMeter = new("Kahuna", "1.0");

    private bool disposed;

    public HistoryHoldPinner(IRaft raft, HistoryHoldStore store, ILogger<IKahuna> logger)
    {
        this.raft = raft;
        this.store = store;
        this.logger = logger;

        gaugeMeter.CreateObservableGauge(
            "kahuna.kv.history_holds.pinned_entries",
            ObservePinnedEntries,
            description: "WAL entries a history hold keeps retained on this node, by partition (the last log index minus the pinned index).");
    }

    /// <summary>The index each partition is pinned at on this node. Test access.</summary>
    internal Dictionary<int, long> PinnedIndexes()
    {
        lock (gate)
            return pins.ToDictionary(p => p.Key, p => p.Value.Index);
    }

    /// <summary>Pins <paramref name="partitionId"/> whole when the registry holds history on it and it is not pinned
    /// yet. Called when the partition's restore finishes, before it takes new entries. Never blocks on I/O.</summary>
    public void PinOnRestore(int partitionId)
    {
        if (!store.HasHolds(partitionId))
            return;

        lock (gate)
        {
            if (disposed || pins.ContainsKey(partitionId))
                return;

            pins[partitionId] = new Pin(1, raft.AcquireRetentionHold(partitionId, 1));
        }
    }

    /// <summary>
    /// Brings every pin in line with the registry: pins each hosted partition that has a hold at its exact index, and
    /// releases the pin of a partition with no hold left or no longer hosted here. Reads the WAL, so it runs on the
    /// pinner's own actor, never on an apply or restore thread.
    /// </summary>
    public void Reconcile()
    {
        Dictionary<int, HLCTimestamp> lowest = store.LowestTimestamps();

        foreach ((int partitionId, HLCTimestamp timestamp) in lowest)
        {
            if (!raft.HostsPartition(partitionId))
            {
                Unpin(partitionId);
                continue;
            }

            long index;
            try
            {
                index = IndexFor(partitionId, timestamp);
            }
            catch (Exception ex)
            {
                // A WAL read that fails leaves the current pin in place; the next reconcile tries again.
                logger.LogWarning(ex, "Could not map the history hold of partition {PartitionId} to a log index", partitionId);
                continue;
            }

            lock (gate)
            {
                if (disposed)
                    return;

                if (pins.TryGetValue(partitionId, out Pin current) && current.Index == index)
                    continue;

                pins[partitionId] = new Pin(index, raft.AcquireRetentionHold(partitionId, index));

                if (current.Handle is not null)
                    current.Handle.Dispose();
            }
        }

        List<int> stale;
        lock (gate)
            stale = pins.Keys.Where(p => !lowest.ContainsKey(p)).ToList();

        foreach (int partitionId in stale)
            Unpin(partitionId);
    }

    /// <summary>
    /// The lowest index a resume from <paramref name="timestamp"/> reads on this replica: the entry before the replay's
    /// first index (<see cref="KeyValueWatchReplayer.FirstReplayIndex"/>), which the replay reads as proof that it did
    /// not start after missing history; or, when every retained entry is older than the replay start, the last entry,
    /// which an empty replay reads for the same proof.
    /// </summary>
    internal long IndexFor(int partitionId, HLCTimestamp timestamp)
    {
        long first = KeyValueWatchReplayer.FirstReplayIndex(raft.WalAdapter, partitionId, timestamp);
        if (first > 0)
            return Math.Max(1, first - 1);

        return Math.Max(1, raft.WalAdapter.GetMaxLog(partitionId));
    }

    private void Unpin(int partitionId)
    {
        lock (gate)
        {
            if (pins.Remove(partitionId, out Pin pin))
                pin.Handle.Dispose();
        }
    }

    private IEnumerable<Measurement<long>> ObservePinnedEntries()
    {
        List<(int PartitionId, long Index)> snapshot;
        lock (gate)
            snapshot = pins.Select(p => (p.Key, p.Value.Index)).ToList();

        foreach ((int partitionId, long index) in snapshot)
        {
            long max = raft.WalAdapter.GetMaxLog(partitionId);
            yield return new Measurement<long>(Math.Max(0, max - index + 1), new KeyValuePair<string, object?>("partition", partitionId));
        }
    }

    public void Dispose()
    {
        gaugeMeter.Dispose();

        lock (gate)
        {
            disposed = true;

            foreach (Pin pin in pins.Values)
                pin.Handle.Dispose();

            pins.Clear();
        }
    }
}

internal sealed class HistoryHoldPinnerRequest
{
}

/// <summary>
/// Runs the <see cref="HistoryHoldPinner"/>'s reconcile off the apply path: on a signal (a registry change, a hosted
/// partition change, a finished restore) and on a fixed period, which also covers a partition that started to be hosted
/// after the last signal.
/// </summary>
internal sealed class HistoryHoldPinnerActor : IActor<HistoryHoldPinnerRequest>
{
    private static readonly TimeSpan Period = TimeSpan.FromSeconds(5);

    private readonly HistoryHoldPinner pinner;

    private readonly ILogger<IKahuna> logger;

    public HistoryHoldPinnerActor(
        IActorContext<HistoryHoldPinnerActor, HistoryHoldPinnerRequest> context,
        HistoryHoldPinner pinner,
        ILogger<IKahuna> logger)
    {
        this.pinner = pinner;
        this.logger = logger;

        context.ActorSystem.StartPeriodicTimer(context.Self, "history-hold-reconcile", new(), Period, Period);
    }

    public Task Receive(HistoryHoldPinnerRequest message)
    {
        try
        {
            pinner.Reconcile();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "History hold reconcile failed");
        }

        return Task.CompletedTask;
    }
}
