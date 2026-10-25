using Nixie;

using Kommander;

using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Replication;

namespace Kahuna.Server.KeyValues.Watch;

/// <summary>
/// Keeps the watch watermark moving on idle data partitions. A watermark advances only when a partition applies an
/// entry, so a partition with no writes would hold its watches' watermark still. While the commit admission window is
/// on, this actor wakes on a fixed period, and for every data partition this node leads whose last applied entry is
/// older than one period, it proposes one empty <see cref="ReplicationTypes.ProgressTick"/> entry. A busy partition
/// gets no tick, so the cost is one small entry per idle partition per period, cluster-wide.
///
/// <para>The tick runs whether or not a watch is open: the leader cannot see the watches its followers serve, and a
/// fixed period is cheaper than a report from every replica.</para>
/// </summary>
internal sealed class WatchProgressTickActor : IActor<WatchProgressTickRequest>
{
    private static readonly byte[] TickPayload = [0];

    private readonly IRaft raft;

    private readonly KeyValueWatchWatermarks watermarks;

    private readonly RangeMapStore rangeMapStore;

    private readonly ILogger<IKahuna> logger;

    private readonly CancellationToken shutdown;

    private readonly int intervalMs;

    /// <summary>The hash pool, sized from the committed partition map, that every tick covers.</summary>
    private readonly DataPartitionRouter dataPartitionRouter;

    public WatchProgressTickActor(
        IActorContext<WatchProgressTickActor, WatchProgressTickRequest> context,
        IRaft raft,
        KeyValueWatchWatermarks watermarks,
        RangeMapStore rangeMapStore,
        ILogger<IKahuna> logger,
        CancellationToken shutdown)
    {
        this.raft = raft;
        this.watermarks = watermarks;
        this.rangeMapStore = rangeMapStore;
        this.logger = logger;
        this.shutdown = shutdown;

        dataPartitionRouter = new(raft);

        intervalMs = IntervalFor(watermarks.LagMs);
        TimeSpan interval = TimeSpan.FromMilliseconds(intervalMs);

        context.ActorSystem.StartPeriodicTimer(context.Self, "watch-progress-tick", new(), interval, interval);
    }

    /// <summary>The tick period for a lag bound: half the lag, between 100 ms and 1 s. The watermark trails the
    /// newest entry by the lag, so a period much shorter than the lag adds entries without making it fresher.</summary>
    internal static int IntervalFor(long lagMs) => (int)Math.Clamp(lagMs / 2, 100, 1_000);

    public async Task Receive(WatchProgressTickRequest message)
    {
        if (shutdown.IsCancellationRequested)
            return;

        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        List<Task>? proposals = null;

        foreach (int partitionId in DataPartitions())
        {
            if (now - watermarks.LastEntryPhysical(partitionId) < intervalMs)
                continue;

            if (!await raft.AmILeaderQuickIfHosted(partitionId).ConfigureAwait(false))
                continue;

            (proposals ??= []).Add(ProposeAsync(partitionId));
        }

        if (proposals is not null)
            await Task.WhenAll(proposals).ConfigureAwait(false);
    }

    private async Task ProposeAsync(int partitionId)
    {
        try
        {
            RaftReplicationResult result = await raft.ReplicateLogs(partitionId, ReplicationTypes.ProgressTick, TickPayload, cancellationToken: shutdown).ConfigureAwait(false);
            if (result.Success)
                KeyValueWatchMetrics.ProgressTicks.Add(1);
        }
        catch (Exception ex) when (ex is OperationCanceledException || Diagnostics.ProcessFaults.Survivable(ex, "WatchProgressTickActor.ProposeAsync"))
        {
            // A lost tick only delays the watermark until the next period.
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug(ex, "Progress tick on partition {PartitionId} was not proposed", partitionId);
        }
    }

    // Every data partition: the hash pool and the partition of each range descriptor.
    private SortedSet<int> DataPartitions()
    {
        SortedSet<int> partitions = [];

        int poolSize = dataPartitionRouter.PoolSize;
        for (int i = 0; i < poolSize; i++)
            partitions.Add(DataPartitionRouter.FirstUserPartitionId + i);

        foreach (RangeDescriptor descriptor in rangeMapStore.Current.Descriptors)
            partitions.Add(descriptor.PartitionId);

        return partitions;
    }
}
