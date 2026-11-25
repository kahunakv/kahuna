using System.Diagnostics;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Transactions;
using Nixie;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// One per node: periodically drives participant-side recovery for the durable-intent 2PC path, resolving due
/// unresolved prepared intents on the partitions this node leads to their canonical decision, and the retention
/// sweep of the durable-2PC metadata. A single serial mailbox keeps periodic ticks from overlapping; the
/// underlying stores are idempotent, so a race with a request-path finalize is safe. A no-op unless the
/// durable-intent path is enabled.
///
/// <para>The tick is <see cref="KahunaConfiguration.DurableMaintenanceInterval"/> (seconds), not the minute-scale
/// collection interval: the retention sweep enforces a <b>memory</b> budget, and a budget checked once a minute
/// lets a minute of commits — hundreds of thousands of records at speed — pile on top of it before it acts; and
/// an orphaned prepared intent swept within seconds of its deadline is what lets the retention floor be short.
/// The completion-receipt age backstop keeps the collection-interval cadence: it scans every receipt, and what
/// it reclaims (replay orphans) accrues slowly — except under heap pressure, when it runs at once.</para>
///
/// <para>A leadership that arrives at this node sends a message of its own, which settles the decided intents the
/// partition's earlier leader left, ahead of the next tick.</para>
/// </summary>
internal sealed class PreparedIntentRecoveryActor : IActor<PreparedIntentRecoveryRequest>
{
    private readonly KeyValuesManager manager;
    private readonly ILogger<IKahuna> logger;

    private readonly TimeSpan receiptBackstopInterval;

    private long lastReceiptBackstopTicks;

    private static readonly TimeSpan InheritedRetryMinDelay = TimeSpan.FromMilliseconds(100);

    private static readonly TimeSpan InheritedRetryMaxDelay = TimeSpan.FromSeconds(2);

    private readonly IActorContext<PreparedIntentRecoveryActor, PreparedIntentRecoveryRequest> context;

    private TimeSpan inheritedRetryDelay = InheritedRetryMinDelay;

    private bool inheritedRetryScheduled;

    public PreparedIntentRecoveryActor(
        IActorContext<PreparedIntentRecoveryActor, PreparedIntentRecoveryRequest> context,
        KeyValuesManager manager,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    )
    {
        this.context = context;
        this.manager = manager;
        this.logger = logger;

        receiptBackstopInterval = configuration.CollectionInterval;
        lastReceiptBackstopTicks = Stopwatch.GetTimestamp();

        TimeSpan tick = DurableMaintenanceService.MaintenanceTick(configuration);

        context.ActorSystem.StartPeriodicTimer(
            context.Self,
            "recover-prepared-intents",
            new(),
            tick,
            tick
        );
    }

    public async Task Receive(PreparedIntentRecoveryRequest message)
    {
        if (message.InheritedOnly)
        {
            await SettleInherited(message.IsRetry);
            return;
        }

        try
        {
            await manager.RecoverPreparedIntents(CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to recover prepared intents");
        }

        // Same serial tick reclaims durable-2PC metadata whose retention window has elapsed (terminal records +
        // their participants' completion receipts) or that exceeds the resident-metadata budget, so both stores
        // return to a steady-state floor instead of growing for the node's lifetime. Isolated from recovery so a
        // GC fault never stalls recovery.
        try
        {
            await manager.CollectDurableTransactionRecords(CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to collect durable transaction records");
        }

        // Then the receipt age backstop, which runs second so the acknowledgement-driven release above always gets
        // first refusal on a receipt whose record still exists. What it collects is the remainder: receipts a log
        // replay re-created for transactions whose record was already reclaimed, which nothing else would remove.
        // Once per collection interval, or immediately (at the floor) when the sweep just saw heap pressure.
        bool heapPressure = manager.DurableHeapPressureObserved;
        long now = Stopwatch.GetTimestamp();

        if (!heapPressure && Stopwatch.GetElapsedTime(lastReceiptBackstopTicks, now) < receiptBackstopInterval)
            return;

        lastReceiptBackstopTicks = now;

        try
        {
            manager.CollectExpiredCompletionReceipts(heapPressure);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to collect expired completion receipts");
        }

        // At the same cadence, the write marks of the partitions without a write since the time before.
        try
        {
            manager.ReleaseIdleUndeliveredKeys();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to release the write marks of idle partitions");
        }
    }

    /// <summary>One pass over the inherited intents, and the next one after a growing wait while some partition may
    /// still hold one. At most one retry is scheduled at a time; a leadership that arrives meanwhile brings its own
    /// message and starts the waits over.</summary>
    private async Task SettleInherited(bool isRetry)
    {
        if (isRetry)
            inheritedRetryScheduled = false;
        else
            inheritedRetryDelay = InheritedRetryMinDelay;

        bool again = false;

        try
        {
            again = await manager.SettleInheritedIntents(CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to settle inherited prepared intents");
        }

        if (!again || inheritedRetryScheduled || manager.Stopping)
            return;

        inheritedRetryScheduled = true;
        context.ActorSystem.ScheduleOnce(context.Self, new PreparedIntentRecoveryRequest { InheritedOnly = true, IsRetry = true }, inheritedRetryDelay);

        inheritedRetryDelay = inheritedRetryDelay * 2 < InheritedRetryMaxDelay ? inheritedRetryDelay * 2 : InheritedRetryMaxDelay;
    }
}
