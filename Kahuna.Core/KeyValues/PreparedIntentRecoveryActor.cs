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
/// </summary>
internal sealed class PreparedIntentRecoveryActor : IActor<PreparedIntentRecoveryRequest>
{
    private readonly KeyValuesManager manager;
    private readonly ILogger<IKahuna> logger;

    private readonly TimeSpan receiptBackstopInterval;

    private long lastReceiptBackstopTicks;

    public PreparedIntentRecoveryActor(
        IActorContext<PreparedIntentRecoveryActor, PreparedIntentRecoveryRequest> context,
        KeyValuesManager manager,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    )
    {
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
    }
}
