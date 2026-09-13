using System.Diagnostics;
using System.Diagnostics.Metrics;
using Kahuna.Server.Configuration;
using Microsoft.Extensions.Logging;
using Nixie;

namespace Kahuna.Server.Persistence;

/// <summary>
/// Per-node view of the committed writes still waiting for the background flush, and the budget
/// that turns that backlog into write back-pressure.
///
/// <para>
/// Every committed key-value and lock mutation — the leader's and every follower's — is sent to the
/// single <see cref="BackgroundWriterActor"/> as a <c>QueueStore*</c> request and lives on the heap,
/// value included, until the flush that contains it lands. Nothing else bounds that population: a
/// flusher slower than ingest (a maintenance task hogging the writer, a slow volume) lets it grow by
/// the ingest rate until the heap limit ends the process — which loses the replica, and with two
/// replicas the quorum. The backlog is the writer's inbox (requests sent but not yet received, which
/// is where a long-running writer turn accumulates them) plus its dirty queues.
/// </para>
///
/// <para>
/// <see cref="IsOverBudget"/> is the write aggregator's admission probe: ordinary writes are refused
/// retryably while the backlog exceeds <see cref="KahunaConfiguration.PersistenceMaxUnflushedItems"/>
/// or <see cref="KahunaConfiguration.PersistenceMaxUnflushedBytes"/>. Followers cannot refuse a Raft
/// apply, but they run the same flusher over the same stream as the leader, so gating the leader's
/// ingest on its own backlog bounds theirs too. The gauges make the backlog visible before the gate
/// closes.
/// </para>
///
/// <para>
/// The monitor also samples itself every <see cref="SamplePeriod"/> and logs through an
/// <see cref="UnflushedBacklogAlertPolicy"/>: a warning when the backlog crosses 75% of a budget, a
/// warning when back-pressure engages, reminders every ten minutes while either holds, and an
/// information line when it clears. On the 1.7.8 soaks a follower's durable-apply lag saw-toothed to
/// two thirds of the item budget for 45 minutes with nothing in the log; the gauges showed it only to
/// whoever was scraping them.
/// </para>
/// </summary>
internal sealed class PersistenceBacklogMonitor : IDisposable
{
    /// <summary>How often the alert sampler reads the backlog.</summary>
    internal static readonly TimeSpan SamplePeriod = TimeSpan.FromSeconds(5);

    /// <summary>Spacing of reminder lines while a warning or a closed gate persists.</summary>
    internal static readonly TimeSpan ReminderInterval = TimeSpan.FromMinutes(10);

    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> writer;

    private readonly long maxItems;

    private readonly long maxBytes;

    private readonly Meter meter;

    private readonly ILogger? logger;

    private readonly UnflushedBacklogAlertPolicy alertPolicy;

    private readonly Timer? sampler;

    private int sampling;

    public PersistenceBacklogMonitor(IActorRef<BackgroundWriterActor, BackgroundWriteRequest> writer, KahunaConfiguration configuration, ILogger? logger = null)
    {
        this.writer = writer;
        this.logger = logger;
        maxItems = configuration.PersistenceMaxUnflushedItems;
        maxBytes = configuration.PersistenceMaxUnflushedBytes;
        alertPolicy = new UnflushedBacklogAlertPolicy(maxItems, maxBytes, ReminderInterval);

        // Instance meter: the gauge callbacks capture this monitor, so disposing the meter with the
        // node stops them publishing and releases the capture (same ownership as the aggregator gauges).
        meter = new Meter("Kahuna", "1.0");
        meter.CreateObservableGauge("kahuna.persistence.unflushed_items", () => UnflushedItems,
            description: "Committed writes held in memory awaiting the background flush: writer inbox plus dirty queues.");
        meter.CreateObservableGauge("kahuna.persistence.unflushed_bytes", () => UnflushedBytes, unit: "By",
            description: "Value bytes awaiting flush: the dirty queues' exact bytes plus the writer inbox sized at the recent average value size.");
        meter.CreateObservableGauge("kahuna.persistence.writer_inbox_items", () => InboxItems,
            description: "Requests sent to the background writer that it has not received yet.");
        meter.CreateObservableGauge("kahuna.persistence.unflushed_budget_fraction", () => BudgetFraction,
            description: "Unflushed backlog as a fraction of the tighter budget (items over PersistenceMaxUnflushedItems or bytes over PersistenceMaxUnflushedBytes, whichever is larger); alert at 0.75, back-pressure above 1.");
        meter.CreateObservableGauge("kahuna.persistence.backlog_gate_closed", () => IsOverBudget ? 1 : 0,
            description: "1 while the unflushed backlog exceeds a budget and ordinary writes admitted on this node are refused retryably, else 0.");

        // The sampler exists only for the log: without a logger there is nobody to tell. Both bounds
        // disabled means no budget to measure against, so no alerts either.
        if (logger is not null && (maxItems > 0 || maxBytes > 0))
            sampler = new Timer(static state => ((PersistenceBacklogMonitor)state!).Sample(), this, SamplePeriod, SamplePeriod);
    }

    private BackgroundWriterActor? Actor => writer.Runner.Actor as BackgroundWriterActor;

    /// <summary>Requests queued in the writer's mailbox. Grows while a writer turn (a flush, a prune)
    /// runs long; the periodic flush timer message is included and negligible.</summary>
    public long InboxItems => writer.Runner.MessageCount;

    /// <summary>Unflushed committed writes on this node: inbox plus dirty queues.</summary>
    public long UnflushedItems => InboxItems + (Actor?.QueuedItems ?? 0);

    /// <summary>
    /// Value bytes awaiting flush: the dirty queues' exact total plus an estimate for the inbox.
    /// Inbox requests are not sized until the writer receives them, and under load the inbox is where
    /// the backlog lives (a long writer turn leaves the dirty queues empty), so counting only the
    /// queues left the byte bound blind to the very backlog it exists for. The inbox is sized at the
    /// writer's recent average value size; the estimate is exact for a steady value-size mix and
    /// lags briefly when the mix shifts, which is adequate for a coarse admission budget.
    /// </summary>
    public long UnflushedBytes
    {
        get
        {
            BackgroundWriterActor? actor = Actor;
            if (actor is null)
                return 0;

            return actor.QueuedBytes + InboxItems * actor.AverageValueBytes;
        }
    }

    /// <summary>The backlog against the tighter of the two budgets, 0..1 under budget and above 1 while gated.</summary>
    public double BudgetFraction => UnflushedBacklogAlertPolicy.Fraction(UnflushedItems, UnflushedBytes, maxItems, maxBytes);

    /// <summary>True when either configured bound is exceeded; the aggregator then refuses ordinary writes.</summary>
    public bool IsOverBudget =>
        (maxItems > 0 && UnflushedItems > maxItems)
        || (maxBytes > 0 && UnflushedBytes > maxBytes);

    /// <summary>The alert policy's current level (test seam).</summary>
    internal UnflushedBacklogLevel AlertLevel => alertPolicy.Level;

    /// <summary>One sampler tick: read the backlog once, run it through the policy, log what it earns.
    /// Re-entrancy guarded, since a timer tick can overlap a slow predecessor.</summary>
    internal void Sample()
    {
        if (Interlocked.CompareExchange(ref sampling, 1, 0) != 0)
            return;

        try
        {
            long items = UnflushedItems;
            long bytes = UnflushedBytes;
            bool gateClosed = (maxItems > 0 && items > maxItems) || (maxBytes > 0 && bytes > maxBytes);
            long now = Stopwatch.GetTimestamp();

            UnflushedBacklogObservation observation = alertPolicy.Observe(items, bytes, gateClosed, now);
            if (observation.Alert == UnflushedBacklogAlert.None || logger is null)
                return;

            Log(observation, items, bytes, alertPolicy.LevelDuration(now));
        }
        catch (Exception ex)
        {
            // The actor may be stopping under us; a sampler tick must never take the node with it.
            logger?.LogDebug(ex, "Unflushed-backlog sampler tick failed");
        }
        finally
        {
            Volatile.Write(ref sampling, 0);
        }
    }

    private void Log(UnflushedBacklogObservation observation, long items, long bytes, TimeSpan levelDuration)
    {
        switch (observation.Alert)
        {
            case UnflushedBacklogAlert.WarningRaised:
                logger!.LogWarning(
                    "Unflushed write backlog at {Fraction:P0} of its budget: {Items} items / {Bytes} bytes awaiting flush (budget {MaxItems} items / {MaxBytes} bytes). Above the budget ordinary writes admitted on this node are refused retryably. The flusher is not keeping up with ingest: look at the flush and revision-prune instruments before raising PersistenceMaxUnflushedItems / Bytes. Next reminder in {Interval}",
                    observation.Fraction, items, bytes, maxItems, maxBytes, ReminderInterval);
                break;

            case UnflushedBacklogAlert.WarningReminder:
                logger!.LogWarning(
                    "Unflushed write backlog still at {Fraction:P0} of its budget after {Duration}: {Items} items / {Bytes} bytes awaiting flush (budget {MaxItems} items / {MaxBytes} bytes). Next reminder in {Interval}",
                    observation.Fraction, levelDuration, items, bytes, maxItems, maxBytes, ReminderInterval);
                break;

            case UnflushedBacklogAlert.GateClosed:
                logger!.LogWarning(
                    "Persistence back-pressure engaged: unflushed write backlog {Items} items / {Bytes} bytes exceeds its budget ({MaxItems} items / {MaxBytes} bytes; {Fraction:P0}). Ordinary writes admitted on this node are refused retryably (kahuna.kv.write.rejections{{reason=unflushed_backlog}}) until the flusher drains it; terminal work still passes. Next reminder in {Interval}",
                    items, bytes, maxItems, maxBytes, observation.Fraction, ReminderInterval);
                break;

            case UnflushedBacklogAlert.GateReminder:
                logger!.LogWarning(
                    "Persistence back-pressure still engaged after {Duration}: unflushed write backlog {Items} items / {Bytes} bytes over its budget ({MaxItems} items / {MaxBytes} bytes; {Fraction:P0}). Next reminder in {Interval}",
                    levelDuration, items, bytes, maxItems, maxBytes, observation.Fraction, ReminderInterval);
                break;

            case UnflushedBacklogAlert.GateOpened:
                logger!.LogWarning(
                    "Persistence back-pressure released after {Duration}: unflushed write backlog back under its budget at {Fraction:P0} ({Items} items / {Bytes} bytes); ordinary writes are admitted again. Still above the {Clear:P0} clear line",
                    levelDuration, observation.Fraction, items, bytes, UnflushedBacklogAlertPolicy.ClearFraction);
                break;

            case UnflushedBacklogAlert.Cleared:
                if (logger!.IsEnabled(LogLevel.Information))
                    logger.LogInformation(
                        "Unflushed write backlog cleared after {Duration}: {Fraction:P0} of its budget ({Items} items / {Bytes} bytes){Gate}",
                        levelDuration, observation.Fraction, items, bytes,
                        observation.From == UnflushedBacklogLevel.Gated ? "; persistence back-pressure released, ordinary writes are admitted again" : "");
                break;
        }
    }

    public void Dispose()
    {
        sampler?.Dispose();
        meter.Dispose();
    }
}
