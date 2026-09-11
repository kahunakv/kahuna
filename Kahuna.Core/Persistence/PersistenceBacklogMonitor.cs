using System.Diagnostics.Metrics;
using Kahuna.Server.Configuration;
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
/// </summary>
internal sealed class PersistenceBacklogMonitor : IDisposable
{
    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> writer;

    private readonly long maxItems;

    private readonly long maxBytes;

    private readonly Meter meter;

    public PersistenceBacklogMonitor(IActorRef<BackgroundWriterActor, BackgroundWriteRequest> writer, KahunaConfiguration configuration)
    {
        this.writer = writer;
        maxItems = configuration.PersistenceMaxUnflushedItems;
        maxBytes = configuration.PersistenceMaxUnflushedBytes;

        // Instance meter: the gauge callbacks capture this monitor, so disposing the meter with the
        // node stops them publishing and releases the capture (same ownership as the aggregator gauges).
        meter = new Meter("Kahuna", "1.0");
        meter.CreateObservableGauge("kahuna.persistence.unflushed_items", () => UnflushedItems,
            description: "Committed writes held in memory awaiting the background flush: writer inbox plus dirty queues.");
        meter.CreateObservableGauge("kahuna.persistence.unflushed_bytes", () => UnflushedBytes, unit: "By",
            description: "Value bytes held by the background writer's dirty queues awaiting flush.");
        meter.CreateObservableGauge("kahuna.persistence.writer_inbox_items", () => InboxItems,
            description: "Requests sent to the background writer that it has not received yet.");
    }

    private BackgroundWriterActor? Actor => writer.Runner.Actor as BackgroundWriterActor;

    /// <summary>Requests queued in the writer's mailbox. Grows while a writer turn (a flush, a prune)
    /// runs long; the periodic flush timer message is included and negligible.</summary>
    public long InboxItems => writer.Runner.MessageCount;

    /// <summary>Unflushed committed writes on this node: inbox plus dirty queues.</summary>
    public long UnflushedItems => InboxItems + (Actor?.QueuedItems ?? 0);

    /// <summary>Value bytes awaiting flush in the dirty queues. Inbox requests are not sized until the
    /// writer receives them, so this trails <see cref="UnflushedItems"/> during a long writer turn.</summary>
    public long UnflushedBytes => Actor?.QueuedBytes ?? 0;

    /// <summary>True when either configured bound is exceeded; the aggregator then refuses ordinary writes.</summary>
    public bool IsOverBudget =>
        (maxItems > 0 && UnflushedItems > maxItems)
        || (maxBytes > 0 && UnflushedBytes > maxBytes);

    public void Dispose() => meter.Dispose();
}
