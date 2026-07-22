using System.Diagnostics.Metrics;
using Nixie;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// Leader-local aggregator for direct (auto-commit, non-transactional) key/value writes. Owns a small fixed
/// set of lane actors and a per-partition admission registry, and exposes a synchronous admission method the
/// owning key actor calls after it has installed the write's replication intent. Writes to the same partition
/// — from any client, single or many-key call — meet in the same lane's partition queue and share as few Raft
/// proposals as the batch/queue bounds allow.
/// </summary>
internal sealed class PartitionWriteAggregator : IDisposable
{
    private readonly IActorRef<PartitionWriteAggregatorActor, PartitionWriteMessage, PartitionWriteAck>[] lanes;

    private readonly PartitionAdmissionRegistry admission;

    private readonly int laneCount;

    private readonly TimeProvider timeProvider;

    private readonly long stampsPerMs;

    private readonly long maxOperationBytes;

    /// <summary>Cancels in-flight batch round trips on shutdown so a hung executor cannot hold the drain open past
    /// its timeout. Handed to every lane; cancelled by <see cref="SignalStop"/>.</summary>
    private readonly CancellationTokenSource shutdownCts = new();

    /// <summary>Instance-owned meter carrying this aggregator's observable gauges; disposed on <see cref="Stop"/>
    /// so a disposed node's admission registry is not kept reachable by gauge callbacks.</summary>
    private readonly Meter gaugeMeter;

    private volatile bool stopping;

    public PartitionWriteAggregator(
        ActorSystem actorSystem,
        IPartitionBatchExecutor executor,
        PartitionWriteAggregatorOptions options,
        IWriteRangeFence fence,
        ILogger<IKahuna> logger,
        TimeProvider? timeProvider = null
    )
    {
        laneCount = Math.Max(1, options.LaneCount);
        this.timeProvider = timeProvider ?? TimeProvider.System;
        stampsPerMs = Math.Max(1, this.timeProvider.TimestampFrequency / 1000);
        maxOperationBytes = options.MaxOperationBytes;
        admission = new PartitionAdmissionRegistry(
            options.MaxQueuedItemsPerPartition,
            options.MaxQueuedBytesPerPartition,
            options.TerminalReserveItemsPerPartition,
            options.TerminalReserveBytesPerPartition,
            options.MaxQueuedItemsGlobal,
            options.MaxQueuedBytesGlobal,
            options.TerminalReserveItemsGlobal,
            options.TerminalReserveBytesGlobal,
            options.MaxOperationBytes);

        // Submit is an ordinary bounded admission; TimerWake/BatchComplete/Stop are control and bypass the bound
        // so a saturated lane can still flush a linger deadline, release a completed batch's capacity, and be
        // told to drain — a stop that a full inbox could reject would strand queued items on shutdown.
        ActorRunnerOptions<PartitionWriteMessage> laneOptions = new()
        {
            MaxInboxSize = options.AggregatorInboxSize > 0 ? options.AggregatorInboxSize : null,
            IsControlMessage = static m => m.Kind is PartitionWriteMessageKind.TimerWake
                or PartitionWriteMessageKind.BatchComplete
                or PartitionWriteMessageKind.Stop
        };

        lanes = new IActorRef<PartitionWriteAggregatorActor, PartitionWriteMessage, PartitionWriteAck>[laneCount];
        for (int i = 0; i < laneCount; i++)
            lanes[i] = actorSystem.SpawnWithOptions<PartitionWriteAggregatorActor, PartitionWriteMessage, PartitionWriteAck>(
                "kv-write-lane-" + i,
                laneOptions,
                executor,
                admission,
                options,
                fence,
                logger,
                this.timeProvider,
                shutdownCts.Token
            );

        gaugeMeter = PartitionWriteAggregatorMetrics.RegisterGauges(admission);
    }

    /// <summary>
    /// Reserves partition capacity and hands the write to its lane. Returns false (retryable backpressure) if
    /// the partition's queued item/byte bound is full — the caller unwinds its just-installed intent and
    /// returns MustRetry without a completion message. A true result means the item is owned by the lane and
    /// will terminate exactly once via <c>CompleteProposal</c> or <c>ReleaseProposal</c>.
    /// </summary>
    public bool TryEnqueue(IProposalSubmission item)
    {
        // Reject once shutdown has begun so the caller unwinds its intent and retries elsewhere, rather than
        // handing a write to a draining lane.
        if (stopping)
        {
            PartitionWriteAggregatorMetrics.RejectedStopping();
            return false;
        }

        // Hard per-operation ceiling: a submission larger than the limit is rejected (retryable) rather than
        // dispatched alone. The registry also guards this, but reject here to record the precise reason.
        if (maxOperationBytes > 0 && item.ByteLength > maxOperationBytes)
        {
            PartitionWriteAggregatorMetrics.RejectedOversized();
            return false;
        }

        if (!admission.TryReserve(item.PartitionId, item.ByteLength, item.AdmissionClass))
        {
            PartitionWriteAggregatorMetrics.RejectedQueueFull();
            return false;
        }

        // Stamp the admission time from the aggregator's clock so the owning lane's queue-age math and wake
        // timers measure against the same TimeProvider (real in production, controllable under test).
        item.EnqueueTicks = timeProvider.GetTimestamp() / stampsPerMs;

        // TrySend reports admission synchronously: a full lane inbox (or an already-shut-down lane) returns
        // false WITHOUT enqueuing. Release the just-made reservation and reject so the caller unwinds its
        // installed intent and returns MustRetry — otherwise the reservation, the proposal, and the caller's
        // promise would leak with no completion ever arriving.
        if (!lanes[LaneIndex(item.PartitionId)].TrySend(PartitionWriteMessage.Submit(item)))
        {
            admission.Release(item.PartitionId, item.ByteLength);
            PartitionWriteAggregatorMetrics.RejectedInboxFull();
            return false;
        }

        PartitionWriteAggregatorMetrics.AdmittedItems.Add(1);
        return true;
    }

    /// <summary>
    /// Begins shutdown: new admissions are rejected and each lane releases its still-pending items retryably.
    /// In-flight batches are left to report their real Raft outcome (each already bounded by the per-batch
    /// execution deadline, so a hung one cannot linger indefinitely). Prefer <see cref="StopAsync"/> for an
    /// observable drain; this is the synchronous disposal fallback.
    /// </summary>
    public void Stop()
    {
        SignalStop();
        DisposeMeter();
    }

    /// <summary>Disposes via the same synchronous <see cref="Stop"/> path (idempotent), so the facade satisfies
    /// deterministic disposal of its shutdown token source and gauge meter.</summary>
    public void Dispose() => Stop();

    /// <summary>
    /// Asynchronous, observable drain: rejects new admissions, delivers the priority stop that releases every
    /// queued (not-yet-dispatched) item retryably, then awaits until all in-flight batches have settled — so
    /// every admitted write terminates (completed or released) and every reservation is freed before the caller
    /// tears down the actor system and Raft. Bounded by <paramref name="timeout"/>; on timeout it proceeds
    /// anyway (a stuck executor must not hang shutdown forever). Must be awaited <b>before</b> the lanes' actor
    /// system or the underlying Raft/executor is disposed, or in-flight batches cannot report their outcome.
    /// </summary>
    public async Task StopAsync(TimeSpan timeout)
    {
        SignalStop();

        // Let in-flight batches settle naturally (so they report their real Raft outcome), bounded by the drain
        // timeout. Only if the deadline elapses with work still in flight do we cancel it — a stuck executor must
        // not hang shutdown forever, but a batch that would settle promptly should not be aborted early.
        long deadlineTicks = Environment.TickCount64 + (long)timeout.TotalMilliseconds;
        while ((admission.TotalReservedItems() != 0 || admission.InFlightPartitions != 0)
               && Environment.TickCount64 < deadlineTicks)
            await Task.Delay(10).ConfigureAwait(false);

        CancelShutdown();
        DisposeMeter();
    }

    /// <summary>Rejects new admissions and delivers the priority stop to every lane. Idempotent — a second
    /// call (e.g. sync Dispose after an async drain) just re-signals stopped lanes harmlessly.</summary>
    private void SignalStop()
    {
        stopping = true;
        // Stop is a control message, so a saturated inbox cannot reject it; TrySend also avoids leaving an
        // unobserved task behind when a lane is already shut down.
        foreach (IActorRef<PartitionWriteAggregatorActor, PartitionWriteMessage, PartitionWriteAck> lane in lanes)
            lane.TrySend(PartitionWriteMessage.StopSignal);
    }

    /// <summary>Cancels in-flight batch round trips so a hung executor cannot outlast teardown; a cancelled batch
    /// settles retryably and frees its capacity. Tolerates a second call after the token source was disposed.</summary>
    private void CancelShutdown()
    {
        try { shutdownCts.Cancel(); } catch (ObjectDisposedException) { /* already disposed */ }
    }

    /// <summary>Disposes the instance meter so its observable gauges stop publishing and release their capture
    /// of the admission registry, and the shutdown token source. Idempotent (both tolerate repeated calls).</summary>
    private void DisposeMeter()
    {
        gaugeMeter.Dispose();
        shutdownCts.Dispose();
    }

    /// <summary>Currently reserved items for a partition — capacity accounting that must return to zero once
    /// every admitted write has completed or been released. Test/observability only.</summary>
    internal int ReservedItems(int partitionId) => admission.ReservedItems(partitionId);

    /// <summary>Partitions currently tracked in the admission registry; returns to zero once all reservations
    /// are released (test/observability — proves per-partition counters are pruned, not accumulated).</summary>
    internal int TrackedPartitionCount => admission.TrackedPartitionCount;

    /// <summary>The instance-owned gauge meter (test-only) so a listener can assert it stops publishing after
    /// <see cref="Stop"/> disposes it.</summary>
    internal Meter GaugeMeter => gaugeMeter;

    private int LaneIndex(int partitionId) => (partitionId & int.MaxValue) % laneCount;
}
