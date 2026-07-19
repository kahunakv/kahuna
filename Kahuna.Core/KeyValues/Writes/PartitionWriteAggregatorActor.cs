using Nixie;
using Kommander;
using Kommander.Data;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>Reply value for a lane actor. Lanes are addressed fire-and-forget; the ack exists only because
/// Nixie honors the priority-control predicate on reply-capable runners, not fire-and-forget ones.</summary>
internal sealed class PartitionWriteAck
{
    public static readonly PartitionWriteAck Instance = new();
}

/// <summary>
/// One aggregator lane: a single-threaded Nixie actor owning the <see cref="PartitionWriteState"/> for each
/// partition it serves. It never awaits Raft on the mailbox — dispatch starts a detached batch that calls the
/// executor and sends a <c>BatchComplete</c> control message back — so a slow partition cannot park another.
/// Before dispatching, each selected item is re-fenced against the current range map and released
/// individually if its range moved; queue-age-expired items are released before dispatch and swept on the
/// linger timer even while a batch is in flight. Every release path also frees the admission reservation.
/// </summary>
internal sealed class PartitionWriteAggregatorActor : IActor<PartitionWriteMessage, PartitionWriteAck>
{
    private const long FailureLogIntervalMs = 1000;

    /// <summary>Maximum items released in one <see cref="Dispatch"/> invocation before yielding the lane back
    /// to its mailbox, so an all-stale/all-expired partition cannot starve others sharing the lane. The drain
    /// resumes on the re-posted wake; every remaining item is still released by its queue-age deadline.</summary>
    private const int MaxReleasesPerDispatch = 1024;

    private readonly ActorRef<PartitionWriteAggregatorActor, PartitionWriteMessage, PartitionWriteAck> self;

    private readonly IPartitionBatchExecutor executor;

    private readonly IWriteCompletionRouter completionRouter;

    private readonly PartitionAdmissionRegistry admission;

    private readonly PartitionWriteAggregatorOptions options;

    private readonly IWriteRangeFence fence;

    private readonly ILogger<IKahuna> logger;

    private readonly TimeProvider timeProvider;

    private readonly long stampsPerMs;

    private readonly Dictionary<int, PartitionWriteState> states = [];

    private bool stopping;

    private long lastFailureLogTicks;

    private int suppressedFailures;

    public PartitionWriteAggregatorActor(
        IActorContext<PartitionWriteAggregatorActor, PartitionWriteMessage, PartitionWriteAck> context,
        IPartitionBatchExecutor executor,
        IWriteCompletionRouter completionRouter,
        PartitionAdmissionRegistry admission,
        PartitionWriteAggregatorOptions options,
        IWriteRangeFence fence,
        ILogger<IKahuna> logger,
        TimeProvider timeProvider
    )
    {
        this.self = context.Self;
        this.executor = executor;
        this.completionRouter = completionRouter;
        this.admission = admission;
        this.options = options;
        this.fence = fence;
        this.logger = logger;
        this.timeProvider = timeProvider;
        stampsPerMs = Math.Max(1, timeProvider.TimestampFrequency / 1000);
    }

    /// <summary>Monotonic millisecond clock shared with the admission stamp, so queue-age deadlines are exact
    /// and controllable under a test <see cref="TimeProvider"/>.</summary>
    private long NowMs() => timeProvider.GetTimestamp() / stampsPerMs;

    public Task<PartitionWriteAck?> Receive(PartitionWriteMessage message)
    {
        switch (message.Kind)
        {
            case PartitionWriteMessageKind.Submit:
                OnSubmit(message.Item!);
                break;

            case PartitionWriteMessageKind.TimerWake:
                OnTimerWake(message.PartitionId);
                break;

            case PartitionWriteMessageKind.BatchComplete:
                OnBatchComplete(message);
                break;

            case PartitionWriteMessageKind.Stop:
                OnStop();
                break;
        }

        return Task.FromResult<PartitionWriteAck?>(PartitionWriteAck.Instance);
    }

    private void OnSubmit(KeyValueProposalRequest item)
    {
        // A submission that raced past shutdown is released retryably rather than stranded.
        if (stopping)
        {
            PartitionWriteAggregatorMetrics.RejectedStopping();
            ReleaseItem(item.PartitionId, item, transient: true);
            return;
        }

        PartitionWriteState state = GetState(item.PartitionId);
        PartitionWriteState.EnqueueResult result = state.Enqueue(item, options.MaxBatchItems, options.MaxBatchBytes);

        // Never dispatch while a batch is already in flight for this partition (one-in-flight invariant); the
        // buffer behind it is re-driven on BatchComplete. ShouldFlushNow already encodes !InFlight; the
        // linger-disabled fast path must guard it too. Otherwise schedule a wake so the buffer flushes at its
        // linger deadline and its items are released no later than their queue-age deadline — even while a
        // batch is in flight (the in-flight case schedules an age-only wake).
        if (result.ShouldFlushNow || (result.OpenedBuffer && options.LingerMs <= 0 && !state.InFlight))
            Dispatch(item.PartitionId, state);
        else
            ScheduleWake(item.PartitionId, state);
    }

    private void OnTimerWake(int partitionId)
    {
        if (!states.TryGetValue(partitionId, out PartitionWriteState? state))
            return;

        state.ClearArmedWake();

        // Release over-age items first (idempotent; front-of-queue is oldest), then flush if the oldest item's
        // linger window has elapsed and nothing is in flight. Finally re-arm for whatever remains — the age
        // deadline of a buffer retained behind an in-flight batch, or a still-future linger deadline.
        SweepExpired(partitionId, state);

        if (state.LingerElapsed(NowMs(), options.LingerMs))
            Dispatch(partitionId, state);

        ScheduleWake(partitionId, state);
        PruneIfIdle(partitionId, state);
    }

    private void OnBatchComplete(PartitionWriteMessage message)
    {
        PartitionWriteState state = GetState(message.PartitionId);
        state.OnBatchComplete();
        admission.DecInFlight();

        // Rate-limited failure logging runs here, on the single-threaded lane mailbox — never on the detached,
        // concurrent RunBatch path — so its per-lane counters are not raced across partitions' completions.
        if (!message.Success)
            LogFailureRateLimited(message.PartitionId, message.Batch!.Count, message.Status);

        foreach (KeyValueProposalRequest item in message.Batch!)
        {
            if (message.Success)
                completionRouter.Complete(item);
            else
                completionRouter.Release(item, message.Transient);

            admission.Release(message.PartitionId, item.ByteLength);
        }

        // Re-drive the buffer that accumulated behind the just-completed batch, then re-arm a wake so any
        // items now in flight behind this re-dispatch (or still waiting) are released at their queue-age
        // deadline. Completion is the point that re-dispatches, since a wake while in flight only swept.
        if (!stopping && state.PendingCount > 0)
            Dispatch(message.PartitionId, state);

        ScheduleWake(message.PartitionId, state);
        PruneIfIdle(message.PartitionId, state);
    }

    private void OnStop()
    {
        stopping = true;

        // Release everything still pending (not yet selected into an in-flight batch) retryably. In-flight
        // batches are left to report their real Raft outcome via BatchComplete.
        foreach ((int partitionId, PartitionWriteState state) in states)
        {
            foreach (KeyValueProposalRequest item in state.DrainPending())
            {
                PartitionWriteAggregatorMetrics.RejectedStopping();
                ReleaseItem(partitionId, item, transient: true);
            }
        }
    }

    private void Dispatch(int partitionId, PartitionWriteState state)
    {
        // Loop over the all-released case iteratively rather than recursing: with a small MaxBatchItems and a
        // full queue of expired/stale items, one recursive frame per selected item would overflow the stack.
        // Each pass selects a batch; if every item is released, clear the in-flight marker and select the next.
        // The first pass with a proposable item dispatches it (one in-flight batch) and returns. Cleanup is
        // bounded per invocation so one all-stale partition cannot monopolize a lane shared with others.
        int released = 0;

        while (true)
        {
            List<KeyValueProposalRequest> selected = state.SelectBatch(options.MaxBatchItems, options.MaxBatchBytes);
            if (selected.Count == 0)
                return;

            long now = NowMs();
            List<KeyValueProposalRequest> valid = new(selected.Count);

            foreach (KeyValueProposalRequest item in selected)
            {
                // Release queue-age-expired items and key-range items whose descriptor moved since admission,
                // individually and retryably, rather than proposing them or letting them fail their siblings.
                if (now - item.EnqueueTicks > options.MaxQueueDelayMs)
                {
                    PartitionWriteAggregatorMetrics.ReleasedQueueExpired();
                    ReleaseItem(partitionId, item, transient: true);
                }
                else if (fence.IsStale(item.Key, item.FenceGeneration, item.PartitionId))
                {
                    PartitionWriteAggregatorMetrics.ReleasedFenceStale();
                    ReleaseItem(partitionId, item, transient: true);
                }
                else
                {
                    valid.Add(item);
                }
            }

            // Every selected item was released — nothing to propose. Clear the in-flight marker that
            // SelectBatch set and, if the partition still has queued work and we are not stopping, select the
            // next batch — but yield the lane after a bounded amount of cleanup so this partition cannot starve
            // others; the re-posted wake resumes the drain on a fresh mailbox turn.
            if (valid.Count == 0)
            {
                state.OnBatchComplete();
                released += selected.Count;

                if (stopping || state.PendingCount == 0)
                    return;

                if (released >= MaxReleasesPerDispatch)
                {
                    // Schedule (not raw-send) the resume so it is deduplicated against the caller's own
                    // re-arm — otherwise OnTimerWake's post plus this one would compound turn over turn.
                    ScheduleWake(partitionId, state);
                    return;
                }

                continue;
            }

            long batchBytes = 0;
            byte[][] payloads = new byte[valid.Count][];
            for (int i = 0; i < valid.Count; i++)
            {
                payloads[i] = valid[i].SerializedMessage;
                batchBytes += valid[i].ByteLength;
            }

            PartitionWriteAggregatorMetrics.BatchDispatched(valid.Count, batchBytes, now - valid[0].EnqueueTicks);
            admission.IncInFlight();

            _ = RunBatch(partitionId, valid, payloads);
            return;
        }
    }

    /// <summary>Detached Raft round trip for one batch. Owns only the immutable batch + payloads; never touches
    /// lane state. Converts every failure into a normal completion and sends it back as priority control.</summary>
    private async Task RunBatch(int partitionId, IReadOnlyList<KeyValueProposalRequest> batch, byte[][] payloads)
    {
        bool success;
        bool threw = false;
        RaftOperationStatus status;
        long start = Environment.TickCount64;

        try
        {
            RaftReplicationResult result = await executor.ReplicateAsync(partitionId, payloads);
            success = result.Success;
            status = result.Status;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Batched write propose threw Partition={Partition} Count={Count}", partitionId, payloads.Length);
            success = false;
            status = RaftOperationStatus.Errored;
            threw = true;
        }

        (bool committed, bool transient) = ClassifyOutcome(success, status, threw);

        PartitionWriteAggregatorMetrics.BatchSettled(committed, transient, Environment.TickCount64 - start);

        self.Send(PartitionWriteMessage.BatchComplete(partitionId, batch, committed, transient, status));
    }

    private void SweepExpired(int partitionId, PartitionWriteState state)
    {
        long now = NowMs();
        foreach (KeyValueProposalRequest item in state.PopExpired(now, options.MaxQueueDelayMs))
        {
            PartitionWriteAggregatorMetrics.ReleasedQueueExpired();
            ReleaseItem(partitionId, item, transient: true);
        }
    }

    private void ReleaseItem(int partitionId, KeyValueProposalRequest item, bool transient)
    {
        completionRouter.Release(item, transient);
        admission.Release(partitionId, item.ByteLength);
    }

    /// <summary>Arms a single timer for this partition's next wake — the earlier of the oldest item's linger
    /// and queue-age deadlines — but only when it is earlier than any wake already armed, so a steady arrival
    /// stream does not start a timer per item. The wake re-arms itself from <see cref="OnTimerWake"/>.</summary>
    private void ScheduleWake(int partitionId, PartitionWriteState state)
    {
        long deadline = state.NextWakeDeadline(options.LingerMs, options.MaxQueueDelayMs);
        if (deadline == PartitionWriteState.NoWake)
            return;

        if (state.TryArmWake(deadline))
            _ = WakeDelay(partitionId, deadline);
    }

    private async Task WakeDelay(int partitionId, long deadline)
    {
        long delayMs = deadline - NowMs();
        try
        {
            if (delayMs > 0)
                await Task.Delay(TimeSpan.FromMilliseconds(delayMs), timeProvider);
        }
        catch
        {
            // ignore
        }

        self.Send(PartitionWriteMessage.TimerWake(partitionId));
    }

    private PartitionWriteState GetState(int partitionId)
    {
        if (!states.TryGetValue(partitionId, out PartitionWriteState? state))
            states[partitionId] = state = new PartitionWriteState();

        return state;
    }

    /// <summary>Drops a fully idle partition's state (nothing pending, no in-flight batch, so no armed wake) so
    /// historical and split-churned partitions do not accumulate for the lane's lifetime; a later write to the
    /// partition re-creates it. Safe against a stale pending wake — its <see cref="OnTimerWake"/> just no-ops on
    /// the missing state.</summary>
    private void PruneIfIdle(int partitionId, PartitionWriteState state)
    {
        if (state.PendingCount == 0 && !state.InFlight)
            states.Remove(partitionId);
    }

    /// <summary>Logs a failed batch at most once per <see cref="FailureLogIntervalMs"/> per lane, folding the
    /// suppressed count into the next line, so a cluster-wide outage does not emit a warning per batch.</summary>
    private void LogFailureRateLimited(int partitionId, int count, RaftOperationStatus status)
    {
        long now = Environment.TickCount64;
        if (now - lastFailureLogTicks < FailureLogIntervalMs)
        {
            suppressedFailures++;
            return;
        }

        int suppressed = suppressedFailures;
        suppressedFailures = 0;
        lastFailureLogTicks = now;

        if (suppressed > 0)
            logger.LogWarning("Batched write propose failed Partition={Partition} Count={Count} Status={Status} (+{Suppressed} similar suppressed)", partitionId, count, status, suppressed);
        else
            logger.LogWarning("Batched write propose failed Partition={Partition} Count={Count} Status={Status}", partitionId, count, status);
    }

    /// <summary>
    /// The single mapper for a settled direct-write batch → (committed?, retryable-if-not?). Used by both the
    /// result and the exception paths so they cannot diverge. A non-success outcome defaults to
    /// <b>retryable</b>: neither a thrown exception nor most returned statuses prove the write did not commit,
    /// and a retry (MustRetry) never loses a write, whereas a terminal error on a write that actually committed
    /// does. Only a provably-terminal status (see <see cref="IsPermanentStatus"/>) releases non-retryably.
    /// </summary>
    internal static (bool Committed, bool Transient) ClassifyOutcome(bool success, RaftOperationStatus status, bool threw)
    {
        if (success)
            return (true, false);

        // A thrown exception is an uncertain outcome around the round trip (disposed executor, actor
        // infrastructure, transient Raft fault) — it does not establish that the write did not commit, so it
        // is always retryable, never the terminal Errored the catch synthesizes.
        if (threw)
            return (false, true);

        return (false, !IsPermanentStatus(status));
    }

    /// <summary>The returned statuses that definitively mean the same request cannot succeed by retrying — the
    /// Raft layer reported a structural error, or the cluster can no longer form a majority. Every other
    /// non-success status (leadership churn, timeout, queue-full, restore, moved partition, proposal-not-found,
    /// cancellation, or an unknown value) is retryable per <see cref="ClassifyOutcome"/>.</summary>
    internal static bool IsPermanentStatus(RaftOperationStatus status) => status switch
    {
        RaftOperationStatus.Errored or RaftOperationStatus.InsufficientVoters => true,
        _ => false
    };
}
