using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// A slot held by an admitted transaction. Disposing it returns the slot to the node and lets the
/// orderer start the next waiting transaction. Disposal is idempotent, so the mandatory
/// <c>finally</c> that releases it may run alongside any other completion path without
/// double-counting.
/// </summary>
internal sealed class AdmissionLease : IDisposable
{
    /// <summary>Shared no-op lease handed out when the orderer is in pass-through mode. Stateless, so a
    /// single instance is safe to share across every caller on the node.</summary>
    internal static readonly AdmissionLease PassThrough = new(null, TransactionPriority.Normal);

    private readonly TransactionPriorityOrderer? orderer;

    private readonly TransactionPriority priority;

    private int released;

    internal AdmissionLease(TransactionPriorityOrderer? orderer, TransactionPriority priority)
    {
        this.orderer = orderer;
        this.priority = priority;
    }

    public void Dispose()
    {
        if (orderer is null)
            return;

        // A leaked lease permanently shrinks node capacity and a double release inflates it past the
        // ceiling, so release exactly once no matter how many paths dispose this lease.
        if (Interlocked.Exchange(ref released, 1) != 0)
            return;

        orderer.Release(priority);
    }
}

/// <summary>
/// Per-node admission gate that decides, when the node is saturated, which waiting transaction starts
/// next. Below the configured ceiling the gate is transparent: a caller is admitted synchronously with
/// no queueing and no reordering, which is the common case. At the ceiling a caller parks on an
/// awaitable and is resumed when a slot frees, highest priority first.
///
/// <para>The gate governs <b>start</b> only. Once admitted a transaction runs to completion exactly as it
/// would without the orderer; nothing here preempts or aborts running work, and nothing here is
/// replicated — the queue is per-node in-memory state like the lock table, and a restart legitimately
/// loses waiters that had not started.</para>
///
/// <para>Two independent ceilings exist because the two transaction shapes hold a slot for very different
/// durations: a script transaction holds one for its own bounded execution, whereas an interactive session
/// holds one for as long as the client keeps it open. They are separate orderer instances so idle
/// client-paced sessions can never consume the slots script transactions need.</para>
///
/// <para><b>Time.</b> How long a caller has waited is measured with a monotonic <see cref="TimeProvider"/>
/// timestamp, deliberately <i>not</i> with the Raft HLC. Queue waiting is a purely local duration, not a
/// distributed happened-before decision, and the shared HLC's physical component can be dragged forward by
/// any peer's timestamp — a single skewed node would otherwise age every local waiter to the front at once
/// and collapse the priority separation the gate exists to provide. Arrival <i>order</i> uses a monotonic
/// sequence, which is a strict total order and so needs no timestamp tiebreak at all.</para>
/// </summary>
internal sealed class TransactionPriorityOrderer : IDisposable
{
    private const int LevelCount = 5;

    /// <summary>Lowest priority permitted to draw on the reserve. Kept as an int for indexing.</summary>
    private const int ReserveEligibleFrom = (int)TransactionPriority.High;

    private sealed class Waiter
    {
        /// <summary>The orderer this waiter is parked in, so a cancellation callback can settle its queue
        /// accounting without capturing a closure per waiter.</summary>
        public required TransactionPriorityOrderer Owner { get; init; }

        public required TransactionPriority Priority { get; init; }

        /// <summary>Monotonic timestamp captured when this caller started waiting, used only to measure
        /// elapsed local wait for aging.</summary>
        public required long EnqueuedAt { get; init; }

        /// <summary>Monotonic arrival counter. A strict total order over arrivals, so it alone decides
        /// first-come-first-served among equally ranked waiters.</summary>
        public required long Sequence { get; init; }

        public required TaskCompletionSource<AdmissionLease?> Completion { get; init; }

        /// <summary>This waiter's node in its priority queue, so cancellation can unlink it in constant time
        /// instead of leaving it to be discovered whenever a dispatch next reaches the head.</summary>
        public LinkedListNode<Waiter>? Node { get; set; }

        /// <summary>Cancellation registration. Written and read only under the orderer's lock until the
        /// waiter settles, after which it has no concurrent writer.</summary>
        public CancellationTokenRegistration Registration { get; set; }

        /// <summary>Set once this waiter has been promoted at least once, so the promotion metric counts
        /// waiters rather than observations.</summary>
        public bool CountedAsPromoted { get; set; }

        private int settled;

        /// <summary>Claims this waiter for exactly one outcome — dispatch, cancellation, or disposal. The
        /// loser of the race must leave the waiter completely alone. Always called under the lock.</summary>
        public bool TrySettle() => Interlocked.Exchange(ref settled, 1) == 0;

        public bool IsSettled => Volatile.Read(ref settled) != 0;
    }

    private readonly int maxConcurrent;

    private readonly int reserved;

    private readonly int maxQueued;

    private readonly int agingThresholdMs;

    private readonly TimeProvider timeProvider;

    /// <summary>Guards every counter and queue below. Held only for bookkeeping — never across an await,
    /// and never while completing a waiter's task, so a continuation can never run under it.</summary>
    private readonly object mutex = new();

    private readonly LinkedList<Waiter>[] queues;

    private readonly long[] admittedTotal = new long[LevelCount];

    private readonly int[] inFlightByLevel = new int[LevelCount];

    private readonly int[] queuedByLevel = new int[LevelCount];

    private readonly int[] maxQueueDepthByLevel = new int[LevelCount];

    private readonly long[] promotedTotal = new long[LevelCount];

    private readonly long[] abandonedWhileWaitingTotal = new long[LevelCount];

    private readonly long[] rejectedQueueFullTotal = new long[LevelCount];

    private int inFlight;

    /// <summary>
    /// Admitted transactions whose <b>base</b> priority is below <see cref="TransactionPriority.High"/>.
    /// Tracked separately from <see cref="inFlight"/> because the reserve is a bound on how much of the node
    /// ordinary work may occupy, not a bound on total occupancy. Deciding ordinary eligibility from the
    /// aggregate instead would leave a genuinely free slot idle whenever high-priority work held the rest,
    /// and under a sustained high-priority stream that idle slot never becomes available — starving the
    /// low-priority work that aging is supposed to rescue.
    /// </summary>
    private int ordinaryInFlight;

    private int totalQueued;

    private long sequence;

    private bool disposed;

    /// <summary>
    /// Builds an orderer. A <paramref name="maxConcurrent"/> of zero or less selects pass-through mode:
    /// every caller is admitted immediately and priority is recorded for observability but never gates,
    /// which is the configuration under which behavior is identical to having no admission gate at all.
    /// </summary>
    /// <param name="maxConcurrent">Transactions that may run concurrently on this node. 0 = pass-through.</param>
    /// <param name="reserved">Slots out of <paramref name="maxConcurrent"/> that only High and Critical may
    /// occupy. Clamped so at least one slot always remains reachable by lower priorities, otherwise ordinary
    /// traffic could never start.</param>
    /// <param name="agingThresholdMs">How long a waiter must wait to gain one effective priority level.
    /// 0 or less disables aging.</param>
    /// <param name="maxQueued">Callers that may wait at once before further ones are refused outright.
    /// 0 or less leaves the queue unbounded.</param>
    /// <param name="timeProvider">Monotonic source used to measure elapsed waiting.</param>
    public TransactionPriorityOrderer(
        int maxConcurrent,
        int reserved,
        int agingThresholdMs,
        int maxQueued = 0,
        TimeProvider? timeProvider = null)
    {
        this.maxConcurrent = Math.Max(0, maxConcurrent);
        this.reserved = this.maxConcurrent <= 0 ? 0 : Math.Clamp(reserved, 0, this.maxConcurrent - 1);
        this.agingThresholdMs = agingThresholdMs;
        this.maxQueued = Math.Max(0, maxQueued);
        this.timeProvider = timeProvider ?? TimeProvider.System;

        queues = new LinkedList<Waiter>[LevelCount];
        for (int i = 0; i < LevelCount; i++)
            queues[i] = new();
    }

    /// <summary>True when the orderer never gates and only records priority.</summary>
    public bool IsPassThrough => maxConcurrent <= 0;

    /// <summary>
    /// Maps any priority value to one this orderer will honour. An ordinal outside the defined scale can
    /// reach here from a REST payload carrying a raw number or from a cast enum on the embedded API; left
    /// alone it would be treated as the nearest extreme, which for a large value means <c>Critical</c> —
    /// letting untrusted input jump the queue and claim reserved capacity. Unknown values become
    /// <see cref="TransactionPriority.Normal"/>, matching what the gRPC wire contract already does.
    /// </summary>
    internal static TransactionPriority Normalize(TransactionPriority priority)
        => Enum.IsDefined(priority) ? priority : TransactionPriority.Normal;

    /// <summary>
    /// Acquires a slot for a transaction of the given priority. Returns a completed task when headroom
    /// exists — the overwhelmingly common case, and the reason an unsaturated node pays nothing for the
    /// gate. Otherwise the returned task completes when a slot frees and this waiter wins dispatch.
    ///
    /// <para>Returns <c>null</c> when the wait queue is already at its configured bound. That is the gate
    /// shedding load rather than accumulating it: an admission queue that grows without limit consumes the
    /// very memory the ceiling exists to protect. Callers surface it as retryable backpressure.</para>
    ///
    /// <para><paramref name="cancellationToken"/> is normally the transaction's own timeout: a caller that
    /// gives up stops competing for slots immediately rather than being handed one it can no longer use.</para>
    /// </summary>
    public ValueTask<AdmissionLease?> AdmitAsync(TransactionPriority priority, CancellationToken cancellationToken)
    {
        priority = Normalize(priority);

        if (maxConcurrent <= 0)
        {
            // Pass-through: still record the priority so operators can see the distribution of work before
            // deciding what ceiling to configure.
            lock (mutex)
                admittedTotal[Level(priority)]++;

            return new(AdmissionLease.PassThrough);
        }

        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled<AdmissionLease?>(cancellationToken);

        Waiter waiter;

        lock (mutex)
        {
            ObjectDisposedException.ThrowIf(disposed, this);

            // Fast path. Safe to take without consulting the queues: a slot is only ever freed inside
            // Release, which dispatches every eligible waiter before releasing the lock, so a queue that is
            // still non-empty here holds only waiters this level's capacity check would also reject.
            if (HasCapacityFor(priority))
            {
                Occupy(priority);
                return new(new AdmissionLease(this, priority));
            }

            int level = Level(priority);

            if (maxQueued > 0 && totalQueued >= maxQueued)
            {
                rejectedQueueFullTotal[level]++;
                return new((AdmissionLease?)null);
            }

            waiter = new()
            {
                Owner = this,
                Priority = priority,
                EnqueuedAt = timeProvider.GetTimestamp(),
                Sequence = ++sequence,
                Completion = new(TaskCreationOptions.RunContinuationsAsynchronously)
            };

            waiter.Node = queues[level].AddLast(waiter);

            totalQueued++;
            queuedByLevel[level]++;
            if (queuedByLevel[level] > maxQueueDepthByLevel[level])
                maxQueueDepthByLevel[level] = queuedByLevel[level];
        }

        // The registration cannot be created under the lock: if the token is already cancelled the callback
        // runs inline and would re-enter it. So it is created here and then published under the lock only if
        // the waiter has not already been settled by a concurrent dispatch, cancellation, or disposal.
        // Publishing it unconditionally would let a registration outlive the waiter it belongs to — never
        // disposed, keeping the waiter and this orderer reachable — and would race the field against readers.
        if (cancellationToken.CanBeCanceled)
        {
            CancellationTokenRegistration registration =
                cancellationToken.Register(static state => Abandon((Waiter)state!), waiter);

            bool alreadySettled;

            lock (mutex)
            {
                alreadySettled = waiter.IsSettled;

                if (!alreadySettled)
                    waiter.Registration = registration;
            }

            if (alreadySettled)
                registration.Dispose();
        }

        return new(waiter.Completion.Task);
    }

    /// <summary>
    /// Returns a slot and starts the highest-priority waiters that now fit. Called only through
    /// <see cref="AdmissionLease.Dispose"/>, which guarantees exactly one release per admission.
    /// </summary>
    internal void Release(TransactionPriority priority)
    {
        List<Waiter>? winners;

        lock (mutex)
        {
            int level = Level(priority);

            inFlight--;
            inFlightByLevel[level]--;

            if (level < ReserveEligibleFrom)
                ordinaryInFlight--;

            winners = DispatchWaiters();
        }

        CompleteWinners(winners);
    }

    /// <summary>
    /// Starts as many waiters as current capacity allows, best first. Must be called holding the lock; the
    /// winners it returns are completed by the caller <b>after</b> the lock is dropped so no continuation
    /// ever runs inside the critical section.
    /// </summary>
    private List<Waiter>? DispatchWaiters()
    {
        List<Waiter>? winners = null;

        while (true)
        {
            Waiter? next = TakeNextDispatchable();
            if (next is null)
                break;

            Occupy(next.Priority);

            (winners ??= []).Add(next);
        }

        return winners;
    }

    /// <summary>
    /// Removes and returns the waiter that should start next, or null when nothing eligible is waiting.
    /// Selection is by <b>effective</b> priority (base priority plus whatever aging has earned), with the
    /// earlier arrival winning a tie so equal-priority work stays first-come-first-served.
    /// </summary>
    private Waiter? TakeNextDispatchable()
    {
        while (true)
        {
            Waiter? best = null;
            int bestLevel = -1;
            int bestEffective = -1;

            // One reading for the whole selection, so every candidate is aged against the same instant and
            // the comparison between them is consistent. Skipped entirely when aging is off.
            long now = agingThresholdMs > 0 ? timeProvider.GetTimestamp() : 0;

            for (int level = 0; level < LevelCount; level++)
            {
                LinkedList<Waiter>? queue = queues[level];

                if (queue.First is null)
                    continue;

                // Eligibility is decided on the level's own capacity, so a queue that cannot start yet is
                // skipped rather than blocking a higher-priority queue that can.
                if (!HasCapacityFor((TransactionPriority)level))
                    continue;

                Waiter head = queue.First.Value;
                int effective = EffectivePriority(head, now);

                if (best is null || effective > bestEffective || (effective == bestEffective && head.Sequence < best.Sequence))
                {
                    best = head;
                    bestLevel = level;
                    bestEffective = effective;
                }
            }

            if (best is null)
                return null;

            // A cancellation callback that already claimed this waiter outside the lock may not have reached
            // its bookkeeping yet; unlink it here and look for the next candidate.
            if (!best.TrySettle())
            {
                Unlink(best, bestLevel);
                continue;
            }

            Unlink(best, bestLevel);

            return best;
        }
    }

    /// <summary>Removes a settled waiter from its queue and updates the depth counters. Under the lock.</summary>
    private void Unlink(Waiter waiter, int level)
    {
        if (waiter.Node is null)
            return;

        queues[level].Remove(waiter.Node);
        waiter.Node = null;

        totalQueued--;
        queuedByLevel[level]--;
    }

    /// <summary>
    /// Priority this waiter competes at right now. A waiter gains one level per <c>AgingThreshold</c> spent
    /// waiting, capped just below <see cref="TransactionPriority.Critical"/>, so background work queued
    /// behind an unending stream of important work still reaches the front in bounded time instead of
    /// starving. Critical is never aged because it already sorts above everything.
    ///
    /// <para>Aging deliberately moves a waiter's <i>position in line</i> and not its <i>class</i>: capacity
    /// eligibility below still consults the base priority, so an aged background transaction can overtake
    /// ordinary work but can never consume a slot reserved for High/Critical.</para>
    /// </summary>
    private int EffectivePriority(Waiter waiter, long now)
    {
        int basePriority = Level(waiter.Priority);

        if (agingThresholdMs <= 0 || basePriority >= ReserveEligibleFrom)
            return basePriority;

        long waitedMs = (long)timeProvider.GetElapsedTime(waiter.EnqueuedAt, now).TotalMilliseconds;
        if (waitedMs < agingThresholdMs)
            return basePriority;

        int steps = (int)Math.Min(waitedMs / agingThresholdMs, LevelCount);
        int effective = Math.Min(basePriority + steps, ReserveEligibleFrom);

        if (!waiter.CountedAsPromoted)
        {
            waiter.CountedAsPromoted = true;
            promotedTotal[basePriority]++;
        }

        return effective;
    }

    /// <summary>
    /// Whether a transaction of this priority may take a slot right now. High and Critical may fill the node
    /// to its ceiling. Everything below is additionally bounded by how much of the node ordinary work already
    /// occupies, which is what keeps <see cref="reserved"/> slots permanently reachable by latency-critical
    /// work without leaving usable capacity idle.
    /// </summary>
    private bool HasCapacityFor(TransactionPriority priority)
    {
        if (inFlight >= maxConcurrent)
            return false;

        return Level(priority) >= ReserveEligibleFrom || ordinaryInFlight < maxConcurrent - reserved;
    }

    private void Occupy(TransactionPriority priority)
    {
        int level = Level(priority);

        inFlight++;
        inFlightByLevel[level]++;
        admittedTotal[level]++;

        if (level < ReserveEligibleFrom)
            ordinaryInFlight++;
    }

    private void CompleteWinners(List<Waiter>? winners)
    {
        if (winners is null)
            return;

        foreach (Waiter winner in winners)
        {
            // Safe to read outside the lock: the waiter is settled, so nothing will publish a registration
            // onto it any more.
            winner.Registration.Dispose();
            winner.Completion.SetResult(new(this, winner.Priority));
        }
    }

    /// <summary>Called when a waiter's own timeout fires before it was ever started. It stops competing and
    /// its caller observes cancellation, exactly as a transaction that timed out before doing any work. The
    /// queue entry is unlinked immediately rather than left for a later dispatch to discover, so callers that
    /// walked away cannot accumulate behind a long-lived occupant.</summary>
    private static void Abandon(Waiter waiter)
    {
        TransactionPriorityOrderer owner = waiter.Owner;
        int level = Level(waiter.Priority);
        bool settled;

        lock (owner.mutex)
        {
            settled = waiter.TrySettle();

            if (settled)
            {
                owner.Unlink(waiter, level);
                owner.abandonedWhileWaitingTotal[level]++;
            }
        }

        if (settled)
            waiter.Completion.TrySetCanceled();
    }

    /// <summary>
    /// Fails every waiter still parked when the node tears down, so no caller is left awaiting a slot that
    /// will never be granted.
    /// </summary>
    public void Dispose()
    {
        List<Waiter> parked = [];

        lock (mutex)
        {
            if (disposed)
                return;

            disposed = true;

            for (int level = 0; level < LevelCount; level++)
            {
                while (queues[level].First is not null)
                {
                    Waiter waiter = queues[level].First!.Value;

                    if (waiter.TrySettle())
                        parked.Add(waiter);

                    Unlink(waiter, level);
                }
            }
        }

        foreach (Waiter waiter in parked)
        {
            waiter.Registration.Dispose();
            waiter.Completion.TrySetCanceled();
        }
    }

    private static int Level(TransactionPriority priority) => (int)Normalize(priority);

    // ---- Observability ------------------------------------------------------------------------------

    /// <summary>Transactions currently holding a slot, across all priorities.</summary>
    public int InFlight
    {
        get { lock (mutex) return inFlight; }
    }

    /// <summary>Transactions currently waiting for a slot, across all priorities. Greater than zero is the
    /// signal that the orderer is actually gating rather than running transparently.</summary>
    public int Queued
    {
        get { lock (mutex) return totalQueued; }
    }

    public int InFlightAt(TransactionPriority priority)
    {
        lock (mutex) return inFlightByLevel[Level(priority)];
    }

    public int QueuedAt(TransactionPriority priority)
    {
        lock (mutex) return queuedByLevel[Level(priority)];
    }

    public long AdmittedAt(TransactionPriority priority)
    {
        lock (mutex) return admittedTotal[Level(priority)];
    }

    public int MaxQueueDepthAt(TransactionPriority priority)
    {
        lock (mutex) return maxQueueDepthByLevel[Level(priority)];
    }

    /// <summary>Waiters of this priority that aging promoted at least once while they waited.</summary>
    public long PromotedAt(TransactionPriority priority)
    {
        lock (mutex) return promotedTotal[Level(priority)];
    }

    /// <summary>Waiters of this priority whose own timeout fired before they were ever started.</summary>
    public long AbandonedWhileWaitingAt(TransactionPriority priority)
    {
        lock (mutex) return abandonedWhileWaitingTotal[Level(priority)];
    }

    /// <summary>Requests of this priority refused outright because the wait queue was already full. Distinct
    /// from <see cref="AbandonedWhileWaitingAt"/>: this is the gate shedding load, not a caller giving up.</summary>
    public long RejectedQueueFullAt(TransactionPriority priority)
    {
        lock (mutex) return rejectedQueueFullTotal[Level(priority)];
    }
}
