namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// Pure, single-threaded FIFO state for one partition's pending direct writes. The lane actor owns one of
/// these per partition and drives it from its (single-threaded) message loop; keeping the ordering, batching,
/// one-in-flight, and wake-scheduling logic here — with no actors, timers, or Raft — makes it deterministically
/// testable.
///
/// <para>Wakes are scheduled by deadline (a millisecond tick), not by a clock the state reads. The actor asks
/// for <see cref="NextWakeDeadline"/> — the earlier of the oldest item's linger-flush and queue-age-expiry
/// deadlines (only the age deadline while a batch is in flight, since a flush cannot start then) — and arms a
/// single timer for it via <see cref="TryArmWake"/>. A steady arrival stream does not re-arm because the oldest
/// item's deadlines do not move. Count/byte thresholds still flush immediately without waiting for a timer.
/// Because linger ≤ queue-age (enforced by configuration), the earliest armed timer always fires no later than
/// any deadline that could come due, so the chain re-arms itself and never misses an expiry.</para>
/// </summary>
internal sealed class PartitionWriteState
{
    /// <summary>Sentinel deadline meaning "no wake needed / none armed".</summary>
    public const long NoWake = long.MaxValue;

    private readonly List<IProposalSubmission> pending = [];
    private int head;

    private long armedWakeDeadline = NoWake;

    /// <summary>Serialized bytes of the items still waiting (not yet selected into a batch).</summary>
    public long QueuedBytes { get; private set; }

    /// <summary>True while a batch for this partition is awaiting its Raft result.</summary>
    public bool InFlight { get; private set; }

    /// <summary>Items waiting behind the consumed head (excludes any already selected into an in-flight batch).</summary>
    public int PendingCount => pending.Count - head;

    /// <summary>Enqueue tick (ms) of the oldest pending item, or <see cref="NoWake"/> when empty. FIFO: the
    /// front of the queue is always the most-aged item.</summary>
    public long OldestPendingTicks => head < pending.Count ? pending[head].EnqueueTicks : NoWake;

    public readonly record struct EnqueueResult(bool ShouldFlushNow, bool OpenedBuffer);

    /// <summary>
    /// Appends an item. Signals whether it should flush now (count/byte threshold met and nothing in flight)
    /// and whether it opened a previously-empty buffer (the actor schedules a wake for the new coalescing
    /// window). A single item that already exceeds the byte target opens and immediately flushes so it
    /// dispatches alone.
    /// </summary>
    public EnqueueResult Enqueue(IProposalSubmission item, int maxItems, long maxBytes)
    {
        bool opened = PendingCount == 0;

        pending.Add(item);
        QueuedBytes += item.ByteLength;

        bool flushNow = !InFlight && (PendingCount >= maxItems || QueuedBytes >= maxBytes);
        return new EnqueueResult(flushNow, opened);
    }

    /// <summary>
    /// The next wake deadline (ms) for this partition's pending work, or <see cref="NoWake"/> if nothing is
    /// pending. While a batch is in flight only the age-expiry deadline matters (a flush cannot start);
    /// otherwise it is the earlier of the linger-flush and age-expiry deadlines, both anchored to the oldest
    /// pending item.
    /// </summary>
    public long NextWakeDeadline(int lingerMs, int maxQueueDelayMs)
    {
        if (PendingCount == 0)
            return NoWake;

        long oldest = OldestPendingTicks;
        long ageDeadline = oldest + maxQueueDelayMs;
        return InFlight ? ageDeadline : Math.Min(oldest + lingerMs, ageDeadline);
    }

    /// <summary>Arms a wake for <paramref name="deadline"/> only if it is earlier than the one already armed,
    /// so a steady arrival stream does not start a timer per item. Returns true if the caller should start the
    /// timer.</summary>
    public bool TryArmWake(long deadline)
    {
        if (deadline >= armedWakeDeadline)
            return false;

        armedWakeDeadline = deadline;
        return true;
    }

    /// <summary>Clears the armed-wake marker when its timer fires, so the next schedule can re-arm.</summary>
    public void ClearArmedWake() => armedWakeDeadline = NoWake;

    /// <summary>True when the linger window of the oldest pending item has elapsed at <paramref name="nowMs"/>
    /// and a flush is possible (nothing in flight) — the point at which a coalescing buffer is dispatched.</summary>
    public bool LingerElapsed(long nowMs, int lingerMs) =>
        !InFlight && PendingCount > 0 && nowMs - OldestPendingTicks >= lingerMs;

    /// <summary>
    /// Selects the next FIFO batch up to the item and byte caps and marks the partition in flight. The first
    /// item is always included even if it alone exceeds the byte cap (an oversized item dispatches alone; the
    /// cap is a batching target, not a value-size limit). Returns the selected items in admission order.
    /// </summary>
    public List<IProposalSubmission> SelectBatch(int maxItems, long maxBytes)
    {
        List<IProposalSubmission> batch = [];
        long batchBytes = 0;

        while (head < pending.Count && batch.Count < maxItems)
        {
            IProposalSubmission item = pending[head];
            if (batch.Count > 0 && batchBytes + item.ByteLength > maxBytes)
                break;

            batch.Add(item);
            batchBytes += item.ByteLength;
            QueuedBytes -= item.ByteLength;
            head++;
        }

        Compact();
        InFlight = true;
        return batch;
    }

    /// <summary>Clears the in-flight marker once a batch settled; the caller decides whether to re-dispatch the
    /// buffer behind it.</summary>
    public void OnBatchComplete() => InFlight = false;

    /// <summary>
    /// Pops and returns the oldest pending items whose age (<paramref name="nowTicks"/> − enqueue tick)
    /// exceeds <paramref name="maxDelayMs"/>, relieving queue-age pressure even while a batch is in flight.
    /// FIFO order means the front items are the most aged, so stopping at the first non-expired item is safe.
    /// </summary>
    public List<IProposalSubmission> PopExpired(long nowTicks, int maxDelayMs)
    {
        List<IProposalSubmission> expired = [];
        while (head < pending.Count && nowTicks - pending[head].EnqueueTicks > maxDelayMs)
        {
            IProposalSubmission item = pending[head];
            QueuedBytes -= item.ByteLength;
            expired.Add(item);
            head++;
        }

        if (expired.Count > 0)
            Compact();

        return expired;
    }

    /// <summary>Removes and returns every still-pending (not yet selected into an in-flight batch) item, for
    /// shutdown drain. In-flight items are untouched — they settle via their own completion.</summary>
    public List<IProposalSubmission> DrainPending()
    {
        List<IProposalSubmission> drained = [];
        for (int i = head; i < pending.Count; i++)
            drained.Add(pending[i]);

        pending.Clear();
        head = 0;
        QueuedBytes = 0;
        return drained;
    }

    private void Compact()
    {
        if (head >= pending.Count)
        {
            pending.Clear();
            head = 0;
        }
        else if (head > 1024)
        {
            pending.RemoveRange(0, head);
            head = 0;
        }
    }
}
