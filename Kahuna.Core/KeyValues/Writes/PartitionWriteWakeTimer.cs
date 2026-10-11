using System.Diagnostics;
using Nixie;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// One dedicated thread that fires the aggregator lanes' partition wakes at their deadline, replacing a
/// <see cref="Task.Delay(TimeSpan, TimeProvider)"/> per wake. The timer queue fires a millisecond-scale delay
/// 1-3 ms late — its own granularity plus a thread-pool hop for the continuation — and a post-completion hold
/// pays that on every cycle. A kernel timed wait on a thread of its own returns 0.07-0.25 ms after its
/// whole-millisecond timeout, so this thread sleeps the whole milliseconds that wait can honor and spins only
/// a sub-millisecond remainder, bounded to well under a millisecond per wake and shared by every partition on
/// the node. Compared with spinning the last 2 ms of each wake on a thread-pool thread, that is the same
/// lateness at a fraction of the CPU.
///
/// <para>Each partition owns one <see cref="Wake"/> handle; arming it again replaces the outstanding
/// deadline (superseded entries are skipped when they surface), so a partition never has two wakes in flight
/// and a queue-age wake armed behind a round is simply overwritten by the hold wake that follows. The fire
/// posts the lane's <c>TimerWake</c> control message carrying the wake id the lane armed, so a lane can tell a
/// live wake from a superseded one.</para>
/// </summary>
internal sealed class PartitionWriteWakeTimer : IDisposable
{
    /// <summary>How late a whole-millisecond kernel wait is expected to return on the hosting runtime. The
    /// sleep is rounded against it so the common case — a deadline a whole number of milliseconds away —
    /// sleeps exactly that many and lands inside the overshoot, with no spin; a fractional remainder spins at
    /// most about half a millisecond plus the overshoot.</summary>
    private const double KernelWaitOvershootMs = 0.25;

    /// <summary>One partition's wake slot. Owned by the lane that armed it; the deadline and the armed id are
    /// read and written only under the timer's gate.</summary>
    internal sealed class Wake
    {
        internal readonly IActorRef<PartitionWriteAggregatorActor, PartitionWriteMessage, PartitionWriteAck> Lane;

        internal readonly int PartitionId;

        /// <summary>Id of the outstanding entry, or 0 when none is armed. An entry whose id no longer matches
        /// was superseded by a later arm and is dropped when it surfaces.</summary>
        internal long ArmedId;

        /// <summary>Whether the outstanding entry is a flush (linger or hold) wake or a queue-age wake; carried
        /// to the lateness metric only.</summary>
        internal bool Flush;

        public Wake(IActorRef<PartitionWriteAggregatorActor, PartitionWriteMessage, PartitionWriteAck> lane, int partitionId)
        {
            Lane = lane;
            PartitionId = partitionId;
        }
    }

    private readonly object gate = new();

    private readonly PriorityQueue<(Wake Wake, long Id), long> queue = new();

    private readonly TimeProvider clock;

    private readonly long stampsPerMs;

    // Started by the first arm, so an aggregator whose lanes never use the timer owns no thread.
    private Thread? thread;

    private bool stopped;

    // Bumped on every arm so a spin on the current head notices an earlier deadline and re-evaluates.
    private int generation;

    public PartitionWriteWakeTimer(TimeProvider clock)
    {
        this.clock = clock;
        stampsPerMs = Math.Max(1, clock.TimestampFrequency / 1000);
    }

    /// <summary>Arms <paramref name="wake"/> for the clock timestamp <paramref name="due"/>, replacing any
    /// outstanding deadline of the same partition. <paramref name="wakeId"/> is echoed on the fired message.</summary>
    public void Arm(Wake wake, long due, long wakeId, bool flush)
    {
        lock (gate)
        {
            if (stopped)
                return;

            wake.ArmedId = wakeId;
            wake.Flush = flush;
            queue.Enqueue((wake, wakeId), due);
            generation++;

            if (thread is null)
            {
                thread = new Thread(Run) { IsBackground = true, Name = "kv-write-wake-timer" };
                thread.Start();
            }

            // Only a new head changes what the thread should be waiting for.
            if (queue.Peek().Id == wakeId && queue.Peek().Wake == wake)
                Monitor.Pulse(gate);
        }
    }

    /// <summary>Stops the thread; entries still queued never fire. Idempotent.</summary>
    public void Stop()
    {
        Thread? running;
        lock (gate)
        {
            if (stopped)
                return;

            stopped = true;
            running = thread;
            Monitor.PulseAll(gate);
        }

        running?.Join(TimeSpan.FromSeconds(1));
    }

    public void Dispose() => Stop();

    private void Run()
    {
        while (true)
        {
            Wake? fire = null;
            long fireId = 0;
            long due = 0;
            long spinUntil = 0;

            lock (gate)
            {
                while (true)
                {
                    if (stopped)
                        return;

                    if (!queue.TryPeek(out (Wake Wake, long Id) head, out due))
                    {
                        Monitor.Wait(gate);
                        continue;
                    }

                    // Superseded by a later arm of the same partition.
                    if (head.Wake.ArmedId != head.Id)
                    {
                        queue.Dequeue();
                        continue;
                    }

                    long remaining = due - clock.GetTimestamp();
                    if (remaining <= 0)
                    {
                        queue.Dequeue();
                        head.Wake.ArmedId = 0;
                        fire = head.Wake;
                        fireId = head.Id;
                        break;
                    }

                    int sleepMs = (int)Math.Min(int.MaxValue, Math.Round(remaining / (double)stampsPerMs - KernelWaitOvershootMs, MidpointRounding.AwayFromZero));
                    if (sleepMs >= 1)
                    {
                        // An earlier arm pulses the gate and this re-evaluates the head.
                        Monitor.Wait(gate, sleepMs);
                        continue;
                    }

                    // Sub-millisecond remainder: spin outside the gate so lanes can still arm meanwhile.
                    spinUntil = due;
                    break;
                }
            }

            if (fire is not null)
            {
                long fired = clock.GetTimestamp();
                PartitionWriteAggregatorMetrics.WakeFired((fired - due) / (double)stampsPerMs, precise: true, fire.Flush);
                fire.Lane.Send(PartitionWriteMessage.TimerWake(fire.PartitionId, Stopwatch.GetTimestamp(), fireId));
                continue;
            }

            int seen = Volatile.Read(ref generation);
            while (clock.GetTimestamp() < spinUntil && Volatile.Read(ref generation) == seen && !Volatile.Read(ref stopped))
                Thread.SpinWait(32);
        }
    }
}
