using System.Diagnostics;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>What started the lane turn that dispatched a batch.</summary>
internal enum PartitionWriteDispatchTrigger
{
    /// <summary>The completion turn of the previous batch re-dispatched the buffer behind it.</summary>
    Completion,

    /// <summary>A timer wake (post-completion hold, linger, or age deadline) dispatched it.</summary>
    Wake,

    /// <summary>An arriving submission dispatched it (a full batch, or linger 0 on an empty buffer).</summary>
    Submit
}

/// <summary>
/// One partition's dispatch-to-dispatch cycle split into consecutive stages. The eight stage fields sum
/// exactly to <see cref="Cycle"/>; all values are milliseconds on the <see cref="Stopwatch"/> clock.
/// </summary>
internal readonly record struct PartitionWriteCycleStages(
    PartitionWriteDispatchTrigger Trigger,
    // Dispatch → the detached Raft round trip returned.
    double Raft,
    // Raft return → the lane began the batch-completion turn.
    double CompletionMailbox,
    // Completion turn start → submissions completed and the post-completion hold armed.
    double CompletionTurn,
    // Completion end → the requested end of the post-completion hold, or the dispatch when a full batch cut
    // the hold short; 0 without a hold.
    double Hold,
    // Time after the hold (or after the completion, without one) during which the buffer was empty — the
    // cycle waited for a submission, not for the timer.
    double ArrivalWait,
    // From the later of the hold end and the first arrival to the timer firing: timer slop, plus the linger
    // window when the dispatching wake was a linger wake.
    double WakeLate,
    // Timer fired → the lane began the wake turn.
    double WakeMailbox,
    // Dispatching turn start → the batch was handed to the Raft round trip (sweep, select, fence check,
    // entry assembly).
    double Dispatch,
    double Cycle);

/// <summary>
/// Stamps one partition's aggregator cycle on the shared <see cref="Stopwatch"/> clock: the dispatch, the Raft
/// return, the completion turn, the hold, the wake and the next dispatch. A cycle is closed only when the batch
/// that completed was the partition's most recent dispatch, so with more than one batch in flight the
/// overlapping rounds are not folded into one cycle. Owned by the lane's single-threaded state; no locking.
/// </summary>
internal sealed class PartitionWriteCycleTrace
{
    private long lastDispatch;

    private bool completed;

    private long raftReturned;

    private long completionStarted;

    private long completionEnded;

    // Requested end of the post-completion hold, or 0 when none was armed by the last completion.
    private long holdEnds;

    // When a submission opened an empty buffer after the last completion, or 0 while none has.
    private long bufferOpened;

    /// <summary>A submission opened an empty buffer at <paramref name="timestamp"/>. Only the first opening
    /// after a completion matters: before it, the cycle was waiting for work, not for the timer.</summary>
    public void OnBufferOpened(long timestamp)
    {
        if (completed && bufferOpened == 0)
            bufferOpened = timestamp;
    }

    /// <summary>A batch settled. <paramref name="dispatched"/> identifies it: a batch that is not the most
    /// recent dispatch overlapped a later one and does not close a cycle.</summary>
    public void OnCompletion(long dispatched, long raftReturnedAt, long turnStarted, long turnEnded, long holdEndsAt, bool hasPending)
    {
        if (dispatched != lastDispatch)
            return;

        completed = true;
        raftReturned = raftReturnedAt;
        completionStarted = turnStarted;
        completionEnded = turnEnded;
        holdEnds = holdEndsAt;

        // A buffer that already holds work at the completion was never empty in this cycle.
        bufferOpened = hasPending ? turnEnded : 0;
    }

    /// <summary>
    /// Records a dispatch at <paramref name="dispatched"/> from a turn that started at
    /// <paramref name="turnStarted"/> and, when the previous batch closed a cycle, returns its stages.
    /// <paramref name="wakeFired"/> is the dispatching timer's fire time (only read for
    /// <see cref="PartitionWriteDispatchTrigger.Wake"/>).
    /// </summary>
    public bool TryClose(long dispatched, long turnStarted, PartitionWriteDispatchTrigger trigger, long wakeFired, out PartitionWriteCycleStages stages)
    {
        bool close = completed;
        long previousDispatch = lastDispatch;
        completed = false;
        lastDispatch = dispatched;

        if (!close)
        {
            stages = default;
            return false;
        }

        long gapStart = completionEnded;

        // The dispatching turn cannot start before the completion ended; a completion-triggered dispatch
        // runs inside the completion turn itself.
        long turn = trigger == PartitionWriteDispatchTrigger.Completion ? gapStart : Math.Max(turnStarted, gapStart);

        // The timer can fire before the completion turn ends (a wake armed earlier) and still dispatch after
        // it; clamp so the stages stay consecutive.
        long fired = trigger == PartitionWriteDispatchTrigger.Wake ? Math.Clamp(wakeFired, gapStart, turn) : turn;

        long holdEnd = holdEnds > 0 ? Math.Clamp(holdEnds, gapStart, fired) : gapStart;

        long arrivalEnd = holdEnd;
        if (bufferOpened > holdEnd)
            arrivalEnd = Math.Min(bufferOpened, fired);
        else if (bufferOpened == 0)
            arrivalEnd = fired; // nothing arrived before the dispatching turn: the whole remainder was a wait for work

        stages = new PartitionWriteCycleStages(
            trigger,
            Raft: Ms(raftReturned - previousDispatch),
            CompletionMailbox: Ms(completionStarted - raftReturned),
            CompletionTurn: Ms(completionEnded - completionStarted),
            Hold: Ms(holdEnd - gapStart),
            ArrivalWait: Ms(arrivalEnd - holdEnd),
            WakeLate: Ms(fired - arrivalEnd),
            WakeMailbox: Ms(turn - fired),
            Dispatch: Ms(dispatched - turn),
            Cycle: Ms(dispatched - previousDispatch));

        return true;
    }

    private static double Ms(long stopwatchTicks) => stopwatchTicks * 1000.0 / Stopwatch.Frequency;
}
