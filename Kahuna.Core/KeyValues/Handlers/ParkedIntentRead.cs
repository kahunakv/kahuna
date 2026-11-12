using System.Diagnostics;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// A transactional latest read (a point get or a batched probe) that parked behind a live write intent of another
/// transaction instead of pinning the committed head below it. The read holds no actor turn while it is parked: its
/// reply is deferred, and a <see cref="KeyValueRequestType.ResumeIntentWait"/> message runs it again from the start.
///
/// <para>The read keeps its own copy of the request (<see cref="Request"/>). The caller's request is pooled and is
/// the caller's to return, so it is never kept past the turn that parked the read.</para>
///
/// <para>Generations. Every park arms one timer and increments <see cref="Generation"/>. A resume is accepted only
/// for the current generation and only once (see <see cref="IntentWaitRegistry.TryAcceptResume"/>), so a timer of an
/// earlier park, or a timer that fires after a wake already ran the read, is dropped. Actor thread only, except the
/// timer callback, which only sends a message.</para>
/// </summary>
internal sealed class ParkedIntentRead
{
    private readonly Action<KeyValueRequest> sendToActor;

    /// <param name="sendToActor">Sends a message to the mailbox of the actor the read parks on: the actor's own
    /// <c>Self.Send</c>.</param>
    internal ParkedIntentRead(
        KeyValueRequest request,
        KeyValueReplyRef reply,
        long deadlineTick,
        Action<KeyValueRequest> sendToActor)
    {
        Request = request;
        Reply = reply;
        DeadlineTick = deadlineTick;
        StartTimestamp = Stopwatch.GetTimestamp();
        this.sendToActor = sendToActor;
    }

    /// <summary>The read to run again: a <c>TryGet</c> or a <c>TryProbeMany</c>, owned by this object.</summary>
    internal KeyValueRequest Request { get; }

    /// <summary>The caller's deferred reply.</summary>
    internal KeyValueReplyRef Reply { get; }

    /// <summary><see cref="Environment.TickCount64"/> at which the wait ends and the read is served as if it had
    /// not asked to wait. A local timeout on this node, not an ordering of distributed events.</summary>
    internal long DeadlineTick { get; }

    /// <summary><see cref="Stopwatch"/> timestamp of the first park, for the wait-time histogram.</summary>
    internal long StartTimestamp { get; }

    /// <summary>Incremented at every park.</summary>
    internal long Generation { get; private set; }

    /// <summary>True from a park until a resume for that park is accepted.</summary>
    internal bool AwaitingResume { get; set; }

    /// <summary>The entry the read is parked on, or null when it is on no entry's list.</summary>
    internal KeyValueEntry? Entry { get; set; }

    /// <summary>The transaction whose write intent the read waits for.</summary>
    internal HLCTimestamp Holder { get; private set; }

    /// <summary>Records a new park on <paramref name="entry"/> behind <paramref name="holder"/> and returns its generation.</summary>
    internal long BeginPark(KeyValueEntry entry, HLCTimestamp holder)
    {
        Entry = entry;
        Holder = holder;
        AwaitingResume = true;
        return ++Generation;
    }

    /// <summary>Sends the resume of <paramref name="generation"/> to the actor the read parked on.</summary>
    internal void SendResume(long generation) =>
        sendToActor(KeyValueRequestPool.RentResumeIntentWait(this, generation));

    /// <summary>
    /// Arms the timer of park <paramref name="generation"/>: after <paramref name="delayMs"/> the actor gets a resume
    /// for it. The timer is not cancelled when a wake comes first; its resume is then stale and dropped.
    /// </summary>
    internal void ArmTimer(long generation, int delayMs)
    {
        _ = Task.Delay(Math.Max(delayMs, 1)).ContinueWith(
            static (_, state) =>
            {
                (ParkedIntentRead read, long armed) = ((ParkedIntentRead, long))state!;
                read.SendResume(armed);
            },
            (this, generation),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }
}

/// <summary>
/// The reads parked on one key-value actor behind live write intents of other transactions. Each parked read sits on
/// the list of the resident entry whose intent it waits for (<see cref="KeyValueEntry.ParkedReads"/>); this registry
/// counts them, applies the bounds, and wakes them.
///
/// <para>Wake paths. A read wakes when the intent it waits for is no longer on the entry: every change of
/// <see cref="KeyValueEntry.WriteIntent"/> queues the entry for the end-of-turn head publication, and that flush
/// calls <see cref="WakeIfIntentGone"/> (commit, rollback, release, the durable resolution, a lapsed intent cleared
/// by another request, a take-over, a leader-state drop). An entry that leaves the store or is replaced by a new
/// object wakes all its reads (<see cref="WakeAll"/>). A lease that lapses with no request on the key, and the wait
/// limit, are found by the read's own timer.</para>
///
/// <para>Actor thread only.</para>
/// </summary>
internal sealed class IntentWaitRegistry
{
    private readonly KeyValueContext context;

    private int parkedCount;

    internal IntentWaitRegistry(KeyValueContext context)
    {
        this.context = context;
    }

    /// <summary>Reads parked on this actor now.</summary>
    internal int ParkedCount => parkedCount;

    /// <summary>Whether one more read may park on <paramref name="entry"/>.</summary>
    internal bool HasRoom(KeyValueEntry entry) =>
        parkedCount < context.Configuration.MaxParkedReadsPerActor
        && (entry.ParkedReads?.Count ?? 0) < context.Configuration.MaxParkedReadsPerKey;

    /// <summary>
    /// Parks <paramref name="read"/> on <paramref name="entry"/> behind its live write intent, and arms the read's
    /// timer for the earlier of its deadline and the end of the intent's lease.
    /// </summary>
    internal void Park(ParkedIntentRead read, KeyValueEntry entry, HLCTimestamp currentTime)
    {
        KeyValueWriteIntent intent = entry.WriteIntent!;

        (entry.ParkedReads ??= []).Add(read);
        parkedCount++;

        long generation = read.BeginPark(entry, intent.TransactionId);

        long delay = read.DeadlineTick - Environment.TickCount64;

        // A positive lease that is not claimed for the commit ends on its own; wake at its end so the read does not
        // wait for a holder that is gone. Any other intent ends only through a change that wakes the read.
        if (intent.Expires != HLCTimestamp.Zero && !intent.HeldForCommit)
        {
            long leaseMs = (long)Math.Ceiling((intent.Expires - currentTime).TotalMilliseconds);
            if (leaseMs < delay)
                delay = leaseMs;
        }

        read.ArmTimer(generation, (int)Math.Clamp(delay, 1, int.MaxValue));

        IntentWaitMetrics.Parked.Add(1);
    }

    /// <summary>
    /// Accepts the resume of park <paramref name="generation"/> of <paramref name="read"/>, once, and takes the read
    /// off its entry if a timer, not a wake, sent the resume. False for a stale or repeated resume.
    /// </summary>
    internal bool TryAcceptResume(ParkedIntentRead read, long generation)
    {
        if (!read.AwaitingResume || read.Generation != generation)
            return false;

        read.AwaitingResume = false;
        Detach(read);
        return true;
    }

    /// <summary>Wakes the reads of <paramref name="entry"/> whose holder no longer has the entry's write intent.</summary>
    internal void WakeIfIntentGone(KeyValueEntry entry)
    {
        List<ParkedIntentRead>? reads = entry.ParkedReads;
        if (reads is null)
            return;

        HLCTimestamp current = entry.WriteIntent?.TransactionId ?? HLCTimestamp.Zero;
        bool hasIntent = entry.WriteIntent is not null;

        for (int i = reads.Count - 1; i >= 0; i--)
        {
            ParkedIntentRead read = reads[i];
            if (hasIntent && read.Holder == current)
                continue;

            reads.RemoveAt(i);
            Wake(read);
        }

        if (reads.Count == 0)
            entry.ParkedReads = null;
    }

    /// <summary>Wakes every read parked on <paramref name="entry"/>: the entry left the store or was replaced.</summary>
    internal void WakeAll(KeyValueEntry entry)
    {
        List<ParkedIntentRead>? reads = entry.ParkedReads;
        if (reads is null)
            return;

        entry.ParkedReads = null;

        foreach (ParkedIntentRead read in reads)
            Wake(read);
    }

    private void Wake(ParkedIntentRead read)
    {
        read.Entry = null;
        parkedCount--;
        read.SendResume(read.Generation);
    }

    private void Detach(ParkedIntentRead read)
    {
        if (read.Entry is not { } entry)
            return;

        read.Entry = null;
        parkedCount--;

        if (entry.ParkedReads is { } reads)
        {
            reads.Remove(read);
            if (reads.Count == 0)
                entry.ParkedReads = null;
        }
    }
}
