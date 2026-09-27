
using System.Collections.Concurrent;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Bounded idempotency window of finalized transaction outcomes. After a session leaves the coordinator's
/// live set, a duplicate commit/rollback for it replays the retained terminal answer (Committed/RolledBack/
/// expired) instead of reporting an unknown result; after eviction the duplicate reports an unknown
/// <see cref="Kahuna.Shared.KeyValue.KeyValueResponseType.Errored"/>, never a conflict Aborted.
///
/// <para><b>Why this class takes no lock:</b> retention runs on the commit hot path of every finalized
/// transaction, and the window sits at its cap in steady state, so every retain also evicts. Two earlier
/// implementations serialized retention behind one monitor: first with a full scan per insert to find the
/// oldest entry, then with an O(1) list whose size check still read <c>ConcurrentDictionary.Count</c> — which
/// acquires every bucket lock — inside the monitor, while the TTL prune walked the whole window under the same
/// monitor. Under a saturating commit load either one convoyed the committing threads (a leader trace showed
/// ~15 thread-pool threads parked on it at once, driving thread-pool injection). A monitor on this path is also
/// exposed to lock-holder preemption on an oversubscribed host, which turns even a short critical section into
/// a convoy. So no path through this class takes a monitor of its own; the only locks are the dictionary's
/// per-bucket locks, held for a single insert, update, or remove.</para>
///
/// <para><b>Structure:</b> <see cref="outcomes"/> maps a transaction id to its retained outcome, stamped with
/// a unique <see cref="Entry.Version"/> per retain. <see cref="order"/> is a FIFO of tickets
/// (<c>id, version</c>), one per retain, in retention order. A ticket is <i>current</i> while the map still
/// holds that id at that version, and <i>stale</i> once the id is re-retained (a newer ticket supersedes it),
/// evicted, or pruned. Eviction dequeues from the head and removes the entry only through the dictionary's
/// compare-and-remove on the ticket's version, so a stale ticket can never remove a newer retain of the same
/// id. The live size and the ticket count are atomic counters; nothing on the hot path counts the map.</para>
///
/// <para><b>Invariant:</b> every live entry's current ticket is either in <see cref="order"/> or held by the
/// one call that is about to enqueue it (a retain between its insert and its enqueue, or a caller putting back
/// a live ticket it dequeued). A ticket is dropped only after its entry is removed or superseded. So eviction
/// always finds the entries it must remove, and no entry is left without a way out of the window.</para>
///
/// <para><b>Size cap:</b> <c>max</c> bounds the window at quiescence exactly. Under concurrent retains it can be
/// exceeded transiently by at most the number of retains in flight — each caller inserts one entry and then
/// evicts back to the cap before it returns. That bounds memory by the caller count, not by the load, and the
/// transient extra entries are correct terminal outcomes, so a lookup that sees one replays a true answer.</para>
///
/// <para><b>Ordering:</b> eviction is FIFO on retention order. Re-retaining an id already in the window (a
/// duplicate finalize) enqueues a new ticket and leaves the old one stale, so the id is again the newest.</para>
///
/// <para><b>Stale tickets:</b> a re-retain or a TTL prune leaves a stale ticket behind. Eviction drops stale
/// tickets for free as it reaches them, and the prune drains them from the head, so they normally clear
/// quickly. If stale tickets still outnumber <c>max</c> — re-retains with the window under its cap and age
/// pruning disabled — a retain drains them from the head; a live ticket met on the way goes back to the tail.
/// That and a prune that loses the peeked head to a concurrent eviction (it puts back the live ticket it took
/// instead) are the only cases where eviction order departs from FIFO. The drain keeps the queue within about
/// twice the cap under any access pattern.</para>
/// </summary>
internal sealed class TerminalOutcomeWindow
{
    /// <summary>A finalized outcome held in the window, stamped with the HLC at retention time.</summary>
    internal readonly record struct RetainedOutcome(FinalizeOutcome Outcome, HLCTimestamp RetainedAt);

    /// <summary>
    /// A retained outcome tagged with the retain that wrote it. Equality is the version alone, so the
    /// dictionary's compare-and-update and compare-and-remove act on "this exact retain" without comparing
    /// the payload.
    /// </summary>
    private readonly struct Entry : IEquatable<Entry>
    {
        public readonly RetainedOutcome Retained;

        public readonly long Version;

        public Entry(RetainedOutcome retained, long version)
        {
            Retained = retained;
            Version = version;
        }

        public bool Equals(Entry other) => Version == other.Version;

        public override bool Equals(object? obj) => obj is Entry other && Equals(other);

        public override int GetHashCode() => Version.GetHashCode();
    }

    /// <summary>One retain of <see cref="TransactionId"/>, queued in retention order.</summary>
    private readonly record struct Ticket(HLCTimestamp TransactionId, long Version);

    private readonly ConcurrentDictionary<HLCTimestamp, Entry> outcomes = new();

    /// <summary>Retention order, oldest first; the head is the next size-cap eviction candidate.</summary>
    private readonly ConcurrentQueue<Ticket> order = new();

    /// <summary>Source of <see cref="Entry.Version"/>; unique per retain.</summary>
    private long versions;

    /// <summary>Entries in <see cref="outcomes"/>; changed only by a successful insert or remove.</summary>
    private int count;

    /// <summary>Tickets in <see cref="order"/>, current and stale; a put-back ticket is counted throughout.</summary>
    private int tickets;

    /// <summary>1 while a prune runs; a second concurrent prune returns at once instead of competing for the head.</summary>
    private int pruning;

    public bool IsEmpty => Volatile.Read(ref count) == 0;

    public int Count => Volatile.Read(ref count);

    /// <summary>Tickets queued, current and stale. For diagnostics and tests of the stale-ticket bound.</summary>
    internal int TicketCount => Volatile.Read(ref tickets);

    /// <summary>
    /// Lock-free lookup of a retained outcome; safe to call concurrently with any mutation. A concurrent
    /// evict/prune may make the entry disappear between two calls — callers already treat absence as
    /// "unknown", so that race is benign.
    /// </summary>
    public bool TryGet(HLCTimestamp transactionId, out RetainedOutcome retained)
    {
        if (outcomes.TryGetValue(transactionId, out Entry entry))
        {
            retained = entry.Retained;
            return true;
        }

        retained = default;
        return false;
    }

    /// <summary>
    /// Records a finalized outcome, then evicts from the front of the retention order until the window is back
    /// within <paramref name="max"/>. Takes no monitor and never counts or scans the map: the work is one map
    /// upsert, one enqueue, and one dequeue-and-remove per evicted entry. A non-positive <paramref name="max"/>
    /// must be filtered by the caller (the coordinator treats it as "retention disabled" and never calls in).
    /// </summary>
    public void Retain(HLCTimestamp transactionId, FinalizeOutcome outcome, HLCTimestamp now, int max)
    {
        Entry entry = new(new RetainedOutcome(outcome, now), Interlocked.Increment(ref versions));

        while (true)
        {
            if (outcomes.TryAdd(transactionId, entry))
            {
                Interlocked.Increment(ref count);
                break;
            }

            // Duplicate finalize inside the window: replace the entry only if it is still the one read, so a
            // concurrent evict or prune of it sends this retain back to the insert instead of reviving a
            // removed entry without counting it.
            if (outcomes.TryGetValue(transactionId, out Entry current) && outcomes.TryUpdate(transactionId, entry, current))
                break;
        }

        // Enqueued after the map write, so a dequeued ticket always finds its own retain already applied.
        order.Enqueue(new Ticket(transactionId, entry.Version));
        Interlocked.Increment(ref tickets);

        Evict(max);
    }

    /// <summary>
    /// Removes every outcome whose retention HLC is at least <paramref name="ttl"/> old. Called per reaper
    /// sweep, not per commit, and takes no lock, so it never delays a concurrent <see cref="Retain"/>. It first
    /// drains the head of the retention order — expired entries and stale tickets — up to the first live,
    /// fresh entry, which keeps the queue from collecting the tickets of pruned entries. It then walks the map
    /// for expired entries behind that head, because retention HLCs of racing finalizers can be slightly out of
    /// insertion order; the walk is a lock-free enumeration with a compare-and-remove per expired entry.
    /// </summary>
    public void PruneExpired(HLCTimestamp now, TimeSpan ttl)
    {
        // A second prune racing for the head could dequeue a fresh entry and have to put it back at the tail,
        // which reorders eviction. One prune at a time avoids that; the skipped call loses nothing, since the
        // running prune covers the same entries and the next sweep repeats it.
        if (Interlocked.Exchange(ref pruning, 1) == 1)
            return;

        try
        {
            while (order.TryPeek(out Ticket head))
            {
                if (IsLiveAndFresh(head, now, ttl))
                    break;

                if (!order.TryDequeue(out Ticket ticket))
                    break;

                // A concurrent eviction can take the peeked head first; settle whichever ticket this call took.
                if (IsLiveAndFresh(ticket, now, ttl))
                {
                    order.Enqueue(ticket);
                    break;
                }

                // Expired: remove it. Stale: the compare-and-remove misses and the ticket is simply dropped.
                if (TryRemoveCurrent(ticket))
                    Interlocked.Decrement(ref count);

                Interlocked.Decrement(ref tickets);
            }

            foreach (KeyValuePair<HLCTimestamp, Entry> pair in outcomes)
            {
                if (now - pair.Value.Retained.RetainedAt >= ttl && outcomes.TryRemove(pair))
                    Interlocked.Decrement(ref count);
            }
        }
        finally
        {
            Volatile.Write(ref pruning, 0);
        }
    }

    /// <summary>
    /// Evicts from the head while the window is over <paramref name="max"/>, and drains stale tickets while
    /// they outnumber <paramref name="max"/>. Under the cap, a stale head is dropped and a live head goes back
    /// to the tail, ending the call.
    /// </summary>
    private void Evict(int max)
    {
        while (true)
        {
            int live = Volatile.Read(ref count);

            if (live > max)
            {
                // Claim one eviction on the counter first. Two callers that both read max + 1 must not both
                // remove an entry, or the second removal would drop an outcome the window still has room for
                // (a duplicate finalize of it would then fall through to the durable-record consult early). The
                // compare-and-swap lets exactly one of them evict for that excess; the other re-reads the counter.
                if (Interlocked.CompareExchange(ref count, live - 1, live) != live)
                    continue;

                if (!EvictOldest())
                {
                    // Every remaining entry's ticket is in the hands of a caller that has yet to enqueue it;
                    // that caller evicts after its enqueue. Give the claim back.
                    Interlocked.Increment(ref count);
                    return;
                }

                continue;
            }

            if (Volatile.Read(ref tickets) - live <= max)
                return;

            if (!order.TryDequeue(out Ticket ticket))
                return;

            if (!IsCurrent(ticket, out _))
            {
                Interlocked.Decrement(ref tickets);
                continue;
            }

            // Under the cap with a live head: that entry must stay. Put its ticket back at the tail, and keep
            // evicting only if concurrent retains pushed the window over the cap meanwhile.
            order.Enqueue(ticket);

            if (Volatile.Read(ref count) <= max)
                return;
        }
    }

    /// <summary>
    /// Dequeues from the head until one current entry is removed, dropping the stale tickets on the way. The
    /// caller already took the removed entry off <see cref="count"/>. Returns false if the queue ran out first.
    /// </summary>
    private bool EvictOldest()
    {
        while (order.TryDequeue(out Ticket ticket))
        {
            Interlocked.Decrement(ref tickets);

            // A miss means the ticket was stale: its id was re-retained, evicted, or pruned.
            if (TryRemoveCurrent(ticket))
                return true;
        }

        return false;
    }

    private bool IsCurrent(Ticket ticket, out Entry entry)
    {
        return outcomes.TryGetValue(ticket.TransactionId, out entry) && entry.Version == ticket.Version;
    }

    private bool IsLiveAndFresh(Ticket ticket, HLCTimestamp now, TimeSpan ttl)
    {
        return IsCurrent(ticket, out Entry entry) && now - entry.Retained.RetainedAt < ttl;
    }

    /// <summary>
    /// Removes the ticket's entry only if the map still holds that id at that version, so a stale ticket never
    /// removes a newer retain of the same id. The caller owns the <see cref="count"/> adjustment.
    /// </summary>
    private bool TryRemoveCurrent(Ticket ticket)
    {
        return outcomes.TryRemove(new KeyValuePair<HLCTimestamp, Entry>(ticket.TransactionId, new Entry(default, ticket.Version)));
    }
}
