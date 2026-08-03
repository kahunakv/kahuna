
using System.Collections.Concurrent;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Bounded idempotency window of finalized transaction outcomes. After a session leaves the coordinator's
/// live set, a duplicate commit/rollback for it replays the retained terminal answer (Committed/RolledBack/
/// expired) instead of reporting an unknown result; after eviction the duplicate reports an unknown
/// <see cref="Kahuna.Shared.KeyValue.KeyValueResponseType.Errored"/>, never a conflict Aborted.
///
/// <para><b>Why this class exists:</b> retention runs on the commit hot path of every finalized
/// transaction. An earlier in-coordinator implementation enforced the size cap with a full scan of the
/// window per insert to find the oldest entry; under a saturating commit load the scan serialized every
/// committing thread behind one monitor and profiled as the single largest contention point in the server.
/// This class keeps an explicit insertion-order list beside the map, so the eviction victim is
/// <c>order.First</c> and both insert and evict are O(1). The lock is held only for those pointer
/// updates.</para>
///
/// <para><b>Concurrency model:</b> lookups (<see cref="TryGet"/>) are lock-free reads of a
/// <see cref="ConcurrentDictionary{TKey,TValue}"/>. All mutations — retain, size eviction, TTL prune —
/// serialize on one private lock, which keeps the size cap a <b>strict</b> upper bound (no two writers can
/// insert past it or race for the same victim) and keeps <c>order</c>/<c>nodes</c>/<c>outcomes</c>
/// mutually consistent.</para>
///
/// <para><b>Ordering:</b> eviction is FIFO on retention order. Re-retaining an id its slot already holds
/// (a duplicate finalize inside the window) moves it to the back, so it is again the newest — the same
/// policy the previous oldest-by-retention-HLC scan produced, since the retention HLC is monotonic under
/// the lock.</para>
/// </summary>
internal sealed class TerminalOutcomeWindow
{
    /// <summary>A finalized outcome held in the window, stamped with the HLC at retention time.</summary>
    internal readonly record struct RetainedOutcome(FinalizeOutcome Outcome, HLCTimestamp RetainedAt);

    private readonly ConcurrentDictionary<HLCTimestamp, RetainedOutcome> outcomes = new();

    /// <summary>Retention order, oldest first; the head is always the next size-cap eviction victim.</summary>
    private readonly LinkedList<HLCTimestamp> order = new();

    /// <summary>Locates an id's <see cref="order"/> node so a re-retain can move it to the back in O(1).</summary>
    private readonly Dictionary<HLCTimestamp, LinkedListNode<HLCTimestamp>> nodes = new();

    private readonly object sync = new();

    public bool IsEmpty => outcomes.IsEmpty;

    public int Count => outcomes.Count;

    /// <summary>
    /// Lock-free lookup of a retained outcome; safe to call concurrently with any mutation. A concurrent
    /// evict/prune may make the entry disappear between two calls — callers already treat absence as
    /// "unknown", so that race is benign.
    /// </summary>
    public bool TryGet(HLCTimestamp transactionId, out RetainedOutcome retained)
    {
        return outcomes.TryGetValue(transactionId, out retained);
    }

    /// <summary>
    /// Records a finalized outcome, evicting from the front of the retention order until the window is back
    /// within <paramref name="max"/>. O(1): no scan is ever taken on this path — see the class summary for
    /// why that matters. A non-positive <paramref name="max"/> must be filtered by the caller (the
    /// coordinator treats it as "retention disabled" and never calls in).
    /// </summary>
    public void Retain(HLCTimestamp transactionId, FinalizeOutcome outcome, HLCTimestamp now, int max)
    {
        lock (sync)
        {
            outcomes[transactionId] = new RetainedOutcome(outcome, now);

            if (nodes.TryGetValue(transactionId, out LinkedListNode<HLCTimestamp>? node))
            {
                // Duplicate finalize inside the window: the id is retained anew, so it becomes the
                // newest entry again rather than keeping its old eviction slot.
                order.Remove(node);
                order.AddLast(node);
            }
            else
                nodes[transactionId] = order.AddLast(transactionId);

            while (outcomes.Count > max && order.First is { } oldest)
            {
                order.RemoveFirst();
                nodes.Remove(oldest.Value);
                outcomes.TryRemove(oldest.Value, out _);
            }
        }
    }

    /// <summary>
    /// Removes every outcome whose retention HLC is at least <paramref name="ttl"/> old. Called per reaper
    /// sweep (not per commit), so the full walk is acceptable; it deliberately walks all entries rather than
    /// stopping at the first fresh one, because retention HLCs of racing finalizers can be slightly out of
    /// insertion order.
    /// </summary>
    public void PruneExpired(HLCTimestamp now, TimeSpan ttl)
    {
        lock (sync)
        {
            LinkedListNode<HLCTimestamp>? node = order.First;

            while (node is not null)
            {
                LinkedListNode<HLCTimestamp>? next = node.Next;

                if (outcomes.TryGetValue(node.Value, out RetainedOutcome retained) && now - retained.RetainedAt >= ttl)
                {
                    outcomes.TryRemove(node.Value, out _);
                    nodes.Remove(node.Value);
                    order.Remove(node);
                }

                node = next;
            }
        }
    }
}
