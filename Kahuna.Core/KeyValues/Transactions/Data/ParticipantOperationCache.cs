
using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Participant-node-local cache of transaction operations whose actor has already executed but whose
/// completion has not yet been acknowledged by the coordinator. It lets a retry re-drive the coordinator
/// completion — folding the confirmed effect exactly once — without reapplying the participant operation,
/// closing the window where a lost or cancelled completion would otherwise strand a mutation with a
/// coordinator record stuck pending forever.
///
/// An entry stays pinned while its outcome is in doubt: it is removed only when the completion is
/// acknowledged, or when the coordinator reports the operation as already completed (the coordinator is
/// then authoritative). To stay bounded when many operations are simultaneously in doubt — or when their
/// sessions are abandoned without a completing retry — the cache evicts the oldest entry once a hard cap
/// is exceeded. Eviction never happens while there is room, so an entry survives long enough for a normal
/// retry to recover it.
///
/// <para>The detached retry of a completion holds its <see cref="Entry"/> and stops once the entry is
/// <see cref="Entry.IsSettled"/>: a removal or a newer result for the same operation settles it. An eviction
/// does not, because the retry holds its own reference to the payload and is the only completer left once a
/// caller's retry can no longer find the entry.</para>
/// </summary>
internal sealed class ParticipantOperationCache
{
    /// <summary>Upper bound on simultaneously in-doubt operation results before the oldest is evicted.</summary>
    internal const int MaxEntries = 8192;

    private readonly object gate = new();
    private readonly Dictionary<(HLCTimestamp, TransactionOperationId), Entry> entries = new();

    /// <summary>Monotonic insertion counter; the smallest sequence is the oldest entry for eviction.</summary>
    private long sequence;

    /// <summary>One kept result: the response for a recovering retry and the payload that re-drives the fold.</summary>
    internal sealed class Entry
    {
        public required object Response { get; init; }
        public required OperationCompletionPayload Payload { get; init; }
        public long Sequence { get; init; }

        private int settled;

        /// <summary>
        /// True once nothing more must be sent for this result: the completion was acknowledged, the coordinator
        /// answered that the operation completed, or a newer result of the same operation replaced it.
        /// </summary>
        internal bool IsSettled => Volatile.Read(ref settled) != 0;

        internal void Settle() => Volatile.Write(ref settled, 1);
    }

    /// <summary>
    /// Records an in-doubt operation result: the response to hand a recovering retry and the completion
    /// payload to re-drive the fold. Replaces and settles any prior entry for the same operation id, and returns
    /// the new entry.
    /// </summary>
    internal Entry Store(HLCTimestamp transactionId, TransactionOperationId operationId, object response, OperationCompletionPayload payload)
    {
        Entry entry;

        lock (gate)
        {
            entry = new() { Response = response, Payload = payload, Sequence = sequence++ };

            if (entries.Remove((transactionId, operationId), out Entry? replaced))
                replaced.Settle();

            entries[(transactionId, operationId)] = entry;

            if (entries.Count > MaxEntries)
                EvictOldestLocked();
        }

        return entry;
    }

    /// <summary>Returns the cached response and completion payload for an in-doubt operation, if present.</summary>
    internal bool TryGet(HLCTimestamp transactionId, TransactionOperationId operationId, out object? response, out OperationCompletionPayload? payload)
    {
        lock (gate)
        {
            if (entries.TryGetValue((transactionId, operationId), out Entry? entry))
            {
                response = entry.Response;
                payload = entry.Payload;
                return true;
            }
        }

        response = null;
        payload = null;
        return false;
    }

    /// <summary>Drops and settles an operation once its completion is acknowledged or the coordinator owns it.</summary>
    internal void Remove(HLCTimestamp transactionId, TransactionOperationId operationId)
    {
        lock (gate)
        {
            if (entries.Remove((transactionId, operationId), out Entry? entry))
                entry.Settle();
        }
    }

    /// <summary>
    /// Settles <paramref name="entry"/> and drops it if the cache still holds it for its operation. A newer entry
    /// for the same operation stays.
    /// </summary>
    internal void Remove(HLCTimestamp transactionId, TransactionOperationId operationId, Entry entry)
    {
        entry.Settle();

        lock (gate)
        {
            if (entries.TryGetValue((transactionId, operationId), out Entry? current) && ReferenceEquals(current, entry))
                entries.Remove((transactionId, operationId));
        }
    }

    /// <summary>The number of kept results. Test access.</summary>
    internal int Count
    {
        get { lock (gate) return entries.Count; }
    }

    private void EvictOldestLocked()
    {
        (HLCTimestamp, TransactionOperationId) oldestKey = default;
        long oldest = long.MaxValue;
        bool found = false;

        foreach (KeyValuePair<(HLCTimestamp, TransactionOperationId), Entry> pair in entries)
        {
            if (pair.Value.Sequence < oldest)
            {
                oldest = pair.Value.Sequence;
                oldestKey = pair.Key;
                found = true;
            }
        }

        // An evicted entry is not settled: its detached retry, if one runs, keeps driving the completion.
        if (found)
            entries.Remove(oldestKey);
    }
}
