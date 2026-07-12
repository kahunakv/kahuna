
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
/// </summary>
internal sealed class ParticipantOperationCache
{
    /// <summary>Upper bound on simultaneously in-doubt operation results before the oldest is evicted.</summary>
    private const int MaxEntries = 8192;

    private readonly object gate = new();
    private readonly Dictionary<(HLCTimestamp, TransactionOperationId), Entry> entries = new();

    /// <summary>Monotonic insertion counter; the smallest sequence is the oldest entry for eviction.</summary>
    private long sequence;

    private sealed class Entry
    {
        public required object Response { get; init; }
        public required OperationCompletionPayload Payload { get; init; }
        public long Sequence { get; init; }
    }

    /// <summary>
    /// Records an in-doubt operation result: the response to hand a recovering retry and the completion
    /// payload to re-drive the fold. Overwrites any prior entry for the same operation id.
    /// </summary>
    internal void Store(HLCTimestamp transactionId, TransactionOperationId operationId, object response, OperationCompletionPayload payload)
    {
        lock (gate)
        {
            entries[(transactionId, operationId)] = new() { Response = response, Payload = payload, Sequence = sequence++ };

            if (entries.Count > MaxEntries)
                EvictOldestLocked();
        }
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

    /// <summary>Drops an operation once its completion is acknowledged or the coordinator owns it.</summary>
    internal void Remove(HLCTimestamp transactionId, TransactionOperationId operationId)
    {
        lock (gate)
            entries.Remove((transactionId, operationId));
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

        if (found)
            entries.Remove(oldestKey);
    }
}
