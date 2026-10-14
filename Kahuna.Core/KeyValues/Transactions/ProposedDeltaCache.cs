
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Kahuna.Utils;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Remembers the decoded form of a locally serialized replication record, keyed by the exact byte
/// array that was handed to Raft. The node that proposes a record serialized it from live objects
/// moments before Raft hands the same array back to the local apply path, so that apply — and, when an
/// in-process transport shares the array, the applies of the other nodes hosted in this process — can
/// reuse the decoded form instead of parsing and rebuilding it. The payload is whatever the producer
/// had in hand: a command batch for a transaction delta, the proposal itself for a direct write.
///
/// <para>Each entry carries a take budget equal to the live in-process node count at registration
/// (<see cref="InProcessNodeCensus"/>). With the redundant-apply ledger each node runs one local apply
/// per committed entry, so the budget bounds how long an entry can pin its payload. The entry is
/// removed once the budget is spent. A reader that misses — a node in another process, WAL replay on
/// restart, state transfer, all of which see freshly materialized arrays — decodes the bytes instead,
/// so correctness never depends on a hit. The table is weak on the byte array, so an entry that some
/// budgeted taker never claims (for example a partition a co-hosted node does not replicate) vanishes
/// with the proposal bytes. Reusing the producer's instances across nodes is safe only because the
/// payload and everything it references are immutable.</para>
/// </summary>
internal sealed class ProposedDeltaCache<TPayload> where TPayload : class
{
    private sealed class Entry
    {
        internal readonly TPayload Payload;
        internal int RemainingTakes;

        internal Entry(TPayload payload, int takeBudget)
        {
            Payload = payload;
            RemainingTakes = takeBudget;
        }
    }

    private readonly ConditionalWeakTable<byte[], Entry> table = new();

    /// <summary>Registers the decoded form of freshly produced record bytes, with a take budget of the
    /// current live node count. The floor of one preserves the single-take behavior for callers that
    /// serialize outside any node's lifetime (tests, tools).</summary>
    internal void Register(byte[] data, TPayload payload) =>
        Register(data, payload, Math.Max(1, InProcessNodeCensus.Count));

    /// <summary>Budget-explicit form for tests that exercise the take accounting directly.</summary>
    internal void Register(byte[] data, TPayload payload, int takeBudget) =>
        table.AddOrUpdate(data, new Entry(payload, takeBudget));

    /// <summary>Takes one reuse of the payload registered for <paramref name="data"/>. The entry is
    /// removed when its budget is spent. A taker that races the removal can briefly overdraw the
    /// budget; the extra winner reuses the same immutable payload, which is harmless.</summary>
    internal bool TryTake(byte[] data, [NotNullWhen(true)] out TPayload? payload)
    {
        if (!table.TryGetValue(data, out Entry? entry))
        {
            payload = null;
            return false;
        }

        if (Interlocked.Decrement(ref entry.RemainingTakes) <= 0)
            table.Remove(data);

        payload = entry.Payload;
        return true;
    }
}
