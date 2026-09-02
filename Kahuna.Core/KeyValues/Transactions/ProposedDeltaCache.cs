
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Kahuna.Utils;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Remembers the decoded commands of a locally serialized replication delta, keyed by the exact byte
/// array that was handed to Raft. The node that proposes a delta serialized it from live command objects
/// moments before Raft hands the same array back to the local apply path, so that apply — and, when an
/// in-process transport shares the array, the applies of the other nodes hosted in this process — can
/// reuse the decoded form instead of parsing and rebuilding every command.
///
/// <para>Each entry carries a take budget equal to the live in-process node count at registration
/// (<see cref="InProcessNodeCensus"/>). With the redundant-apply ledger each node runs one local apply
/// per committed entry, so the budget bounds how long an entry can pin its commands. The entry is
/// removed once the budget is spent. A reader that misses — a node in another process, WAL replay on
/// restart, state transfer, all of which see freshly materialized arrays — decodes the bytes instead,
/// so correctness never depends on a hit. The table is weak on the byte array, so an entry that some
/// budgeted taker never claims (for example a partition a co-hosted node does not replicate) vanishes
/// with the proposal bytes. Reusing the producer's instances across nodes is safe only because the
/// commands and everything they reference are immutable.</para>
/// </summary>
internal sealed class ProposedDeltaCache<TCommand> where TCommand : class
{
    private sealed class Entry
    {
        internal readonly TCommand[] Commands;
        internal int RemainingTakes;

        internal Entry(TCommand[] commands, int takeBudget)
        {
            Commands = commands;
            RemainingTakes = takeBudget;
        }
    }

    private readonly ConditionalWeakTable<byte[], Entry> table = new();

    /// <summary>Registers the decoded form of freshly produced delta bytes, with a take budget of the
    /// current live node count. The floor of one preserves the single-take behavior for callers that
    /// serialize outside any node's lifetime (tests, tools).</summary>
    internal void Register(byte[] data, TCommand[] commands) =>
        Register(data, commands, Math.Max(1, InProcessNodeCensus.Count));

    /// <summary>Budget-explicit form for tests that exercise the take accounting directly.</summary>
    internal void Register(byte[] data, TCommand[] commands, int takeBudget) =>
        table.AddOrUpdate(data, new Entry(commands, takeBudget));

    /// <summary>Takes one reuse of the commands registered for <paramref name="data"/>. The entry is
    /// removed when its budget is spent. A taker that races the removal can briefly overdraw the
    /// budget; the extra winner reuses the same immutable commands, which is harmless.</summary>
    internal bool TryTake(byte[] data, [NotNullWhen(true)] out TCommand[]? commands)
    {
        if (!table.TryGetValue(data, out Entry? entry))
        {
            commands = null;
            return false;
        }

        if (Interlocked.Decrement(ref entry.RemainingTakes) <= 0)
            table.Remove(data);

        commands = entry.Commands;
        return true;
    }
}
