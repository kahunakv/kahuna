using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// A transition submitted to the prepared-intent state machine. Applied deterministically on leader, follower,
/// WAL replay, and state-transfer replay against the current intent (if any) at the target key.
/// </summary>
internal abstract record PreparedIntentCommand;

/// <summary>Installs a prepared intent for its key. Idempotent for an exact-duplicate mutation; a different live
/// transaction on the same key conflicts and is never overwritten.</summary>
internal sealed record PrepareIntentCommand(PreparedIntent Intent) : PreparedIntentCommand;

/// <summary>Records the transaction's terminal decision on its intent: commit (value will materialize) or abort
/// (value is discarded). Idempotent; never flips an already-resolved intent to the other outcome.</summary>
internal sealed record ResolveIntentCommand(
    HLCTimestamp TransactionId,
    long Epoch,
    string Key,
    bool Commit) : PreparedIntentCommand;

/// <summary>Removes a resolved intent after its outcome has been materialized/discarded (garbage collection). A
/// pending intent may not be removed.</summary>
internal sealed record RemoveIntentCommand(
    HLCTimestamp TransactionId,
    long Epoch,
    string Key) : PreparedIntentCommand;
