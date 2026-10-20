using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// A transition submitted to the prepared-intent state machine. Applied deterministically on leader, follower,
/// WAL replay, and state-transfer replay against the current intent (if any) at the target key.
/// </summary>
internal abstract record PreparedIntentCommand;

/// <summary>Installs a prepared intent for its key. Idempotent for an exact-duplicate mutation; a different live
/// transaction on the same key conflicts and is never overwritten.</summary>
/// <param name="AdmissionLagMs">The lag bound of the commit admission window the prepare was proposed under; zero
/// when the window was off or when the prepare copies an existing intent (a range move's handoff). A positive bound
/// makes every replica admit the prepare only if the intent's commit timestamp falls inside the window around the
/// time of the log entry that carries it (see <see cref="CommitAdmission"/>); a refused prepare installs nothing.</param>
internal sealed record PrepareIntentCommand(PreparedIntent Intent, long AdmissionLagMs = 0) : PreparedIntentCommand;

/// <summary>Records the transaction's terminal decision on its intent: commit (value will materialize) or abort
/// (value is discarded). Idempotent; never flips an already-resolved intent to the other outcome.</summary>
/// <param name="MaterializeOnResolve">On a commit, the apply of this resolve also installs the intent's committed
/// value as the key's visible revision, on every replica, from that replica's own copy of the intent — so no
/// separate materialization record is needed. Only written once every node applies it: an older node skips the
/// field and resolves without installing, which loses the value on that node.</param>
/// <param name="CommitTimestamp">The transaction's commit timestamp, carried only with
/// <paramref name="MaterializeOnResolve"/> so a reader of the log alone (the backup's applied barrier) can place
/// the rows this resolve installs without the intent. <see cref="HLCTimestamp.Zero"/> otherwise.</param>
internal sealed record ResolveIntentCommand(
    HLCTimestamp TransactionId,
    long Epoch,
    string Key,
    bool Commit,
    bool MaterializeOnResolve = false,
    HLCTimestamp CommitTimestamp = default) : PreparedIntentCommand;

/// <summary>Removes a resolved intent after its outcome has been materialized/discarded (garbage collection). A
/// pending intent may not be removed.</summary>
internal sealed record RemoveIntentCommand(
    HLCTimestamp TransactionId,
    long Epoch,
    string Key) : PreparedIntentCommand;
