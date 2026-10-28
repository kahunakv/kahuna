using Kahuna.Shared.KeyValue;
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

/// <summary>
/// Seeds one key's committed head into the ledger slice of the partition whose log carries the command: the
/// range-move hand-off of the committed-head memory. A split or merge moves a key's rows to another partition, but
/// the head its last transactional commit left behind lives in the SOURCE partition's slice, fed by the source
/// log. Without this seed the destination slice starts empty for the moved keys, and the staged-base fence on the
/// destination falls back to each node's copy of the source slice — a copy the destination's leader may hold
/// behind its replicas (apply lag), or not hold at all (it never hosted the source, or purged it after a
/// merge). A leader that admits a prepare its replicas refuse is exactly the shape that lets a stale-base commit
/// be acknowledged. Seeded through the destination's log, the head is replicated state ordered ahead of every
/// prepare the destination later judges, so leader and replicas reach the same verdict.
///
/// <para>Touches no intent: it applies as a monotonic head record (an older revision never overwrites a newer
/// one), so a re-driven hand-off and a restart replay fold idempotently. Carries no transaction identity.</para>
/// </summary>
internal sealed record SeedCommittedHeadCommand(
    string Key,
    long Revision,
    KeyValueState State,
    HLCTimestamp CommittedAt) : PreparedIntentCommand;
