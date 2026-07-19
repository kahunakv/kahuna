using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// The terminal outcome of a transaction under the durable-intent 2PC model. A record starts
/// <see cref="Undecided"/> at initialization and moves exactly once to a terminal <see cref="Commit"/> or
/// <see cref="Abort"/>; a terminal decision is immutable.
/// </summary>
internal enum TransactionDecision
{
    Undecided = 0,
    Commit = 1,
    Abort = 2
}

/// <summary>
/// Why a transaction aborted. Storage resolution is identical for every class (every abort discards prepared
/// values); the class preserves result truth at the API boundary: <see cref="Conflict"/> maps to
/// <c>Aborted</c>, the rest map to <c>MustRetry</c> at the API boundary.
/// </summary>
internal enum TransactionAbortClass
{
    None = 0,
    Conflict = 1,
    RetryableFailure = 2,
    ExplicitRollback = 3,
    PresumedAbort = 4
}

/// <summary>One manifest entry: a logical modified key and its durability. The key is the routing authority.</summary>
internal readonly record struct TransactionParticipantRef(string Key, KeyValueDurability Durability);

/// <summary>
/// The canonical, immutable-once-terminal transaction record of the durable-intent 2PC model, keyed by
/// <see cref="TransactionId"/> and <see cref="Epoch"/> and routed by <see cref="RecordAnchorKey"/>. This is the
/// single source of truth for "did transaction T commit or abort?" — it replaces the manual-ticket commit/abort
/// mechanism. Instances are produced only by <see cref="TransactionRecordStateMachine.Apply"/>; the immutable
/// fields are frozen at initialization (or, for a manifestless abort tombstone, at absence→abort) and never
/// change, and the decision transitions exactly once.
/// </summary>
internal sealed record TransactionRecord(
    HLCTimestamp TransactionId,
    long Epoch,
    string CoordinatorKey,
    string RecordAnchorKey,
    HLCTimestamp CommitTimestamp,
    HLCTimestamp DecisionDeadline,
    long ManifestHash,
    IReadOnlyList<TransactionParticipantRef> Participants,
    // False for an abort tombstone created from absence (the remote-prepare / anchor-missing crash window): it
    // carries enough identity to reject late commits and let orphan intents resolve, but not the full manifest.
    bool ManifestPresent,
    TransactionDecision Decision,
    TransactionAbortClass AbortClass,
    // The operation id that won the terminal transition; HLCTimestamp.Zero while Undecided. A caller compares its
    // own op id against this to learn whether its commit/abort was the winner (the containing proposal committing
    // is not proof its op won).
    HLCTimestamp WinningOpId,
    HLCTimestamp CreatedAt,
    HLCTimestamp DecidedAt)
{
    public bool IsTerminal => Decision is TransactionDecision.Commit or TransactionDecision.Abort;
}

/// <summary>
/// Deterministic hash of a transaction's immutable manifest fields, used as the identity a commit/abort
/// transition must present to bind to the frozen record. It must be stable across processes, runs, and
/// participant input order (apply/replay never consults process-local hashing), so it is a fixed FNV-1a fold over
/// the ordinally-sorted participant set plus the frozen scalar identity — never <see cref="object.GetHashCode"/>.
/// </summary>
internal static class TransactionManifest
{
    public static long ComputeHash(
        HLCTimestamp transactionId,
        long epoch,
        string anchorKey,
        HLCTimestamp commitTimestamp,
        IReadOnlyList<TransactionParticipantRef> participants)
    {
        const ulong offset = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;

        ulong h = offset;
        h = FoldString(h, transactionId.ToString(), prime);
        h = FoldLong(h, epoch, prime);
        h = FoldString(h, anchorKey, prime);
        h = FoldString(h, commitTimestamp.ToString(), prime);

        // Order-independent: sort participants by ordinal key (then durability) so the hash does not depend on
        // the order the caller enumerated the modified keys.
        List<TransactionParticipantRef> ordered = new(participants);
        ordered.Sort(static (a, b) =>
        {
            int byKey = string.CompareOrdinal(a.Key, b.Key);
            return byKey != 0 ? byKey : ((int)a.Durability).CompareTo((int)b.Durability);
        });

        foreach (TransactionParticipantRef p in ordered)
        {
            h = FoldString(h, p.Key, prime);
            h = FoldLong(h, (long)p.Durability, prime);
        }

        return unchecked((long)h);
    }

    private static ulong FoldString(ulong h, string value, ulong prime)
    {
        foreach (char c in value)
        {
            h ^= c;
            h *= prime;
        }

        // Length-delimit so ["ab","c"] and ["a","bc"] do not collide.
        h ^= (ulong)value.Length;
        h *= prime;
        return h;
    }

    private static ulong FoldLong(ulong h, long value, ulong prime)
    {
        ulong v = unchecked((ulong)value);
        for (int i = 0; i < 8; i++)
        {
            h ^= v & 0xFF;
            h *= prime;
            v >>= 8;
        }

        return h;
    }
}
