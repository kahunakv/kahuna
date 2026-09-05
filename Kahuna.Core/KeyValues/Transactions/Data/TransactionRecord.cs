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
/// values); the class records why the decision went the way it did, for diagnostics and metrics.
///
/// <para>It does not change the outcome reported to the caller: a durable abort is terminal whatever its class —
/// the record's decision transitions exactly once and the state machine rejects a later commit — so every class
/// maps to <c>Aborted</c> at the API boundary. Reporting a non-conflict abort as <c>MustRetry</c> would promise a
/// retry that can never succeed, because each retry re-reads the same aborted record. The caller's recourse for
/// any class is the same: start a new transaction.</para>
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

    /// <summary>
    /// Operation ids of one-phase bundled commits that the bundled commit gate rejected while this record was
    /// Undecided, in the order they were rejected. Written by the store at the rejection's own apply position
    /// and persisted with the record, so a replay of the same bundled commit — WAL restore, or the entries
    /// after an installed snapshot's boundary — is rejected again by lookup instead of being re-judged against
    /// a ledger that may since have moved past what the live apply saw. Null while none was rejected.
    /// </summary>
    public IReadOnlyList<HLCTimestamp>? RejectedBundledCommitOpIds { get; init; }

    /// <summary>Whether a bundled commit with <paramref name="opId"/> was already rejected against this record.</summary>
    public bool WasBundledCommitRejected(HLCTimestamp opId)
    {
        if (RejectedBundledCommitOpIds is null)
            return false;

        foreach (HLCTimestamp rejected in RejectedBundledCommitOpIds)
            if (rejected == opId)
                return true;

        return false;
    }
}

/// <summary>
/// Deterministic hash of a transaction's immutable manifest fields, used as the identity a commit/abort
/// transition must present to bind to the frozen record. It must be stable across processes, runs, and
/// participant input order (apply/replay never consults process-local hashing), so it is a fixed FNV-1a fold over
/// the ordinally-sorted participant set plus the frozen scalar identity — never <see cref="object.GetHashCode"/>.
/// HLC timestamps are folded component-wise (N, L, C) rather than through their string form, so computing the
/// hash allocates nothing. Changing the fold recipe is safe only because the hash is computed exactly once per
/// transaction — at freeze in <see cref="DurableFinalizeInputBuilder"/> — and carried as an opaque value
/// thereafter; no apply, replay, or recovery path ever recomputes it from persisted state to compare.
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
        h = FoldHlc(h, transactionId, prime);
        h = FoldLong(h, epoch, prime);
        h = FoldString(h, anchorKey, prime);
        h = FoldHlc(h, commitTimestamp, prime);

        // Order-independent: sort participants by ordinal key (then durability) so the hash does not depend on
        // the order the caller enumerated the modified keys. Zero or one participant has exactly one order, so
        // those fold directly without copying the list; the multi-key fold is unchanged.
        if (participants.Count > 1)
        {
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
        }
        else if (participants.Count == 1)
        {
            TransactionParticipantRef p = participants[0];
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

    /// <summary>Folds an HLC timestamp component-wise (N, L, C) — the allocation-free equivalent of folding a
    /// canonical string form; all three components participate because N is part of HLC identity.</summary>
    private static ulong FoldHlc(ulong h, HLCTimestamp value, ulong prime)
    {
        h = FoldLong(h, value.N, prime);
        h = FoldLong(h, value.L, prime);
        h = FoldLong(h, value.C, prime);
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
