using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>What a fresh transactional MVCC snapshot of a key should capture.</summary>
internal enum SnapshotDecision
{
    /// <summary>No superseding intent: snapshot the resident base entry (the caller's ordinary path).</summary>
    UseBase,

    /// <summary>A committed-but-unsettled foreign intent supersedes the base: snapshot its committed value.</summary>
    UseIntent,

    /// <summary>An undecided foreign intent covers the key: the snapshot must wait rather than bind a stale base.</summary>
    Retry
}

/// <summary>
/// Chooses the committed state a first transactional read/scan should capture into its MVCC snapshot for a key.
/// <para>
/// Under deferred settlement a committed value lingers as a prepared intent until it settles into base MVCC, so the
/// resident base entry can still be empty (or an older revision) while the value is already committed. A snapshot
/// built from that stale base binds the reading transaction to a view that never happened: every later read of the
/// key in that transaction then returns <c>DoesNotExist</c> (the snapshot is <c>Undefined</c>) or an OCC
/// <c>Aborted</c> (the base later materializes to a higher revision than the snapshot).
/// </para>
/// <para>
/// Every transactional latest read — point get, exists, and the scans — records its snapshot through here, so the
/// first read of a key always leaves an MVCC pin behind. The pin is what the early write-conflict check compares
/// against: a transaction whose first read were answered straight from the intent, with no pin, would re-read a
/// newer committed revision on its next read or base its write on it, and its lost race would surface only at
/// commit through read-set validation.
/// </para>
/// </summary>
internal static class DurableSnapshotSource
{
    /// <summary>
    /// Resolves what a fresh latest-read MVCC snapshot of <paramref name="key"/> must capture. Returns
    /// <see cref="SnapshotDecision.UseIntent"/> with a ready-built snapshot when a committed foreign intent
    /// supersedes the base, <see cref="SnapshotDecision.Retry"/> when an undecided intent means the read must wait,
    /// and <see cref="SnapshotDecision.UseBase"/> for the ordinary path: no intent, an aborted one, or a
    /// <paramref name="resident"/> head strictly newer than the intent (a later committed write superseded it).
    /// Only meaningful for a latest read; an as-of snapshot read is served through the revision-history path, never
    /// this OCC snapshot.
    /// </summary>
    public static SnapshotDecision Resolve(
        KeyValueContext context, string key, HLCTimestamp readerTransactionId, KeyValueEntry? resident,
        HLCTimestamp currentTime, out KeyValueMvccEntry intentSnapshot, ForeignDecisionHint hint = default)
    {
        intentSnapshot = null!;

        if (context.PreparedIntentStore?.Get(key) is not { } foreign || foreign.TransactionId == readerTransactionId)
            return SnapshotDecision.UseBase;

        // A resident head strictly newer than the intent has moved past it: snapshotting the intent would pin an
        // older revision than the head and abort the reader on its first read. Strictly newer only, as for snapshot
        // reads (ResidentHeadSupersedesIntent): an extend reuses the base revision, so at an equal revision the
        // intent is the authoritative copy, and pinning it pins the head's own revision.
        if (resident is not null && resident.Revision > foreign.Revision)
            return SnapshotDecision.UseBase;

        switch (DurableReadVisibility.Resolve(context, foreign, HLCTimestamp.Zero, hint))
        {
            case ReadVisibilityAction.UseIntentValue:
                // A committed delete or an expired committed value snapshots as absent (State carries it); the
                // caller's own Undefined/Deleted/expiry check then treats the row as not visible.
                bool dead = foreign.State == KeyValueState.Deleted || PreparedIntentVisibility.IsExpired(foreign, currentTime);
                intentSnapshot = new KeyValueMvccEntry
                {
                    Value = dead ? null : foreign.Value,
                    Revision = foreign.Revision,
                    Expires = foreign.Expires,
                    LastUsed = currentTime,
                    LastModified = foreign.CommitTimestamp,
                    State = dead ? KeyValueState.Deleted : foreign.State
                };
                return SnapshotDecision.UseIntent;

            case ReadVisibilityAction.Retry:
                return SnapshotDecision.Retry;

            default:
                return SnapshotDecision.UseBase;
        }
    }

    /// <summary>
    /// True when <paramref name="readerTransactionId"/> already holds its own MVCC version of <paramref name="key"/> —
    /// i.e. the scanning transaction has read or written (set/tombstoned) the key within this transaction. The
    /// scan-merge uses this to keep that own version authoritative over a lingering committed foreign intent for the
    /// same key (read-your-own-write / snapshot consistency), instead of letting the foreign intent override, exclude,
    /// or resurrect it. The MVCC entry — not the write intent — is the durable signal: a pending write intent is
    /// short-lived (its lease can lapse and it is cleared on prepare/abort) while the transaction's MVCC version
    /// persists for the transaction's lifetime, so gating on the write intent misses a still-open transaction whose
    /// intent has already been cleared.
    /// </summary>
    public static bool ReaderHasOwnVersion(KeyValueContext context, string key, HLCTimestamp readerTransactionId)
        => readerTransactionId != HLCTimestamp.Zero
           && context.Store.TryGetValue(key, out KeyValueEntry? entry)
           && entry.MvccEntries is { } mvcc
           && mvcc.ContainsKey(readerTransactionId);

    /// <summary>
    /// True when a committed head of <paramref name="intent"/>'s key strictly newer than the intent is known, so the
    /// scan-merge must not serve the intent: the resident entry, or a row the scan evaluated and left out of its page
    /// (<paramref name="excludedHeads"/>, a newer delete or expired value that is not resident). Strictly newer only:
    /// an extend reuses the base revision, so at an equal revision the intent is the authoritative copy.
    /// </summary>
    public static bool HeadSupersedesIntent(KeyValueContext context, PreparedIntent intent, Dictionary<string, long>? excludedHeads)
        => (context.Store.TryGetValue(intent.Key, out KeyValueEntry? entry) && entry.Revision > intent.Revision)
           || (excludedHeads is not null && excludedHeads.TryGetValue(intent.Key, out long revision) && revision > intent.Revision);

    /// <summary>
    /// Records the head revision of a row a latest-read scan evaluated but left out of its page (a delete or an
    /// expired value), for <see cref="HeadSupersedesIntent"/>. Only needed while some prepared intent lingers, and
    /// only for a latest read: a snapshot page's rows are as-of revisions, not heads.
    /// </summary>
    public static void RecordExcludedHead(KeyValueContext context, ref Dictionary<string, long>? excludedHeads, string key, long revision)
    {
        if (context.PreparedIntentStore is not { Count: > 0 })
            return;

        (excludedHeads ??= new(StringComparer.Ordinal))[key] = revision;
    }
}
