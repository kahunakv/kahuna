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
/// <c>Aborted</c> (the base later materializes to a higher revision than the snapshot). Point reads avoid this
/// because they consult the intent before ever snapshotting, but a scan snapshots the base directly — this is where
/// that gap is closed, so a scan followed by a batch read of the same rows in one transaction stays consistent.
/// </para>
/// </summary>
internal static class DurableSnapshotSource
{
    /// <summary>
    /// Resolves what a fresh latest-read MVCC snapshot of <paramref name="key"/> must capture. Returns
    /// <see cref="SnapshotDecision.UseIntent"/> with a ready-built snapshot when a committed foreign intent
    /// supersedes the base, <see cref="SnapshotDecision.Retry"/> when an undecided intent means the read must wait,
    /// and <see cref="SnapshotDecision.UseBase"/> (no intent) for the ordinary path. Only meaningful for a latest
    /// read; an as-of snapshot read is served through the revision-history path, never this OCC snapshot.
    /// </summary>
    public static SnapshotDecision Resolve(
        KeyValueContext context, string key, HLCTimestamp readerTransactionId, HLCTimestamp currentTime,
        out KeyValueMvccEntry intentSnapshot)
    {
        intentSnapshot = null!;

        if (context.PreparedIntentStore?.Get(key) is not { } foreign || foreign.TransactionId == readerTransactionId)
            return SnapshotDecision.UseBase;

        switch (DurableReadVisibility.Resolve(context, foreign, HLCTimestamp.Zero))
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
}
