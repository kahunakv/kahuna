
using Kommander.Time;

using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues;

internal sealed class KeyValueWriteIntent
{
    public HLCTimestamp TransactionId { get; set; }

    public HLCTimestamp Expires { get; set; }

    /// <summary>
    /// The actor's HLC at the moment this intent was planted. Anchors the liveness ceiling that bounds a
    /// session-owned (zero-deadline) intent: past the ceiling no legitimate session can still own it, so the
    /// intent is orphaned and stops holding the key. The stamp is read from the actor that plants the intent
    /// rather than taken from the caller's transaction id, so a caller-supplied clock cannot shorten the life
    /// of a lock. It is at or after the transaction id, which keeps the derived deadline at or after the
    /// reaper's own bound for the owning session.
    /// </summary>
    public HLCTimestamp AcquiredAt { get; set; }

    /// <summary>
    /// The timestamp the committed revision will carry (= mvccEntry.LastModified stamped at write time).
    /// Zero means this is a plain per-key lock or a not-yet-prepared intent — commit ts is undetermined.
    /// Non-Zero means the intent has been prepared via 2PC and the pending commit ts is known.
    /// </summary>
    public HLCTimestamp CommitTimestamp { get; set; }

    /// <summary>
    /// The transaction's canonical record anchor (its first confirmed persistent modified key), supplied by
    /// the coordinator at prepare. Null for a plain per-key lock or a transaction with no persistent write.
    /// Participant-side metadata that a durable completion receipt is later keyed by.
    /// </summary>
    public string? RecordAnchorKey { get; set; }

    /// <summary>
    /// True once this intent's ceiling expiry has been counted and logged. Several paths may evaluate the
    /// same expired intent before one of them drops it, and each occurrence names one orphaned owner; the
    /// flag keeps the counter equal to the number of orphaned intents rather than the number of reads that
    /// met them.
    /// </summary>
    public bool CeilingExpiryReported { get; set; }
}

/// <summary>
/// The safe-time rule a snapshot read applies to a live foreign write intent: the read must wait
/// only when the intent's pending commit could become visible at or before the read's snapshot.
///
/// A prepared intent answers from its known pending commit timestamp. An un-prepared intent
/// (<see cref="KeyValueWriteIntent.CommitTimestamp"/> still Zero) has no commit timestamp yet, but
/// one hard lower bound exists: every staged MVCC write is stamped from an HLC event that folded
/// the writer's transaction id in (<c>TrySetHandler.ExecuteTransactional</c> uses
/// <c>ReceiveEvent(node, TransactionId)</c>), and the prepare copies that stamp into
/// <see cref="KeyValueWriteIntent.CommitTimestamp"/>. The eventual commit timestamp is therefore
/// strictly above <see cref="KeyValueWriteIntent.TransactionId"/>.
///
/// So a transaction that began after the read's snapshot can never commit inside that snapshot,
/// and the read must not wait on its intent. This is what keeps a snapshot scan live under a
/// saturating writer: without it, every retry of the scan meets the writer's newest in-flight
/// intent, waits, and retries into the next one — a split's bulk copy or a split probe then
/// starves forever even though no blocking write can ever land inside its snapshot.
/// </summary>
internal static class KeyValueWriteIntentSafeTime
{
    /// <summary>
    /// Returns whether the intent's pending commit could land at or before
    /// <paramref name="readTimestamp"/> — that is, whether a snapshot read at that timestamp
    /// must wait for the intent to resolve. Callers apply this only to a live intent owned by a
    /// foreign transaction, on a read that carries a snapshot timestamp.
    /// </summary>
    internal static bool MayCommitAtOrBefore(KeyValueWriteIntent intent, HLCTimestamp readTimestamp)
    {
        HLCTimestamp commitTs = intent.CommitTimestamp;

        if (!commitTs.IsNull())
            return commitTs.CompareTo(readTimestamp) <= 0;

        // Un-prepared: the eventual commit timestamp is strictly above the transaction id (see the
        // class summary). A transaction id at or above the snapshot proves the commit lands after
        // it. A Zero transaction id carries no proof, so the read stays conservative and waits.
        HLCTimestamp transactionId = intent.TransactionId;

        return transactionId.IsNull() || transactionId.CompareTo(readTimestamp) < 0;
    }
}

/// <summary>
/// Defines the lease contract shared by point and predicate write intents. A zero-duration request
/// creates a session-owned intent with no clock deadline; it stays live until explicit transaction
/// cleanup releases it, or until the liveness ceiling proves no session can still own it. Positive
/// durations start at the actor's acquisition-time HLC, preventing a long-running transaction from
/// receiving a lock whose deadline predates the successful acquire. Keeping this policy centralized
/// prevents readers and writers from disagreeing about whether an accepted lock is still active.
/// </summary>
internal static class KeyValueWriteIntentLease
{
    /// <summary>Distinguishes point and prefix intents from range locks on the shared expiry counter.</summary>
    private static readonly KeyValuePair<string, object?> IntentKind = new("kind", "intent");

    /// <summary>
    /// Converts a validated lock duration into its stored deadline. Callers must reject negative
    /// durations before invoking this method.
    /// </summary>
    internal static HLCTimestamp FromRequest(HLCTimestamp currentTime, int expiresMs) =>
        expiresMs == 0 ? HLCTimestamp.Zero : currentTime + expiresMs;

    /// <summary>
    /// Returns whether an intent still owns its lock, and reports the first ceiling expiry of that intent —
    /// the counter and a warning naming the key and the owning transaction. Every path that consults an
    /// intent uses this entry point, so one orphaned intent is attributed wherever it is first met: a read,
    /// a write claiming the key, or the collector reclaiming the entry.
    /// </summary>
    internal static bool IsLive(KeyValueContext context, string? key, KeyValueWriteIntent intent, HLCTimestamp currentTime)
    {
        int ceilingMs = context.SessionOwnedIntentCeilingMs;

        if (IsLive(intent, currentTime, ceilingMs))
            return true;

        // A zero deadline can only fail the policy through the ceiling arm, so this branch identifies an
        // orphaned session-owned intent without re-deriving the reason. An ordinary lease expiry is routine
        // and stays unreported.
        if (intent.Expires == HLCTimestamp.Zero && !intent.CeilingExpiryReported)
        {
            intent.CeilingExpiryReported = true;
            DurableTransactionMetrics.SessionOwnedIntentCeilingExpiries.Add(1, IntentKind);
            context.Logger.LogSessionOwnedIntentCeilingExpiry(key, intent.TransactionId, ceilingMs);
        }

        return false;
    }

    /// <summary>
    /// The liveness policy itself, in three arms:
    /// <list type="bullet">
    /// <item>A positive deadline is live until the deadline passes.</item>
    /// <item>A zero deadline on a prepared intent (<c>CommitTimestamp != Zero</c>) is live. Its fate belongs
    /// to the decision machinery — the finalizer, the recovery sweep and the settle paths resolve it against
    /// the canonical record — and expiring one that later commits would discard the only route to an
    /// already-committed value.</item>
    /// <item>A zero deadline on an un-prepared intent is live only while its age stays below
    /// <paramref name="sessionOwnedCeilingMs"/>. Past that age the owning session has been finalized or
    /// reaped, so no legitimate session can still hold the key.</item>
    /// </list>
    /// </summary>
    internal static bool IsLive(KeyValueWriteIntent intent, HLCTimestamp currentTime, int sessionOwnedCeilingMs)
    {
        if (intent.Expires != HLCTimestamp.Zero)
            return intent.Expires - currentTime > TimeSpan.Zero;

        if (intent.CommitTimestamp != HLCTimestamp.Zero)
            return true;

        // No ceiling configured (a bare context built without a configuration): keep the historical
        // session-owned semantics rather than expire an intent against an unknown bound.
        if (sessionOwnedCeilingMs <= 0)
            return true;

        // AcquiredAt is the actor's own clock reading at the plant. An intent that predates the stamp falls
        // back to the transaction id, which bounds the same session one step more tightly.
        HLCTimestamp anchor = intent.AcquiredAt != HLCTimestamp.Zero ? intent.AcquiredAt : intent.TransactionId;

        if (anchor == HLCTimestamp.Zero)
            return true;

        return (currentTime - anchor).TotalMilliseconds < sessionOwnedCeilingMs;
    }
}
