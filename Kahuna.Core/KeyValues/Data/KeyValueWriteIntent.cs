
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

    /// <summary>
    /// True when the owning transaction was started with <see cref="TransactionConflictPolicy.Yield"/>: a
    /// request from a transaction that does not yield, or from no transaction, takes this intent over instead of
    /// being denied, as long as it is not <see cref="Pinned"/>, not prepared (<see cref="CommitTimestamp"/> is
    /// Zero) and not backed by a durable prepared intent. Set when the intent is planted; a refresh by the same
    /// owner keeps it.
    /// </summary>
    public bool Yielding { get; set; }

    /// <summary>
    /// True once the owning yielding transaction's finalize claimed this intent: from here on the intent is not
    /// stealable, and a foreground writer that meets it waits for the owner's decision instead of taking the key
    /// over. Set in the key's actor turn by the finalize pin, so no takeover can interleave between the pin and
    /// the durable prepare that follows it. Meaningless on a non-yielding intent.
    /// </summary>
    public bool Pinned { get; set; }

    /// <summary>
    /// True once any path found this intent no longer live (its lease or liveness ceiling ran out). Sticky: a
    /// later refresh of the lease by the owner does not clear it. While the intent was not live, a snapshot read
    /// that met it did not wait for the owner's staged write, so the owner can no longer prove that its commit
    /// lands outside every snapshot already served on the key; the commit-time probe refuses it.
    /// </summary>
    public bool Lapsed { get; set; }

    /// <summary>
    /// True once the owning transaction's commit probe found this intent holding a staged write and claimed it
    /// for the commit. From here on the intent ignores its lease deadline: it stays live until the commit or the
    /// rollback clears it, or until the session liveness ceiling proves that no session can still own it. The
    /// commit timestamp is frozen before the probe, and a one-phase bundle decides in the same durable batch as
    /// its prepare, so nothing after the probe can refuse the commit if the intent lapses while the bundle is in
    /// flight; a snapshot read that then stepped over the lapsed intent would answer the old value at a snapshot
    /// the commit lands inside. Holding the intent makes that read wait instead. Set in the key's actor turn by
    /// the probe; never cleared.
    /// </summary>
    public bool HeldForCommit { get; set; }

    /// <summary>
    /// The highest snapshot timestamp at which a read was served the committed head over this intent while the
    /// owner's commit could still be moved: before the intent was prepared (<see cref="CommitTimestamp"/> Zero)
    /// and before the commit probe claimed it (<see cref="HeldForCommit"/>). Such a read did not wait for the
    /// staged write, so the owner's commit must land strictly above the floor, or the same read asked again
    /// would see the write. The durable coordinator learns the floor from the commit probe and mints the commit
    /// timestamp above it; the manual prepare re-stamps the staged write above it. Raised only by snapshot reads
    /// in the key's actor turn, which fold their timestamp into the actor clock first, so every stamp the actor
    /// mints afterwards is above it. Zero when no snapshot read passed over the intent.
    /// </summary>
    public HLCTimestamp CommitFloor { get; set; }
}

/// <summary>
/// The safe-time rule a snapshot read applies to a live foreign write intent: the read must wait
/// only when the intent's pending commit could become visible at or before the read's snapshot.
///
/// <para>A prepared intent answers from its known pending commit timestamp. An un-prepared intent
/// (<see cref="KeyValueWriteIntent.CommitTimestamp"/> still Zero) has no commit timestamp yet, but its
/// staged write bounds it from below. <c>TrySetHandler.ExecuteTransactional</c> and the transactional
/// delete stamp the staged MVCC entry from the actor's clock, and that stamp is the lowest the commit can
/// carry: the durable coordinator mints the commit timestamp strictly above every staged stamp
/// (<c>TransactionCoordinator.MintDurableCommitTimestamp</c>), and the manual prepare copies the stamp
/// itself into <see cref="KeyValueWriteIntent.CommitTimestamp"/>. A write staged above the snapshot
/// therefore commits above it, and the read must not wait for it.</para>
///
/// <para>Without a staged write on the key — a point, prefix or range lock that planted the intent before
/// any write — the bound is the intent's <see cref="KeyValueWriteIntent.AcquiredAt"/>: a write the owner
/// stages later on this key is stamped by the same clock, after the plant, so its commit lands above the
/// plant time. An intent that carries no plant time falls back to the transaction id, which every stamp
/// of the transaction is above.</para>
///
/// <para>The bound is the whole cost of the wait. A reader at <c>T</c> that waits for a writer which began
/// before <c>T</c> but staged after it pays the writer's open time for a commit that can never land inside
/// its snapshot; a heartbeat-style writer that begins a transaction, takes a lock, and only then writes
/// made every snapshot scan of its table wait that way. The rule is also what keeps a snapshot scan live
/// under a saturating writer: every retry of the scan meets the writer's newest in-flight intent, and
/// without the bound it would wait, retry into the next one, and starve.</para>
///
/// <para>A write staged at or below the snapshot does not make the read wait either, as long as the owner's
/// commit can still be moved. The read records its timestamp as the intent's commit floor
/// (<see cref="KeyValueWriteIntent.CommitFloor"/>) and is served the committed head; the commit is then minted
/// above the floor (<see cref="MustWait"/>). What the read waits for is a commit whose timestamp is fixed: a
/// prepared intent at or below the snapshot, or an intent the commit probe already claimed
/// (<see cref="KeyValueWriteIntent.HeldForCommit"/>), whose frozen commit timestamp the actor does not know.
/// Both waits last the commit's own latency, not the writer's open time.</para>
/// </summary>
internal static class KeyValueWriteIntentSafeTime
{
    /// <summary>
    /// Whether a snapshot read at <paramref name="readTimestamp"/> must wait for the live foreign
    /// <paramref name="intent"/>. A read that need not wait falls through to the committed state. When the
    /// intent's commit could land inside the snapshot (<see cref="MayCommitAtOrBefore(KeyValueEntry, KeyValueWriteIntent, HLCTimestamp)"/>)
    /// but can still be moved (<see cref="CanPushCommit"/>), the read raises the intent's commit floor to its
    /// timestamp and is served the committed head instead of waiting: the owner's commit is minted above the
    /// floor, so the read asked again at the same timestamp still answers the head. Callers apply this only to
    /// a live intent owned by a foreign transaction, on a read that carries a snapshot timestamp, inside the
    /// key's actor turn, after the actor's clock was fenced at the read's timestamp.
    /// </summary>
    internal static bool MustWait(KeyValueEntry entry, KeyValueWriteIntent intent, HLCTimestamp readTimestamp)
    {
        if (!MayCommitAtOrBefore(entry, intent, readTimestamp))
            return false;

        if (!CanPushCommit(intent))
            return true;

        if (intent.CommitFloor.CompareTo(readTimestamp) < 0)
            intent.CommitFloor = readTimestamp;

        KeyValueSnapshotReadMetrics.CommitsPushedAboveSnapshot.Add(1);
        return false;
    }

    /// <summary>
    /// Whether the owner's commit timestamp is not fixed yet, so a snapshot read may push it above itself
    /// instead of waiting: the intent is not prepared (no known commit timestamp) and the commit probe has not
    /// claimed it. After the claim the coordinator's frozen commit timestamp is final and the one-phase bundle
    /// may already be in flight, so a read that could be inside it waits for the apply.
    /// </summary>
    internal static bool CanPushCommit(KeyValueWriteIntent intent) =>
        intent.CommitTimestamp.IsNull() && !intent.HeldForCommit;

    /// <summary>
    /// Returns whether the intent's pending commit could land at or before <paramref name="readTimestamp"/>:
    /// whether a snapshot read at that timestamp must wait for the intent to resolve. Callers apply this only
    /// to a live intent owned by a foreign transaction, on a read that carries a snapshot timestamp.
    /// <paramref name="entry"/> is the key the intent sits on; its MVCC entry staged under the owner, when
    /// there is one, supplies the tightest bound.
    /// </summary>
    internal static bool MayCommitAtOrBefore(KeyValueEntry entry, KeyValueWriteIntent intent, HLCTimestamp readTimestamp)
    {
        if (!intent.CommitTimestamp.IsNull())
            return intent.CommitTimestamp.CompareTo(readTimestamp) <= 0;

        // A staged write on this key: the commit carries this stamp (manual prepare) or a timestamp above it
        // (durable finalize). Only an entry the owner staged under the intent counts; a read pin the owner left
        // on the key carries the base's stamp and proves nothing.
        if (entry.MvccEntries is not null
            && entry.MvccEntries.TryGetValue(intent.TransactionId, out KeyValueMvccEntry? staged)
            && staged.StagedUnder is not null)
            return staged.LastModified.CompareTo(readTimestamp) <= 0;

        return MayCommitAtOrBefore(intent, readTimestamp);
    }

    /// <summary>
    /// The bound without a staged write on the key: the plant time of the intent when it carries one, the
    /// transaction id otherwise (see the class summary). A Zero transaction id carries no proof, so the read
    /// stays conservative and waits.
    /// </summary>
    internal static bool MayCommitAtOrBefore(KeyValueWriteIntent intent, HLCTimestamp readTimestamp)
    {
        HLCTimestamp commitTs = intent.CommitTimestamp;

        if (!commitTs.IsNull())
            return commitTs.CompareTo(readTimestamp) <= 0;

        HLCTimestamp plantedAt = intent.AcquiredAt;

        if (!plantedAt.IsNull())
            return plantedAt.CompareTo(readTimestamp) <= 0;

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
    /// durations before invoking this method. A positive duration is cut to <paramref name="maxLeaseMs"/>
    /// (see <see cref="Bound"/>).
    /// </summary>
    internal static HLCTimestamp FromRequest(HLCTimestamp currentTime, int expiresMs, int maxLeaseMs) =>
        expiresMs == 0 ? HLCTimestamp.Zero : currentTime + Bound(expiresMs, maxLeaseMs);

    /// <summary>
    /// Cuts a requested lock duration to <paramref name="maxLeaseMs"/>, the session-owned ceiling. No session
    /// can still own a lock older than that ceiling, so a longer explicit deadline serves no transaction: it
    /// only lets a caller pin a key — and the store entry under it, which the collector cannot evict — for as
    /// long as an int of milliseconds allows, and renew it forever. Zero (session-owned) and a non-positive
    /// bound pass through unchanged.
    /// </summary>
    internal static int Bound(int expiresMs, int maxLeaseMs) =>
        maxLeaseMs > 0 && expiresMs > maxLeaseMs ? maxLeaseMs : expiresMs;

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

        intent.Lapsed = true;

        // A zero deadline, or an intent held for its commit, can only fail the policy through the ceiling arm,
        // so this branch identifies an orphaned session-bound intent without re-deriving the reason. An
        // ordinary lease expiry is routine and stays unreported.
        if ((intent.Expires == HLCTimestamp.Zero || intent.HeldForCommit) && !intent.CeilingExpiryReported)
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
    /// <item>A positive deadline is live until the deadline passes — unless the intent is
    /// <see cref="KeyValueWriteIntent.HeldForCommit"/>: the owner's commit probe claimed it, so from then on it
    /// is bound to the session like a zero-deadline intent and the two arms below decide it.</item>
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
        if (intent.Expires != HLCTimestamp.Zero && !intent.HeldForCommit)
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
