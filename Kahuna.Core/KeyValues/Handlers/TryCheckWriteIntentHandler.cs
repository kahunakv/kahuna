
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Commit-time conflict probe.
///
/// Answers the conflict classes the caller selected in <see cref="KeyValueRequest.ConflictChecks"/>, so one
/// batched probe can ask different questions of different keys: a transaction's read set asks about concurrent
/// write intents (the write-skew guard below), while its write set asks whether a foreign range lock covers the
/// key — the fence that catches a range lock acquired after the write was staged, which the write-time fence in
/// TrySetHandler/TryDeleteHandler cannot see. A Shared or Exclusive acquire is itself refused over a key that
/// carries a live foreign write intent, so for those modes this fence is a backstop; a WriteFence (a split's or
/// merge's quiesce) steps around intents by design, and this probe is what makes the fence bite on a writer
/// that staged before it landed.
///
/// Checks whether a key carries a live write intent from a transaction other than the caller.
/// A positive result indicates that a concurrent transaction is preparing or has prepared a
/// write to this key, meaning a read-set entry for this key in the calling transaction is at
/// risk of write skew if both transactions commit.
///
/// Two sources of a concurrent writer are checked: the in-memory write intent placed while a peer stages its
/// writes, and — for the durable-intent path — a durable prepared intent. The durable intent is replicated, so
/// unlike the in-memory write intent (which a new Raft leader cannot reconstruct) it still detects a concurrent
/// writer after a leader change, closing the write-skew window across failover. Only an undecided durable intent
/// is a live concurrent writer here; committed/aborted outcomes are settled by the revision-based read validation.
///
/// This handler is intentionally separate from TryExistsHandler so that ordinary user reads
/// are never blocked by write intents (read-committed semantics), while the commit protocol
/// can still detect concurrent writers as a best-effort serialization guard.
///
/// The write-intent check is called during TwoPhaseCommit for optimistic transactions only, after
/// ValidateReadSet passes; pessimistic transactions do not need it (exclusive locks provide full
/// serializability there). The range-lock check applies to every transaction's write set regardless of
/// locking mode, because a range lock acquired mid-transaction steps around a key lock just as it does an
/// optimistic write intent.
/// </summary>
internal sealed class TryCheckWriteIntentHandler : BaseHandler
{
    public TryCheckWriteIntentHandler(KeyValueContext context) : base(context)
    {
    }

    public async ValueTask<KeyValueResponse> Execute(KeyValueRequest message)
    {
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        // Finalize pin for a yielding transaction: claim this transaction's own intent so no foreground writer
        // can take it over between here and the durable prepare. Runs in the key's actor turn, the same
        // discipline as a takeover, so the two cannot interleave on one key. This is what makes the hard
        // invariant true — a yielding transaction never commits a write to a key it lost.
        if ((message.ConflictChecks & KeyValueConflictChecks.PinOwnIntent) != 0)
            return await PinOwnIntent(message, currentTime);

        // Foreign range lock covering the key — the write set's decide-time fence. Answered before (and without)
        // the entry load: the bounds check needs only the key's bucket, which is derived from the key itself. An
        // entry-derived bucket would answer "no lock" for a phantom insert or a key that is not resident, exactly
        // the cases a range lock exists to cover.
        if ((message.ConflictChecks & KeyValueConflictChecks.ForeignRangeLock) != 0
            && RangeLockChecks.KeyCoveredByForeignRangeLock(context, message.Key, GetBucket(message.Key), message.TransactionId, currentTime))
            return KeyValueStaticResponses.AbortedResponse;

        if ((message.ConflictChecks & (KeyValueConflictChecks.WriteIntent | KeyValueConflictChecks.StagedBase)) == 0)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        bool resident = context.Store.TryGetValue(message.Key, out KeyValueEntry? entry);

        if (!resident)
        {
            entry = await GetKeyValueEntry(message.Key, message.Durability, populateCache: false, currentTime: currentTime);

            // Same stale-base refusal as the transactional reads: a persistent row hydrated from below this
            // node's committed-head memory would compare a validated base against history this node lost, and
            // report a moved base (or a matching one) that the committed state does not support. Refuse and let
            // the convergence repair land first.
            if ((message.ConflictChecks & KeyValueConflictChecks.StagedBase) != 0 && HydratedRowProvablyStale(message.Key, entry))
                return KeyValueStaticResponses.MustRetryResponse;

            if (entry is not null)
                context.InsertStoreEntry(message.Key, entry);
        }

        // Staged-base compare for a read-modify-write key: the finalizer's write-side compare-and-set, run before
        // anything durable is proposed and, for the one-phase bundle, again immediately before its propose. It is
        // served here as a conflict check and not as a read so the caller's own locks never refuse it: a
        // pessimistic transaction that scanned a bucket holds that bucket's prefix lock, and an ordinary
        // non-transactional read of a member under it answers MustRetry, which would spin the finalize until the
        // transaction times out. MVCC stagings and intents never touch entry.Revision (only materialization
        // advances it), so the committed head is exactly what a validated base was recorded against; a head
        // parked behind an in-flight operation is converged first, as the grant that observed the base did.
        // Answered with NotSet — "the compare failed" — so the caller can attribute the abort to a moved base
        // rather than to the range-lock fence. The authoritative half of the compare runs at the prepare's own
        // apply position (the intent store's staged-base fence) and at the bundled commit gate.
        if ((message.ConflictChecks & KeyValueConflictChecks.StagedBase) != 0)
        {
            if (entry?.PendingCommittedHead is not null)
                TryDrainPendingCommittedHead(message.Key, entry, currentTime);

            bool existsNow = entry is not null
                && entry.State == KeyValueState.Set
                && (entry.Expires == HLCTimestamp.Zero || entry.Expires - currentTime > TimeSpan.Zero);

            // CompareRevision carries the validated base: >= 0 when the base existed at that revision,
            // -1 when the read-modify-write was validated against "key does not exist".
            bool baseExisted = message.CompareRevision >= 0;

            if (existsNow != baseExisted || (existsNow && entry!.Revision != message.CompareRevision))
                return KeyValueStaticResponses.NotSetResponse;
        }

        if ((message.ConflictChecks & KeyValueConflictChecks.WriteIntent) == 0)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        // Live in-memory write intent from a different transaction — signal conflict to the caller.
        if (entry?.WriteIntent is not null && entry.WriteIntent.TransactionId != message.TransactionId)
        {
            if (KeyValueWriteIntentLease.IsLive(context, message.Key, entry.WriteIntent, currentTime))
                return KeyValueStaticResponses.AbortedResponse;

            entry.WriteIntent = null;
        }

        // Durable prepared intent from a concurrent transaction that is still undecided: a live writer that
        // survives leader change (the in-memory intent above does not). A pending intent whose canonical decision
        // is already commit/abort is not in flight — commit-staleness is caught by revision-based read validation
        // and an abort is no conflict — so it is not flagged here. No-op off the durable-intent path.
        if (context.PreparedIntentStore?.Get(message.Key) is { } foreignIntent
            && foreignIntent.TransactionId != message.TransactionId
            && DurableReadVisibility.IsUndecidedWriter(context, foreignIntent, message.ForeignDecisionHint))
            return KeyValueStaticResponses.AbortedResponse;

        return KeyValueStaticResponses.DoesNotExistContextResponse;
    }

    /// <summary>
    /// Claims this transaction's own write intent on the key so it cannot be taken over before the durable
    /// prepare, or reports that the key was already lost. In the key's actor turn:
    /// <list type="bullet">
    /// <item>the key is recorded as taken over from this transaction: answer <c>Aborted</c> — the loss stands;</item>
    /// <item>a live intent owned by this transaction: set <c>Pinned</c> and answer no conflict;</item>
    /// <item>no such intent (missing or foreign): answer <c>Aborted</c>. A pin can only make an intent this
    /// transaction still holds unstealable; a missing one means it was lost or a leader change dropped it, and
    /// committing over it is exactly what the pin exists to prevent.</item>
    /// </list>
    /// A durably-prepared intent is already safe (never stealable), so a key already backed by this
    /// transaction's durable prepared intent answers no conflict without needing an in-memory intent.
    /// </summary>
    private async ValueTask<KeyValueResponse> PinOwnIntent(KeyValueRequest message, HLCTimestamp currentTime)
    {
        if (context.HasYieldedIntent(message.Key, message.TransactionId))
            return KeyValueStaticResponses.AbortedResponse;

        // A key already carried by this transaction's durable prepared intent is safe regardless of the
        // in-memory intent: durable intents are never stolen.
        if (context.PreparedIntentStore?.Get(message.Key) is { } durable && durable.TransactionId == message.TransactionId)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        KeyValueEntry? entry = await GetKeyValueEntry(message.Key, message.Durability, currentTime: currentTime);

        if (entry?.WriteIntent is not null
            && entry.WriteIntent.TransactionId == message.TransactionId
            && KeyValueWriteIntentLease.IsLive(context, message.Key, entry.WriteIntent, currentTime))
        {
            entry.WriteIntent.Pinned = true;
            return KeyValueStaticResponses.DoesNotExistContextResponse;
        }

        return KeyValueStaticResponses.AbortedResponse;
    }
}
