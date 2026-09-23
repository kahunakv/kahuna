using Kahuna.Server.Configuration;
using Nixie;
using Kommander;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Acquires a range lock (Shared, Exclusive or WriteFence) over the keys of one key space.
///
/// <para>A range lock and a per-key write intent are two halves of one two-phase-locking matrix. The write
/// path enforces one half: a write into a range under a foreign Shared or Exclusive lock is refused. This
/// handler enforces the other half: a Shared or Exclusive acquire is refused while a covered key carries a
/// live write intent of another transaction. Without this half a reader whose lock lands after the writer
/// staged its value reads the value from before that write, and the writer, whose only later check is its
/// commit-time probe, commits anyway once the lock arrived after that probe — a write-skew cycle that no
/// serial order explains. A WriteFence deliberately keeps stepping around intents: it exists to stop new
/// writers during a split or merge without wedging on the in-flight ones, which the write-path refusal and
/// the writer's commit-time probe drain.</para>
///
/// <para>"Live" means undecided. A foreign intent whose transaction is already durably decided is not about to
/// change anything: its outcome is fixed, and every read path resolves such an intent inline to the committed
/// value (or the pre-image on abort) without waiting for the background settlement that clears it. A Shared
/// acquire is therefore granted over it. An Exclusive acquire still cannot place its own per-key intent while
/// the predecessor's occupies the slot, so it answers a wait — but names the keys, so the acquire loop settles
/// those decided intents itself instead of waiting for the deferred-settlement backlog to reach them.</para>
/// </summary>
internal sealed class TryAcquireExclusiveRangeLockHandler : BaseHandler
{
    /// <summary>
    /// Upper bound on the decided-but-unsettled keys one wait answer names. The acquire loop settles the named
    /// keys and retries; a range with more of them is drained over successive answers, so the bound caps one
    /// response and one helping pass, not the acquire.
    /// </summary>
    internal const int MaxReportedBlockingKeys = 4096;

    /// <summary>What a covered resident entry holds against the acquiring transaction.</summary>
    private enum ForeignWriter
    {
        /// <summary>No foreign write on the key; it is free for this transaction.</summary>
        None,

        /// <summary>A non-transactional write is still replicating: wait, as any write does.</summary>
        ReplicationInFlight,

        /// <summary>A foreign write intent whose transaction is durably decided but whose settlement has not run.</summary>
        DecidedUnsettled,

        /// <summary>A foreign write intent whose transaction is still undecided: a genuine concurrent writer.</summary>
        Undecided,

        /// <summary>A durable prepared intent of an undecided transaction whose in-memory intent is gone (a leader
        /// change dropped it), or whose decision is not locally known: wait for the decision.</summary>
        UndecidedDurable,
    }

    public TryAcquireExclusiveRangeLockHandler(KeyValueContext context) : base(context)
    {
    }

    public KeyValueResponse Execute(KeyValueRequest message)
    {
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        if (message.TransactionId == HLCTimestamp.Zero || message.ExpiresMs < 0)
            return KeyValueStaticResponses.ErroredResponse;

        // The lock is recorded under the key space the prefix names, which is the bucket the write path
        // looks up for a key, so a prefix spelled with or without a trailing slash guards the same keys.
        string keySpace = KeyValueKeySpace.OfPrefix(message.Key);

        // Prune abandoned expired range locks on the way in so they neither block a fresh acquire
        // nor accumulate on a hot key space; drop the bucket entirely once it holds no live locks.
        if (context.LocksByRange.TryGetValue(keySpace, out List<KeyValueRangeLock>? existingLocks)
            && RangeLockChecks.PruneExpired(context, keySpace, existingLocks, currentTime, int.MaxValue))
        {
            context.LocksByRange.Remove(keySpace);
            existingLocks = null;
        }

        if (existingLocks is not null)
        {
            // Idempotency / upgrade: same tx, same range bounds
            foreach (KeyValueRangeLock existing in existingLocks)
            {
                if (existing.TransactionId != message.TransactionId)
                    continue;
                if (existing.StartKey != message.StartKey
                    || existing.EndKey != message.EndKey
                    || existing.StartInclusive != message.StartInclusive
                    || existing.EndInclusive != message.EndInclusive)
                    continue;

                // S → X upgrade: must pass the same conflict gate as a fresh Exclusive acquire.
                // Another tx may hold an overlapping Shared lock (S∩S coexistence made that reachable).
                // Promoting without checking would leave X(tx1) ∩ S(tx2) — a matrix violation.
                if (message.RangeLockMode == RangeLockMode.Exclusive && existing.Mode == RangeLockMode.Shared)
                {
                    foreach (KeyValueRangeLock other in existingLocks)
                    {
                        if (other.TransactionId == message.TransactionId)
                            continue;
                        if (!RangeLockChecks.IsLive(other, currentTime, context.SessionOwnedIntentCeilingMs))
                            continue;
                        if (RangeLockChecks.RangesOverlap(message.StartKey, message.StartInclusive, message.EndKey, message.EndInclusive,
                                other.StartKey, other.StartInclusive, other.EndKey, other.EndInclusive))
                            return KeyValueResponse.Denied(KeyValueResponseType.AlreadyLocked, other.TransactionId);
                    }

                    // PlaceWriteIntents is atomic: a mid-loop conflict rolls back every intent it wrote, so a
                    // failed promotion leaves the lock Shared with no stray exclusive intents. The caller
                    // retries the transaction on the non-Locked response.
                    KeyValueResponse intents = PlaceWriteIntents(currentTime, message, keySpace);
                    if (intents.Type != KeyValueResponseType.Locked)
                        return intents;
                    existing.Mode = RangeLockMode.Exclusive;
                }
                // X → S downgrade or same-mode re-entry: refresh the expiry from *now* so the
                // caller can extend the lock beyond its original TTL (heartbeat / lease-renewal).
                existing.Expires = KeyValueWriteIntentLease.FromRequest(currentTime, message.ExpiresMs);
                return KeyValueStaticResponses.LockedResponse;
            }

            // Conflict check: S∩S coexist; a WriteFence coexists with S in both directions (the fence
            // blocks writers through the write-path check, not through reader exclusion, and a reader's
            // Shared lock must not starve a split's quiesce); every pairing involving X conflicts, and
            // two WriteFences conflict (two concurrent splits over one range must serialize).
            foreach (KeyValueRangeLock existing in existingLocks)
            {
                if (existing.TransactionId == message.TransactionId)
                    continue;

                if (!RangeLockChecks.IsLive(existing, currentTime, context.SessionOwnedIntentCeilingMs))
                    continue; // expired, or orphaned past the session-owned ceiling

                if (message.RangeLockMode == RangeLockMode.Shared && existing.Mode == RangeLockMode.Shared)
                    continue; // S∩S always compatible

                if (message.RangeLockMode == RangeLockMode.WriteFence && existing.Mode == RangeLockMode.Shared)
                    continue; // a fence tolerates readers

                if (message.RangeLockMode == RangeLockMode.Shared && existing.Mode == RangeLockMode.WriteFence)
                    continue; // readers tolerate a fence

                if (RangeLockChecks.RangesOverlap(message.StartKey, message.StartInclusive, message.EndKey, message.EndInclusive,
                        existing.StartKey, existing.StartInclusive, existing.EndKey, existing.EndInclusive))
                    return KeyValueResponse.Denied(KeyValueResponseType.AlreadyLocked, existing.TransactionId);
            }
        }

        return LockExistingKeysByRange(currentTime, message, keySpace);
    }

    private KeyValueResponse LockExistingKeysByRange(HLCTimestamp currentTime, KeyValueRequest message, string keySpace)
    {
        // Exclusive acquires place per-key write intents so existing keys are immediately locked, and are
        // refused over a foreign undecided intent. Shared acquires place no intents but are refused over a
        // foreign undecided intent all the same: the reader's lock must conflict with the writer's, or the
        // reader holds a lock over a value that is about to change and the writer never learns of the reader
        // (its commit-time probe ran before the lock existed). A decided intent changes nothing any more, so a
        // Shared acquire is granted over it. WriteFence acquires skip intents in both
        // directions: the fence must block new writers (the write path refuses a mutation under any foreign
        // range lock) without waiting for the in-flight ones, and a per-key write intent would make every
        // snapshot scan of the range wait for the holder.
        if (message.RangeLockMode == RangeLockMode.Exclusive)
        {
            KeyValueResponse intents = PlaceWriteIntents(currentTime, message, keySpace);
            if (intents.Type != KeyValueResponseType.Locked)
                return intents;
        }
        else if (message.RangeLockMode == RangeLockMode.Shared)
        {
            KeyValueResponse? writer = FindForeignWriter(currentTime, message, keySpace);
            if (writer is not null)
                return writer;
        }

        KeyValueRangeLock rangeLock = new()
        {
            TransactionId  = message.TransactionId,
            Expires        = KeyValueWriteIntentLease.FromRequest(currentTime, message.ExpiresMs),
            StartKey       = message.StartKey,
            StartInclusive = message.StartInclusive,
            EndKey         = message.EndKey,
            EndInclusive   = message.EndInclusive,
            Mode           = message.RangeLockMode,
        };

        if (!context.LocksByRange.TryGetValue(keySpace, out List<KeyValueRangeLock>? locks))
        {
            locks = [];
            context.LocksByRange[keySpace] = locks;
        }

        locks.Add(rangeLock);

        return KeyValueStaticResponses.LockedResponse;
    }

    /// <summary>
    /// The reader-side conflict check of a Shared acquire: the first covered key that carries another
    /// transaction's undecided write decides the answer. A decided-but-unsettled foreign intent is stepped over:
    /// the value under it is fixed and every read under the lock resolves it inline, so the lock protects exactly
    /// what those reads observe. Inspects only; places nothing.
    /// </summary>
    private KeyValueResponse? FindForeignWriter(HLCTimestamp currentTime, KeyValueRequest message, string keySpace)
    {
        bool steppedOverDecided = false;

        foreach ((string key, KeyValueEntry entry) in CoveredResidentEntries(message, keySpace))
        {
            switch (ClassifyForeignWriter(key, entry, message, currentTime, out HLCTimestamp holder))
            {
                case ForeignWriter.ReplicationInFlight:
                    return KeyValueStaticResponses.WaitingForReplicationResponse;

                case ForeignWriter.DecidedUnsettled:
                    steppedOverDecided = true;
                    break;

                case ForeignWriter.Undecided:
                    DurableTransactionMetrics.RangeLockAcquireIntentConflicts.Add(1);
                    return KeyValueResponse.Blocked(KeyValueResponseType.AlreadyLocked, holder, [key]);

                case ForeignWriter.UndecidedDurable:
                    return KeyValueResponse.Blocked(KeyValueResponseType.WaitingForReplication, holder, [key]);
            }
        }

        KeyValueResponse? durable = DurableWriterOnPointLock(message);
        if (durable is not null)
            return durable;

        if (steppedOverDecided)
            DurableTransactionMetrics.RangeLockSharedGrantsOverDecidedIntent.Add(1);

        return null;
    }

    private KeyValueResponse PlaceWriteIntents(HLCTimestamp currentTime, KeyValueRequest message, string keySpace)
    {
        HLCTimestamp requestedExpiry = KeyValueWriteIntentLease.FromRequest(currentTime, message.ExpiresMs);

        // Stamp per-key write intents atomically: a mid-loop conflict rolls back every intent written this
        // call, so a failed acquire/promotion never strands intents on the range's keys. The LocksByRange
        // record installed by the caller is what blocks the write path.
        List<(KeyValueEntry Entry, KeyValueWriteIntent? Prior)>? stamped = null;

        // A decided-but-unsettled predecessor occupies the key's intent slot until its settlement clears it.
        // From the first such key on, nothing more is stamped: the rest of the scan only gathers the keys those
        // predecessors hold, so the acquire loop can settle all of them in one helping pass and retry once,
        // instead of discovering them one wait at a time.
        List<string>? decidedBlockers = null;
        HLCTimestamp decidedHolder = HLCTimestamp.Zero;

        foreach ((string key, KeyValueEntry entry) in CoveredResidentEntries(message, keySpace))
        {
            switch (ClassifyForeignWriter(key, entry, message, currentTime, out HLCTimestamp holder))
            {
                case ForeignWriter.ReplicationInFlight:
                    if (decidedBlockers is not null)
                        continue; // transient; the wait already being answered covers it

                    RollBack(stamped);
                    return KeyValueStaticResponses.WaitingForReplicationResponse;

                case ForeignWriter.Undecided:
                    // A live writer is the stronger fact, whether or not decided predecessors were gathered
                    // before it: the caller applies its own wait-or-abort ordering against that transaction,
                    // and a caller that waits meets the gathered keys again on its retry.
                    RollBack(stamped);
                    DurableTransactionMetrics.RangeLockAcquireIntentConflicts.Add(1);
                    return KeyValueResponse.Blocked(KeyValueResponseType.AlreadyLocked, holder, [key]);

                case ForeignWriter.UndecidedDurable:
                    if (decidedBlockers is not null)
                        continue;

                    RollBack(stamped);
                    return KeyValueResponse.Blocked(KeyValueResponseType.WaitingForReplication, holder, [key]);

                case ForeignWriter.DecidedUnsettled:
                    if (decidedBlockers is null)
                    {
                        RollBack(stamped);
                        stamped = null;
                        decidedBlockers = [];
                        decidedHolder = holder;
                    }

                    decidedBlockers.Add(key);
                    if (decidedBlockers.Count >= MaxReportedBlockingKeys)
                        return ExclusiveSettlementWait(decidedHolder, decidedBlockers);

                    continue;
            }

            if (decidedBlockers is not null)
                continue; // gathering only

            if (entry.WriteIntent is not null)
            {
                // Only this transaction's own intent survives the classification; refresh its lease.
                entry.WriteIntent.Expires = requestedExpiry;
                continue;
            }

            stamped ??= [];
            stamped.Add((entry, null));

            entry.WriteIntent = new()
            {
                TransactionId = message.TransactionId,
                Expires       = requestedExpiry,
                AcquiredAt    = currentTime,
            };

            context.Logger.LogAssignedWriteIntentRangeLock(key, message.TransactionId);
        }

        if (decidedBlockers is not null)
            return ExclusiveSettlementWait(decidedHolder, decidedBlockers);

        KeyValueResponse? durable = DurableWriterOnPointLock(message);
        if (durable is not null)
        {
            RollBack(stamped);
            return durable;
        }

        return KeyValueStaticResponses.LockedResponse;
    }

    private static void RollBack(List<(KeyValueEntry Entry, KeyValueWriteIntent? Prior)>? stamped)
    {
        if (stamped is null)
            return;

        foreach ((KeyValueEntry rollback, KeyValueWriteIntent? prior) in stamped)
            rollback.WriteIntent = prior;
    }

    /// <summary>The Exclusive answer over decided-but-unsettled predecessors: a wait that names the keys they hold,
    /// so the acquire loop settles them itself and retries, bounded by their resolution rather than by the
    /// deferred-settlement backlog.</summary>
    private static KeyValueResponse ExclusiveSettlementWait(HLCTimestamp holder, List<string> blockingKeys)
    {
        DurableTransactionMetrics.RangeLockExclusiveSettlementWaits.Add(1);
        return KeyValueResponse.Blocked(KeyValueResponseType.WaitingForReplication, holder, blockingKeys);
    }

    /// <summary>
    /// What <paramref name="entry"/> holds against the acquiring transaction. Clears an intent whose lease lapsed,
    /// exactly as the read and write handlers do when they meet one, so it never blocks a lock. Afterwards
    /// <c>entry.WriteIntent</c> is either null, this transaction's own, or the reported foreign holder's.
    /// <list type="bullet">
    /// <item>A live replication intent (a non-transactional write still in flight): wait, as any write does.</item>
    /// <item>A foreign write intent whose transaction is already decided but not yet settled: its decision is
    /// durable and the resolution that clears the intent is on its way. A Shared acquire is granted over it; an
    /// Exclusive acquire waits for the slot and reports the key so the wait ends with that resolution.</item>
    /// <item>Any other live foreign write intent: refused with the holder, so the caller can apply its own
    /// wait-or-abort ordering against that transaction.</item>
    /// <item>A durable prepared intent of another transaction that is still undecided while its in-memory
    /// intent is gone (a leader change dropped it), or whose decision is not locally known: wait for the
    /// decision. The key is reported so the acquire loop can fetch that decision from the anchor leader.</item>
    /// </list>
    /// </summary>
    private ForeignWriter ClassifyForeignWriter(string key, KeyValueEntry entry, KeyValueRequest message, HLCTimestamp currentTime, out HLCTimestamp holder)
    {
        holder = HLCTimestamp.Zero;

        if (entry.ReplicationIntent is not null)
        {
            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)
                return ForeignWriter.ReplicationInFlight;

            entry.ReplicationIntent = null;
        }

        if (entry.WriteIntent is { } intent && intent.TransactionId != message.TransactionId)
        {
            if (!KeyValueWriteIntentLease.IsLive(context, key, intent, currentTime))
            {
                entry.WriteIntent = null;
            }
            else
            {
                holder = intent.TransactionId;
                return IsAwaitingSettlement(key, intent.TransactionId, message.ForeignDecisionHint)
                    ? ForeignWriter.DecidedUnsettled
                    : ForeignWriter.Undecided;
            }
        }

        if (UndecidedDurableWriter(key, message) is { } durable)
        {
            holder = durable.TransactionId;
            return ForeignWriter.UndecidedDurable;
        }

        return ForeignWriter.None;
    }

    /// <summary>
    /// A point lock (<c>[k,k]</c>, both bounds inclusive) also covers a key that is not resident, whose only
    /// trace of an in-flight write is a durable prepared intent left by a leader change. The resident scan
    /// cannot see it, so the point key is asked directly. Wider ranges are not swept through the intent store
    /// here: the read paths already hold a read or scan behind an undecided foreign prepared intent, so the
    /// value such a lock protects can never be served stale.
    /// </summary>
    private KeyValueResponse? DurableWriterOnPointLock(KeyValueRequest message)
    {
        if (message.StartKey is null || message.EndKey is null
            || !message.StartInclusive || !message.EndInclusive
            || !string.Equals(message.StartKey, message.EndKey, StringComparison.Ordinal)
            || context.Store.TryGetValue(message.StartKey, out _))
            return null;

        return UndecidedDurableWriter(message.StartKey, message) is { } durable
            ? KeyValueResponse.Blocked(KeyValueResponseType.WaitingForReplication, durable.TransactionId, [message.StartKey])
            : null;
    }

    /// <summary>The durable prepared intent of another transaction on <paramref name="key"/> whose decision is not
    /// known here (still undecided, or anchored on a partition whose record this node does not hold and no routed
    /// hint names it); null when the key carries none.</summary>
    private PreparedIntent? UndecidedDurableWriter(string key, KeyValueRequest message)
    {
        if (context.PreparedIntentStore?.Get(key) is { } durable
            && durable.TransactionId != message.TransactionId
            && DurableReadVisibility.IsUndecidedWriter(context, durable, message.ForeignDecisionHint))
            return durable;

        return null;
    }

    /// <summary>
    /// The resident entries a range lock covers: inside the requested bounds and belonging to the key space the
    /// lock is recorded under, which is the bucket the write-path fence consults for a key. A key nested deeper
    /// (<c>space/a/b</c> under a lock on <c>space</c>) belongs to another bucket and is fenced by none of this
    /// lock's checks, so it is skipped here for the same reason; and an unbounded end stops at the key space's
    /// last key rather than running to the end of the store.
    /// </summary>
    private IEnumerable<KeyValuePair<string, KeyValueEntry>> CoveredResidentEntries(KeyValueRequest message, string keySpace)
    {
        string bucketPrefix = keySpace + "/";

        string start = message.StartKey ?? bucketPrefix;
        bool startInclusive = message.StartKey is null || message.StartInclusive;

        if (string.CompareOrdinal(start, bucketPrefix) < 0)
        {
            start = bucketPrefix;
            startInclusive = true;
        }

        foreach (KeyValuePair<string, KeyValueEntry> kv in context.Store.GetByRange(start, startInclusive, message.EndKey, message.EndInclusive, int.MaxValue))
        {
            if (!kv.Key.StartsWith(bucketPrefix, StringComparison.Ordinal))
                yield break; // ordinal order: every key of the key space is contiguous and was passed

            if (kv.Key.IndexOf('/', bucketPrefix.Length) >= 0)
                continue; // a deeper bucket, outside this lock's fence

            yield return kv;
        }
    }
}
