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
/// </summary>
internal sealed class TryAcquireExclusiveRangeLockHandler : BaseHandler
{
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
        // refused over a foreign live intent. Shared acquires place no intents but are refused over a foreign
        // live intent all the same: the reader's lock must conflict with the writer's, or the reader holds a
        // lock over a value that is about to change and the writer never learns of the reader (its
        // commit-time probe ran before the lock existed). WriteFence acquires skip intents in both
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
    /// transaction's in-flight write decides the answer. Inspects only; places nothing.
    /// </summary>
    private KeyValueResponse? FindForeignWriter(HLCTimestamp currentTime, KeyValueRequest message, string keySpace)
    {
        foreach ((string key, KeyValueEntry entry) in CoveredResidentEntries(message, keySpace))
        {
            KeyValueResponse? writer = ForeignWriterOn(key, entry, message, currentTime);
            if (writer is not null)
                return writer;
        }

        return DurableWriterOnPointLock(message);
    }

    private KeyValueResponse PlaceWriteIntents(HLCTimestamp currentTime, KeyValueRequest message, string keySpace)
    {
        HLCTimestamp requestedExpiry = KeyValueWriteIntentLease.FromRequest(currentTime, message.ExpiresMs);

        // Stamp per-key write intents atomically: a mid-loop conflict rolls back every intent written this
        // call, so a failed acquire/promotion never strands intents on the range's keys. The LocksByRange
        // record installed by the caller is what blocks the write path.
        List<(KeyValueEntry Entry, KeyValueWriteIntent? Prior)>? stamped = null;

        foreach ((string key, KeyValueEntry entry) in CoveredResidentEntries(message, keySpace))
        {
            KeyValueResponse? writer = ForeignWriterOn(key, entry, message, currentTime);
            if (writer is not null)
            {
                if (stamped is not null)
                    foreach ((KeyValueEntry rollback, KeyValueWriteIntent? prior) in stamped)
                        rollback.WriteIntent = prior;

                return writer;
            }

            if (entry.WriteIntent is not null)
            {
                // Only this transaction's own intent survives ForeignWriterOn; refresh its lease.
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

        KeyValueResponse? durable = DurableWriterOnPointLock(message);
        if (durable is not null)
        {
            if (stamped is not null)
                foreach ((KeyValueEntry rollback, KeyValueWriteIntent? prior) in stamped)
                    rollback.WriteIntent = prior;

            return durable;
        }

        return KeyValueStaticResponses.LockedResponse;
    }

    /// <summary>
    /// Whether <paramref name="entry"/> carries another transaction's in-flight write, and what the acquire
    /// must answer if so. Clears an intent whose lease lapsed, exactly as the read and write handlers do when
    /// they meet one, so it never blocks a lock. Null means the key is free for this transaction; afterwards
    /// <c>entry.WriteIntent</c> is either null or this transaction's own.
    /// <list type="bullet">
    /// <item>A live replication intent (a non-transactional write still in flight): wait, as any write does.</item>
    /// <item>A live foreign write intent whose transaction is already decided but not yet settled: wait. Its
    /// decision is durable and the resolution that clears the intent is on its way; refusing would abort a
    /// caller that merely arrived inside that window.</item>
    /// <item>Any other live foreign write intent: refused with the holder, so the caller can apply its own
    /// wait-or-abort ordering against that transaction.</item>
    /// <item>A durable prepared intent of another transaction that is still undecided while its in-memory
    /// intent is gone (a leader change dropped it): wait for the decision.</item>
    /// </list>
    /// </summary>
    private KeyValueResponse? ForeignWriterOn(string key, KeyValueEntry entry, KeyValueRequest message, HLCTimestamp currentTime)
    {
        if (entry.ReplicationIntent is not null)
        {
            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)
                return KeyValueStaticResponses.WaitingForReplicationResponse;

            entry.ReplicationIntent = null;
        }

        if (entry.WriteIntent is { } intent && intent.TransactionId != message.TransactionId)
        {
            if (!KeyValueWriteIntentLease.IsLive(context, key, intent, currentTime))
            {
                entry.WriteIntent = null;
            }
            else if (IsAwaitingSettlement(key, intent.TransactionId, message.ForeignDecisionHint))
            {
                return KeyValueStaticResponses.WaitingForReplicationResponse;
            }
            else
            {
                DurableTransactionMetrics.RangeLockAcquireIntentConflicts.Add(1);
                return KeyValueResponse.Denied(KeyValueResponseType.AlreadyLocked, intent.TransactionId);
            }
        }

        return UndecidedDurableWriter(key, message);
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

        return UndecidedDurableWriter(message.StartKey, message);
    }

    private KeyValueResponse? UndecidedDurableWriter(string key, KeyValueRequest message)
    {
        if (context.PreparedIntentStore?.Get(key) is { } durable
            && durable.TransactionId != message.TransactionId
            && DurableReadVisibility.IsUndecidedWriter(context, durable, message.ForeignDecisionHint))
            return KeyValueStaticResponses.WaitingForReplicationResponse;

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
