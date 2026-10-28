
using System.Collections.Concurrent;
using System.Diagnostics;
using DotNext;
using Kommander;
using Kommander.Time;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.Replication;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Communication.Grpc;
using TransactionHandle = Kahuna.Shared.KeyValue.TransactionHandle;
using Kahuna.Server.KeyValues.Handlers;

namespace Kahuna.Server.KeyValues.Transactions;

// Release of the locks a session holds.
internal sealed partial class TransactionCoordinator
{

    /// <summary>
    /// Releases every confirmed lock shape held by the transaction and cleans its per-key MVCC state from the
    /// coordinator-owned working set. Called from commit, rollback, the reaper, and the script executor.
    /// </summary>
    /// <remarks>
    /// The ticket-free exclusive-lock release removes the transaction's MVCC entry <b>and</b> clears its write
    /// intent, so one per-key call cleans a point lock, a staged (un-prepared) write, or a read snapshot. Point
    /// locks and staged writes are <b>mandatory</b>: their write intent blocks other writers until cleared.
    /// Read-only MVCC snapshots are <b>best-effort</b> (no intent; trimmed lazily). On a committed transaction
    /// 2PC already finalized the modified keys, so their locks are not released again. Every item is attempted
    /// even after a failure. Returns true when every mandatory release acknowledged; the caller uses it to
    /// decide whether a rollback may report <c>RolledBack</c> or must retry.
    /// </remarks>
    internal async Task<bool> ReleaseWorkingSet(TransactionContext context)
    {
        // Exclude from renewal and drain in-flight renewals before releasing any lock. Cleanup then owns the
        // range locks: no renewal snapshot includes this session, and no re-acquire already sent can land after
        // the release. Otherwise, between renewal stopping (lifecycle fence) and cleanup running, the predicate
        // lock would be unprotected.
        await context.ExcludeAndDrainRangeRenewalsAsync().ConfigureAwait(false);

        // Each mandatory release this pass cannot get acknowledged is recorded on the context, so the commit's
        // detached retry resends exactly those (see RetryUnacknowledgedReleases). A rerun pass, such as a
        // rollback retry, starts the list over.
        context.ClearUnacknowledgedReleases();

        bool committed = context.State == KeyValueTransactionState.Committed;
        bool allMandatoryAcked = true;

        int lockCount = context.LocksAcquired?.Count ?? 0;
        // Clean staged writes only when no prepare ran. While Pending, each modified key holds only its
        // write-time intent and no proposal ticket, so the ticket-free release is the only cleanup. Once a
        // prepare ran (Preparing/Prepared/Committing/RollingBack/RolledBack), 2PC owns those proposals and
        // cleans them via their tickets; releasing here could clear an intent under an in-flight rollback.
        int pendingModifiedCount = context.State == KeyValueTransactionState.Pending ? (context.ModifiedKeys?.Count ?? 0) : 0;
        int readCount = context.ReadKeys?.Count ?? 0;

        // No per-key footprint, or one already released inside its actor turn: skip to the prefix/range releases.
        if (!context.PerKeyWorkingSetReleased && (lockCount > 0 || pendingModifiedCount > 0 || readCount > 0))
        {
            // Intent-bearing per-key cleanup: point locks and, while Pending, staged writes. The set is
            // authoritative for the acknowledgement checks below; read keys append straight to the batch list
            // (their source dictionary already guarantees uniqueness).
            HashSet<(string, KeyValueDurability)> mandatoryKeys = new(lockCount + pendingModifiedCount);

            if (context.LocksAcquired is not null)
            {
                foreach ((string, KeyValueDurability) lockKey in context.LocksAcquired)
                {
                    // A committed modified key was already unlocked by the commit phase; don't release it twice.
                    if (committed && context.ModifiedKeys is not null && context.ModifiedKeys.Contains(lockKey))
                        continue;

                    mandatoryKeys.Add(lockKey);
                }
            }

            if (pendingModifiedCount > 0 && context.ModifiedKeys is not null)
            {
                foreach ((string, KeyValueDurability) modified in context.ModifiedKeys)
                    mandatoryKeys.Add(modified);
            }

            // One batched round-trip cleans every per-key entry; the release handler is idempotent, so a key
            // already cleaned is a harmless no-op.
            List<(string, KeyValueDurability)> perKey = new(mandatoryKeys.Count + readCount);
            foreach ((string, KeyValueDurability) mandatory in mandatoryKeys)
                perKey.Add(mandatory);

            // Best-effort read MVCC cleanup for keys not already covered by a mandatory release or a committed
            // mutation (which the commit phase already cleaned).
            if (context.ReadKeys is not null)
            {
                foreach ((string, KeyValueDurability) readKey in context.ReadKeys.Keys)
                {
                    if (mandatoryKeys.Contains(readKey))
                        continue;

                    if (committed && context.ModifiedKeys is not null && context.ModifiedKeys.Contains(readKey))
                        continue;

                    perKey.Add(readKey);
                }
            }

            try
            {
                if (perKey.Count == 1)
                {
                    (string key, KeyValueDurability durability) = perKey[0];

                    (KeyValueResponseType type, string _) = await manager.LocateAndTryReleaseExclusiveLock(context.TransactionId, key, durability, CancellationToken.None);

                    if (mandatoryKeys.Contains((key, durability)) && !IsReleaseAcked(type))
                    {
                        allMandatoryAcked = false;
                        context.RecordUnacknowledgedRelease(PendingLockRelease.OfPoint(key, durability));
                    }
                }
                else if (perKey.Count > 1)
                {
                    List<(KeyValueResponseType, string, KeyValueDurability)> results =
                        await manager.LocateAndTryReleaseManyExclusiveLocks(context.TransactionId, perKey, CancellationToken.None);

                    // Index responses by key to verify every mandatory key. A mandatory key missing from the
                    // results, or with a non-proof response, is not acknowledged.
                    Dictionary<(string, KeyValueDurability), KeyValueResponseType> releasedByKey = new(results.Count);
                    foreach ((KeyValueResponseType type, string key, KeyValueDurability durability) in results)
                        releasedByKey[(key, durability)] = type;

                    foreach ((string, KeyValueDurability) mandatory in mandatoryKeys)
                    {
                        if (!releasedByKey.TryGetValue(mandatory, out KeyValueResponseType type) || !IsReleaseAcked(type))
                        {
                            allMandatoryAcked = false;
                            context.RecordUnacknowledgedRelease(PendingLockRelease.OfPoint(mandatory.Item1, mandatory.Item2));
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogError("ReleaseWorkingSet keys: {Type} {Message}\n{StackTrace}", ex.GetType().Name, ex.Message, ex.StackTrace);
                allMandatoryAcked = false;

                foreach ((string key, KeyValueDurability durability) in mandatoryKeys)
                    context.RecordUnacknowledgedRelease(PendingLockRelease.OfPoint(key, durability));
            }
        }

        // Prefix locks (mandatory).
        if (context.PrefixLocksAcquired is not null)
        {
            foreach ((string prefixKey, KeyValueDurability durability) in context.PrefixLocksAcquired)
            {
                try
                {
                    KeyValueResponseType type = await manager.LocateAndTryReleaseExclusivePrefixLock(context.TransactionId, prefixKey, durability, CancellationToken.None);
                    if (!IsReleaseAcked(type))
                    {
                        allMandatoryAcked = false;
                        context.RecordUnacknowledgedRelease(PendingLockRelease.OfPrefix(prefixKey, durability));
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError("ReleaseWorkingSet prefix {Prefix}: {Type} {Message}", prefixKey, ex.GetType().Name, ex.Message);
                    allMandatoryAcked = false;
                    context.RecordUnacknowledgedRelease(PendingLockRelease.OfPrefix(prefixKey, durability));
                }
            }
        }

        // Range locks (mandatory).
        if (context.RangeLocksAcquired is not null)
        {
            foreach (RangeLockKey range in context.RangeLocksAcquired.Keys)
            {
                try
                {
                    KeyValueResponseType type = await manager.LocateAndTryReleaseExclusiveRangeLock(
                        context.TransactionId, range.Prefix, range.StartKey, range.StartInclusive,
                        range.EndKey, range.EndInclusive, range.Durability, CancellationToken.None);
                    if (!IsReleaseAcked(type))
                    {
                        allMandatoryAcked = false;
                        context.RecordUnacknowledgedRelease(PendingLockRelease.OfRange(range));
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError("ReleaseWorkingSet range {Prefix}: {Type} {Message}", range.Prefix, ex.GetType().Name, ex.Message);
                    allMandatoryAcked = false;
                    context.RecordUnacknowledgedRelease(PendingLockRelease.OfRange(range));
                }
            }
        }

        return allMandatoryAcked;
    }

    /// <summary>
    /// Whether a lock/MVCC release response is <b>proof</b> that this transaction's effect on the key is gone.
    /// Only an explicit allowlist qualifies, because <c>RolledBack</c> must never be reported while an intent
    /// may remain:
    /// <list type="bullet">
    /// <item><c>Unlocked</c> — the handler removed this transaction's MVCC entry and cleared its write intent.</item>
    /// <item><c>DoesNotExist</c> — no entry, so no effect of this transaction.</item>
    /// <item><c>AlreadyLocked</c> — the current intent belongs to <em>another</em> transaction, so this one's
    /// intent is absent and its MVCC entry was removed. Not counting this as proof would livelock the rollback
    /// against a lock it does not own.</item>
    /// </list>
    /// Anything else (<c>MustRetry</c>/<c>WaitingForReplication</c>: transient; <c>Errored</c>/<c>InvalidInput</c>:
    /// the release did not run; or a missing batch result) is <b>not</b> proof and forces a retry.
    /// </summary>
    internal static bool IsReleaseAcked(KeyValueResponseType type) =>
        type is KeyValueResponseType.Unlocked
             or KeyValueResponseType.DoesNotExist
             or KeyValueResponseType.AlreadyLocked;
}
