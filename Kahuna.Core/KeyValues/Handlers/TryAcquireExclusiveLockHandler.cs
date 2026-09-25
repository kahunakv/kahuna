
using Kahuna.Server.Configuration;
using Nixie;
using Kommander;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Handles the process of attempting to acquire an exclusive lock on a key-value resource.
/// </summary>
/// <see cref="BaseHandler"/>
internal sealed class TryAcquireExclusiveLockHandler : BaseHandler
{
    public TryAcquireExclusiveLockHandler(KeyValueContext context) : base(context)
    {
        
    }

    public async ValueTask<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero || message.ExpiresMs < 0)
            return KeyValueStaticResponses.ErroredResponse;

        // A yielding transaction that already lost this key to a foreground writer must not re-acquire it: it
        // learns of the loss now instead of at its finalize pin, and never recreates the intent.
        if (context.HasYieldedIntent(message.Key, message.TransactionId))
        {
            Transactions.DurableTransactionMetrics.RecordYieldAbortAtFollowUp();
            return KeyValueStaticResponses.AbortedResponse;
        }

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.ReceiveEvent(context.Raft.GetLocalNodeId(), message.TransactionId);

        if (!context.Store.TryGetValue(message.Key, out KeyValueEntry? entry))
        {
            KeyValueEntry? newEntry = null;

            /// Try to retrieve KeyValue context from persistence
            if (message.Durability == KeyValueDurability.Persistent)
                newEntry = await context.BackendReadScheduler.EnqueueBatchableTask(message.PartitionId, message.Key, context.PointReadExecutor);

            newEntry ??= new() { Bucket = GetBucket(message.Key), State = KeyValueState.Undefined, Revision = -1 };

            entry = newEntry;

            context.InsertStoreEntry(message.Key, newEntry);
        }
        
        // Validate if there's an active replication enty on the key/value entry
        // clients must retry operations to make sure the entry is fully replicated
        // before modifying the entry
        if (entry.ReplicationIntent is not null)
        {
            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)                
                return KeyValueStaticResponses.WaitingForReplicationResponse;
                
            entry.ReplicationIntent = null;
        }

        if (entry.WriteIntent is not null)
        {
            // if the transactionId is the same owner no need to acquire the lock
            if (entry.WriteIntent.TransactionId == message.TransactionId)
            {
                entry.WriteIntent.Expires = KeyValueWriteIntentLease.FromRequest(currentTime, message.ExpiresMs);

                // The intent may have been planted by this transaction's own range lock, which stamps every
                // covered key without materializing a decided-but-unsettled predecessor's committed value.
                // The grant's base must be the committed head, so converge the entry exactly as a fresh grant
                // does before observing it: a base read off the stale resident head would be refused at
                // finalize as overtaken by the very commit this lock was granted over.
                if (!ConvergeCommittedHead(message, ref entry, currentTime))
                    return KeyValueStaticResponses.WaitingForReplicationResponse;

                return new(KeyValueResponseType.Locked, PointLockBase.Observe(entry, currentTime));
            }

            // Check if the lease is still active
            if (KeyValueWriteIntentLease.IsLive(context, message.Key, entry.WriteIntent, currentTime))
            {
                // A holder whose transaction is already durably decided is not a live conflict: its intent is
                // only waiting for the resolution that clears it, which under deferred settlement runs in the
                // background after the commit was reported. Reporting AlreadyLocked there would abort a caller
                // that merely arrived inside that window, so the transient wait is surfaced instead and the
                // acquire loop re-issues until the resolution lands.
                if (IsAwaitingSettlement(message.Key, entry.WriteIntent.TransactionId, message.ForeignDecisionHint))
                    return KeyValueStaticResponses.WaitingForReplicationResponse;

                if (RequesterMayTakeOverYieldingIntent(message) && entry.WriteIntent.Yielding)
                {
                    // The owner's finalize already claimed the intent: its decision is on the way, so wait for
                    // it under the existing acquire loop instead of failing.
                    if (entry.WriteIntent.Pinned)
                    {
                        Transactions.DurableTransactionMetrics.PinnedYieldingWaits.Add(1);
                        return KeyValueStaticResponses.WaitingForReplicationResponse;
                    }

                    // Take the key over and plant the requester's intent below.
                    if (IsStealableYieldingIntent(message.Key, entry.WriteIntent))
                        TakeOverYieldingIntent(message, message.Key, entry, currentTime);
                    else
                        return KeyValueResponse.Denied(KeyValueResponseType.AlreadyLocked, entry.WriteIntent.TransactionId);
                }
                else
                {
                    return KeyValueResponse.Denied(KeyValueResponseType.AlreadyLocked, entry.WriteIntent.TransactionId);
                }
            }
        }

        // Under deferred settlement a committed transaction releases its write intent as soon as its decision is
        // durable, while its value may still sit only in the durable prepared-intent store. A lock granted over
        // that state would hold a key whose committed head the holder cannot see: the holder's pinned read would
        // still answer the pre-commit value, and the settlement applied later would land under its lock. So the
        // acquire resolves the durable intent the way a write does before it grants: a committed value is
        // materialized into the entry, and an undecided one is waited out (the acquire loop retries this answer
        // and routes the holder's decision when it is not local).
        if (!ConvergeCommittedHead(message, ref entry, currentTime))
            return KeyValueStaticResponses.WaitingForReplicationResponse;

        entry.WriteIntent = new()
        {
            TransactionId = message.TransactionId,
            Expires = KeyValueWriteIntentLease.FromRequest(currentTime, message.ExpiresMs),
            AcquiredAt = currentTime,
            Yielding = message.ConflictPolicy == TransactionConflictPolicy.Yield,
        };
        
        context.Logger.LogAssignedWriteIntent(message.Key, message.TransactionId);

        // The grant answers the committed base it protects, so the coordinator can fold the lock into the
        // transaction's read set and refuse a later write of the key if the exclusion was lost to a leader
        // change and another transaction committed over that base.
        return new(KeyValueResponseType.Locked, PointLockBase.Observe(entry, currentTime));
    }

    /// <summary>
    /// Brings <paramref name="entry"/> to the key's committed head before a grant observes its base, so the
    /// base a lock reports is what a commit-time compare will find. Two sources can leave the resident entry
    /// behind the committed head: a decided-but-unsettled durable intent whose value has not materialized yet
    /// (materialized here as a write would), and a head parked behind an in-flight operation (drained here).
    /// Returns false when a foreign durable intent on the key is still undecided: the grant must wait for
    /// that decision, and the caller answers the transient wait the acquire loop retries.
    /// </summary>
    private bool ConvergeCommittedHead(KeyValueRequest message, ref KeyValueEntry entry, HLCTimestamp currentTime)
    {
        if (message.Durability == KeyValueDurability.Persistent)
        {
            KeyValueEntry? resolvedEntry = entry;
            if (ForeignIntentWriteResolver.Resolve(
                    context, message.Key, message.TransactionId, ref resolvedEntry, ApplyCommittedHead, message.ForeignDecisionHint)
                == ForeignIntentWriteDecision.MustRetry)
                return false;

            entry = resolvedEntry!;
        }

        // Converge a head parked behind an in-flight operation before the base is observed: a lock that
        // reported a stale resident head as its base would be refused at commit against the committed history
        // the parked head carries, even though nothing wrote over it.
        if (entry.PendingCommittedHead is not null)
            TryDrainPendingCommittedHead(message.Key, entry, currentTime);

        return true;
    }
}
