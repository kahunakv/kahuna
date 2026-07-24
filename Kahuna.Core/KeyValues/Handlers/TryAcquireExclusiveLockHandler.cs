
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

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero || message.ExpiresMs < 0)
            return KeyValueStaticResponses.ErroredResponse;
        
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.ReceiveEvent(context.Raft.GetLocalNodeId(), message.TransactionId);

        if (!context.Store.TryGetValue(message.Key, out KeyValueEntry? entry))
        {
            KeyValueEntry? newEntry = null;

            /// Try to retrieve KeyValue context from persistence
            if (message.Durability == KeyValueDurability.Persistent)
                newEntry = await context.Raft.ReadScheduler.EnqueueTask(message.PartitionId, () => context.PersistenceBackend.GetKeyValue(message.Key));

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
                return KeyValueStaticResponses.LockedResponse;
            }

            // Check if the lease is still active
            if (KeyValueWriteIntentLease.IsLive(entry.WriteIntent, currentTime))
            {
                // A holder whose transaction is already durably decided is not a live conflict: its intent is
                // only waiting for the resolution that clears it, which under deferred settlement runs in the
                // background after the commit was reported. Reporting AlreadyLocked there would abort a caller
                // that merely arrived inside that window, so the transient wait is surfaced instead and the
                // acquire loop re-issues until the resolution lands.
                if (IsAwaitingSettlement(message, entry.WriteIntent.TransactionId))
                    return KeyValueStaticResponses.WaitingForReplicationResponse;

                return KeyValueResponse.Denied(KeyValueResponseType.AlreadyLocked, entry.WriteIntent.TransactionId);
            }
        }

        entry.WriteIntent = new()
        {
            TransactionId = message.TransactionId,
            Expires = KeyValueWriteIntentLease.FromRequest(currentTime, message.ExpiresMs),
        };
        
        context.Logger.LogAssignedWriteIntent(message.Key, message.TransactionId);

        return KeyValueStaticResponses.LockedResponse;
    }

    /// <summary>
    /// Whether the live write intent held by <paramref name="holder"/> only survives because its transaction's
    /// resolution has not run yet. True when the holder owns a durable prepared intent on this key whose canonical
    /// outcome is already terminal — locally recorded, routed from the anchor leader, or flipped on the intent
    /// itself. An intent whose transaction is still undecided is a genuine concurrent writer and is not covered.
    /// </summary>
    private bool IsAwaitingSettlement(KeyValueRequest message, HLCTimestamp holder)
    {
        if (context.PreparedIntentStore?.Get(message.Key) is not { } intent || intent.TransactionId != holder)
            return false;

        return !DurableReadVisibility.IsUndecidedWriter(context, intent, message.ForeignDecisionHint);
    }
}
