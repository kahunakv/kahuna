
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
                return KeyValueResponse.Denied(KeyValueResponseType.AlreadyLocked, entry.WriteIntent.TransactionId);
        }

        entry.WriteIntent = new()
        {
            TransactionId = message.TransactionId,
            Expires = KeyValueWriteIntentLease.FromRequest(currentTime, message.ExpiresMs),
        };
        
        context.Logger.LogAssignedWriteIntent(message.Key, message.TransactionId);
        
        return KeyValueStaticResponses.LockedResponse;
    }
}
