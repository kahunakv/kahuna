
using Kahuna.Server.Configuration;
using Nixie;
using Kommander;
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
        if (message.TransactionId == HLCTimestamp.Zero)
            return KeyValueStaticResponses.ErroredResponse;
        
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.ReceiveEvent(context.Raft.GetLocalNodeId(), message.TransactionId);

        if (!context.Store.TryGetValue(message.Key, out KeyValueEntry? entry))
        {
            KeyValueEntry? newEntry = null;

            /// Try to retrieve KeyValue context from persistence
            if (message.Durability == KeyValueDurability.Persistent)
                newEntry = await context.Raft.ReadThreadPool.EnqueueTask(() => context.PersistenceBackend.GetKeyValue(message.Key));

            newEntry ??= new() { Bucket = GetBucket(message.Key), State = KeyValueState.Undefined, Revision = -1 };
            
            entry = newEntry;

            context.Store.Insert(message.Key, newEntry);
        }

        if (entry.WriteIntent is not null)
        {
            // if the transactionId is the same owner no need to acquire the lock
            if (entry.WriteIntent.TransactionId == message.TransactionId) 
                return KeyValueStaticResponses.LockedResponse;

            // Check if the lease is still active
            if (entry.WriteIntent.Expires != HLCTimestamp.Zero && entry.WriteIntent.Expires - currentTime > TimeSpan.Zero)
                return KeyValueStaticResponses.AlreadyLockedResponse;
        }

        entry.WriteIntent = new()
        {
            TransactionId = message.TransactionId,
            Expires = message.TransactionId + message.ExpiresMs,
        };
        
        context.Logger.LogDebug("Assigned {Key} write intent to TxId={TransactionId}", message.Key, message.TransactionId);
        
        return KeyValueStaticResponses.LockedResponse;
    }
}