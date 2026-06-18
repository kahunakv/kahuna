
using Kahuna.Server.Configuration;
using Nixie;
using Kommander;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryReleaseExclusiveLockHandler : BaseHandler
{
    public TryReleaseExclusiveLockHandler(KeyValueContext context) : base(context)
    {
        
    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
            return KeyValueStaticResponses.ErroredResponse;
        
        KeyValueEntry? entry = await GetKeyValueEntry(message.Key, message.Durability);
        
        if (entry is null)
            return KeyValueStaticResponses.DoesNotExistResponse;
        
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        if (entry.MvccEntries is null)
            context.Logger.LogWarning("Trying to release exclusive lock for {Key} but MVCC entries are null", message.Key);
        else if (entry.MvccEntries.Remove(message.TransactionId, out KeyValueMvccEntry? removedMvcc))
            context.AdjustEstimatedEntryBytes(entry, -KeyValueStoreAccounting.MvccEntryRemovedBytes(entry.MvccEntries.Count == 0, removedMvcc.Value));

        TrimExpiredMvccEntries(entry, currentTime);

        if (entry.ReplicationIntent is not null)
            return KeyValueStaticResponses.WaitingForReplicationResponse;
        
        if (entry.WriteIntent is not null && entry.WriteIntent.TransactionId != message.TransactionId)
            return KeyValueStaticResponses.AlreadyLockedResponse;

        entry.WriteIntent = null;

        return KeyValueStaticResponses.UnlockedResponse;
    }
}