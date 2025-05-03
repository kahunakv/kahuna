
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
        
        if (entry.MvccEntries is null)
            context.Logger.LogWarning("Trying to release exclusive lock for {Key} but MVCC entries are null", message.Key);
        else
            entry.MvccEntries.Remove(message.TransactionId);
        
        if (entry.WriteIntent is not null && entry.WriteIntent.TransactionId != message.TransactionId)
            return KeyValueStaticResponses.AlreadyLockedResponse;

        entry.WriteIntent = null;

        return KeyValueStaticResponses.UnlockedResponse;
    }
}