
using Kahuna.Server.Persistence;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryExtendHandler : BaseHandler
{
    public TryExtendHandler(
        Dictionary<string, KeyValueContext> keyValuesStore,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistence persistence,
        IRaft raft,
        ILogger<IKahuna> logger
    ) : base(keyValuesStore, backgroundWriter, persistence, raft, logger)
    {

    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Durability);
        
        if (context is null)
            return KeyValueStaticResponses.DoesNotExistResponse;
        
        if (context.State == KeyValueState.Deleted)
            return KeyValueStaticResponses.DoesNotExistResponse;
        
        if (context.WriteIntent is not null)
            return new(KeyValueResponseType.MustRetry, 0);
        
        HLCTimestamp currentTime = await raft.HybridLogicalClock.TrySendOrLocalEvent();
        
        if (context.Expires != HLCTimestamp.Zero && context.Expires - currentTime < TimeSpan.Zero)
            return new(KeyValueResponseType.DoesNotExist, context.Revision);
        
        KeyValueProposal proposal = new(
            message.Key,
            context.Value,
            context.Revision,
            currentTime + message.ExpiresMs,
            currentTime,
            context.State
        );
        
        if (message.Durability == KeyValueDurability.Persistent)
        {
            bool success = await PersistAndReplicateKeyValueMessage(message.Type, proposal, currentTime);
            if (!success)
                return KeyValueStaticResponses.ErroredResponse;
        }
        
        context.Expires = proposal.Expires;
        context.LastUsed = proposal.LastUsed;

        return new(KeyValueResponseType.Extended, context.Revision);
    }
}