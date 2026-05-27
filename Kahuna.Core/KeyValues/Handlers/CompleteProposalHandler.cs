
using Kahuna.Server.Persistence;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class CompleteProposalHandler : BaseHandler
{
    public CompleteProposalHandler(KeyValueContext context) : base(context)
    {

    }

    public KeyValueResponse Execute(KeyValueRequest message)
    {
        if (!context.Store.TryGetValue(message.Key, out KeyValueEntry? entry))
        {
            context.Logger.LogWarning("KeyValueActor/CompleteProposal: Key/Value not found for resource {Key}", message.Key);
            
            message.Promise?.TrySetResult(KeyValueStaticResponses.ErroredResponse);

            return KeyValueStaticResponses.DoesNotExistResponse;
        }

        if (entry.ReplicationIntent is null)
        {
            context.Logger.LogWarning("KeyValueActor/CompleteProposal: Couldn't find an active write intent on key/value {Key}", message.Key);
            
            message.Promise?.TrySetResult(KeyValueStaticResponses.ErroredResponse);

            return KeyValueStaticResponses.DoesNotExistResponse;
        }

        if (entry.ReplicationIntent.ProposalId != message.ProposalId)
        {
            context.Logger.LogWarning("KeyValueActor/CompleteProposal: Current write intent on key/value {Key} doesn't match passed id {Current} {Passed}", message.Key, entry.ReplicationIntent.ProposalId, message.ProposalId);
            
            message.Promise?.TrySetResult(KeyValueStaticResponses.ErroredResponse);

            return KeyValueStaticResponses.DoesNotExistResponse;
        }

        if (!context.Proposals.TryGetValue(message.ProposalId, out KeyValueProposal? proposal))
        {
            context.Logger.LogWarning("KeyValueActor/CompleteProposal: Proposal on key/value {Key} doesn't exist {ProposalId}", message.Key, message.ProposalId);

            message.Promise?.TrySetResult(KeyValueStaticResponses.ErroredResponse);

            return KeyValueStaticResponses.DoesNotExistResponse;
        }
        
        if (entry.Revisions is not null)
            RemoveExpiredRevisions(entry, proposal.Revision);        
        
        entry.Revisions ??= new();                       
        entry.Revisions.Add(entry.Revision, entry.Value);
        
        entry.Value = proposal.Value;
        entry.Revision = proposal.Revision;
        entry.Expires = proposal.Expires;
        entry.LastUsed = proposal.LastUsed;
        entry.LastModified = proposal.LastModified;
        entry.State = proposal.State;
        
        context.BackgroundWriter.Send(new(
            BackgroundWriteType.QueueStoreKeyValue,
            message.PartitionId,
            proposal.Key,
            proposal.Value,
            proposal.Revision,
            proposal.Expires,
            proposal.LastUsed,
            proposal.LastModified,
            (int)proposal.State
        ));
        
        entry.ReplicationIntent = null;
        context.Proposals.Remove(message.ProposalId);

        if (message.Promise is null)
            return new(KeyValueResponseType.Set, proposal.Revision);

        switch (proposal.Type)
        {
            case KeyValueRequestType.TrySet:
                message.Promise.TrySetResult(new(KeyValueResponseType.Set, entry.Revision));
                break;

            case KeyValueRequestType.TryExtend:
                message.Promise.TrySetResult(new(KeyValueResponseType.Extended, entry.Revision));
                break;

            case KeyValueRequestType.TryDelete:
                message.Promise.TrySetResult(new(KeyValueResponseType.Deleted, entry.Revision));
                break;
            
            default:
                throw new NotImplementedException();
        }
        
        return KeyValueStaticResponses.ErroredResponse;
    }
}