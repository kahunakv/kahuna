
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

        bool revisionsCreated = entry.Revisions is null;
        entry.Revisions ??= new();
        entry.Revisions[entry.Revision] = new KeyValueRevisionEntry(entry.Value, entry.LastModified, entry.Expires, entry.State);
        context.AdjustEstimatedEntryBytes(entry, KeyValueStoreAccounting.EstimateRevisionAddedBytes(revisionsCreated, entry.Value));

        int previousValueLength = entry.Value?.Length ?? 0;

        entry.Value = proposal.Value;
        entry.Revision = proposal.Revision;
        entry.Expires = proposal.Expires;
        context.TouchEntry(entry, proposal.LastUsed);
        entry.LastModified = proposal.LastModified;
        entry.State = proposal.State;

        context.AdjustEntryValueBytes(entry, previousValueLength, entry.Value?.Length ?? 0);
        context.EnqueueExpiry(proposal.Key, proposal.Expires);
        if (proposal.State is KeyValueState.Deleted or KeyValueState.Undefined)
            context.EnqueueTombstone(proposal.Key);

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

        KeyValueResponse response;

        switch (proposal.Type)
        {
            case KeyValueRequestType.TrySet:
                response = new(KeyValueResponseType.Set, entry.Revision);
                break;

            case KeyValueRequestType.TryExtend:
                response = new(KeyValueResponseType.Extended, entry.Revision);
                break;

            case KeyValueRequestType.TryDelete:
                response = new(KeyValueResponseType.Deleted, entry.Revision);
                break;

            default:
                throw new NotImplementedException();
        }

        message.Promise?.TrySetResult(response);

        return response;
    }
}
