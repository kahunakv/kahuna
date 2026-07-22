
using Kahuna.Server.Persistence;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Completes an actor-owned direct-write proposal only after Raft confirms it, advancing the
/// resident head through the shared archival routine before releasing replication state.
/// </summary>
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
        
        ApplyCommittedHead(entry, proposal, HLCTimestamp.Zero);

        context.BackgroundWriter.Send(BackgroundWriteRequestPool.Rent(
            BackgroundWriteType.QueueStoreKeyValue,
            message.PartitionId,
            proposal.Key,
            proposal.Value,
            proposal.Revision,
            proposal.Expires,
            proposal.LastUsed,
            proposal.LastModified,
            (int)proposal.State,
            proposal.NoRevision
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
