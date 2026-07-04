
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class ReleaseProposalHandler : BaseHandler
{
    public ReleaseProposalHandler(KeyValueContext context) : base(context)
    {

    }

    public KeyValueResponse Execute(KeyValueRequest message)
    {
        if (!context.Store.TryGetValue(message.Key, out KeyValueEntry? entry))
        {
            context.Logger.LogWarning("KeyValueActor/ReleaseProposal: Key not found for key/value {Key}", message.Key);
            
            message.Promise?.TrySetResult(KeyValueStaticResponses.ErroredResponse);

            return KeyValueStaticResponses.DoesNotExistResponse;
        }

        if (entry.ReplicationIntent is null)
        {
            context.Logger.LogWarning("KeyValueActor/ReleaseProposal: Couldn't find an active write intent on key/value {Key}", message.Key);
            
            message.Promise?.TrySetResult(KeyValueStaticResponses.ErroredResponse);

            return KeyValueStaticResponses.DoesNotExistResponse;
        }

        if (entry.ReplicationIntent.ProposalId != message.ProposalId)
        {
            context.Logger.LogWarning("KeyValueActor/ReleaseProposal: Current write intent on key/value {Key} doesn't match passed id {Current} {Passed}", message.Key, entry.ReplicationIntent.ProposalId, message.ProposalId);
            
            message.Promise?.TrySetResult(KeyValueStaticResponses.ErroredResponse);

            return KeyValueStaticResponses.DoesNotExistResponse;
        }

        if (!context.Proposals.ContainsKey(message.ProposalId))
        {
            context.Logger.LogWarning("KeyValueActor/ReleaseProposal: Proposal on key/value {Key} doesn't exist {ProposalId}", message.Key, message.ProposalId);

            message.Promise?.TrySetResult(KeyValueStaticResponses.ErroredResponse);

            return KeyValueStaticResponses.DoesNotExistResponse;
        }        

        entry.ReplicationIntent = null;
        context.Proposals.Remove(message.ProposalId);

        // A proposal released for a retryable reason resolves as MustRetry so the caller retries
        // rather than treating a transient failure as terminal: a fence rejection (key-range
        // generation moved → re-resolve LocateRange on the correct partition) or a transient
        // replication failure (leadership change, timeout, queue full → retry once the partition
        // settles). Any other release is a genuine error.
        KeyValueResponse response = (message.Flags & (KeyValueFlags.FenceRetry | KeyValueFlags.ReplicationRetry)) != 0
            ? new KeyValueResponse(KeyValueResponseType.MustRetry, 0)
            : KeyValueStaticResponses.ErroredResponse;

        if (message.Promise is null)
            return KeyValueStaticResponses.LockedResponse;

        message.Promise.TrySetResult(response);

        return response;
    }
}