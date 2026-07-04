
using Google.Protobuf;
using Nixie;
using Kommander;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

internal sealed class KeyValueProposalActor : IActor<KeyValueProposalRequest>
{
    private readonly IRaft raft;

    private readonly IPersistenceBackend persistenceBackend;

    private readonly KahunaConfiguration configuration;

    private readonly KeySpaceRegistry keySpaceRegistry;

    private readonly RangeMapStore rangeMapStore;

    private readonly DataPartitionRouter dataPartitionRouter;

    private readonly ILogger<IKahuna> logger;

    public KeyValueProposalActor(
        IActorContext<KeyValueProposalActor, KeyValueProposalRequest> context,
        IRaft raft,
        IPersistenceBackend persistenceBackend,
        KahunaConfiguration configuration,
        KeySpaceRegistry keySpaceRegistry,
        RangeMapStore rangeMapStore,
        ILogger<IKahuna> logger
    )
    {
        this.raft = raft;
        this.persistenceBackend = persistenceBackend;
        this.configuration = configuration;
        this.keySpaceRegistry = keySpaceRegistry;
        this.rangeMapStore = rangeMapStore;
        this.dataPartitionRouter = new(raft);
        this.logger = logger;
    }

    public async Task Receive(KeyValueProposalRequest message)
    {
        if (!raft.Joined)
            return;
        
        KeyValueProposal proposal = message.Proposal;
        HLCTimestamp currentTime = message.Timestamp;

        int partitionId;

        // Generation fence. For key-range spaces the partition + generation come
        // from the replicated descriptor map; if the range moved or split since the request routed
        // (no covering descriptor, or a bumped generation), reject with MustRetry so the client
        // re-resolves LocateRange and retries — never a double-apply into the stale partition. Hash
        // spaces keep the static GetPartitionKey routing and are not fenced.
        if (RangeRouting.IsKeyRange(keySpaceRegistry, proposal.Key))
        {
            if (!RangeRouting.TryFenceKeyRange(rangeMapStore.Current, proposal.Key, proposal.RoutedGeneration, out partitionId))
            {
                logger.LogWarning(
                    "Generation fence rejected key/value {Key} RoutedGen={Gen} — range moved/split; MustRetry",
                    proposal.Key, proposal.RoutedGeneration);

                message.KeyValueActor.Send(new(
                    KeyValueRequestType.ReleaseProposal,
                    HLCTimestamp.Zero,
                    HLCTimestamp.Zero,
                    proposal.Key,
                    null,
                    null,
                    -1,
                    KeyValueFlags.FenceRetry,
                    0,
                    HLCTimestamp.Zero,
                    proposal.Durability,
                    message.ProposalId,
                    0,
                    message.Promise
                ));

                return;
            }
        }
        else
        {
            partitionId = dataPartitionRouter.Locate(proposal.Key);
        }

        KeyValueMessage kvm = new()
        {
            Type = (int)message.Type,
            Key = proposal.Key,
            Revision = proposal.Revision,
            ExpireNode = proposal.Expires.N,
            ExpirePhysical = proposal.Expires.L,
            ExpireCounter = proposal.Expires.C,
            LastUsedNode = proposal.LastUsed.N,
            LastUsedPhysical = proposal.LastUsed.L,
            LastUsedCounter = proposal.LastUsed.C,
            LastModifiedNode = proposal.LastModified.N,
            LastModifiedPhysical = proposal.LastModified.L,
            LastModifiedCounter = proposal.LastModified.C,
            TimeNode = currentTime.N,
            TimePhysical = currentTime.L,
            TimeCounter = currentTime.C
        };
        
        if (proposal.Value is not null)
            kvm.Value = UnsafeByteOperations.UnsafeWrap(proposal.Value);
        
        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> keyValueActor = message.KeyValueActor;

        RaftReplicationResult result = await raft.ReplicateLogs(
            partitionId,
            ReplicationTypes.KeyValues,
            ReplicationSerializer.Serialize(kvm)
        );

        if (!result.Success)
        {
            logger.LogWarning("Failed to replicate key/value {Key} Partition={Partition} Status={Status} Ticket={Ticket}", proposal.Key, partitionId, result.Status, result.TicketId);

            // A transient replication failure (leadership moved mid-propose, proposal timed out,
            // queue full, restore in progress, …) is retryable: release as MustRetry so the caller
            // retries once the partition settles, rather than surfacing it as terminal Errored.
            KeyValueFlags releaseFlags = IsTransientReplicationStatus(result.Status)
                ? KeyValueFlags.ReplicationRetry
                : KeyValueFlags.None;

            keyValueActor.Send(new(
                KeyValueRequestType.ReleaseProposal,
                HLCTimestamp.Zero,
                HLCTimestamp.Zero,
                proposal.Key,
                null,
                null,
                -1,
                releaseFlags,
                0,
                HLCTimestamp.Zero,
                proposal.Durability,
                message.ProposalId,
                partitionId,
                message.Promise
            ));

            return;
        }

        keyValueActor.Send(new(
            KeyValueRequestType.CompleteProposal,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            proposal.Key, 
            null, 
            null,
            -1,
            KeyValueFlags.None,
            0, 
            HLCTimestamp.Zero,
            proposal.Durability,
            message.ProposalId,
            partitionId,
            message.Promise
        ));
    }

    /// <summary>
    /// True when a failed replication attempt is transient and the write should be retried rather
    /// than reported as a terminal error. These cover leadership churn and back-off-and-retry
    /// conditions that resolve on their own once the partition settles: a follower that just lost
    /// (or has not yet won) leadership, a proposal that timed out or is still pending, a full
    /// proposal queue, a partition still restoring from the WAL, or a partition that moved
    /// generation (the caller re-resolves and retries). A generic <see cref="RaftOperationStatus.Errored"/>
    /// or a structural Raft/membership fault is left terminal — retrying it would not help.
    /// </summary>
    private static bool IsTransientReplicationStatus(RaftOperationStatus status) => status switch
    {
        RaftOperationStatus.NodeIsNotLeader
            or RaftOperationStatus.LeaderInOldTerm
            or RaftOperationStatus.LeaderAlreadyElected
            or RaftOperationStatus.ActiveProposal
            or RaftOperationStatus.ProposalTimeout
            or RaftOperationStatus.ReplicationFailed
            or RaftOperationStatus.Pending
            or RaftOperationStatus.ProposalQueueFull
            or RaftOperationStatus.RestoreInProgress
            or RaftOperationStatus.PartitionMoved => true,
        _ => false
    };
}