
using Google.Protobuf;

using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Handles the preparation of mutations for the key-value store.
/// Responsible for validating and preparing mutation requests before they are executed or persisted.
/// Ensures the mutations adhere to required conditions for proper operation within the key-value system.
/// </summary>
internal sealed class TryPrepareMutationsHandler : BaseHandler
{
    private const int DefaultTxCompleteTimeout = 15000;
    
    public TryPrepareMutationsHandler(KeyValueContext context) : base(context)
    {
        
    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
        {
            context.Logger.LogWarning("Cannot prepare mutations for missing transaction id");
            
            return KeyValueStaticResponses.ErroredResponse;
        }
        
        if (message.CommitId == HLCTimestamp.Zero)
        {
            context.Logger.LogWarning("Cannot prepare mutations for missing commit id");
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        KeyValueEntry? entry = await GetKeyValueEntry(message.Key, message.Durability);
        if (entry is null)
        {
            context.Logger.LogWarning("Key/Value context is missing for {TransactionId}", message.TransactionId);
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        if (entry.WriteIntent is not null && entry.WriteIntent.TransactionId != message.TransactionId)
        {
            context.Logger.LogWarning("Write intent conflict between {CurrentTransactionId} and {TransactionId}", entry.WriteIntent.TransactionId, message.TransactionId);
        
            return KeyValueStaticResponses.ErroredResponse;
        }
        
        if (entry.Bucket is not null && context.LocksByPrefix.TryGetValue(entry.Bucket, out KeyValueWriteIntent? intent))
        {
            if (intent.TransactionId != message.TransactionId)
            {
                if (intent.Expires - message.CommitId > TimeSpan.Zero)
                    return new(KeyValueResponseType.MustRetry, 0);
            
                context.LocksByPrefix.Remove(entry.Bucket);
            }
        }

        if (entry.MvccEntries is null)
        {
            context.Logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [1]", message.TransactionId);
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        if (!entry.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? mvccEntry))
        {
            context.Logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [2]", message.TransactionId);
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        /// A new revision is available in the context which means the transaction was modified
        if (entry.Revision > mvccEntry.Revision)
        {
            context.Logger.LogWarning("Transaction CommitId={TransactionId} conflicts with Revision={Revision} NewRevision={NewRevision} [3]", message.CommitId, entry.Revision, mvccEntry.Revision);
            
            return KeyValueStaticResponses.ErroredResponse;
        }
        
        /// Last modified is higher than the commit id which means the transaction was modified
        if (entry.LastModified.CompareTo(message.CommitId) > 0)
        {
            context.Logger.LogWarning("Transaction CommitId={TransactionId} conflicts with LastModified={LastModified} [4]", message.CommitId, entry.LastModified);
            
            return KeyValueStaticResponses.ErroredResponse;
        }

        // Higher transactions with staged mutations have seen the committed value.
        // Read-only MVCC snapshots must not abort the lock owner; otherwise two
        // optimistic read-modify-write transactions can both abort when the lower
        // transaction wins the write lock and the higher transaction only read.
        foreach ((HLCTimestamp key, KeyValueMvccEntry otherMvccEntry) in entry.MvccEntries)
        {
            bool hasStagedMutation =
                otherMvccEntry.Revision != entry.Revision ||
                otherMvccEntry.State != entry.State ||
                otherMvccEntry.Expires != entry.Expires ||
                otherMvccEntry.LastModified != entry.LastModified;
            
            if (hasStagedMutation && key.CompareTo(message.TransactionId) > 0)
            {
                context.Logger.LogWarning("Transaction {TransactionId} conflicts with {ExistingTransactionId} [5]", message.TransactionId, key);
            
                return KeyValueStaticResponses.ErroredResponse;
            }
        }
        
        // Transaction queried a value that didn't exist
        if (mvccEntry.State == KeyValueState.Undefined)
            return KeyValueStaticResponses.PrepareResponse;

        // In optimistic concurrency, we create the write intent if it doesn't exist
        // this is to ensure that the assigned transaction will win the race.
        // The write intent lease will by extended by DefaultTxCompleteTimeout
        // it will give the transaction enough time to commit or rollback
        // CommitTimestamp records the ts the committed revision will carry (mvccEntry.LastModified,
        // stamped at MVCC write time in TrySetHandler). This lets snapshot readers determine whether the
        // in-flight write will commit at-or-before their readTimestamp without blocking the actor.
        // CommitId is a different coordinator-supplied fence value and is NOT used here.
        if (entry.WriteIntent is null)
        {
            entry.WriteIntent = new()
            {
                TransactionId    = message.TransactionId,
                Expires          = message.TransactionId + DefaultTxCompleteTimeout,
                CommitTimestamp  = mvccEntry.LastModified,
                RecordAnchorKey  = message.RecordAnchorKey,
                EmbeddedDecision = message.EmbeddedDecision
            };
        }
        else
        {
            entry.WriteIntent.Expires         = message.TransactionId + DefaultTxCompleteTimeout;
            entry.WriteIntent.CommitTimestamp = mvccEntry.LastModified;
            // Stamp the anchor once the coordinator supplies it; a plain pre-prepare lock had none.
            if (message.RecordAnchorKey is not null)
                entry.WriteIntent.RecordAnchorKey = message.RecordAnchorKey;
            // Likewise the initial decision, present only on the anchor key of a Durable transaction.
            if (message.EmbeddedDecision is not null)
                entry.WriteIntent.EmbeddedDecision = message.EmbeddedDecision;
        }

        if (message.Durability != KeyValueDurability.Persistent)
            return KeyValueStaticResponses.PrepareResponse;

        // Phantom enforcement: reject if a foreign tx holds a range lock covering this key.
        if (RangeLockChecks.KeyCoveredByForeignRangeLock(context, message.Key, entry.Bucket, message.TransactionId, message.CommitId))
            return new(KeyValueResponseType.MustRetry, 0);

        // Key-range generation fence for 2PC (prepare path). A non-zero RoutedGeneration
        // was set by the locator at route time; if the descriptor was bumped since then (split or
        // cutover) the proposal would land on the stale partition. Reject with MustRetry so the
        // coordinator re-resolves and retries the transaction on the correct partition.
        if (RangeRouting.IsKeyRange(context.KeySpaceRegistry, message.Key) &&
            !RangeRouting.TryFenceKeyRange(context.RangeMapStore.Current, message.Key, message.RoutedGeneration, out _))
        {
            context.Logger.LogWarning(
                "2PC prepare fence rejected key {Key} RoutedGen={Gen} — range moved/split; MustRetry",
                message.Key, message.RoutedGeneration);
            return new(KeyValueResponseType.MustRetry, 0);
        }

        KeyValueProposal proposal = new(
            message.Type,
            message.Key,
            mvccEntry.Value,
            mvccEntry.Revision,
            mvccEntry.NoRevision,
            mvccEntry.Expires,
            mvccEntry.LastUsed,
            mvccEntry.LastModified,
            mvccEntry.State,
            message.Durability
        );

        KeyValueRequestType proposalType = proposal.State == KeyValueState.Deleted
            ? KeyValueRequestType.TryDelete
            : KeyValueRequestType.TrySet;

        // Persistent prepare. When not joined to a Raft cluster (embedded single-node) the proposal is an
        // immediate success. Otherwise build the committed message once and dispatch the ReplicateLogs
        // round trip to the off-mailbox worker so this actor is free while it runs; the completion resolves
        // Prepared(ticket) on success or Errored on replication failure. When no worker is wired (bare test
        // contexts) replicate inline. The write intent set above pins the entry across the window.
        if (!context.Raft.Joined)
            return new(KeyValueResponseType.Prepared, HLCTimestamp.Zero);

        byte[] serialized = BuildPreparedMessage(proposalType, proposal, message.TransactionId, message.RecordAnchorKey, message.EmbeddedDecision);
        int partitionId = ResolvePartition(message.Key);

        if (context.PhaseTwoRouter is null)
        {
            (bool success, HLCTimestamp proposalTicket) = await ReplicatePreparedInline(partitionId, serialized, message.Key);
            return success
                ? new(KeyValueResponseType.Prepared, proposalTicket)
                : KeyValueStaticResponses.ErroredResponse;
        }

        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext = context.ActorContext;
        if (!actorContext.Reply.HasValue)
            return KeyValueStaticResponses.ErroredResponse;

        int phaseTwoId = context.NextPhaseTwoId();
        context.PendingPhaseTwos[phaseTwoId] = PendingPhaseTwo.ForPrepare(message.TransactionId, message.Key, message.Durability);

        context.PhaseTwoRouter.Send(KeyValuePhaseTwoRequest.ForPrepare(
            phaseTwoId, partitionId, serialized, actorContext.Self, actorContext.Reply.Value.Promise));

        actorContext.ByPassReply = true;

        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }

    /// <summary>
    /// Builds and serializes the committed key/value message for a prepared mutation.
    /// </summary>
    private static byte[] BuildPreparedMessage(KeyValueRequestType type, KeyValueProposal proposal, HLCTimestamp transactionId, string? recordAnchorKey, Transactions.Data.CoordinatorDecisionRecord? embeddedDecision)
    {
        KeyValueMessage kvm = new()
        {
            Type = (int)type,
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
            TimePhysical = transactionId.L,
            TimeCounter = transactionId.C,
            // Carry the transaction identity so applying or restoring this committed record records a
            // durable completion receipt (recovers Committed after a leader change erases prepare state).
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            NoRevision = proposal.NoRevision
        };

        if (recordAnchorKey is not null)
            kvm.RecordAnchorKey = recordAnchorKey;

        // Serialize the initial decision into the committed envelope so a follower's replication apply and a
        // cold-restart restore install it from the same log record that carries the anchor value. The leader
        // installs it inline at commit from the write intent; this copy covers every other node.
        if (embeddedDecision is not null)
            kvm.EmbeddedDecision = UnsafeByteOperations.UnsafeWrap(
                Transactions.CoordinatorDecisionStore.SerializeRecord(embeddedDecision));

        if (proposal.Value is not null)
            kvm.Value = UnsafeByteOperations.UnsafeWrap(proposal.Value);

        return ReplicationSerializer.Serialize(kvm);
    }

    /// <summary>
    /// Replicates a prepared message inline (used only when no off-mailbox worker is wired). Returns
    /// success and the proposal ticket, or failure on a replication error.
    /// </summary>
    private async Task<(bool, HLCTimestamp)> ReplicatePreparedInline(int partitionId, byte[] serialized, string key)
    {
        RaftReplicationResult result = await context.Raft.ReplicateLogs(
            partitionId,
            ReplicationTypes.KeyValues,
            serialized,
            autoCommit: false
        );

        if (!result.Success)
        {
            context.Logger.LogWarning("Failed to propose key/value {Key} Partition={Partition} Status={Status}", key, partitionId, result.Status);

            return (false, HLCTimestamp.Zero);
        }

        context.Logger.LogSuccessfullyProposedKeyValue(key, partitionId, result.LogIndex);

        return (true, result.TicketId);
    }
}
