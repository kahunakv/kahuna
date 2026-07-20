
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
        (KeyValueResponse? terminal, byte[]? serialized, int partitionId) = await PrepareAndBuild(message);

        // A terminal outcome means the key needs no proposal: a validation rejection (Errored/MustRetry),
        // an ephemeral prepare, an Undefined read, or a not-joined single-node success. Return it as-is.
        if (terminal is not null)
            return terminal;

        // Persistent prepare that produced a proposal. When not joined to a Raft cluster (embedded
        // single-node) PrepareAndBuild already returned Prepared, so we are joined here. Build the committed
        // message once and dispatch the ReplicateLogs round trip to the off-mailbox worker so this actor is
        // free while it runs; the completion resolves Prepared(ticket) on success or Errored on replication
        // failure. When no worker is wired (bare test contexts) replicate inline. The write intent set by
        // PrepareAndBuild pins the entry across the window.
        if (context.PhaseTwoRouter is null)
        {
            (bool success, HLCTimestamp proposalTicket) = await ReplicatePreparedInline(partitionId, serialized!, message.Key);
            return success
                ? new(KeyValueResponseType.Prepared, proposalTicket)
                : KeyValueStaticResponses.ErroredResponse;
        }

        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext = context.ActorContext;
        if (!actorContext.Reply.HasValue)
            return KeyValueStaticResponses.ErroredResponse;

        int phaseTwoId = context.NextPhaseTwoId();
        long deadlineTicks = KeyValuePhaseTwoRequest.DeadlineFrom(context.Configuration.Phase2CommitTimeout);

        PendingPhaseTwo pending = PendingPhaseTwo.ForPrepare(message.TransactionId, message.Key, message.Durability);
        pending.Promise = actorContext.Reply.Value.Promise!;
        pending.DeadlineTicks = deadlineTicks;
        context.PendingPhaseTwos[phaseTwoId] = pending;

        context.PhaseTwoRouter.Send(KeyValuePhaseTwoRequest.ForPrepare(
            phaseTwoId, partitionId, serialized!,
            deadlineTicks, message.RoutedGeneration, actorContext.Self, actorContext.Reply.Value.Promise!));

        actorContext.ByPassReply = true;

        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }

    /// <summary>
    /// Prepare for the partition-batched path: runs the same validation and installs the same write intent
    /// as <see cref="Execute"/>, but instead of proposing per key it hands the serialized proposal back so
    /// the manager can batch every key of a partition into one <c>ReplicateLogs</c>. No Raft call, no
    /// pending registration, no ByPassReply — the manager owns the round trip. A terminal response
    /// (Errored/MustRetry, or Prepared with nothing to propose) is returned unchanged; its type tells the
    /// manager whether to abort the batch or count the key as prepared-with-no-proposal.
    /// </summary>
    public async Task<KeyValueResponse> StageExecute(KeyValueRequest message)
    {
        (KeyValueResponse? terminal, byte[]? serialized, int partitionId) = await PrepareAndBuild(message);

        if (terminal is not null)
            return terminal;

        return KeyValueResponse.Staged(serialized, partitionId);
    }

    /// <summary>
    /// Runs the full prepare validation, installs/refreshes the write intent, and — for a persistent
    /// mutation that needs a proposal — builds the serialized committed message. The single source of truth
    /// for prepare semantics shared by the per-key dispatch path (<see cref="Execute"/>) and the batched
    /// staging path, so the two cannot drift.
    ///
    /// <para>Returns a non-null <c>Terminal</c> response when the key is done without a proposal — a
    /// validation rejection (Errored/MustRetry), an ephemeral prepare, a read of an Undefined value, or a
    /// not-joined single-node success. Otherwise <c>Terminal</c> is null and <c>Serialized</c>/<c>PartitionId</c>
    /// carry the proposal for the caller to replicate (dispatch) or batch (stage).</para>
    /// </summary>
    private async Task<(KeyValueResponse? Terminal, byte[]? Serialized, int PartitionId)> PrepareAndBuild(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
        {
            context.Logger.LogWarning("Cannot prepare mutations for missing transaction id");

            return (KeyValueStaticResponses.ErroredResponse, null, 0);
        }
        
        if (message.CommitId == HLCTimestamp.Zero)
        {
            context.Logger.LogWarning("Cannot prepare mutations for missing commit id");

            return (KeyValueStaticResponses.ErroredResponse, null, 0);
        }

        KeyValueEntry? entry = await GetKeyValueEntry(message.Key, message.Durability);
        if (entry is null)
        {
            context.Logger.LogWarning("Key/Value context is missing for {TransactionId}", message.TransactionId);

            return (KeyValueStaticResponses.ErroredResponse, null, 0);
        }

        if (entry.WriteIntent is not null && entry.WriteIntent.TransactionId != message.TransactionId)
        {
            context.Logger.LogWarning("Write intent conflict between {CurrentTransactionId} and {TransactionId}", entry.WriteIntent.TransactionId, message.TransactionId);

            return (KeyValueStaticResponses.ErroredResponse, null, 0);
        }

        if (entry.Bucket is not null && context.LocksByPrefix.TryGetValue(entry.Bucket, out KeyValueWriteIntent? intent))
        {
            if (intent.TransactionId != message.TransactionId)
            {
                if (intent.Expires - message.CommitId > TimeSpan.Zero)
                    return (new(KeyValueResponseType.MustRetry, 0), null, 0);

                context.LocksByPrefix.Remove(entry.Bucket);
            }
        }

        if (entry.MvccEntries is null)
        {
            context.Logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [1]", message.TransactionId);

            return (KeyValueStaticResponses.ErroredResponse, null, 0);
        }

        if (!entry.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? mvccEntry))
        {
            context.Logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [2]", message.TransactionId);

            return (KeyValueStaticResponses.ErroredResponse, null, 0);
        }

        /// A new revision is available in the context which means the transaction was modified
        if (entry.Revision > mvccEntry.Revision)
        {
            context.Logger.LogWarning("Transaction CommitId={TransactionId} conflicts with Revision={Revision} NewRevision={NewRevision} [3]", message.CommitId, entry.Revision, mvccEntry.Revision);

            return (KeyValueStaticResponses.ErroredResponse, null, 0);
        }

        /// Last modified is higher than the commit id which means the transaction was modified
        if (entry.LastModified.CompareTo(message.CommitId) > 0)
        {
            context.Logger.LogWarning("Transaction CommitId={TransactionId} conflicts with LastModified={LastModified} [4]", message.CommitId, entry.LastModified);

            return (KeyValueStaticResponses.ErroredResponse, null, 0);
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

                return (KeyValueStaticResponses.ErroredResponse, null, 0);
            }
        }

        // Transaction queried a value that didn't exist
        if (mvccEntry.State == KeyValueState.Undefined)
            return (KeyValueStaticResponses.PrepareResponse, null, 0);

        // Persistent-only fences that can reject run BEFORE the write intent is installed, so a rejection
        // leaves no intent behind. The batched staging path only unwinds participants whose staged result was
        // Prepared; a MustRetry emitted after installing the intent would therefore leak an entry-pinning
        // intent the coordinator never sees as prepared and never rolls back. Both checks are read-only, so
        // running them ahead of the install does not change their outcome.
        if (message.Durability == KeyValueDurability.Persistent)
        {
            // Phantom enforcement: reject if a foreign tx holds a range lock covering this key.
            if (RangeLockChecks.KeyCoveredByForeignRangeLock(context, message.Key, entry.Bucket, message.TransactionId, message.CommitId))
                return (new(KeyValueResponseType.MustRetry, 0), null, 0);

            // Key-range generation fence for 2PC (prepare path). A non-zero RoutedGeneration was set by the
            // locator at route time; if the descriptor was bumped since then (split or cutover) the proposal
            // would land on the stale partition. Reject with MustRetry so the coordinator re-resolves and
            // retries the transaction on the correct partition.
            if (RangeRouting.IsKeyRange(context.KeySpaceRegistry, message.Key) &&
                !RangeRouting.TryFenceKeyRange(context.RangeMapStore.Current, message.Key, message.RoutedGeneration, out _))
            {
                context.Logger.LogWarning(
                    "2PC prepare fence rejected key {Key} RoutedGen={Gen} — range moved/split; MustRetry",
                    message.Key, message.RoutedGeneration);
                return (new(KeyValueResponseType.MustRetry, 0), null, 0);
            }
        }

        // In optimistic concurrency, we create the write intent if it doesn't exist
        // this is to ensure that the assigned transaction will win the race.
        // The write intent lease is extended by DefaultTxCompleteTimeout from *now* (prepare/dispatch
        // time), NOT from the transaction start (message.TransactionId). A long-running transaction can
        // reach prepare well after TransactionId + DefaultTxCompleteTimeout has already elapsed; a
        // start-relative lease would be born already-expired, letting the collector, lazy expiry, or a
        // competing transaction clear the intent and evict the entry while the phase-two Raft op is still
        // detached in flight. A dispatch-relative deadline keeps the entry pinned across that window.
        // CommitTimestamp records the ts the committed revision will carry (mvccEntry.LastModified,
        // stamped at MVCC write time in TrySetHandler). This lets snapshot readers determine whether the
        // in-flight write will commit at-or-before their readTimestamp without blocking the actor.
        // CommitId is a different coordinator-supplied fence value and is NOT used here.
        HLCTimestamp intentExpires = context.Raft.HybridLogicalClock
            .TrySendOrLocalEvent(context.Raft.GetLocalNodeId()) + DefaultTxCompleteTimeout;

        if (entry.WriteIntent is null)
        {
            entry.WriteIntent = new()
            {
                TransactionId    = message.TransactionId,
                Expires          = intentExpires,
                CommitTimestamp  = mvccEntry.LastModified,
                RecordAnchorKey  = message.RecordAnchorKey
            };
        }
        else
        {
            entry.WriteIntent.Expires         = intentExpires;
            entry.WriteIntent.CommitTimestamp = mvccEntry.LastModified;
            // Stamp the anchor once the coordinator supplies it; a plain pre-prepare lock had none.
            if (message.RecordAnchorKey is not null)
                entry.WriteIntent.RecordAnchorKey = message.RecordAnchorKey;
        }

        if (message.Durability != KeyValueDurability.Persistent)
            return (KeyValueStaticResponses.PrepareResponse, null, 0);

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

        // Persistent prepare that produced a proposal. When not joined to a Raft cluster (embedded
        // single-node) there is nothing to replicate — the prepare succeeds immediately and hands back no
        // proposal. Otherwise build the committed message once; the caller replicates it (per-key dispatch)
        // or batches it (staging). The write intent set above pins the entry across that window.
        if (!context.Raft.Joined)
            return (new(KeyValueResponseType.Prepared, HLCTimestamp.Zero), null, 0);

        byte[] serialized = BuildPreparedMessage(proposalType, proposal, message.TransactionId, message.RecordAnchorKey);
        int partitionId = ResolvePartition(message.Key);

        return (null, serialized, partitionId);
    }

    /// <summary>
    /// Builds and serializes the committed key/value message for a prepared mutation.
    /// </summary>
    private static byte[] BuildPreparedMessage(KeyValueRequestType type, KeyValueProposal proposal, HLCTimestamp transactionId, string? recordAnchorKey)
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
