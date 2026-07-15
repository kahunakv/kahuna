
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Handles the rollback of mutations in the key-value store based on the given transaction ID and durability requirements.
/// </summary>
/// <remarks>
/// The <c>TryRollbackMutationsHandler</c> is responsible for ensuring that any mutations associated
/// with a given transaction are correctly rolled back if necessary. This handler validates the transaction ID,
/// checks the corresponding key-value context, and performs cleanup of write intents and MVCC entries as needed.
/// It also supports persistence operations for cases requiring data durability.
/// </remarks>
/// <example>
/// This class is typically invoked as part of the Two Phase Commit (2PC) workflow when a rollback request
/// is received.
/// </example>
internal sealed class TryRollbackMutationsHandler : BaseHandler
{
    public TryRollbackMutationsHandler(KeyValueContext context) : base(context)
    {

    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
        {
            context.Logger.LogWarning("Cannot rollback mutations for missing transaction id");

            return KeyValueStaticResponses.ErroredResponse;
        }

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        KeyValueEntry? entry = await GetKeyValueEntry(message.Key, message.Durability);

        if (entry is null)
        {
            context.Logger.LogWarning("Key/Value context is missing for {TransactionId}", message.TransactionId);

            return KeyValueStaticResponses.ErroredResponse;
        }

        if (entry.ReplicationIntent is not null)
        {
            context.Logger.LogWarning("Replication intent is active on key {Key}", message.Key);

            return KeyValueStaticResponses.WaitingForReplicationResponse;
        }

        if (entry.WriteIntent is null)
        {
            bool mvccGone = entry.MvccEntries is null || !entry.MvccEntries.ContainsKey(message.TransactionId);
            if (mvccGone)
            {
                // Distinguish ack-loss re-rollback (this actor made the terminal decision) from a
                // request arriving at a node that never prepared the transaction. Prepare state
                // (WriteIntent + MVCC) lives only in the preparing leader's actor memory and is not
                // Raft-replicated, so after a leader change the new leader has neither — same surface
                // as a true re-rollback. Without an explicit record, returning RolledBack here would
                // silently succeed on a node that holds no state to roll back.
                if (context.WasRolledBackHere(message.TransactionId))
                    return new(KeyValueResponseType.RolledBack, 0);

                // Never prepared here: return MustRetry so the coordinator can re-route to the node
                // that still holds the write intent.
                return KeyValueStaticResponses.MustRetryResponse;
            }

            context.Logger.LogWarning("Write intent is missing for {TransactionId}", message.TransactionId);

            return KeyValueStaticResponses.ErroredResponse;
        }

        if (entry.WriteIntent.TransactionId != message.TransactionId)
        {
            context.Logger.LogWarning("Write intent conflict between {CurrentTransactionId} and {TransactionId}", entry.WriteIntent.TransactionId, message.TransactionId);

            return KeyValueStaticResponses.ErroredResponse;
        }

        if (entry.MvccEntries is null)
        {
            context.Logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [1]", message.TransactionId);

            return KeyValueStaticResponses.ErroredResponse;
        }

        if (!entry.MvccEntries.ContainsKey(message.TransactionId))
        {
            context.Logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [2]", message.TransactionId);

            return KeyValueStaticResponses.ErroredResponse;
        }

        if (message.Durability != KeyValueDurability.Persistent)
        {
            RemoveMvccEntry(entry, message.TransactionId);
            TrimExpiredMvccEntries(entry, currentTime);
            entry.WriteIntent = null;

            context.RecordRolledBack(message.TransactionId);
            return new(KeyValueResponseType.RolledBack);
        }

        (bool success, long rollbackIndex, RaftOperationStatus raftStatus) = await RollbackKeyValueMessage(message.Key, message.ProposalTicketId);

        // Transient Raft failure (e.g. NodeIsNotLeader): preserve MVCC + write intent so the
        // coordinator can retry the rollback against the newly elected leader.
        if (!success && IsTransientRaftStatus(raftStatus))
            return KeyValueStaticResponses.MustRetryResponse;

        // Post-await: write intent may have been cleared by a concurrent idempotent rollback.
        if (entry.WriteIntent is null)
        {
            return success
                ? new(KeyValueResponseType.RolledBack, rollbackIndex)
                : KeyValueStaticResponses.ErroredResponse;
        }

        // Permanent Raft failure: preserve state so the coordinator can retry later. Propagate error.
        if (!success)
            return KeyValueStaticResponses.ErroredResponse;

        // Confirmed terminal rollback: clear MVCC + write intent.
        RemoveMvccEntry(entry, message.TransactionId);
        TrimExpiredMvccEntries(entry, currentTime);
        entry.WriteIntent = null;

        context.RecordRolledBack(message.TransactionId);
        return new(KeyValueResponseType.RolledBack, rollbackIndex);
    }

    /// <summary>
    /// Returns true when the Raft status represents a transient condition that warrants preserving
    /// participant state and returning MustRetry.
    /// </summary>
    private static bool IsTransientRaftStatus(RaftOperationStatus status) => status is
        RaftOperationStatus.NodeIsNotLeader or
        RaftOperationStatus.ProposalQueueFull or
        RaftOperationStatus.RestoreInProgress or
        RaftOperationStatus.ProposalTimeout or
        RaftOperationStatus.ReplicationFailed or
        RaftOperationStatus.OperationCancelled;

    /// <summary>
    /// Rollbacks a previously proposed key value message
    /// </summary>
    private async Task<(bool, long, RaftOperationStatus)> RollbackKeyValueMessage(string key, HLCTimestamp proposalTicketId)
    {
        if (!context.Raft.Joined)
            return (true, 0, RaftOperationStatus.Success);

        int partitionId = ResolvePartition(key);

        // Bound the phase-two Raft wait so a stuck partition cannot park this actor indefinitely.
        // On the deadline the rollback returns the retryable OperationCancelled (classified transient
        // below), and the coordinator re-drives the same ticket — idempotent once it rolls back.
        // A non-positive timeout disables the bound.
        int timeoutMs = context.Configuration.Phase2CommitTimeout;
        using CancellationTokenSource? cts = timeoutMs > 0 ? new CancellationTokenSource(timeoutMs) : null;

        (bool success, RaftOperationStatus status, long logIndex) = await context.Raft.RollbackLogs(
            partitionId,
            proposalTicketId,
            cts?.Token ?? CancellationToken.None
        );

        if (!success)
        {
            context.Logger.LogWarning("Failed to rollback key/value {Key} Partition={Partition} Status={Status}", key, partitionId, status);

            return (false, 0, status);
        }

        context.Logger.LogSuccessfullyRolledBackKeyValue(key, partitionId, logIndex);

        return (success, logIndex, status);
    }
}
