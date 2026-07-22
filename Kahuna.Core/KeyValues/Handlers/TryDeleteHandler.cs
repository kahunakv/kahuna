
using Kahuna.Server.Configuration;
using Nixie;

using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Handles the execution of delete operations for key-value data. This handler is responsible
/// for processing requests to delete entries in a key-value store while ensuring proper
/// synchronization and persistence via raft consensus and background writing mechanisms.
/// </summary>
internal sealed class TryDeleteHandler : BaseHandler
{
    public TryDeleteHandler(KeyValueContext context) : base(context)
    {

    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        // Transactional delete (a transaction id is present) stages an MVCC tombstone and returns without a
        // Raft proposal — a different lifecycle from the non-transactional direct delete below.
        if (message.TransactionId != HLCTimestamp.Zero)
            return await ExecuteTransactional(message);

        (KeyValueResponse? terminal, KeyValueEntry? entry, KeyValueProposal? proposal, HLCTimestamp currentTime) =
            await BuildNonTransactionalDelete(message);

        if (terminal is not null)
            return terminal;

        if (message.Durability == KeyValueDurability.Persistent)
            return CreateProposal(message, entry!, proposal!, currentTime);

        return ApplyEphemeralDelete(message, entry!, proposal!);
    }

    /// <summary>
    /// Non-transactional delete path used by <see cref="Execute"/>: loads the entry, runs the lock/intent
    /// validation, and builds the tombstone proposal. Returns a non-null <c>Terminal</c> when the delete
    /// resolves without a
    /// proposal (the key is missing or already deleted → DoesNotExist, a live replication intent →
    /// WaitingForReplication, a conflicting lock → MustRetry); otherwise the entry/proposal/currentTime are set.
    /// </summary>
    private async Task<(KeyValueResponse? Terminal, KeyValueEntry? Entry, KeyValueProposal? Proposal, HLCTimestamp CurrentTime)>
        BuildNonTransactionalDelete(KeyValueRequest message)
    {
        (KeyValueResponse? terminal, KeyValueEntry? entry, HLCTimestamp currentTime) = await LoadAndValidateEntry(message);
        if (terminal is not null)
            return (terminal, null, null, currentTime);

        if (entry!.State == KeyValueState.Deleted)
            return (new(KeyValueResponseType.DoesNotExist, entry.Revision), null, null, currentTime);

        // Operation type is always TryDelete here. Use the literal rather than message.Type so the proposal the
        // aggregator carries records a TryDelete, which is what CompleteProposal and the replicated log record
        // expect.
        KeyValueProposal proposal = new(
            KeyValueRequestType.TryDelete,
            message.Key,
            null,
            entry.Revision,
            false,
            entry.Expires,
            currentTime,
            currentTime,
            KeyValueState.Deleted,
            message.Durability
        );

        return (null, entry, proposal, currentTime);
    }

    /// <summary>
    /// Loads the entry (missing → DoesNotExist) and runs the shared pre-write validation: an in-flight
    /// replication intent (retry), a foreign exclusive/prefix/range lock (MustRetry). Returns a terminal
    /// response when the caller must not proceed; otherwise fills the entry and the resolved current time.
    /// </summary>
    private async Task<(KeyValueResponse? Terminal, KeyValueEntry? Entry, HLCTimestamp CurrentTime)> LoadAndValidateEntry(KeyValueRequest message)
    {
        KeyValueEntry? entry = await GetKeyValueEntry(message.Key, message.Durability);

        // Make sure the current time is ahead of the transactionId
        HLCTimestamp currentTime = message.TransactionId == HLCTimestamp.Zero
            ? context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId())
            : context.Raft.HybridLogicalClock.ReceiveEvent(context.Raft.GetLocalNodeId(), message.TransactionId);

        // Deferred-settlement writer visibility: a foreign durable prepared intent covering this key may hold a
        // committed value not yet materialized locally, so the key is not really missing. Resolve its canonical
        // outcome before treating the key as absent — a committed set materializes into a resident entry (so the
        // tombstone deletes the committed value), an undecided intent retries, and an aborted or committed-delete
        // intent leaves the key absent. No-op off the durable-intent path.
        if (ForeignIntentWriteResolver.Resolve(
                context, message.Key, message.TransactionId, ref entry, ApplyCommittedHead, message.ForeignDecisionHint)
            == ForeignIntentWriteDecision.MustRetry)
            return (KeyValueStaticResponses.MustRetryResponse, entry, currentTime);

        if (entry is null)
            return (KeyValueStaticResponses.DoesNotExistResponse, null, currentTime);

        // Validate if there's an active replication enty on the key/value entry
        // clients must retry operations to make sure the entry is fully replicated
        // before modifying the entry
        if (entry.ReplicationIntent is not null)
        {
            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)
                return (KeyValueStaticResponses.WaitingForReplicationResponse, entry, currentTime);

            entry.ReplicationIntent = null;
        }

        // Validate if there's an exclusive key acquired on the lock and whether it is expired
        // if we find expired write intents we can remove to allow new transactions to proceed
        if (entry.WriteIntent is not null)
        {
            if (entry.WriteIntent.TransactionId != message.TransactionId)
            {
                if (KeyValueWriteIntentLease.IsLive(entry.WriteIntent, currentTime))
                    return (KeyValueStaticResponses.MustRetryResponse, entry, currentTime);

                entry.WriteIntent = null;
            }
        }

        // Validate if there's a prefix lock acquired on the bucket
        // if we find expired write intents we can remove it to allow new transactions to proceed
        if (entry.Bucket is not null && context.LocksByPrefix.TryGetValue(entry.Bucket, out KeyValueWriteIntent? intent))
        {
            if (intent.TransactionId != message.TransactionId)
            {
                if (KeyValueWriteIntentLease.IsLive(intent, currentTime))
                    return (new(KeyValueResponseType.MustRetry, 0), entry, currentTime);

                context.LocksByPrefix.Remove(entry.Bucket);
            }
        }

        // Validate if the key falls within any active range lock from another transaction
        if (RangeLockChecks.KeyCoveredByForeignRangeLock(context, message.Key, entry.Bucket, message.TransactionId, currentTime))
            return (new(KeyValueResponseType.MustRetry, 0), entry, currentTime);

        return (null, entry, currentTime);
    }

    /// <summary>
    /// Applies an ephemeral (non-persistent) delete inline: no Raft proposal, tombstone the in-memory entry
    /// and return.
    /// </summary>
    private KeyValueResponse ApplyEphemeralDelete(KeyValueRequest message, KeyValueEntry entry, KeyValueProposal proposal)
    {
        ApplyCommittedHead(entry, proposal, message.TransactionId);
        entry.FlushedRevision = entry.Revision;

        return new(KeyValueResponseType.Deleted, entry.Revision, entry.LastModified);
    }

    /// <summary>
    /// Transactional delete (an MVCC-staged tombstone under an open transaction). Preserved verbatim from the
    /// original handler; shares only the entry load + lock/intent validation with the non-transactional path.
    /// </summary>
    private async Task<KeyValueResponse> ExecuteTransactional(KeyValueRequest message)
    {
        (KeyValueResponse? terminal, KeyValueEntry? entry, HLCTimestamp currentTime) = await LoadAndValidateEntry(message);
        if (terminal is not null)
            return terminal;

        entry!.MvccEntries ??= new();

        if (!entry.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? mvccEntry))
        {
            bool mvccDictJustCreated = entry.MvccEntries.Count == 0;
            mvccEntry = new()
            {
                Value = entry.Value,
                Revision = entry.Revision,
                Expires = entry.Expires,
                LastUsed = entry.LastUsed,
                LastModified = entry.LastModified,
                State = entry.State
            };

            entry.MvccEntries.Add(message.TransactionId, mvccEntry);
            context.AdjustEstimatedEntryBytes(entry, KeyValueStoreAccounting.MvccEntryAddedBytes(mvccDictJustCreated, mvccEntry.Value));
        }

        if (entry.Revision > mvccEntry.Revision) // early conflict detection
            return KeyValueStaticResponses.AbortedResponse;

        if (entry.State == KeyValueState.Deleted)
            return new(KeyValueResponseType.DoesNotExist, mvccEntry.Revision);

        mvccEntry.State = KeyValueState.Deleted;
        mvccEntry.LastModified = currentTime;

        return new(KeyValueResponseType.Deleted, mvccEntry.Revision, mvccEntry.LastModified);
    }
}
