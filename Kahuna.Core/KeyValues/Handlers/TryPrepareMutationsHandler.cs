
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
        // Only ephemeral participants reach this handler: a crash-atomic (persistent) mutation prepares
        // through the durable-intent canonical-record path, and the manual persistent prepare is rejected at
        // the manager boundary. PrepareAndBuild validates, installs the write intent, and returns a terminal
        // response (Prepared for an ephemeral key or an Undefined read, or a validation rejection); a
        // persistent key returns Errored (the invariant guard).
        return await PrepareAndBuild(message);
    }

    /// <summary>
    /// Runs the full prepare validation and installs/refreshes the write intent. The single source of truth
    /// for prepare semantics.
    ///
    /// <para>Returns a terminal response: a validation rejection (Errored/MustRetry), an ephemeral prepare, or
    /// a read of an Undefined value. A persistent mutation must not reach this handler (it prepares through the
    /// durable-intent path), so it returns Errored.</para>
    /// </summary>
    private async Task<KeyValueResponse> PrepareAndBuild(KeyValueRequest message)
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
                if (KeyValueWriteIntentLease.IsLive(intent, message.CommitId))
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
            return KeyValueStaticResponses.PrepareResponse;

        // A crash-atomic (persistent) mutation prepares through the durable-intent path, never here.
        context.Logger.LogError("Persistent prepare reached the manual mutation handler for {TransactionId} {Key}", message.TransactionId, message.Key);

        return KeyValueStaticResponses.ErroredResponse;
    }
}
