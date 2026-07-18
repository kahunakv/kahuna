
using Google.Protobuf;
using Nixie;
using Kommander;
using Kommander.Time;

using Kahuna.Utils;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Executes the TrySet operation in the key-value store.
/// </summary>
internal sealed class TrySetHandler : BaseHandler
{
    public TrySetHandler(KeyValueContext context) : base(context)
    {

    }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        // Transactional set (a transaction id is present) stages an MVCC entry and returns without a Raft
        // proposal — a different lifecycle from the non-transactional direct write below.
        if (message.TransactionId != HLCTimestamp.Zero)
            return await ExecuteTransactional(message);

        (KeyValueResponse? terminal, KeyValueEntry? entry, KeyValueProposal? proposal, HLCTimestamp currentTime) =
            await BuildNonTransactionalSet(message);

        if (terminal is not null)
            return terminal;

        if (message.Durability == KeyValueDurability.Persistent)
            return CreateProposal(message, entry!, proposal!, currentTime);

        return ApplyEphemeralSet(message, entry!, proposal!);
    }

    /// <summary>
    /// Prepare for the batched write path: runs the same validation and installs the same replication intent
    /// as the persistent branch of <see cref="Execute"/>, but instead of proposing per key it hands the
    /// serialized proposal, its partition, and the proposal id back so the manager can batch every key of a
    /// partition into one <c>ReplicateLogs</c> and then complete/release each by id. Persistent only — the
    /// caller routes ephemeral and transactional sets through <see cref="Execute"/>. A terminal response
    /// (WaitingForReplication / MustRetry / NotSet / Errored) is returned unchanged and installs no intent.
    /// </summary>
    public async Task<KeyValueResponse> StageExecute(KeyValueRequest message)
    {
        if (message.TransactionId != HLCTimestamp.Zero || message.Durability != KeyValueDurability.Persistent)
            return KeyValueStaticResponses.ErroredResponse;

        (KeyValueResponse? terminal, KeyValueEntry? entry, KeyValueProposal? proposal, HLCTimestamp currentTime) =
            await BuildNonTransactionalSet(message);

        if (terminal is not null)
            return terminal;

        // Generation fence, mirroring KeyValueProposalActor: a key-range write whose routed generation no
        // longer matches the live descriptor (range moved/split since routing) is rejected with MustRetry —
        // before installing any intent, so a fenced key leaves nothing pinned. Hash spaces are unfenced.
        if (RangeRouting.IsKeyRange(context.KeySpaceRegistry, message.Key) &&
            !RangeRouting.TryFenceKeyRange(context.RangeMapStore.Current, message.Key, message.RoutedGeneration, out _))
        {
            context.Logger.LogWarning(
                "Batched set fence rejected key {Key} RoutedGen={Gen} — range moved/split; MustRetry",
                message.Key, message.RoutedGeneration);
            return new(KeyValueResponseType.MustRetry, 0);
        }

        // Install the replication intent and register the proposal exactly as CreateProposal would, then hand
        // the serialized log record back instead of sending it to the proposal actor. CompleteProposal (on a
        // committed batch) or ReleaseProposal (on a failed one) applies or unwinds this by ProposalId.
        int currentProposalId = RentProposalId();
        proposal!.RoutedGeneration = message.RoutedGeneration;

        entry!.ReplicationIntent = new()
        {
            ProposalId = currentProposalId,
            Expires = currentTime + ProposalWaitTimeout
        };

        context.Proposals.Add(currentProposalId, proposal);

        byte[] serialized = SerializeProposal(proposal.Type, proposal, currentTime);
        int partitionId = ResolvePartition(message.Key);

        return KeyValueResponse.StagedSet(serialized, partitionId, currentProposalId);
    }

    /// <summary>
    /// Non-transactional set path shared by <see cref="Execute"/> (which proposes) and
    /// <see cref="StageExecute"/> (which batches): loads/creates the entry, runs the lock/intent/flag
    /// validation, and builds the proposal. Returns a non-null <c>Terminal</c> when the set resolves without
    /// a proposal (WaitingForReplication on a live replication intent, MustRetry on a conflicting lock, NotSet
    /// on a failed conditional); otherwise <c>Terminal</c> is null and the entry/proposal/currentTime are set.
    /// </summary>
    private async Task<(KeyValueResponse? Terminal, KeyValueEntry? Entry, KeyValueProposal? Proposal, HLCTimestamp CurrentTime)>
        BuildNonTransactionalSet(KeyValueRequest message)
    {
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        (KeyValueResponse? terminal, KeyValueEntry entry, bool exists) = await LoadAndValidateEntry(message, currentTime);
        if (terminal is not null)
            return (terminal, null, null, currentTime);

        HLCTimestamp newExpires = message.ExpiresMs > 0 ? (currentTime + message.ExpiresMs) : HLCTimestamp.Zero;

        // Check if the value must not be changed according to flags
        if (
            (message.Flags & KeyValueFlags.SetIfExists) != 0 && !exists ||
            (message.Flags & KeyValueFlags.SetIfNotExists) != 0 && exists ||
            (message.Flags & KeyValueFlags.SetIfEqualToValue) != 0 && !((ReadOnlySpan<byte>)entry.Value).SequenceEqual(message.CompareValue) ||
            (message.Flags & KeyValueFlags.SetIfEqualToRevision) != 0 && entry.Revision != message.CompareRevision
        )
            return (new(KeyValueResponseType.NotSet, entry.Revision, entry.LastModified), null, null, currentTime);

        // Operation type is always TrySet here (this handler only serves sets). Use the literal rather than
        // message.Type so the staged path — dispatched as StageSet — still records a TrySet proposal, which is
        // what CompleteProposal and the replicated log record expect.
        KeyValueProposal proposal = new(
            KeyValueRequestType.TrySet,
            message.Key,
            message.Value,
            entry.Revision + 1,
            (message.Flags & KeyValueFlags.SetNoRevision) != 0,
            newExpires,
            currentTime,
            currentTime,
            KeyValueState.Set,
            message.Durability
        );

        return (null, entry, proposal, currentTime);
    }

    /// <summary>
    /// Loads the entry (from the store or, for persistent keys, the backend) creating an Undefined placeholder
    /// on a miss, then runs the shared pre-write validation: an in-flight replication intent (retry), a
    /// foreign exclusive/prefix/range lock (MustRetry). Returns a terminal response when the caller must not
    /// proceed; otherwise fills the entry and whether it currently exists.
    /// </summary>
    private async Task<(KeyValueResponse? Terminal, KeyValueEntry Entry, bool Exists)> LoadAndValidateEntry(
        KeyValueRequest message, HLCTimestamp currentTime)
    {
        bool exists = true;

        if (!context.Store.TryGetValue(message.Key, out KeyValueEntry? entry))
        {
            exists = false;
            KeyValueEntry? newEntry = null;

            /// Try to retrieve KeyValue context from persistence
            if (message.Durability == KeyValueDurability.Persistent)
            {
                newEntry = await context.Raft.ReadScheduler.EnqueueTask(message.PartitionId, () => context.PersistenceBackend.GetKeyValue(message.Key));
                if (newEntry is not null)
                {
                    newEntry.FlushedRevision = newEntry.Revision; // already persisted
                    if (newEntry.State is KeyValueState.Deleted or KeyValueState.Undefined)
                    {
                        newEntry.Value = null;
                        exists = false;
                    }
                    else
                    {
                        if (newEntry.Expires != HLCTimestamp.Zero && newEntry.Expires - currentTime < TimeSpan.Zero)
                        {
                            newEntry.State = KeyValueState.Deleted;
                            newEntry.Value = null;
                            exists = false;
                        }
                        else
                        {
                            exists = true;
                        }
                    }
                }
            }

            newEntry ??= new() { Bucket = GetBucket(message.Key), State = KeyValueState.Undefined, Revision = -1 };

            entry = newEntry;

            context.InsertStoreEntry(message.Key, newEntry);
        }
        else
        {
            if (entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero)
            {
                entry.State = KeyValueState.Deleted;
                context.EnqueueTombstone(message.Key);
            }

            if (entry.State is KeyValueState.Deleted or KeyValueState.Undefined)
            {
                entry.Value = null;
                exists = false;
            }
        }

        // Validate if there's an active replication enty on the key/value entry
        // clients must retry operations to make sure the entry is fully replicated
        // before modifying the entry
        if (entry.ReplicationIntent is not null)
        {
            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)
                return (KeyValueStaticResponses.WaitingForReplicationResponse, entry, exists);

            entry.ReplicationIntent = null;
        }

        // Validate if there's an exclusive key acquired on the lock and whether it is expired
        // if we find expired write intents we can remove it to allow new transactions to proceed
        if (entry.WriteIntent != null)
        {
            if (entry.WriteIntent.TransactionId != message.TransactionId)
            {
                if (entry.WriteIntent.Expires - currentTime > TimeSpan.Zero)
                    return (new(KeyValueResponseType.MustRetry, 0), entry, exists);

                entry.WriteIntent = null;
            }
        }

        // Validate if there's a prefix lock acquired on the bucket
        // if we find expired write intents we can remove it to allow new transactions to proceed
        if (entry.Bucket is not null && context.LocksByPrefix.TryGetValue(entry.Bucket, out KeyValueWriteIntent? intent))
        {
            if (intent.TransactionId != message.TransactionId)
            {
                if (intent.Expires - currentTime > TimeSpan.Zero)
                    return (new(KeyValueResponseType.MustRetry, 0), entry, exists);

                context.LocksByPrefix.Remove(entry.Bucket);
            }
        }

        // Validate if the key falls within any active range lock from another transaction
        if (RangeLockChecks.KeyCoveredByForeignRangeLock(context, message.Key, entry.Bucket, message.TransactionId, currentTime))
            return (new(KeyValueResponseType.MustRetry, 0), entry, exists);

        return (null, entry, exists);
    }

    /// <summary>
    /// Applies an ephemeral (non-persistent) set inline: no Raft proposal, mutate the in-memory entry and
    /// return. Split out so the persistent and ephemeral tails of <see cref="Execute"/> stay readable.
    /// </summary>
    private KeyValueResponse ApplyEphemeralSet(KeyValueRequest message, KeyValueEntry entry, KeyValueProposal proposal)
    {
        if (entry.Revisions is not null)
            RemoveExpiredRevisions(entry, proposal.Revision);

        if (!proposal.NoRevision)
        {
            bool revisionsCreated = entry.Revisions is null || entry.Revisions.Count == 0;
            entry.Revisions ??= new();
            entry.Revisions[entry.Revision] = new KeyValueRevisionEntry(entry.Value, entry.LastModified, entry.Expires, entry.State);
            context.AdjustEstimatedEntryBytes(entry, KeyValueStoreAccounting.EstimateRevisionAddedBytes(revisionsCreated, entry.Value));
        }

        int previousValueLength = entry.Value?.Length ?? 0;

        entry.Value = proposal.Value;
        entry.Revision = proposal.Revision;
        entry.FlushedRevision = entry.Revision; // ephemeral: no disk, always clean
        entry.Expires = proposal.Expires;
        context.TouchEntry(entry, proposal.LastUsed);
        entry.LastModified = proposal.LastModified;
        entry.State = proposal.State;

        context.AdjustEntryValueBytes(entry, previousValueLength, entry.Value?.Length ?? 0);
        context.EnqueueExpiry(message.Key, proposal.Expires);

        return new(KeyValueResponseType.Set, entry.Revision);
    }

    /// <summary>
    /// Transactional set (an MVCC-staged write under an open transaction). Preserved verbatim from the
    /// original handler; shares only the entry load + lock/intent validation with the non-transactional path.
    /// </summary>
    private async Task<KeyValueResponse> ExecuteTransactional(KeyValueRequest message)
    {
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.ReceiveEvent(context.Raft.GetLocalNodeId(), message.TransactionId);

        (KeyValueResponse? terminal, KeyValueEntry entry, bool exists) = await LoadAndValidateEntry(message, currentTime);
        if (terminal is not null)
            return terminal;

        HLCTimestamp newExpires = message.ExpiresMs > 0 ? (currentTime + message.ExpiresMs) : HLCTimestamp.Zero;

        // Temporarily store the value in the MVCC entry
        exists = true;
        entry.MvccEntries ??= new();

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

        if (mvccEntry.State is KeyValueState.Deleted or KeyValueState.Undefined)
        {
            mvccEntry.Value = null;
            exists = false;
        }
        else
        {
            if (mvccEntry.Expires != HLCTimestamp.Zero && mvccEntry.Expires - currentTime < TimeSpan.Zero)
            {
                mvccEntry.State = KeyValueState.Deleted;
                mvccEntry.Value = null;
                exists = false;
            }
        }

        // Check if the value must not be changed according to flags
        if (
            (message.Flags & KeyValueFlags.SetIfExists) != 0 && !exists ||
            (message.Flags & KeyValueFlags.SetIfNotExists) != 0 && exists ||
            (message.Flags & KeyValueFlags.SetIfEqualToValue) != 0 && !((ReadOnlySpan<byte>)mvccEntry.Value).SequenceEqual(message.CompareValue) ||
            (message.Flags & KeyValueFlags.SetIfEqualToRevision) != 0 && mvccEntry.Revision != message.CompareRevision
        )
            return new(KeyValueResponseType.NotSet, mvccEntry.Revision, mvccEntry.LastModified);

        mvccEntry.Value = message.Value;
        mvccEntry.Expires = newExpires;
        mvccEntry.Revision++;
        mvccEntry.LastUsed = currentTime;
        mvccEntry.LastModified = currentTime;
        mvccEntry.State = KeyValueState.Set;
        mvccEntry.NoRevision = (message.Flags & KeyValueFlags.SetNoRevision) != 0;

        // Set write intent so concurrent transactions detect this pending write during ValidateReadSet.
        // Without this, optimistic transactions writing different keys can both pass validation and commit,
        // producing write skew. The intent is cleared by TryReleaseExclusiveLock on abort or
        // overwritten by TryPrepareMutationsHandler on the commit path.
        entry.WriteIntent ??= new()
        {
            TransactionId = message.TransactionId,
            Expires = currentTime + 15000
        };

        return new(KeyValueResponseType.Set, mvccEntry.Revision, currentTime);
    }
}
