
using Google.Protobuf;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Time;
using Nixie;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Base class for handling key/value operations.
/// </summary>
internal abstract class BaseHandler
{
    protected const int ProposalWaitTimeout = 10000;

    private static int proposalId;

    /// <summary>Allocates the next process-wide proposal id, shared with <see cref="CreateProposal"/> so the
    /// per-key and batched write paths draw from the same monotonic sequence.</summary>
    protected static int RentProposalId() => Interlocked.Increment(ref proposalId);

    /// <summary>
    /// Serializes a proposal into the committed <see cref="KeyValueMessage"/> log record. The bytes are
    /// independent of how many writes share a Raft proposal, so an item aggregated into a bulk batch applies on
    /// followers and restores exactly as a single-key set/delete would. Used by the direct-write path for set,
    /// delete, and extend (a delete carries a null value and Deleted state via its request type).
    /// </summary>
    internal static byte[] SerializeProposal(KeyValueRequestType type, KeyValueProposal proposal, HLCTimestamp currentTime)
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
            TimeNode = currentTime.N,
            TimePhysical = currentTime.L,
            TimeCounter = currentTime.C,
            // The follower apply/restore path (KeyValueReplicator) reads NoRevision; carry it so a
            // SetNoRevision write suppresses revision history on followers exactly as it does on the leader.
            NoRevision = proposal.NoRevision
        };

        if (proposal.Value is not null)
            kvm.Value = UnsafeByteOperations.UnsafeWrap(proposal.Value);

        return ReplicationSerializer.Serialize(kvm);
    }

    /// <summary>
    /// Resolves the target partition for a direct write and, for a key-range space, fences it against the
    /// routed descriptor generation. Returns false when the range moved/split since routing so the caller
    /// rejects with MustRetry before installing any intent; hash spaces are never fenced. Shared by the
    /// per-key dispatch (<see cref="CreateProposal"/>) and the batched staging paths so they cannot drift.
    /// </summary>
    protected bool TryResolveDirectWritePartition(KeyValueRequest message, out int partitionId, out long fenceGeneration)
    {
        if (RangeRouting.IsKeyRange(context.KeySpaceRegistry, message.Key))
            // Capture the live descriptor generation even when the inbound write carried none (delete/extend
            // route with no routed generation): the deferred flush fence needs the real admission-time
            // generation to detect a range move during linger, not the zero that would disable it.
            return RangeRouting.TryFenceKeyRange(context.RangeMapStore.Current, message.Key, message.RoutedGeneration, out partitionId, out fenceGeneration);

        partitionId = ResolvePartition(message.Key);
        fenceGeneration = 0; // hash space: never fenced at flush.
        return true;
    }
    
    /// <summary>
    /// Represents the background writer actor reference.
    /// </summary>
    protected readonly KeyValueContext context;
    
    private readonly HashSet<long> revisionsToRemove = [];
    
    protected BaseHandler(KeyValueContext context)
    {
        this.context = context;
    }

    /// <summary>
    /// Arms the actor-enforced deadline on a resumable read continuation from the configured
    /// timeout, so the periodic collect sweep can expire it (resolving waiters with a retryable
    /// result) if its backend read hangs. A configured timeout &lt;= 0 leaves the read unbounded.
    /// </summary>
    protected void ArmReadDeadline(ReadContinuation continuation, HLCTimestamp currentTime)
    {
        int timeoutMs = context.Configuration.ReadContinuationTimeout;
        if (timeoutMs > 0)
            continuation.Deadline = currentTime + timeoutMs;
    }

    /// <summary>
    /// Resolves <paramref name="key"/> to its owning partition id. Key-range spaces look up the
    /// live descriptor; hash spaces use <see cref="DataPartitionRouter"/> over the user partitions
    /// <c>[1, InitialPartitions]</c>. Both routing call sites (locator and handlers) must call
    /// <see cref="RangeRouting.Locate"/> so they cannot drift.
    /// </summary>
    protected int ResolvePartition(string key)
    {
        (int partitionId, _) = RangeRouting.Locate(
            context.KeySpaceRegistry,
            context.RangeMapStore.Current,
            new DataPartitionRouter(context.Raft),
            key);
        return partitionId;
    }
    
    /// <summary>
    /// Creates a proposal for a key/value operation and sends it to the proposal actor for replication.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="entry"></param>
    /// <param name="proposal"></param>
    /// <param name="currentTime"></param>
    /// <returns></returns>
    protected KeyValueResponse CreateProposal(KeyValueRequest message, KeyValueEntry entry, KeyValueProposal proposal, HLCTimestamp currentTime)
    {
        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext = context.ActorContext;

        if (!actorContext.Reply.HasValue)
            return KeyValueStaticResponses.ErroredResponse;

        // Resolve and fence the partition before installing any intent: a key-range write whose routed
        // generation no longer matches the live descriptor (range moved/split since routing) is rejected with
        // MustRetry, leaving nothing pinned. The proposal actor no longer fences — it only replicates.
        if (!TryResolveDirectWritePartition(message, out int partitionId, out long fenceGeneration))
            return new(KeyValueResponseType.MustRetry, 0);

        int currentProposalId = RentProposalId();

        // Carry the live descriptor generation captured at admission (0 for hash) so the aggregator can
        // re-fence at flush time against a range move during linger — for every write type, not just those
        // that arrived with a routed generation.
        proposal.RoutedGeneration = fenceGeneration;

        // Serialize the committed record here, on the owning actor, so the queued envelope carries bytes — not
        // the value graph, which stays in the proposal dictionary below until completion.
        byte[] serialized = SerializeProposal(proposal.Type, proposal, currentTime);

        entry.ReplicationIntent = new()
        {
            ProposalId = currentProposalId,
            Expires = currentTime + ProposalWaitTimeout
        };

        context.Proposals.Add(currentProposalId, proposal);

        KeyValueProposalRequest envelope = new(
            message.Key,
            partitionId,
            currentProposalId,
            message.Durability,
            fenceGeneration,
            serialized,
            actorContext.Self,
            actorContext.Reply.Value.Promise!,
            0 // placeholder: the aggregator stamps EnqueueTicks from its own TimeProvider at admission.
        );

        // Hand the staged write to the partition aggregator. On synchronous backpressure (the partition's
        // queued item/byte bound is full) unwind the just-installed intent + proposal on this same mailbox and
        // return a retryable MustRetry — no completion message will arrive to do it later.
        if (!context.WriteAggregator.TryEnqueue(envelope))
        {
            entry.ReplicationIntent = null;
            context.Proposals.Remove(currentProposalId);
            return new(KeyValueResponseType.MustRetry, 0);
        }

        actorContext.ByPassReply = true;

        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }

    /// <summary>
    /// Returns an existing KeyValueEntry from memory or retrieves it from the persistence layer if there's a cache miss.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="readKeyValueEntry"></param>
    /// <returns></returns>
    /// <param name="populateCache">
    /// When true (default) a disk-resident entry is inserted into the in-memory store as a
    /// read-through cache. Read-only range scans MUST pass false: they enumerate the store
    /// lazily, and inserting during that enumeration mutates the BTree mid-iteration (leaf
    /// splits invalidate the live cursor) and pollutes the cache with the entire scanned range.
    /// </param>
    protected async ValueTask<KeyValueEntry?> GetKeyValueEntry(string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? readKeyValueEntry = null, bool populateCache = true)
    {
        if (!context.Store.TryGetValue(key, out KeyValueEntry? entry))
        {
            if (durability == KeyValueDurability.Persistent)
            {
                if (readKeyValueEntry is null)
                    entry = await context.Raft.ReadScheduler.EnqueueTask(ResolvePartition(key), () => context.PersistenceBackend.GetKeyValue(key));
                else
                    entry = new()
                    {
                        Bucket = GetBucket(key),
                        Value = readKeyValueEntry.Value,
                        Revision = readKeyValueEntry.Revision,
                        FlushedRevision = readKeyValueEntry.Revision,
                        Expires = readKeyValueEntry.Expires,
                        LastUsed = readKeyValueEntry.LastUsed,
                        LastModified = readKeyValueEntry.LastModified,
                        State = readKeyValueEntry.State
                    };

                if (entry is not null)
                {
                    entry.FlushedRevision = entry.Revision; // already on disk
                    entry.LastUsed = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
                    if (populateCache)
                        context.InsertStoreEntry(key, entry);
                    return entry;
                }
            }

            return null;
        }

        return entry;
    }

    /// <summary>
    /// Calculates the bucket name for the key.
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    protected static string? GetBucket(string key)
    {
        int index = key.LastIndexOf('/');
        return index == -1 ? null : key[..index];
    }

    /// <summary>
    /// Removes expired revisions from the KeyValueEntry dictionary, keeping exactly the newest
    /// RevisionRetention entries when no snapshot floor is active. Called at archive time so the
    /// collector never needs a separate metadata pass to enforce the bound.
    ///
    /// <para>When an effective snapshot floor is set, the single highest revision whose
    /// <see cref="KeyValueRevisionEntry.LastModified"/> is at-or-before the floor is pinned as
    /// the floor-boundary revision and exempted from removal. This ensures the as-of version
    /// that snapshot readers need survives in memory beyond the normal retention window; the full
    /// run of revisions between the boundary and now is left to disk and reached by the
    /// disk-fallback read paths on point and range reads. At most RevisionRetention + 1 revisions are kept per
    /// key when a floor is active.</para>
    /// </summary>
    /// <summary>
    /// True when the trim removal set contains the floor-boundary revision — i.e. a floor-protected
    /// in-memory version would be dropped. Correct trim logic never produces this; it is the
    /// detection seam for the <c>MissingProtectedVersion</c> fail-loud counter and is unit-tested
    /// directly since a real occurrence requires a regression in the boundary computation.
    /// </summary>
    internal static bool RemovalSetDropsFloorBoundary(ISet<long> revisionsToRemove, long floorBoundaryRevision) =>
        floorBoundaryRevision >= 0 && revisionsToRemove.Contains(floorBoundaryRevision);

    protected void RemoveExpiredRevisions(KeyValueEntry entry, long refRevision)
    {
        if (entry.Revisions is null)
            return;

        int toBeKept = context.Configuration.RevisionRetention;
        // Keep the newest RevisionRetention revisions (refRevision down to refRevision-toBeKept+1);
        // anything strictly below the cutoff is trimmed unless the floor pins it as the boundary.
        long cutoff = refRevision - toBeKept + 1;

        // Determine the floor-boundary revision: the highest revision that would normally
        // be trimmed (< cutoff) but whose timestamp is at-or-before the effective floor.
        // We protect exactly this one revision so snapshot reads at floor-timestamp still
        // hit memory without disk I/O for the boundary version itself.
        long floorBoundaryRevision = -1;
        SnapshotFloorStore? floorStore = context.SnapshotFloorStore;
        if (floorStore is not null && floorStore.Holds.Count > 0)
        {
            HLCTimestamp now = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
            HLCTimestamp effectiveFloor = floorStore.GetEffectiveFloor(now);
            if (effectiveFloor != HLCTimestamp.Zero)
            {
                foreach (KeyValuePair<long, KeyValueRevisionEntry> kv in entry.Revisions)
                {
                    if (kv.Key < cutoff && kv.Value.LastModified.CompareTo(effectiveFloor) <= 0)
                    {
                        if (kv.Key > floorBoundaryRevision)
                            floorBoundaryRevision = kv.Key;
                    }
                }
            }
        }

        foreach (KeyValuePair<long, KeyValueRevisionEntry> kv in entry.Revisions)
        {
            if (kv.Key < cutoff && kv.Key != floorBoundaryRevision)
                revisionsToRemove.Add(kv.Key);
        }

        // Fail-loud floor guard. The floor pins exactly one in-memory revision: the boundary
        // (newest revision whose LastModified is at-or-before the floor). The loop above exempts
        // it, so a correct trim never schedules it for removal. If it ever appears in the removal
        // set the boundary computation has regressed and a floor-protected version would be dropped
        // from memory — count it (this counter must stay 0 in normal operation) and retain the revision.
        if (RemovalSetDropsFloorBoundary(revisionsToRemove, floorBoundaryRevision))
        {
            SnapshotFloorMetrics.MissingProtectedVersion.Add(1);
            context.Logger.LogError(
                "Floor-boundary revision {Revision} was scheduled for trimming while a snapshot hold protects it; retaining it instead",
                floorBoundaryRevision);
            revisionsToRemove.Remove(floorBoundaryRevision);
        }

        if (revisionsToRemove.Count > 0)
        {
            long bytesFreed = 0;

            foreach (long revision in revisionsToRemove)
            {
                entry.Revisions.TryGetValue(revision, out KeyValueRevisionEntry removed);
                entry.Revisions.Remove(revision);
                bytesFreed += KeyValueStoreAccounting.EstimateRevisionRemovedBytes(entry.Revisions.Count == 0, removed.Value);
            }

            context.AdjustEstimatedEntryBytes(entry, -bytesFreed);
            revisionsToRemove.Clear();
        }
    }

    /// <summary>
    /// Removes the MVCC snapshot for <paramref name="txId"/> from <paramref name="entry"/>,
    /// reclaims its byte budget, and nulls the dictionary when the last entry is removed so the
    /// dictionary object itself is eligible for GC. No-op when the entry is absent.
    /// </summary>
    protected void RemoveMvccEntry(KeyValueEntry entry, HLCTimestamp txId)
    {
        if (entry.MvccEntries is null) return;
        if (!entry.MvccEntries.Remove(txId, out KeyValueMvccEntry? removed)) return;

        bool lastEntry = entry.MvccEntries.Count == 0;
        context.AdjustEstimatedEntryBytes(entry, -KeyValueStoreAccounting.MvccEntryRemovedBytes(lastEntry, removed.Value));
        if (lastEntry)
            entry.MvccEntries = null;
    }

    /// <summary>
    /// Removes MvccEntries from other transactions that have elapsed their Expires deadline, or whose
    /// transaction is definitively no longer live (committed, rolled back, or older than the maximum
    /// possible session lifetime). Called at transaction resolve time (commit/rollback) so the collector
    /// never needs a separate metadata pass for MVCC cleanup.
    /// </summary>
    protected void TrimExpiredMvccEntries(KeyValueEntry entry, HLCTimestamp currentTime)
    {
        if (entry.MvccEntries is null || entry.MvccEntries.Count == 0)
            return;

        // Maximum wall-clock span a session can remain alive: the server's hard maximum session timeout
        // (every admitted session's timeout is clamped to it at Begin, so no live session can exceed it)
        // plus the reaper grace window plus the maximum time a dispatched participant effect can still land
        // after the session is reaped. Any MVCC snapshot whose owning transaction started more than this
        // span ago must be from a dead session — using the *maximum* (not the default) timeout is what keeps
        // this safe for a long-running transaction started with a larger-than-default timeout.
        long sessionMaxLifespanMs = context.Configuration.MaxTransactionTimeout
            + TransactionCoordinator.ReapGraceMs
            + TransactionCoordinator.MaxParticipantEffectTtlMs;

        List<HLCTimestamp>? stale = null;
        foreach ((HLCTimestamp txId, KeyValueMvccEntry mvcc) in entry.MvccEntries)
        {
            if (mvcc.Expires == HLCTimestamp.Zero)
            {
                // Reclaim read-only snapshots for non-expiring keys whose transaction is no longer live:
                // either this actor recorded a terminal decision for it, or enough time has passed that
                // no session started at txId could still be alive regardless of its per-session timeout.
                if (context.WasCommittedHere(txId) || context.WasRolledBackHere(txId) ||
                    (currentTime - txId).TotalMilliseconds > sessionMaxLifespanMs)
                {
                    stale ??= [];
                    stale.Add(txId);
                }
                continue;
            }

            if ((mvcc.Expires - currentTime) > TimeSpan.Zero) continue;
            stale ??= [];
            stale.Add(txId);
        }

        if (stale is null) return;

        foreach (HLCTimestamp txId in stale)
            RemoveMvccEntry(entry, txId);
    }

    /// <summary>
    /// Applies a confirmed persistent commit to a resident entry: clears the transaction's MVCC snapshot
    /// and write intent, archives the superseded revision, advances the entry to the proposal, enqueues
    /// the durable write, records the completion receipt, and — for the anchor key of a Durable
    /// transaction — installs the initial coordinator decision atomically. Shared by the inline commit
    /// path and the off-mailbox completion so both apply identically. The caller is responsible for the
    /// idempotency guards (write intent present and owned by <paramref name="txId"/>) before calling.
    /// </summary>
    protected void ApplyConfirmedCommit(
        KeyValueEntry entry,
        KeyValueProposal proposal,
        HLCTimestamp txId,
        HLCTimestamp currentTime,
        int partitionId,
        string? recordAnchorKey)
    {
        RemoveMvccEntry(entry, txId);
        TrimExpiredMvccEntries(entry, currentTime);
        entry.WriteIntent = null;

        if (entry.Revisions is not null)
            RemoveExpiredRevisions(entry, proposal.Revision);

        if (!proposal.NoRevision)
        {
            bool revisionsCreated = entry.Revisions is null || entry.Revisions.Count == 0;
            entry.Revisions ??= new();
            // Idempotent archive: a revision number can recur across a delete→re-set cycle for the same
            // key. Dictionary.Add throws on a duplicate key; overwriting is safe (same revision, same value).
            entry.Revisions[entry.Revision] = new KeyValueRevisionEntry(entry.Value, entry.LastModified, entry.Expires, entry.State);
            context.AdjustEstimatedEntryBytes(entry, KeyValueStoreAccounting.EstimateRevisionAddedBytes(revisionsCreated, entry.Value));
        }

        int previousValueLength = entry.Value?.Length ?? 0;

        entry.Value = proposal.Value;
        entry.Expires = proposal.Expires;
        entry.Revision = proposal.Revision;
        context.TouchEntry(entry, proposal.LastUsed);
        entry.LastModified = proposal.LastModified;
        entry.State = proposal.State;

        context.AdjustEntryValueBytes(entry, previousValueLength, entry.Value?.Length ?? 0);
        context.EnqueueExpiry(proposal.Key, proposal.Expires);
        if (proposal.State is KeyValueState.Deleted or KeyValueState.Undefined)
            context.EnqueueTombstone(proposal.Key);

        context.BackgroundWriter.Send(BackgroundWriteRequestPool.Rent(
            BackgroundWriteType.QueueStoreKeyValue,
            partitionId,
            proposal.Key,
            proposal.Value,
            proposal.Revision,
            proposal.Expires,
            proposal.LastUsed,
            proposal.LastModified,
            (int)proposal.State,
            proposal.NoRevision
        ));

        context.RecordCommitted(txId);
        context.CompletionReceiptStore.Record(txId, proposal.Key, recordAnchorKey, KeyValueDurability.Persistent);
    }

    /// <summary>
    /// Applies a confirmed rollback to a resident entry: clears the transaction's MVCC snapshot and write
    /// intent and records the rolled-back decision. Shared by the inline rollback path and the off-mailbox
    /// completion. The caller is responsible for the idempotency guards (write intent present and owned by
    /// <paramref name="txId"/>) before calling.
    /// </summary>
    protected void ApplyConfirmedRollback(KeyValueEntry entry, HLCTimestamp txId, HLCTimestamp currentTime)
    {
        RemoveMvccEntry(entry, txId);
        TrimExpiredMvccEntries(entry, currentTime);
        entry.WriteIntent = null;

        context.RecordRolledBack(txId);
    }

    /// <summary>
    /// Resolves and drops orphaned phase-two entries whose off-mailbox worker never returned a completion
    /// (worker death, or a dropped request/completion). An entry still within its dispatch deadline plus a
    /// grace (the phase-two window) is left alone — its completion may yet arrive. Runs on the periodic
    /// collect so it fires even on an otherwise-idle actor, guaranteeing the caller's ask is eventually
    /// resolved (retryable) rather than hanging forever. This is the safety net behind the worker's own
    /// deadline-bounded completion; the common path resolves the promise long before this runs.
    /// </summary>
    protected void SweepExpiredPhaseTwos()
    {
        if (context.PendingPhaseTwos.Count == 0)
            return;

        long now = Environment.TickCount64;
        int graceMs = context.Configuration.Phase2CommitTimeout > 0 ? context.Configuration.Phase2CommitTimeout : 5000;

        List<int>? orphaned = null;
        foreach ((int id, PendingPhaseTwo pending) in context.PendingPhaseTwos)
        {
            if (pending.DeadlineTicks == KeyValuePhaseTwoRequest.NoDeadline)
                continue;
            if (now - (pending.DeadlineTicks + graceMs) > 0)
                (orphaned ??= []).Add(id);
        }

        if (orphaned is null)
            return;

        foreach (int id in orphaned)
        {
            if (!context.PendingPhaseTwos.Remove(id, out PendingPhaseTwo? pending))
                continue;

            context.Logger.LogWarning(
                "KeyValueActor: sweeping orphaned phase-two {OpKind} for {TxId} — no completion arrived by its deadline",
                pending.OpKind, pending.TxId);

            pending.Promise?.TrySetResult(KeyValueStaticResponses.MustRetryResponse);
        }
    }
}
