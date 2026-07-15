
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
    private const int ProposalWaitTimeout = 10000;
    
    private static int proposalId;
    
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
            
        int currentProposalId = Interlocked.Increment(ref proposalId);

        // Carry the key-range routing generation from the request into the proposal so the proposal
        // actor's generation fence can reject a stale-routed write. 0 for hash spaces.
        proposal.RoutedGeneration = message.RoutedGeneration;

        entry.ReplicationIntent = new()
        {
            ProposalId = currentProposalId, 
            Expires = currentTime + ProposalWaitTimeout
        };
            
        context.Proposals.Add(currentProposalId, proposal);
            
        context.ProposalRouter.Send(new(
            message.Type,
            currentProposalId, 
            proposal,
            actorContext.Self, 
            actorContext.Reply.Value.Promise,
            currentTime
        ));

        actorContext.ByPassReply = true;
            
        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }
    
    /// <summary>
    /// Persists and replicates the key/value messages to the Raft partition
    /// </summary>
    /// <param name="type"></param>
    /// <param name="proposal"></param>
    /// <param name="currentTime"></param>
    /// <returns></returns>
    protected async Task<bool> PersistAndReplicateKeyValueMessage(KeyValueRequestType type, KeyValueProposal proposal, HLCTimestamp currentTime)
    {
        if (!context.Raft.Joined)
            return true;

        int partitionId = ResolvePartition(proposal.Key);

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
            NoRevision = proposal.NoRevision
        };

        if (proposal.Value is not null)
            kvm.Value = UnsafeByteOperations.UnsafeWrap(proposal.Value);

        RaftReplicationResult result = await context.Raft.ReplicateLogs(
            partitionId,
            ReplicationTypes.KeyValues,
            ReplicationSerializer.Serialize(kvm)
        );

        if (!result.Success)
        {
            context.Logger.LogWarning("Failed to replicate key/value {Key} Partition={Partition} Status={Status} Ticket={Ticket}", proposal.Key, partitionId, result.Status, result.TicketId);
            
            return false;
        }
        
        context.BackgroundWriter.Send(new(
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

        return result.Success;
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
    /// Removes MvccEntries from other transactions that have elapsed their Expires deadline, or whose
    /// transaction is definitively no longer live (committed, rolled back, or older than the maximum
    /// possible session lifetime). Called at transaction resolve time (commit/rollback) so the collector
    /// never needs a separate metadata pass for MVCC cleanup.
    /// </summary>
    protected void TrimExpiredMvccEntries(KeyValueEntry entry, HLCTimestamp currentTime)
    {
        if (entry.MvccEntries is null || entry.MvccEntries.Count == 0)
            return;

        // Maximum wall-clock span a session can remain alive: its own timeout (we use the configured
        // default as a conservative upper bound) plus the reaper grace window plus the maximum time a
        // dispatched participant effect can still land after the session is reaped. Any MVCC snapshot
        // whose owning transaction started more than this span ago must be from a dead session.
        long sessionMaxLifespanMs = context.Configuration.DefaultTransactionTimeout
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

        long bytesFreed = 0;
        foreach (HLCTimestamp txId in stale)
        {
            if (entry.MvccEntries.Remove(txId, out KeyValueMvccEntry? removed))
                bytesFreed += KeyValueStoreAccounting.MvccEntryRemovedBytes(false, removed.Value);
        }

        if (entry.MvccEntries.Count == 0)
            bytesFreed += KeyValueStoreAccounting.DictionaryOverheadBytes;

        context.AdjustEstimatedEntryBytes(entry, -bytesFreed);
    }
}
