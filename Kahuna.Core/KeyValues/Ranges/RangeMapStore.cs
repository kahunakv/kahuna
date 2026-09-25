using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// The replicated source of truth for the range-descriptor map. Wraps an
/// immutable in-memory <see cref="RangeMap"/> and keeps it consistent across the cluster by
/// committing every change through the Raft meta partition.
///
/// <para>
/// <b>Where it lives.</b> As of Kommander 0.11.0 the <i>system</i> partition (id 0) hosts consumer
/// data alongside the partition-map coordinator, distinguished by log type: the coordinator owns the
/// <c>_RaftSystem</c> type while any other type is routed to Kahuna's
/// <c>OnReplicationReceived</c>/<c>OnLogRestored</c> callbacks. The map is therefore replicated on
/// <b><see cref="MetaPartitionId"/> (= 0)</b> using <see cref="ReplicationTypes.RangeMap"/>. Hosting
/// the map on P0 collapses the old P0+P1 leader colocation: partition lifecycle
/// (<c>CreatePartitionAsync</c>/<c>RemovePartitionAsync</c>) and the map cutover both require the
/// <b>P0 leader</b>, so a single split/merge orchestrator needs to lead only P0. Ranged <i>data</i>
/// lives on partitions ≥ 1 (P0 is reserved for the map + coordinator; the reservation contract is
/// enforced by <see cref="MutateAsync"/> and consulted by routing).
/// </para>
///
/// <para>
/// <b>Single writer.</b> <see cref="MutateAsync"/> is the only mutator. It is serialized on
/// this node by <see cref="mutateGate"/> and globally by the meta partition's Raft log, so the
/// committed history of the map is linear and every committed map satisfies
/// <see cref="RangeMap.Validate"/> (invariant G1). Snapshot (not delta) semantics: each entry
/// carries the full descriptor set, so replaying the meta log on restore/failover converges on the
/// latest map and re-applying the same entry is idempotent.
/// </para>
/// </summary>
internal sealed class RangeMapStore : IDisposable
{
    /// <summary>
    /// The partition that hosts the range-descriptor map: the Kommander system partition (id 0),
    /// shared with the partition-map coordinator by log type (Kommander 0.11.0+). Always exists.
    /// Ranged data lives on partitions ≥ <see cref="FirstDataPartitionId"/>.
    /// </summary>
    public const int MetaPartitionId = 0;

    /// <summary>Lowest partition id usable for ranged data (partition 0 is reserved for the map +
    /// system coordinator).</summary>
    public const int FirstDataPartitionId = 1;

    /// <summary>Default number of committed mutations between meta-partition checkpoints.</summary>
    public const int DefaultCheckpointEveryMutations = 32;

    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;

    private readonly SemaphoreSlim mutateGate = new(1, 1);

    /// <summary>Path of the durable snapshot file, or null when disk persistence is disabled.</summary>
    private readonly string? snapshotPath;

    /// <summary>Serializes the temp-write + atomic-rename so concurrent writers can't corrupt the file.</summary>
    private readonly object fileLock = new();

    private readonly int checkpointEveryMutations;

    /// <summary>
    /// Upper bound on how long <see cref="MutateAsync"/> waits for the meta partition to settle
    /// after a proposal ended without a verdict, before it reports the mutation unconfirmed.
    /// </summary>
    private readonly TimeSpan indeterminateOutcomeBudget;

    /// <summary>Default for the settle wait: matches the election budget Kommander gives <see cref="IRaft.WaitForLeader"/>.</summary>
    public static readonly TimeSpan DefaultIndeterminateOutcomeBudget = TimeSpan.FromSeconds(10);

    /// <summary>Pause between confirmed-read attempts while the meta partition is still electing or arming its barrier.</summary>
    private static readonly TimeSpan IndeterminateOutcomePollInterval = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Proposals one <see cref="MutateAsync"/> call issues at most: the original plus re-proposals
    /// after a confirmed read showed the entry did not land. Bounds a call under sustained churn.
    /// </summary>
    internal const int MaxProposeAttempts = 3;

    /// <summary>Mutated only under <see cref="mutateGate"/>.</summary>
    private int mutationsSinceCheckpoint;

    private volatile RangeMap current = RangeMap.Empty;

    // Bumped on every swap of <see cref="current"/>. Lets pull-based consumers (the durable-intent
    // stores' per-partition checkpoint guards) detect that key→partition routing may have changed
    // without subscribing to map updates: they record the value they observed and rewrite when it moved.
    private long mapVersion;

    /// <summary>Monotonic stamp of the installed map: changes whenever <see cref="Current"/> is swapped.</summary>
    public long MapVersion => Interlocked.Read(ref mapVersion);

    /// <param name="storagePath">Directory for the durable snapshot file; empty disables disk persistence.</param>
    /// <param name="storageRevision">Per-node revision so each node's snapshot file is distinct and stable across restarts.</param>
    /// <param name="checkpointEveryMutations">Committed mutations between meta-partition checkpoints; ≤ 0 disables periodic checkpointing.</param>
    /// <param name="indeterminateOutcomeBudget">Settle wait after a proposal without a verdict; null selects <see cref="DefaultIndeterminateOutcomeBudget"/>.</param>
    public RangeMapStore(
        IRaft raft,
        string? storagePath,
        string? storageRevision,
        ILogger<IKahuna> logger,
        int checkpointEveryMutations = DefaultCheckpointEveryMutations,
        TimeSpan? indeterminateOutcomeBudget = null)
    {
        this.raft = raft;
        this.logger = logger;
        this.checkpointEveryMutations = checkpointEveryMutations;
        this.indeterminateOutcomeBudget = indeterminateOutcomeBudget ?? DefaultIndeterminateOutcomeBudget;

        snapshotPath = string.IsNullOrEmpty(storagePath)
            ? null
            : Path.Combine(storagePath, $"rangemap_{storageRevision}.snapshot");

        // Seed from the durable snapshot before any WAL replay. This is what makes meta-partition
        // compaction safe: the meta WAL is periodically checkpointed and trimmed of old
        // full-snapshot entries, so on restart the map is reconstructed from disk and then refined by
        // replaying whatever WAL tail survives — both are idempotent full snapshots.
        LoadFromDisk();
    }

    /// <summary>The current committed-and-applied map snapshot. Lock-free read.</summary>
    public RangeMap Current => current;

    /// <summary>
    /// The single descriptor-map writer. Computes the next descriptor set from the current
    /// one via <paramref name="transform"/>, validates it, then commits it as one replicated meta
    /// entry on <see cref="MetaPartitionId"/>. Leader-only — Kommander rejects the
    /// <see cref="IRaft.ReplicateLogs(int,string,byte[],bool,System.Threading.CancellationToken,long)"/>
    /// on a non-leader, and this returns <c>false</c>. The in-memory map is swapped only after the
    /// entry commits, so a failed replication leaves the map untouched.
    ///
    /// <para>
    /// A proposal can end without a verdict: when the meta leader steps down with the entry in
    /// flight, Kommander answers <see cref="RaftOperationStatus.ProposalOutcomeUnknown"/> (or
    /// <see cref="RaftOperationStatus.ProposalTimeout"/>). The entry is in this node's log and the
    /// next leader's promotion barrier may still commit it, so answering <c>false</c> right away
    /// would contradict the map the apply path installs moments later. The call instead waits for
    /// the meta partition to settle and reads the verdict from the committed map: the entry landed
    /// → <c>true</c>; the committed map is not the proposed one → the transform is re-run against
    /// the fresh map and proposed again (at most <see cref="MaxProposeAttempts"/> proposals per call,
    /// and only while this node still leads — a re-proposal from a deposed node is rejected as
    /// not-leader); no confirmed read within the settle budget → <c>false</c>, and the caller must
    /// re-read <see cref="Current"/> before it acts on that answer.
    /// </para>
    /// </summary>
    /// <returns><c>true</c> if the mutation is committed; <c>false</c> if it was rejected (invalid map,
    /// reserved-partition violation), replication failed (not leader, no quorum), or its outcome
    /// could not be confirmed after a leadership change.</returns>
    public Task<bool> MutateAsync(
        Func<IReadOnlyList<RangeDescriptor>, IReadOnlyList<RangeDescriptor>> transform,
        CancellationToken cancellationToken = default) =>
        MutateMapAsync(map => new RangeMap(transform(map.Descriptors), map.RetiredPartitionIds), cancellationToken);

    /// <summary>
    /// The whole-map form of <see cref="MutateAsync"/>: <paramref name="transform"/> receives the
    /// current map and answers the next one, descriptors and retired partitions together, so a merge
    /// cutover can drop a range's descriptor and record its partition as retired in one entry. The
    /// same validation, commit and settle rules apply. A retired partition that is still referenced
    /// by a descriptor is rejected: removing it would retire a range that is serving live data.
    /// </summary>
    public async Task<bool> MutateMapAsync(
        Func<RangeMap, RangeMap> transform,
        CancellationToken cancellationToken = default)
    {
        await mutateGate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            for (int attempt = 1; ; attempt++)
            {
                RangeMap candidate = transform(current);
                IReadOnlyList<RangeDescriptor> next = candidate.Descriptors;

                if (!candidate.Validate(out string? error))
                {
                    logger.LogError("Rejecting range-map mutation (invariant G1): {Error}", error);
                    return false;
                }

                // Reservation contract: ranged data never lives on the meta partition (P0, shared with
                // the system coordinator). Data partitions are >= FirstDataPartitionId (1).
                foreach (RangeDescriptor descriptor in next)
                {
                    if (descriptor.PartitionId < FirstDataPartitionId)
                    {
                        logger.LogError(
                            "Rejecting range-map mutation: descriptor on reserved partition {Partition} ({Descriptor})",
                            descriptor.PartitionId, descriptor);

                        return false;
                    }

                    if (candidate.IsRetired(descriptor.PartitionId))
                    {
                        logger.LogError(
                            "Rejecting range-map mutation: partition {Partition} is marked retired but still routes {Descriptor}",
                            descriptor.PartitionId, descriptor);

                        return false;
                    }
                }

                foreach (int retired in candidate.RetiredPartitionIds)
                {
                    if (retired < FirstDataPartitionId)
                    {
                        logger.LogError("Rejecting range-map mutation: reserved partition {Partition} marked retired", retired);
                        return false;
                    }
                }

                byte[] data = ReplicationSerializer.Serialize(ToMessage(candidate));

                RaftReplicationResult result = await raft.ReplicateLogs(
                    MetaPartitionId,
                    ReplicationTypes.RangeMap,
                    data,
                    cancellationToken: cancellationToken
                ).ConfigureAwait(false);

                if (result.Success)
                {
                    current = candidate;
                    Interlocked.Increment(ref mapVersion);

                    // Durable snapshot first (so this entry survives meta-WAL compaction), then maybe
                    // checkpoint to let Kommander trim the now-redundant log history.
                    PersistToDisk(candidate);
                    TriggerCheckpointIfDue();

                    return true;
                }

                if (result.Status is not (RaftOperationStatus.ProposalOutcomeUnknown or RaftOperationStatus.ProposalTimeout))
                {
                    logger.LogWarning(
                        "Failed to replicate range-map mutation Status={Status} Ticket={Ticket}",
                        result.Status, result.TicketId);

                    return false;
                }

                // The entry was accepted into the log but leadership moved before a verdict. Its fate
                // is sealed once the next leader's barrier commits, so read it from the committed map
                // rather than guess.
                IndeterminateOutcome outcome =
                    await ResolveIndeterminateOutcomeAsync(data, cancellationToken).ConfigureAwait(false);

                if (outcome == IndeterminateOutcome.Installed)
                {
                    // The apply path already installed and persisted the committed map on this node
                    // (leader echo or follower apply); only the checkpoint cadence is still owed.
                    logger.LogWarning(
                        "Range-map mutation committed across a leadership change Status={Status}",
                        result.Status);

                    TriggerCheckpointIfDue();

                    return true;
                }

                if (outcome == IndeterminateOutcome.NotInstalled && attempt < MaxProposeAttempts)
                {
                    logger.LogWarning(
                        "Range-map mutation did not land after a leadership change Status={Status}; re-proposing against the current map (attempt {Attempt} of {MaxAttempts})",
                        result.Status, attempt + 1, MaxProposeAttempts);

                    continue;
                }

                logger.LogWarning(
                    "Range-map mutation unconfirmed Status={Status} Outcome={Outcome} Attempts={Attempts}",
                    result.Status, outcome, attempt);

                return false;
            }
        }
        finally
        {
            mutateGate.Release();
        }
    }

    /// <summary>How a proposal that ended without a verdict was settled by reading the committed map.</summary>
    private enum IndeterminateOutcome
    {
        /// <summary>The committed map equals the proposed snapshot: the mutation is in effect.</summary>
        Installed,

        /// <summary>
        /// The meta partition settled and the committed map is not the proposed snapshot: the entry
        /// was dropped, or a later commit already superseded it. Either way the transform must be
        /// applied to the fresh map again.
        /// </summary>
        NotInstalled,

        /// <summary>No confirmed read of the meta partition succeeded within the settle budget.</summary>
        Unresolved
    }

    /// <summary>
    /// Settles a proposal without a verdict. Waits for the meta partition to elect a leader, then
    /// runs a confirmed read (<see cref="IRaft.ConfirmLocalApplicationAsync"/>): after a true
    /// answer every entry committed before the call — including this node's own inherited entry,
    /// if the new leader's barrier committed it — is applied here, so <see cref="Current"/> carries
    /// the verdict. A false answer is transient (no leader yet, barrier still armed, quorum round
    /// failed) and is polled again until <see cref="indeterminateOutcomeBudget"/> runs out.
    /// </summary>
    /// <param name="proposed">The exact bytes that were proposed. The comparison decodes them the
    /// way the apply path does, so codec details cannot split a landed entry from its proposal.</param>
    private async Task<IndeterminateOutcome> ResolveIndeterminateOutcomeAsync(byte[] proposed, CancellationToken cancellationToken)
    {
        RangeMap expected = FromMessage(ReplicationSerializer.UnserializeRangeMapMessage(proposed));

        long deadline = Environment.TickCount64 + (long)indeterminateOutcomeBudget.TotalMilliseconds;

        while (true)
        {
            bool confirmed = false;

            try
            {
                // Bounded by Kommander's own election budget; a partition that cannot elect surfaces
                // as a RaftException and is retried below until the settle budget runs out.
                await raft.WaitForLeader(MetaPartitionId, cancellationToken).ConfigureAwait(false);

                confirmed = await raft.ConfirmLocalApplicationAsync(MetaPartitionId, cancellationToken).ConfigureAwait(false);
            }
            catch (RaftException ex)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug(ex, "Range-map settle wait: meta partition not ready ({Message})", ex.Message);
            }

            if (confirmed)
                return SameMaps(current, expected) ? IndeterminateOutcome.Installed : IndeterminateOutcome.NotInstalled;

            if (Environment.TickCount64 >= deadline)
                return IndeterminateOutcome.Unresolved;

            await Task.Delay(IndeterminateOutcomePollInterval, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Order-independent equality of two validated maps. Key-space order in <see cref="RangeMap.Descriptors"/>
    /// follows first appearance in the source list, which the codec and the transform can order
    /// differently, so the descriptors are compared as a set (records compare by value). The retired
    /// partition lists are already sorted and de-duplicated, so they compare element-wise.
    /// </summary>
    private static bool SameMaps(RangeMap installed, RangeMap expected)
    {
        IReadOnlyList<int> installedRetired = installed.RetiredPartitionIds;
        IReadOnlyList<int> expectedRetired = expected.RetiredPartitionIds;

        if (installedRetired.Count != expectedRetired.Count)
            return false;

        for (int i = 0; i < installedRetired.Count; i++)
        {
            if (installedRetired[i] != expectedRetired[i])
                return false;
        }

        IReadOnlyList<RangeDescriptor> left = installed.Descriptors;
        IReadOnlyList<RangeDescriptor> right = expected.Descriptors;

        if (left.Count != right.Count)
            return false;

        HashSet<RangeDescriptor> set = new(left.Count);

        for (int i = 0; i < left.Count; i++)
            set.Add(left[i]);

        for (int i = 0; i < right.Count; i++)
        {
            if (!set.Contains(right[i]))
                return false;
        }

        return true;
    }

    /// <summary>
    /// Opens a quiesce window over <c>[startKey, endKey)</c> of <paramref name="keySpace"/>: keys in
    /// that interval stop accepting writes until <paramref name="until"/>, on every node, because the
    /// map is replicated. Used by a range move to close the window between copying the range's
    /// contents and cutting routing over to their new partition.
    ///
    /// <para>
    /// The deadline is what makes the quiesce safe to publish: an owner that dies mid-move leaves a
    /// window that lapses instead of a range that refuses writes forever. <paramref name="owner"/>
    /// stamps the move that opened it so only that move's release can close it.
    /// </para>
    ///
    /// <para>
    /// Returns false when the interval covers no descriptor at all (the map moved under the caller),
    /// when a descriptor it touches is already quiesced by a different move, or when the mutation
    /// could not be committed. In all three cases the caller must not proceed as if the range were
    /// quiesced.
    /// </para>
    ///
    /// <para>
    /// Refusing to stamp over another owner's live window is what keeps two concurrent moves from
    /// corrupting each other. A quiesce does not bump the generation, so a cutover's generation race
    /// guard cannot see one move overwrite another's owner — and the overwriting move's release, which
    /// is owner-scoped, would then reopen a range the first move is still copying. The first mover
    /// wins and the second is told to come back later; nothing waits, because the deadline already
    /// bounds how long "later" can be.
    /// </para>
    /// </summary>
    public async Task<bool> QuiesceRangeAsync(
        string keySpace,
        string? startKey,
        string? endKey,
        HLCTimestamp owner,
        HLCTimestamp until,
        CancellationToken cancellationToken = default)
    {
        bool matched = false;
        bool heldByAnother = false;

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        bool committed = await MutateAsync(existing =>
        {
            // Re-publishing under the same owner is an extension of that move's own window, not a
            // conflict. Anyone else's live window is, and the whole map is left untouched — a partial
            // stamp would leave the interval half quiesced.
            foreach (RangeDescriptor descriptor in existing)
            {
                if (string.Equals(descriptor.KeySpace, keySpace, StringComparison.Ordinal)
                    && Overlaps(descriptor, startKey, endKey)
                    && descriptor.QuiesceOwner != owner
                    && descriptor.IsQuiescedAt(now))
                {
                    heldByAnother = true;
                    return existing;
                }
            }

            List<RangeDescriptor> next = new(existing.Count);

            foreach (RangeDescriptor descriptor in existing)
            {
                // Every descriptor the interval touches is stamped, not only one whose bounds match
                // it exactly: a split quiesces the half of a range it is moving, which is a strict
                // sub-interval of the descriptor that still covers it, and a caller quiescing a whole
                // key space spans however many descriptors it has been split into.
                if (string.Equals(descriptor.KeySpace, keySpace, StringComparison.Ordinal)
                    && Overlaps(descriptor, startKey, endKey))
                {
                    matched = true;
                    next.Add(descriptor with
                    {
                        QuiescedUntil = until,
                        QuiesceOwner = owner,
                        QuiesceStartKey = startKey,
                        QuiesceEndKey = endKey
                    });

                    continue;
                }

                next.Add(descriptor);
            }

            // Leave the map untouched when nothing matched: MutateAsync commits it either way, and
            // re-replicating the identical map is a harmless no-op the caller detects via `matched`.
            return next;
        }, cancellationToken).ConfigureAwait(false);

        if (!committed || !matched || heldByAnother)
            logger.LogWarning(
                "Could not quiesce {Space} [{Start},{End}) — committed: {Committed}, descriptor found: {Matched}, held by another move: {Held}",
                keySpace, startKey ?? "-inf", endKey ?? "+inf", committed, matched, heldByAnother);

        return committed && matched && !heldByAnother;
    }

    /// <summary>
    /// True when any descriptor of <paramref name="keySpace"/> touching <c>[startKey, endKey)</c> is
    /// currently quiesced by a move other than <paramref name="owner"/>. Lets a caller refuse before
    /// it does the expensive part of a move, rather than discovering the conflict at the publish.
    /// Advisory only — the authoritative check is inside <see cref="QuiesceRangeAsync"/>, which runs
    /// under the mutate gate.
    /// </summary>
    public bool IsQuiescedByAnotherMove(string keySpace, string? startKey, string? endKey, HLCTimestamp owner)
    {
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        foreach (RangeDescriptor descriptor in current.Descriptors)
        {
            if (string.Equals(descriptor.KeySpace, keySpace, StringComparison.Ordinal)
                && Overlaps(descriptor, startKey, endKey)
                && descriptor.QuiesceOwner != owner
                && descriptor.IsQuiescedAt(now))
                return true;
        }

        return false;
    }

    /// <summary>
    /// True when <paramref name="descriptor"/>'s half-open interval intersects
    /// <c>[startKey, endKey)</c>. A null bound on either side is that side's infinity, so it always
    /// intersects.
    /// </summary>
    private static bool Overlaps(RangeDescriptor descriptor, string? startKey, string? endKey)
    {
        if (startKey is not null && descriptor.EndKey is not null
            && string.CompareOrdinal(startKey, descriptor.EndKey) >= 0)
            return false;

        if (endKey is not null && descriptor.StartKey is not null
            && string.CompareOrdinal(endKey, descriptor.StartKey) <= 0)
            return false;

        return true;
    }

    /// <summary>
    /// Closes every quiesce window opened by <paramref name="owner"/>. Owner-scoped rather than
    /// bounds-scoped so a move that already cut over (its descriptor is gone) or gave up part-way
    /// clears exactly what it opened and nothing else — in particular it can never open the window
    /// of a later move over the same bounds.
    ///
    /// <para>
    /// Best-effort by design: the deadline stamped at <see cref="QuiesceRangeAsync"/> is the real
    /// guarantee that the window ends, so a release that cannot commit (this node lost the meta
    /// leadership, say) costs latency until the deadline lapses, not correctness.
    /// </para>
    /// </summary>
    public async Task<bool> ReleaseQuiesceAsync(HLCTimestamp owner, CancellationToken cancellationToken = default)
    {
        if (owner == HLCTimestamp.Zero)
            return true;

        bool anyHeld = current.Descriptors.Any(d => d.QuiesceOwner == owner);

        if (!anyHeld)
            return true; // nothing of ours is quiesced (cutover already replaced the descriptor).

        return await MutateAsync(existing =>
        {
            List<RangeDescriptor> next = new(existing.Count);

            foreach (RangeDescriptor descriptor in existing)
                next.Add(descriptor.QuiesceOwner == owner
                    ? descriptor with
                    {
                        QuiescedUntil = HLCTimestamp.Zero,
                        QuiesceOwner = HLCTimestamp.Zero,
                        QuiesceStartKey = null,
                        QuiesceEndKey = null
                    }
                    : descriptor);

            return next;
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Checkpoints the meta partition (leader-only) so Kommander can compact its WAL down to the
    /// tail. Safe because every committed snapshot is also written to <see cref="snapshotPath"/>
    /// before the checkpoint, so a restart reconstructs the map from disk even after the log entry
    /// is trimmed. A no-op (returns false) on followers or when checkpointing is disabled.
    /// </summary>
    public async Task<bool> CheckpointNowAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            if (!await raft.AmILeaderIfHosted(MetaPartitionId, cancellationToken).ConfigureAwait(false))
                return false;

            RaftReplicationResult result =
                await raft.ReplicateCheckpoint(MetaPartitionId, cancellationToken).ConfigureAwait(false);

            if (!result.Success)
            {
                logger.LogWarning("Range-map checkpoint failed Status={Status}", result.Status);
                return false;
            }

            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Range-map checkpoint threw");
            return false;
        }
    }

    /// <summary>Called under <see cref="mutateGate"/>; fires a background checkpoint every N commits.</summary>
    private void TriggerCheckpointIfDue()
    {
        if (checkpointEveryMutations <= 0)
            return;

        if (++mutationsSinceCheckpoint < checkpointEveryMutations)
            return;

        mutationsSinceCheckpoint = 0;
        _ = CheckpointNowAsync();
    }

    private void PersistToDisk(RangeMap map)
    {
        if (snapshotPath is null)
            return;

        try
        {
            byte[] data = ReplicationSerializer.Serialize(ToMessage(map));

            lock (fileLock)
            {
                string tmp = snapshotPath + ".tmp";
                File.WriteAllBytes(tmp, data);
                File.Move(tmp, snapshotPath, overwrite: true);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to persist range-map snapshot to {Path}", snapshotPath);
        }
    }

    private void LoadFromDisk()
    {
        if (snapshotPath is null || !File.Exists(snapshotPath))
            return;

        try
        {
            byte[] data;
            lock (fileLock)
                data = File.ReadAllBytes(snapshotPath);

            // 0 bytes is a valid empty snapshot (see Apply); an empty map is the correct seed.
            RangeMapMessage message = ReplicationSerializer.UnserializeRangeMapMessage(data);
            RangeMap loaded = FromMessage(message);

            if (!loaded.Validate(out string? error))
            {
                logger.LogError("Durable range-map snapshot failed validation (invariant G1): {Error}", error);
                return;
            }

            current = loaded;
            Interlocked.Increment(ref mapVersion);
            logger.LogRangeMapSnapshotLoaded(snapshotPath, loaded.Descriptors.Count);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load range-map snapshot from {Path}", snapshotPath);
        }
    }

    /// <summary>Rebuilds the map from a meta-log entry replayed during WAL restore.</summary>
    public bool Restore(int partitionId, RaftLog log) => Apply(partitionId, log);

    /// <summary>Applies a committed meta-log entry received via replication (follower / leader echo).</summary>
    public bool Replicate(int partitionId, RaftLog log) => Apply(partitionId, log);

    private bool Apply(int partitionId, RaftLog log)
    {
        if (partitionId != MetaPartitionId || log.LogType != ReplicationTypes.RangeMap)
            return true;

        if (log.LogData is null)
            return true;

        try
        {
            // A zero-length payload is a valid *empty* snapshot, not a no-op: proto3 serializes a
            // descriptor-less RangeMapMessage to 0 bytes, and committing an empty map (a drop-table
            // or full-merge end state) must clear the map here too. Skipping on Length == 0 would
            // diverge the cluster — the leader clears its map in MutateAsync while followers/restore
            // keep the stale non-empty one. ParseFrom of an empty buffer yields an empty message.
            RangeMapMessage message = ReplicationSerializer.UnserializeRangeMapMessage(log.LogData);

            RangeMap rebuilt = FromMessage(message);

            if (!rebuilt.Validate(out string? error))
            {
                logger.LogError("Applied range-map snapshot failed validation (invariant G1): {Error}", error);
                return false;
            }

            current = rebuilt;
            Interlocked.Increment(ref mapVersion);

            // Give followers (and the restore replay) a durable local copy too, so a follower whose
            // meta WAL is later compacted can still reconstruct the map from disk on restart.
            PersistToDisk(rebuilt);
            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to apply range-map log on partition {Partition}", partitionId);
            return false;
        }
    }

    /// <summary>
    /// Serializes the current map for the P0 whole-partition state transfer that repairs a node
    /// below the WAL compaction floor. Lock-free read of the volatile map.
    /// </summary>
    public byte[] SerializeState() => ReplicationSerializer.Serialize(ToMessage(current));

    /// <summary>
    /// Parses and validates (does not install) a map from a transfer blob. Validates invariant G1
    /// and throws on failure, before <see cref="CommitState"/> mutates anything, so the unified P0
    /// transfer can validate both meta state machines before swapping either.
    /// </summary>
    public RangeMap ParseState(ReadOnlySpan<byte> data)
    {
        RangeMapMessage message = ReplicationSerializer.UnserializeRangeMapMessage(data);
        RangeMap loaded = FromMessage(message);

        if (!loaded.Validate(out string? error))
            throw new InvalidOperationException(
                $"Range-map state transfer failed validation (invariant G1): {error}");

        return loaded;
    }

    /// <summary>
    /// Atomically installs a parsed map (from <see cref="ParseState"/>) and persists it to disk.
    /// Called on a follower being repaired below the compaction floor, where no local mutation is
    /// in flight (only the meta-partition leader mutates); the volatile swap is itself atomic.
    /// </summary>
    public void CommitState(RangeMap parsed)
    {
        current = parsed;
        Interlocked.Increment(ref mapVersion);
        PersistToDisk(parsed);
    }

    private static RangeMapMessage ToMessage(RangeMap map)
    {
        RangeMapMessage message = new();

        foreach (int retired in map.RetiredPartitionIds)
            message.RetiredPartitionIds.Add(retired);

        foreach (RangeDescriptor descriptor in map.Descriptors)
        {
            RangeDescriptorMessage descriptorMessage = new()
            {
                KeySpace = descriptor.KeySpace,
                PartitionId = descriptor.PartitionId,
                Generation = descriptor.Generation,
                QuiescedUntilNode     = descriptor.QuiescedUntil.N,
                QuiescedUntilPhysical = descriptor.QuiescedUntil.L,
                QuiescedUntilCounter  = descriptor.QuiescedUntil.C,
                QuiesceOwnerNode     = descriptor.QuiesceOwner.N,
                QuiesceOwnerPhysical = descriptor.QuiesceOwner.L,
                QuiesceOwnerCounter  = descriptor.QuiesceOwner.C
            };

            if (descriptor.StartKey is not null)
                descriptorMessage.StartKey = descriptor.StartKey;

            if (descriptor.EndKey is not null)
                descriptorMessage.EndKey = descriptor.EndKey;

            if (descriptor.QuiesceStartKey is not null)
                descriptorMessage.QuiesceStartKey = descriptor.QuiesceStartKey;

            if (descriptor.QuiesceEndKey is not null)
                descriptorMessage.QuiesceEndKey = descriptor.QuiesceEndKey;

            message.Descriptors.Add(descriptorMessage);
        }

        return message;
    }

    private static RangeMap FromMessage(RangeMapMessage message) =>
        new(DescriptorsFromMessage(message), message.RetiredPartitionIds);

    private static IEnumerable<RangeDescriptor> DescriptorsFromMessage(RangeMapMessage message)
    {
        foreach (RangeDescriptorMessage descriptorMessage in message.Descriptors)
        {
            yield return new RangeDescriptor
            {
                KeySpace = descriptorMessage.KeySpace,
                StartKey = descriptorMessage.HasStartKey ? descriptorMessage.StartKey : null,
                EndKey = descriptorMessage.HasEndKey ? descriptorMessage.EndKey : null,
                PartitionId = descriptorMessage.PartitionId,
                Generation = descriptorMessage.Generation,
                QuiescedUntil = new(
                    descriptorMessage.QuiescedUntilNode,
                    descriptorMessage.QuiescedUntilPhysical,
                    descriptorMessage.QuiescedUntilCounter),
                QuiesceOwner = new(
                    descriptorMessage.QuiesceOwnerNode,
                    descriptorMessage.QuiesceOwnerPhysical,
                    descriptorMessage.QuiesceOwnerCounter),
                QuiesceStartKey = descriptorMessage.HasQuiesceStartKey ? descriptorMessage.QuiesceStartKey : null,
                QuiesceEndKey = descriptorMessage.HasQuiesceEndKey ? descriptorMessage.QuiesceEndKey : null
            };
        }
    }

    public void Dispose()
    {
        mutateGate.Dispose();
    }
}
