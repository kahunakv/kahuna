using Kommander;
using Kommander.Data;

using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// The replicated source of truth for the range-descriptor map. Wraps an
/// immutable in-memory <see cref="RangeMap"/> and keeps it consistent across the cluster by
/// committing every change through the Raft meta partition.
///
/// <para>
/// <b>Where it lives.</b> The Kommander <i>system</i> partition (id 0) is reserved for the
/// partition-map coordinator and only accepts its own <c>_RaftSystem</c> log type, so it cannot
/// host application data. Instead the map is replicated on <b>Kahuna partition
/// <see cref="MetaPartitionId"/> (= 1)</b> — an ordinary replicated partition whose committed and
/// restored logs reach Kahuna's <c>OnReplicationReceived</c>/<c>OnLogRestored</c> callbacks. Ranged
/// <i>data</i> therefore lives on partitions ≥ 2 (the reservation contract enforced by
/// <see cref="MutateAsync"/> and consulted by routing in Tasks 3/9). See
/// <c>docs/spec-range-splits-kommander-flag.md</c>.
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
internal sealed class RangeMapStore
{
    /// <summary>
    /// The well-known Kahuna partition that hosts the range-descriptor map. Always exists
    /// (<c>InitialPartitions &gt; 0</c> is enforced). Ranged data lives on partitions ≥ 2.
    /// </summary>
    public const int MetaPartitionId = 1;

    /// <summary>Lowest partition id usable for ranged data (partition 1 is reserved for the map).</summary>
    public const int FirstDataPartitionId = 2;

    /// <summary>Default number of committed mutations between meta-partition checkpoints (Task 2c).</summary>
    public const int DefaultCheckpointEveryMutations = 32;

    private readonly IRaft raft;

    private readonly ILogger<IKahuna> logger;

    private readonly SemaphoreSlim mutateGate = new(1, 1);

    /// <summary>Path of the durable snapshot file, or null when disk persistence is disabled.</summary>
    private readonly string? snapshotPath;

    /// <summary>Serializes the temp-write + atomic-rename so concurrent writers can't corrupt the file.</summary>
    private readonly object fileLock = new();

    private readonly int checkpointEveryMutations;

    /// <summary>Mutated only under <see cref="mutateGate"/>.</summary>
    private int mutationsSinceCheckpoint;

    private volatile RangeMap current = RangeMap.Empty;

    /// <param name="storagePath">Directory for the durable snapshot file; empty disables disk persistence.</param>
    /// <param name="storageRevision">Per-node revision so each node's snapshot file is distinct and stable across restarts.</param>
    /// <param name="checkpointEveryMutations">Committed mutations between meta-partition checkpoints; ≤ 0 disables periodic checkpointing.</param>
    public RangeMapStore(
        IRaft raft,
        string? storagePath,
        string? storageRevision,
        ILogger<IKahuna> logger,
        int checkpointEveryMutations = DefaultCheckpointEveryMutations)
    {
        this.raft = raft;
        this.logger = logger;
        this.checkpointEveryMutations = checkpointEveryMutations;

        snapshotPath = string.IsNullOrEmpty(storagePath)
            ? null
            : Path.Combine(storagePath, $"rangemap_{storageRevision}.snapshot");

        // Seed from the durable snapshot before any WAL replay. This is what makes meta-partition
        // compaction safe (Task 2c): the meta WAL is periodically checkpointed and trimmed of old
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
    /// </summary>
    /// <returns><c>true</c> if the mutation committed; <c>false</c> if it was rejected (invalid map,
    /// reserved-partition violation) or replication failed (not leader, no quorum).</returns>
    public async Task<bool> MutateAsync(
        Func<IReadOnlyList<RangeDescriptor>, IReadOnlyList<RangeDescriptor>> transform,
        CancellationToken cancellationToken = default)
    {
        await mutateGate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            IReadOnlyList<RangeDescriptor> next = transform(current.Descriptors);

            RangeMap candidate = new(next);

            if (!candidate.Validate(out string? error))
            {
                logger.LogError("Rejecting range-map mutation (invariant G1): {Error}", error);
                return false;
            }

            // Reservation contract: ranged data never lives on the meta partition (or below it).
            foreach (RangeDescriptor descriptor in next)
            {
                if (descriptor.PartitionId < FirstDataPartitionId)
                {
                    logger.LogError(
                        "Rejecting range-map mutation: descriptor on reserved partition {Partition} ({Descriptor})",
                        descriptor.PartitionId, descriptor);

                    return false;
                }
            }

            byte[] data = ReplicationSerializer.Serialize(ToMessage(next));

            RaftReplicationResult result = await raft.ReplicateLogs(
                MetaPartitionId,
                ReplicationTypes.RangeMap,
                data,
                cancellationToken: cancellationToken
            ).ConfigureAwait(false);

            if (!result.Success)
            {
                logger.LogWarning(
                    "Failed to replicate range-map mutation Status={Status} Ticket={Ticket}",
                    result.Status, result.TicketId);

                return false;
            }

            current = candidate;

            // Durable snapshot first (so this entry survives meta-WAL compaction), then maybe
            // checkpoint to let Kommander trim the now-redundant log history.
            PersistToDisk(candidate);
            TriggerCheckpointIfDue();

            return true;
        }
        finally
        {
            mutateGate.Release();
        }
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
            if (!await raft.AmILeader(MetaPartitionId, cancellationToken).ConfigureAwait(false))
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
            byte[] data = ReplicationSerializer.Serialize(ToMessage(map.Descriptors));

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
            RangeMap loaded = new(FromMessage(message));

            if (!loaded.Validate(out string? error))
            {
                logger.LogError("Durable range-map snapshot failed validation (invariant G1): {Error}", error);
                return;
            }

            current = loaded;
            logger.LogInformation(
                "Loaded range-map snapshot from {Path} ({Count} descriptors)", snapshotPath, loaded.Descriptors.Count);
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

            RangeMap rebuilt = new(FromMessage(message));

            if (!rebuilt.Validate(out string? error))
            {
                logger.LogError("Applied range-map snapshot failed validation (invariant G1): {Error}", error);
                return false;
            }

            current = rebuilt;

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

    private static RangeMapMessage ToMessage(IReadOnlyList<RangeDescriptor> descriptors)
    {
        RangeMapMessage message = new();

        foreach (RangeDescriptor descriptor in descriptors)
        {
            RangeDescriptorMessage descriptorMessage = new()
            {
                KeySpace = descriptor.KeySpace,
                PartitionId = descriptor.PartitionId,
                Generation = descriptor.Generation
            };

            if (descriptor.StartKey is not null)
                descriptorMessage.StartKey = descriptor.StartKey;

            if (descriptor.EndKey is not null)
                descriptorMessage.EndKey = descriptor.EndKey;

            message.Descriptors.Add(descriptorMessage);
        }

        return message;
    }

    private static IEnumerable<RangeDescriptor> FromMessage(RangeMapMessage message)
    {
        foreach (RangeDescriptorMessage descriptorMessage in message.Descriptors)
        {
            yield return new RangeDescriptor
            {
                KeySpace = descriptorMessage.KeySpace,
                StartKey = descriptorMessage.HasStartKey ? descriptorMessage.StartKey : null,
                EndKey = descriptorMessage.HasEndKey ? descriptorMessage.EndKey : null,
                PartitionId = descriptorMessage.PartitionId,
                Generation = descriptorMessage.Generation
            };
        }
    }
}
