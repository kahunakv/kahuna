
using System.Collections.Concurrent;

namespace Kahuna.Server.Persistence;

/// <summary>
/// The channel a registered WAL entry resolves through. Entries whose durable artifact is a
/// background-flushed row (key-values, locks) resolve individually by log index when their flush
/// batch lands. Entries whose durable artifact is a whole-store snapshot (completion receipts,
/// transaction records, prepared intents) resolve collectively up to the highest applied index
/// when that store's per-partition snapshot persists.
/// </summary>
internal enum DurabilityChannel : byte
{
    Flush = 0,

    Receipts = 1,

    TransactionRecords = 2,

    PreparedIntents = 3
}

/// <summary>
/// Tracks, per Raft partition, the highest WAL log index whose delivered entries are all durably
/// applied in this node's own storage — the application-durability floor Kommander consults so
/// restart replay widens below the checkpoint and WAL compaction never truncates entries whose
/// application-side effects have not been persisted yet.
///
/// <para>
/// The floor may be stale-low (extra redelivery on restore, which the appliers tolerate) but must
/// never be high. Two mechanisms uphold that:
/// </para>
/// <para>
/// <b>Registration precedes advancement.</b> An entry becomes <i>pending</i> before the watermark
/// can pass its index. Every delivered entry — follower replication, restart replay, and the
/// leader's own committed proposals, which Kommander also delivers through the consumer-apply
/// path — arrives in log-id order on the partition executor, so registering inside the apply
/// guarantees no later-index registration can advance the watermark over an earlier index that
/// has not been seen yet.
/// </para>
/// <para>
/// <b>Resolution certifies durability.</b> A pending index resolves only when its durable
/// artifact lands: individually when the background flush batch carrying it succeeds
/// (<see cref="DurabilityChannel.Flush"/>), or collectively up to the highest applied index when
/// the owning store's partition snapshot persists (the snapshot channels). Entries whose apply is
/// synchronously durable register and resolve in one step (<see cref="RegisterDurable"/>).
/// </para>
/// </summary>
internal sealed class PartitionDurabilityTracker
{
    private sealed class PartitionState
    {
        /// <summary>Unresolved delivered indexes, mapped to their resolution channel.</summary>
        public readonly SortedDictionary<long, DurabilityChannel> Pending = [];

        /// <summary>Highest index ever registered (pending or durable).</summary>
        public long HighestRegistered = -1;

        /// <summary>Per snapshot channel, the highest index whose apply has completed in memory —
        /// the resolve ceiling for the next durable snapshot of that store.</summary>
        public readonly long[] HighestApplied = [-1, -1, -1, -1];

        /// <summary>Set under the lock when <see cref="Forget"/> retires this object from the
        /// dictionary, so a concurrent registration that already fetched it detects the retirement
        /// and fetches a fresh one instead of mutating an orphan (whose updates would be lost,
        /// letting a later watermark advance over an entry that was never resolved).</summary>
        public bool Removed;
    }

    private readonly ConcurrentDictionary<int, PartitionState> partitions = new();

    /// <summary>
    /// Registers a delivered entry as pending on its resolution channel. Idempotent by index: an
    /// index at or below the highest registration is left as is (redelivery of an entry already
    /// tracked or already certified durable).
    /// </summary>
    public void RegisterPending(int partitionId, long logIndex, DurabilityChannel channel)
    {
        if (logIndex < 0)
            return;

        while (true)
        {
            PartitionState state = partitions.GetOrAdd(partitionId, static _ => new PartitionState());

            lock (state)
            {
                // Retired between GetOrAdd and the lock (a concurrent Forget); fetch again so the
                // registration lands in live state rather than an orphan whose updates are lost.
                if (state.Removed)
                    continue;

                if (state.HighestRegistered >= logIndex)
                    return;

                state.Pending[logIndex] = channel;
                state.HighestRegistered = logIndex;
                return;
            }
        }
    }

    /// <summary>
    /// Registers an entry whose apply persisted synchronously: it counts toward the watermark with
    /// no pending phase. Idempotent by index.
    /// </summary>
    public void RegisterDurable(int partitionId, long logIndex)
    {
        if (logIndex < 0)
            return;

        while (true)
        {
            PartitionState state = partitions.GetOrAdd(partitionId, static _ => new PartitionState());

            lock (state)
            {
                // Retired between GetOrAdd and the lock (a concurrent Forget); fetch again.
                if (state.Removed)
                    continue;

                if (logIndex > state.HighestRegistered)
                    state.HighestRegistered = logIndex;
                return;
            }
        }
    }

    /// <summary>
    /// Marks a snapshot-channel entry's in-memory apply as completed, raising the resolve ceiling
    /// the next durable snapshot of that store certifies.
    /// </summary>
    public void MarkApplied(int partitionId, long logIndex, DurabilityChannel channel)
    {
        if (logIndex < 0)
            return;

        while (true)
        {
            PartitionState state = partitions.GetOrAdd(partitionId, static _ => new PartitionState());

            lock (state)
            {
                // Retired between GetOrAdd and the lock (a concurrent Forget); fetch again.
                if (state.Removed)
                    continue;

                if (logIndex > state.HighestApplied[(int)channel])
                    state.HighestApplied[(int)channel] = logIndex;
                return;
            }
        }
    }

    /// <summary>Returns the current resolve ceiling for a snapshot channel (-1 = nothing applied).
    /// Never creates tracking state: a resolve-side read of an untracked (or forgotten) partition
    /// must not make it observed again.</summary>
    public long GetHighestApplied(int partitionId, DurabilityChannel channel)
    {
        if (!partitions.TryGetValue(partitionId, out PartitionState? state))
            return -1;

        lock (state)
            return state.HighestApplied[(int)channel];
    }

    /// <summary>Resolves one flush-channel index: its durable artifact (the flushed row) landed.
    /// Never creates tracking state — a flush ack that lands after the partition was forgotten
    /// (this node stopped hosting it) must not resurrect an empty entry.</summary>
    public void Resolve(int partitionId, long logIndex)
    {
        if (!partitions.TryGetValue(partitionId, out PartitionState? state))
            return;

        lock (state)
            state.Pending.Remove(logIndex);
    }

    /// <summary>
    /// Resolves every pending index of <paramref name="channel"/> at or below
    /// <paramref name="upToLogIndex"/>: the store snapshot that covers their applies persisted.
    /// Never creates tracking state (see <see cref="Resolve"/>).
    /// </summary>
    public void ResolveUpTo(int partitionId, DurabilityChannel channel, long upToLogIndex)
    {
        if (upToLogIndex < 0)
            return;

        if (!partitions.TryGetValue(partitionId, out PartitionState? state))
            return;

        lock (state)
        {
            List<long>? resolved = null;

            foreach (KeyValuePair<long, DurabilityChannel> kv in state.Pending)
            {
                if (kv.Key > upToLogIndex)
                    break;

                if (kv.Value == channel)
                    (resolved ??= []).Add(kv.Key);
            }

            if (resolved is not null)
                foreach (long index in resolved)
                    state.Pending.Remove(index);
        }
    }

    /// <summary>
    /// Whether the partition has unresolved snapshot-channel entries — i.e. a durable store
    /// snapshot is what its floor is waiting on.
    /// </summary>
    public bool HasPendingSnapshotWork(int partitionId)
    {
        if (!partitions.TryGetValue(partitionId, out PartitionState? state))
            return false;

        lock (state)
        {
            foreach (KeyValuePair<long, DurabilityChannel> kv in state.Pending)
                if (kv.Value != DurabilityChannel.Flush)
                    return true;

            return false;
        }
    }

    /// <summary>
    /// The partition's current watermark: the highest index such that every registered entry at or
    /// below it is durably applied. -1 when nothing has been registered (no opinion).
    /// </summary>
    public long GetWatermark(int partitionId)
    {
        if (!partitions.TryGetValue(partitionId, out PartitionState? state))
            return -1;

        lock (state)
        {
            foreach (KeyValuePair<long, DurabilityChannel> kv in state.Pending)
                return kv.Key - 1;

            return state.HighestRegistered;
        }
    }

    /// <summary>Partitions this tracker has observed, for floor-persistence sweeps.</summary>
    public IReadOnlyCollection<int> ObservedPartitions => (IReadOnlyCollection<int>)partitions.Keys;

    /// <summary>
    /// Drops all tracking state for a partition this node stopped hosting. A retained entry would
    /// keep reporting a durability floor for a range whose WAL Kommander already reclaimed — and if
    /// the range is ever hosted here again, that stale floor could pin (or worse, misstate) the new
    /// copy's WAL retention. Registration (<see cref="RegisterPending"/> / <see cref="MarkApplied"/>)
    /// recreates state naturally when the partition's entries are applied here again. The retirement
    /// flag is set under the state's lock so a racing registration never mutates the orphan.
    /// </summary>
    public void Forget(int partitionId)
    {
        if (!partitions.TryGetValue(partitionId, out PartitionState? state))
            return;

        lock (state)
        {
            state.Removed = true;
            // Remove only if the dictionary still maps to this exact object (never a replacement).
            partitions.TryRemove(new KeyValuePair<int, PartitionState>(partitionId, state));
        }
    }
}
