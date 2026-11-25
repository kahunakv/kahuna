using System.Collections.Concurrent;
using System.Runtime.InteropServices;

using Kommander.Data;

using Kahuna.Server.Replication;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// The keys that a log entry of this node can still change: an entry that was handed to Raft and that the ordered
/// apply of this node has not delivered yet.
///
/// <para>Raft answers a proposal when a quorum holds its entries, and delivers them to this node one at a time after
/// that. In between an entry is committed, a follower can apply it and show it to a watch, and this node does not
/// serve it yet. A linearizable read must not be answered from that state. The leadership confirmation protects a read
/// by waiting until everything committed is delivered, whatever the read asks for. This type names the keys that can
/// be affected, so a read of any other key needs the proof of leadership alone.</para>
///
/// <para><b>Why the keys of this node are enough.</b> A node publishes its leadership only after it delivered every
/// entry of an earlier leadership, and it proposes every entry of its own leadership that changes a readable value
/// through the partition write scheduler, which marks the keys here before the proposal and before anyone can learn of
/// the entry. A key without a mark is therefore served by this node as of the last entry that touches it.</para>
///
/// <para><b>Conservative on every doubt.</b> A producer that does not name its keys marks the whole partition. So does
/// a proposal whose outcome is not a plain success: its entries may be in the log and commit later, with no index to
/// wait for. That mark stays until an entry of a batch that was dispatched after the outcome was known is delivered:
/// it is above any entry of the failed proposal, and delivery runs in log order. A key is kept as two cells of a table
/// of fixed size, so keys in flight can together cover the cells of another key. That can only make a read wait that
/// did not have to.</para>
///
/// <para><b>Representation.</b> Each submission of a partition that marks something takes the next number. A cell holds
/// the number of the last submission that covers it, and the partition holds the number up to which all submissions
/// are delivered. A key is marked while both of its cells hold a higher number, so a delivery releases a submission
/// with one store, whatever the number of its keys is.</para>
///
/// <para><b>Threads.</b> One lane of the write scheduler begins the batches of a partition, the detached round trip of
/// a batch settles it, the partition's ordered apply delivers, and any thread reads. Reads take no lock.</para>
/// </summary>
internal sealed class UndeliveredKeys
{
    /// <summary>Cells per partition, as a power of two. A partition costs 32 KiB from its first batch until it is
    /// idle.</summary>
    private const int CellBits = 12;

    private const int CellCount = 1 << CellBits;

    private readonly ConcurrentDictionary<int, Partition> partitions = new();

    /// <summary>
    /// Whether an undelivered entry of this node can change what a read of <paramref name="key"/> answers. False is a
    /// proof as of the call; true can be a doubt.
    /// </summary>
    public bool MayChange(int partitionId, string key) =>
        partitions.TryGetValue(partitionId, out Partition? partition) && partition.MayChange(key);

    /// <summary>
    /// Marks the keys of a batch. Called before the batch is handed to Raft. Null when the batch changes no readable
    /// value and nothing needs its delivery; <see cref="Settle"/> takes it as it is.
    /// </summary>
    public Batch? Begin(int partitionId, IReadOnlyList<IProposalSubmission> submissions)
    {
        int marking = 0;

        for (int i = 0; i < submissions.Count; i++)
        {
            if (submissions[i].ChangedKeyCount != 0)
                marking++;
        }

        while (true)
        {
            if (!partitions.TryGetValue(partitionId, out Partition? partition))
            {
                if (marking == 0)
                    return null;

                partition = partitions.GetOrAdd(partitionId, static _ => new Partition());
            }
            else if (marking == 0 && !partition.HasUnknownOutcome)
                return null;

            if (partition.Begin(submissions, marking) is { } batch)
                return batch;

            // Retired in between. The marks go to the state that takes its place.
            partitions.TryRemove(new KeyValuePair<int, Partition>(partitionId, partition));
        }
    }

    /// <summary>
    /// Takes the outcome of a batch that <see cref="Begin"/> marked. The marks of a submission stay until its last
    /// key-value entry is delivered. A <paramref name="result"/> that is null, that does not align with
    /// <paramref name="entries"/>, or that reports any entry as failed leaves the outcome unknown.
    /// </summary>
    public static void Settle(Batch? batch, IReadOnlyList<RaftProposalEntry> entries, RaftBatchReplicationResult? result) =>
        batch?.Owner.Settle(batch, TryLocate(batch.Parts, entries, result));

    /// <summary>The ordered apply of this node delivered the entry at <paramref name="logIndex"/>. Calls for one
    /// partition are serialized and their indexes rise.</summary>
    public void Delivered(int partitionId, long logIndex)
    {
        if (partitions.TryGetValue(partitionId, out Partition? partition))
            partition.Delivered(logIndex);
    }

    /// <summary>
    /// Gives up the state of every partition that began no batch since the call before this one and that marks nothing,
    /// so idle and retired partitions do not keep their tables. A later batch starts over with a new one.
    /// <para>A partition that this node does not host any more is given up with its marks, when no proposal of it is
    /// still out: a read of it is refused here, and what an earlier leadership of this node left in the log is delivered
    /// before this node publishes a new one.</para>
    /// </summary>
    public void ReleaseIdle(Func<int, bool> hosts)
    {
        foreach (KeyValuePair<int, Partition> entry in partitions)
        {
            if (entry.Value.TryRetire(hosts(entry.Key)))
                partitions.TryRemove(entry);
        }
    }

    /// <summary>The partitions that have a table now.</summary>
    internal int PartitionCount => partitions.Count;

    /// <summary>
    /// Finds the index that releases each part of a committed batch: that of the last entry, at or before the end of
    /// the part, that the key-value apply of this node receives. The entries of another subsystem change no key-value
    /// state, and their delivery is not reported here. False when the outcome is unknown.
    /// </summary>
    private static bool TryLocate(Part[] parts, IReadOnlyList<RaftProposalEntry> entries, RaftBatchReplicationResult? result)
    {
        if (result is null || result.Entries.Count != entries.Count)
            return false;

        long last = 0;
        int part = 0;

        for (int i = 0; i < entries.Count; i++)
        {
            RaftEntryResult entry = result.Entries[i];

            if (entry.Status != RaftOperationStatus.Success)
                return false;

            if (ReplicationLogRouter.OwnerOf(entries[i].Type) == ReplicationLogOwner.KeyValues)
            {
                if (entry.LogIndex <= 0)
                    return false;

                last = Math.Max(last, entry.LogIndex);
            }

            while (part < parts.Length && parts[part].EntryEnd == i + 1)
                parts[part++].LastIndex = last;
        }

        // The index of a later part is at or above that of the first one.
        return parts[0].LastIndex > 0;
    }

    // Two fields of the hash that share no bit.
    private static int FirstCell(int hash) => hash & (CellCount - 1);

    private static int SecondCell(int hash) => (hash >>> CellBits) & (CellCount - 1);

    /// <summary>One submission of a batch that marks something.</summary>
    internal struct Part
    {
        /// <summary>The entries of the batch up to and including this submission.</summary>
        internal int EntryEnd;

        /// <summary>The index whose delivery releases the marks. Zero when no key-value entry is at or before it.</summary>
        internal long LastIndex;
    }

    /// <summary>The marks of one batch, from its dispatch to its delivery.</summary>
    internal sealed class Batch
    {
        internal readonly Partition Owner;

        // The number of the first part. The others follow it.
        internal readonly long First;

        internal readonly Part[] Parts;

        // False when the batch is followed only for the proof its delivery gives about an earlier one.
        internal readonly bool Marks;

        // Under the gate of the partition from here on: whether the outcome is known, and the parts before this one
        // are released.
        internal bool Settled;

        internal int Released;

        internal Batch(Partition owner, long first, Part[] parts, bool marks)
        {
            Owner = owner;
            First = first;
            Parts = parts;
            Marks = marks;
        }
    }

    /// <summary>What every delivery touches, apart from what every read looks at: the ordered apply stores an index for
    /// each entry, and the readers must not lose their cache line to it.</summary>
    [StructLayout(LayoutKind.Explicit, Size = 2 * CacheLine)]
    private struct DeliveryState
    {
        private const int CacheLine = 128;

        /// <summary>The last index the ordered apply delivered.</summary>
        [FieldOffset(CacheLine)]
        internal long Index;

        /// <summary>The index that releases the next part, so a delivery that releases nothing takes no lock.</summary>
        [FieldOffset(CacheLine + sizeof(long))]
        internal long NextRelease;
    }

    /// <summary>The marks of one partition.</summary>
    internal sealed class Partition
    {
        private readonly long[] cells = new long[CellCount];

        // The number of the last submission that does not name its keys.
        private long unnamedThrough;

        // Every submission up to this number is delivered, or its batch ended with an unknown outcome.
        private long releasedThrough;

        // The last number given out when a batch ended with an unknown outcome; zero without one. The delivery of an
        // entry of a batch with higher numbers clears it.
        private long unknownThrough;

        // The last number given out. Written by the lane alone.
        private long begun;

        private DeliveryState delivery = new() { NextRelease = long.MaxValue };

        // Guards the batches in flight and what their outcomes and deliveries change.
        private readonly Lock gate = new();

        // In the order of their numbers.
        private readonly Queue<Batch> inFlight = [];

        // The number that was the last one when the idle check looked before.
        private long begunAtLastCheck;

        // Under the gate: a state that is given up takes no batch.
        private bool retired;

        internal bool HasUnknownOutcome => Volatile.Read(ref unknownThrough) != 0;

        /// <summary>The unknown outcome is read last. A batch that ends with one sets it before its numbers count as
        /// released, so a reader that finds them released finds the unknown outcome.</summary>
        internal bool MayChange(string key)
        {
            int hash = key.GetHashCode();
            long released = Volatile.Read(ref releasedThrough);

            return (Volatile.Read(ref cells[FirstCell(hash)]) > released && Volatile.Read(ref cells[SecondCell(hash)]) > released)
                || Volatile.Read(ref unnamedThrough) > released
                || Volatile.Read(ref unknownThrough) != 0;
        }

        /// <summary>Called by the one lane that serves the partition. The cells are written outside the gate, which a
        /// delivery on the Raft executor may wait for: a batch can carry thousands of keys. Null when the state is
        /// retired.</summary>
        internal Batch? Begin(IReadOnlyList<IProposalSubmission> submissions, int marking)
        {
            Part[] parts = new Part[Math.Max(marking, 1)];
            long first = begun + 1;
            long number = begun;
            int part = 0, entryEnd = 0;

            for (int i = 0; i < submissions.Count; i++)
            {
                IProposalSubmission submission = submissions[i];
                int count = submission.ChangedKeyCount;
                entryEnd += submission.Entries.Count;

                if (count == 0)
                    continue;

                parts[part++].EntryEnd = entryEnd;
                number++;

                if (count < 0)
                    Volatile.Write(ref unnamedThrough, number);

                for (int k = 0; k < count; k++)
                {
                    int hash = submission.ChangedKey(k).GetHashCode();
                    Volatile.Write(ref cells[FirstCell(hash)], number);
                    Volatile.Write(ref cells[SecondCell(hash)], number);
                }
            }

            if (marking == 0)
            {
                parts[0].EntryEnd = entryEnd;
                number++;
            }

            Volatile.Write(ref begun, number);

            Batch batch = new(this, first, parts, marking != 0);

            lock (gate)
            {
                if (retired)
                    return null;

                inFlight.Enqueue(batch);
            }

            return batch;
        }

        /// <summary>Whether the state is given up by this call. A batch that enters after it finds the state retired,
        /// and one that entered before it is seen here.</summary>
        internal bool TryRetire(bool hosted)
        {
            lock (gate)
            {
                long last = Volatile.Read(ref begun);

                if (last != begunAtLastCheck)
                {
                    begunAtLastCheck = last;
                    return false;
                }

                if (hosted && (inFlight.Count != 0 || unknownThrough != 0))
                    return false;

                foreach (Batch batch in inFlight)
                {
                    if (!batch.Settled)
                        return false;
                }

                return retired = true;
            }
        }

        internal void Settle(Batch batch, bool located)
        {
            lock (gate)
            {
                batch.Settled = true;

                if (!located)
                {
                    // A batch that marked nothing leaves nothing in doubt, whatever became of it.
                    if (batch.Marks)
                        Volatile.Write(ref unknownThrough, Volatile.Read(ref begun));

                    batch.Released = batch.Parts.Length;
                }

                Advance();
            }
        }

        internal void Delivered(long logIndex)
        {
            if (logIndex <= Volatile.Read(ref delivery.Index))
                return;

            Interlocked.Exchange(ref delivery.Index, logIndex);

            if (Volatile.Read(ref delivery.NextRelease) > logIndex)
                return;

            lock (gate)
                Advance();
        }

        /// <summary>
        /// Releases what is delivered, in the order of the numbers. Under the gate. A part that is delivered while a
        /// batch before it has no outcome yet waits for that outcome.
        /// </summary>
        private void Advance()
        {
            long next;

            do
            {
                next = ReleaseThrough(Volatile.Read(ref delivery.Index));
                Volatile.Write(ref delivery.NextRelease, next);

                // The delivery can be ahead of the answer of the proposal. It stores its index and then reads the index
                // it has to release at; this stores the latter and then reads the former. The fence on each side leaves
                // no order in which both miss the other.
                Interlocked.MemoryBarrier();
            }
            while (Volatile.Read(ref delivery.Index) >= next);
        }

        private long ReleaseThrough(long index)
        {
            while (inFlight.TryPeek(out Batch? batch))
            {
                if (!batch.Settled)
                    return long.MaxValue;

                while (batch.Released < batch.Parts.Length && batch.Parts[batch.Released].LastIndex <= index)
                {
                    batch.Released++;

                    // This entry is above every entry of a proposal that had already failed when its batch was dispatched.
                    if (batch.First > unknownThrough)
                        Volatile.Write(ref unknownThrough, 0);
                }

                Volatile.Write(ref releasedThrough, batch.First + batch.Released - 1);

                if (batch.Released < batch.Parts.Length)
                    return batch.Parts[batch.Released].LastIndex;

                inFlight.Dequeue();
            }

            return long.MaxValue;
        }
    }
}
