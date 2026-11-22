using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>The settled-identity memory of one ledger slice of <see cref="PreparedIntentStore"/>.</summary>
internal sealed partial class PreparedIntentStore
{
    /// <summary>One leg — a transaction attempt's identity on one key — as the settled-identity memory keys it.
    /// Per key, not per transaction: a decided multi-key transaction can have one leg settled here while a
    /// range move hands another of its legs, still pending, to this partition; that leg must install, and only
    /// a prepare of a key that itself settled here is a replay.
    ///
    /// <para>The identity computes its hash once, when it is built, and keeps it. The memory reads the hash to
    /// select a shard, the map of the shard reads it again, and a shard split reads it for every entry it
    /// moves; hashing the key each time was most of the cost of a record. The fields are laid out so that the
    /// stored hash adds no bytes to an entry.</para></summary>
    internal readonly struct SettledIdentity : IEquatable<SettledIdentity>
    {
        private readonly long transactionL;

        private readonly uint transactionC;

        private readonly int transactionN;

        // Every input reaches every bit: the memory selects a shard from the high bits, and the legs of one
        // large transaction differ only in the key.
        private readonly int hash;

        public SettledIdentity(HLCTimestamp transactionId, long epoch, string key)
        {
            transactionL = transactionId.L;
            transactionC = transactionId.C;
            transactionN = transactionId.N;
            Epoch = epoch;
            Key = key;
            hash = HashCode.Combine(transactionL, transactionC, transactionN, epoch, key);
        }

        public HLCTimestamp TransactionId => new(transactionN, transactionL, transactionC);

        public long Epoch { get; }

        public string Key { get; }

        public bool Equals(SettledIdentity other) =>
            hash == other.hash
            && transactionL == other.transactionL
            && transactionC == other.transactionC
            && transactionN == other.transactionN
            && Epoch == other.Epoch
            && string.Equals(Key, other.Key);

        public override bool Equals(object? obj) => obj is SettledIdentity other && Equals(other);

        public override int GetHashCode() => hash;
    }

    /// <summary>
    /// The settled identities of one ledger slice, each with the time base of its retention.
    ///
    /// <para>The identities are spread over many small maps (shards) by the high bits of their hash, as in
    /// extendible hashing: a directory of 2^depth slots points to the shards, a shard that passes
    /// <see cref="MaxShardEntries"/> splits in two, and the directory doubles when the shard that splits already
    /// has a slot of its own. A slice holds one identity for each key settled in the last 60 to 75 seconds, a
    /// million at a few thousand transactions a second. One map of that size is two arrays of tens of megabytes,
    /// allocated again at each resize on the apply path, where a failed allocation ends the process. Here no
    /// array passes about 120 KB, a split moves half of one shard, and no step moves every entry.</para>
    ///
    /// <para>The shards also give a capture and the physical sweep a position they can resume from. Both walk
    /// the hash space in order, one shard at a step, and keep the position where the next shard starts
    /// (<see cref="CopyNext"/>, <see cref="SweepStep"/>). A split divides a shard at the middle of its span of
    /// the hash space and no shards are merged, so a position that is the start of a shard stays the start of a
    /// shard.</para>
    ///
    /// <para>Not thread-safe. The owner reads and writes it under the apply gate.</para>
    /// </summary>
    internal sealed class SettledIdentityMemory
    {
        /// <summary>Entries one shard holds before it splits. With the 72-byte entry of the map, a shard at this
        /// size keeps its arrays below the large object threshold.</summary>
        internal const int MaxShardEntries = 768;

        // A shard at this depth does not split and grows as one map. 2^20 shards hold 800 million identities.
        private const int MaxDepth = 20;

        // One past the last position of the hash space: the position of a finished walk.
        private const long HashSpaceEnd = 1L << 32;

        // Shards one CopyNext call reads at most, so a run of empty shards does not extend the gate hold.
        private const int MaxShardsPerCopy = 256;

        private sealed class Shard(int depth, Dictionary<SettledIdentity, HLCTimestamp> map)
        {
            // Number of high hash bits every identity of the shard shares.
            public readonly int Depth = depth;

            public Dictionary<SettledIdentity, HLCTimestamp> Map = map;
        }

        // 2^directoryDepth slots. A shard of depth d fills 2^(directoryDepth - d) adjacent slots.
        private Shard[] directory = [new Shard(0, [])];

        private int directoryDepth;

        private int count;

        private int shardCount = 1;

        // Where the physical sweep resumes; HashSpaceEnd when no sweep runs.
        private long sweepPosition = HashSpaceEnd;

        /// <summary>Number of identities held, logically retained or not.</summary>
        public int Count => count;

        /// <summary>Number of shards. Test and diagnostic seam.</summary>
        internal int ShardCount => shardCount;

        /// <summary>Whether a sweep started by <see cref="BeginSweep"/> has shards left to visit.</summary>
        public bool SweepInProgress => sweepPosition < HashSpaceEnd;

        public bool TryGetValue(in SettledIdentity identity, out HLCTimestamp settledAt) =>
            directory[SlotOf((uint)identity.GetHashCode())].Map.TryGetValue(identity, out settledAt);

        /// <summary>
        /// Stores <paramref name="settledAt"/> for <paramref name="identity"/> and returns whether the memory
        /// gained an entry. An identity already held keeps its time base, with two exceptions:
        /// <paramref name="replace"/> is set, or the held time base is before <paramref name="retainedFromL"/>.
        /// An entry before that bound is outside the retention window and absent to every verdict, so the record
        /// is that of a new identity. The result then does not depend on whether the sweep reached the entry.
        /// </summary>
        public bool Record(in SettledIdentity identity, HLCTimestamp settledAt, bool replace, long retainedFromL)
        {
            uint hash = (uint)identity.GetHashCode();
            Shard shard = directory[SlotOf(hash)];

            if (shard.Map.TryAdd(identity, settledAt))
            {
                count++;

                if (shard.Map.Count > MaxShardEntries && shard.Depth < MaxDepth)
                    Split(shard, hash);

                return true;
            }

            if (replace || (shard.Map.TryGetValue(identity, out HLCTimestamp held) && held.L < retainedFromL))
                shard.Map[identity] = settledAt;

            return false;
        }

        /// <summary>
        /// Appends to <paramref name="destination"/> the identities of the next shards from
        /// <paramref name="position"/> whose time base is at or after <paramref name="retainedFromL"/>, and moves
        /// the position past them. A walk starts at position 0. Returns false when the walk is complete.
        ///
        /// <para>A call reads whole shards and stops before the shard that does not fit in the capacity of
        /// <paramref name="destination"/>, so the caller bounds one step with that capacity. A shard larger than
        /// the capacity is read alone and grows the list.</para>
        ///
        /// <para>The caller may release the gate between calls. An identity held for the whole walk is copied
        /// once. One recorded during the walk is copied or not. The caller must keep the sweep from running
        /// between the calls of one walk: a sweep can remove an identity the walk did not reach yet.</para>
        /// </summary>
        public bool CopyNext(ref long position, List<SettledIdentityEntry> destination, long retainedFromL)
        {
            int visited = 0;

            while (position < HashSpaceEnd)
            {
                Shard shard = ShardAt(position, out long end);

                if (visited > 0 && (visited >= MaxShardsPerCopy || destination.Count + shard.Map.Count > destination.Capacity))
                    return true;

                foreach (KeyValuePair<SettledIdentity, HLCTimestamp> entry in shard.Map)
                {
                    if (entry.Value.L >= retainedFromL)
                        destination.Add(new SettledIdentityEntry(entry.Key.TransactionId, entry.Key.Epoch, entry.Key.Key, entry.Value));
                }

                position = end;
                visited++;
            }

            return false;
        }

        /// <summary>Starts a sweep of every shard, unless one is already in progress.</summary>
        public void BeginSweep()
        {
            if (sweepPosition >= HashSpaceEnd)
                sweepPosition = 0;
        }

        /// <summary>
        /// Removes from the next shard of the sweep the identities whose time base is before
        /// <paramref name="retainedFromL"/>, and reports how many it removed and the total length of their keys.
        /// One call visits one shard, so the time under the gate is that of a few hundred entries. A no-op when
        /// no sweep is in progress.
        /// </summary>
        public void SweepStep(long retainedFromL, out int removed, out long removedKeyChars)
        {
            removed = 0;
            removedKeyChars = 0;

            if (sweepPosition >= HashSpaceEnd)
                return;

            Shard shard = ShardAt(sweepPosition, out long end);
            Dictionary<SettledIdentity, HLCTimestamp> map = shard.Map;

            // A removal does not invalidate the enumerator of a Dictionary.
            foreach (KeyValuePair<SettledIdentity, HLCTimestamp> entry in map)
            {
                if (entry.Value.L >= retainedFromL)
                    continue;

                map.Remove(entry.Key);
                removed++;
                removedKeyChars += entry.Key.Key.Length;
            }

            if (removed > 0)
            {
                count -= removed;

                // A map keeps the arrays of its largest size. Release them when most of the shard left.
                if (map.Count == 0)
                    shard.Map = [];
                else if (map.Count * 8 <= map.EnsureCapacity(0))
                    shard.Map = new Dictionary<SettledIdentity, HLCTimestamp>(map);
            }

            sweepPosition = end;

            // The end of a sweep that left nothing: drop the directory a burst built.
            if (sweepPosition >= HashSpaceEnd && count == 0 && shardCount > 1)
            {
                directory = [new Shard(0, [])];
                directoryDepth = 0;
                shardCount = 1;
            }
        }

        private int SlotOf(uint hash) => directoryDepth == 0 ? 0 : (int)(hash >> (32 - directoryDepth));

        /// <summary>The shard that holds hash-space position <paramref name="position"/>, and the position one
        /// past its span.</summary>
        private Shard ShardAt(long position, out long end)
        {
            Shard shard = directory[SlotOf((uint)position)];
            long span = HashSpaceEnd >> shard.Depth;
            end = (position & ~(span - 1)) + span;
            return shard;
        }

        /// <summary>Divides <paramref name="shard"/>, the shard of <paramref name="hash"/>, by the next bit of
        /// the hash. The low half keeps the map of the shard, and the entries of the high half move to a new
        /// map, so a split allocates one map and moves about half of one shard.</summary>
        private void Split(Shard shard, uint hash)
        {
            if (shard.Depth == directoryDepth)
            {
                Shard[] doubled = new Shard[directory.Length * 2];
                for (int i = 0; i < directory.Length; i++)
                {
                    doubled[2 * i] = directory[i];
                    doubled[2 * i + 1] = directory[i];
                }

                directory = doubled;
                directoryDepth++;
            }

            int depth = shard.Depth + 1;
            int bit = 32 - depth;

            // The new map is sized for a full shard and the entry that triggers its split, so it does not
            // resize before then.
            Shard low = new(depth, shard.Map);
            Shard high = new(depth, new Dictionary<SettledIdentity, HLCTimestamp>(MaxShardEntries + 1));

            // A removal does not invalidate the enumerator of a Dictionary.
            foreach (KeyValuePair<SettledIdentity, HLCTimestamp> entry in shard.Map)
            {
                if ((((uint)entry.Key.GetHashCode() >> bit) & 1) == 0)
                    continue;

                high.Map.Add(entry.Key, entry.Value);
                shard.Map.Remove(entry.Key);
            }

            int slots = 1 << (directoryDepth - shard.Depth);
            int first = SlotOf(hash) & ~(slots - 1);
            int middle = first + slots / 2;

            for (int i = first; i < middle; i++)
                directory[i] = low;

            for (int i = middle; i < first + slots; i++)
                directory[i] = high;

            shardCount++;
        }
    }
}
