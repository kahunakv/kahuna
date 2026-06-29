
using System.Buffers;
using System.Buffers.Text;
using System.Runtime.InteropServices;
using System.Text;
using Kahuna.Server.Locks;
using Kahuna.Persistence.Protos;
using Kahuna.Server.Persistence.Pitr;
using Kommander.Time;
using RocksDbSharp;
using Google.Protobuf;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks.Data;
namespace Kahuna.Server.Persistence.Backend;

/// <summary>
/// Provides persistence backend functionality using RocksDB as the underlying database.
/// Implements the <see cref="IPersistenceBackend"/> interface along with IDisposable.
/// </summary>
/// <remarks>
/// This class allows storing and retrieving data categorized into different column families
/// such as key-values and locks. The implementation ensures efficient read-write operations
/// and transactional support using RocksDB capabilities.
/// </remarks>
internal sealed class RocksDbPersistenceBackend : IPersistenceBackend, IDisposable
{
    /// <summary>
    /// Defines a constant string value used as a marker suffix appended to keys
    /// within the database operations. This marker is utilized to represent the
    /// "current" version of an item in RocksDB storage, ensuring unique identification
    /// of the latest state of a record.
    /// </summary>
    private const string CurrentMarker = "~CURRENT";

    // UTF-8 encoding of CurrentMarker for allocation-free key construction. The Debug.Assert
    // below catches any future edit to the const that forgets to update this literal.
    private static ReadOnlySpan<byte> CurrentMarkerUtf8 => "~CURRENT"u8;

    // Guard for stackalloc on key buffers. Inputs are caller-controlled, so we must bound the
    // stack frame; anything larger goes through ArrayPool. Real keys are well under this, so the
    // ArrayPool path is rarely taken while the per-call stack zeroing stays cheap.
    private const int KeyStackThreshold = 256;

    /// <summary>
    /// Represents the default write options for write operations in the persistence backend.
    /// Configured with synchronization enabled to ensure durability by flushing changes to disk
    /// before returning control to the calling code.
    /// </summary>
    /// <remarks>
    /// This is a deliberate second fsync: writes already pass through the durable Raft WAL before
    /// reaching this backend. Sync is retained because the checkpoint mechanism can truncate the WAL
    /// once a partition is checkpointed, after which this store becomes the source of truth for that
    /// data — dropping sync here would risk losing checkpointed state on a crash. Relaxing it would
    /// require ordering guarantees between checkpoint/WAL-truncation and this store's flush, which is
    /// out of scope for the table-tuning change.
    /// </remarks>
    private static readonly WriteOptions DefaultWriteOptions = new WriteOptions().SetSync(true);

    private static readonly Comparison<(long Revision, long LastModifiedPhysical, byte[] RawKeyBytes)> RevisionDescComparer =
        static (a, b) => b.Revision.CompareTo(a.Revision);

    /// <summary>
    /// Shared block cache for the table reader. Sized once and reused across both column families so
    /// hot index/filter/data blocks stay resident instead of being re-read from disk on every point
    /// lookup. Held in a static field to keep it alive for the lifetime of the process.
    /// </summary>
    private static readonly Cache BlockCache = Cache.CreateLru(256 * 1024 * 1024);

    private readonly RocksDb db;

    private readonly ColumnFamilyHandle columnFamilyKeys;
    
    private readonly ColumnFamilyHandle columnFamilyLocks;
    
    private readonly string path;

    private readonly string dbRevision;

    /// <summary>
    /// Raw key bytes at which the next backend-wide revision sweep should resume, or <c>null</c> to
    /// start from the beginning of the column family. Carried across sweep passes so each pass scans
    /// a bounded slice of the keyspace instead of the whole column family every interval.
    /// </summary>
    private byte[]? sweepCursor;

    /// <summary>
    /// RocksDbPersistenceBackend is a persistence backend implementation based on RocksDB.
    /// It provides methods for storing and retrieving key-value pairs and locks, categorized
    /// into distinct column families for better organization and efficiency.
    /// </summary>
    /// <remarks>
    /// The class offers support for efficient data management, including transactional
    /// operations, prefix-based queries, and version-specific retrievals. It integrates
    /// RocksDB's advanced features, ensuring high-performance persistent storage.
    /// </remarks>
    public RocksDbPersistenceBackend(string path = ".", string dbRevision = "v1")
    {
        this.path = path;
        this.dbRevision = dbRevision;

        string fullPath = $"{path}/{dbRevision}";

        DbOptions dbOptions = new DbOptions()
            .SetCreateIfMissing(true)
            .SetCreateMissingColumnFamilies(true)
            .SetWalRecoveryMode(Recovery.AbsoluteConsistency)
            // Use available cores for background flush/compaction so writes don't stall behind a
            // single-threaded compactor under sustained load.
            .IncreaseParallelism(Math.Max(2, Environment.ProcessorCount))
            .SetMaxBackgroundFlushes(2)
            .SetMaxBackgroundCompactions(Math.Max(2, Environment.ProcessorCount / 2))
            // Larger memtable absorbs more writes before flushing, reducing write amplification.
            .SetWriteBufferSize(64 * 1024 * 1024)
            .SetMaxWriteBufferNumber(3)
            .SetMinWriteBufferNumberToMerge(1);

        // Bloom filters + a shared block cache make point lookups (GetKeyValue/GetLock, the read hot
        // path) avoid touching SSTs on a miss and keep hot blocks resident across reads.
        BlockBasedTableOptions tableOptions = new BlockBasedTableOptions()
            .SetBlockCache(BlockCache)
            .SetFilterPolicy(BloomFilterPolicy.Create(10, false))
            .SetCacheIndexAndFilterBlocks(true)
            .SetWholeKeyFiltering(true);

        ColumnFamilyOptions kvOptions = new ColumnFamilyOptions().SetBlockBasedTableFactory(tableOptions);
        ColumnFamilyOptions locksOptions = new ColumnFamilyOptions().SetBlockBasedTableFactory(tableOptions);

        ColumnFamilies columnFamilies = new()
        {
            { "kv", kvOptions },
            { "locks", locksOptions }
        };

        db = RocksDb.Open(dbOptions, fullPath, columnFamilies);
        
        columnFamilyKeys = db.GetColumnFamily("kv");
        columnFamilyLocks = db.GetColumnFamily("locks");
    }

    /// <summary>
    /// Stores a batch of lock-related items into the RocksDB database within the designated column family.
    /// </summary>
    /// <param name="items">A list of <see cref="PersistenceRequestItem"/> representing the locks and their associated metadata to be stored.</param>
    /// <returns>Returns <c>true</c> if the operation completes successfully.</returns>
    public bool StoreLocks(List<PersistenceRequestItem> items)
    {
        using WriteBatch batch = new();

        foreach (PersistenceRequestItem item in items)
        {
            RocksDbLockMessage kvm = new()
            {
                ExpiresPhysical = item.ExpiresPhysical,
                ExpiresCounter = item.ExpiresCounter,
                LastUsedPhysical = item.LastUsedPhysical,
                LastUsedCounter = item.LastUsedCounter,
                LastModifiedPhysical = item.LastModifiedPhysical,
                LastModifiedCounter = item.LastModifiedCounter,
                FencingToken = item.Revision,
                State = item.State
            };

            if (item.Value != null)
                kvm.Owner = UnsafeByteOperations.UnsafeWrap(item.Value);
            
            PutLocksItems(batch, item, kvm, columnFamilyLocks);
        }
        
        db.Write(batch, DefaultWriteOptions);

        return true;
    }

    /// <summary>
    /// Inserts lock items into the RocksDB database using the specified write batch, item data,
    /// lock message, and column family handle.
    /// </summary>
    /// <param name="batch">The write batch used for batching multiple write operations.</param>
    /// <param name="item">The lock item containing key, revision, and additional metadata.</param>
    /// <param name="kvm">The RocksDB lock message to be serialized and persisted.</param>
    /// <param name="columnFamily">The column family handle identifying the column family where the data is stored.</param>
    private static void PutLocksItems(WriteBatch batch, PersistenceRequestItem item, RocksDbLockMessage kvm, ColumnFamilyHandle columnFamily)
    {
        int serializedSize = kvm.CalculateSize();
        int keyLen = Encoding.UTF8.GetByteCount(item.Key);
        // The ~<revision> key is the larger of the two indices; one bounded buffer serves both.
        // 21 = '~'(1) + 20 digits (long.MinValue = -9223372036854775808 is 20 chars including '-').
        int maxLen = keyLen + 21;

        byte[]? rentedKey = null;
        byte[] rentedSer = ArrayPool<byte>.Shared.Rent(serializedSize);
        Span<byte> buffer = maxLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rentedKey = ArrayPool<byte>.Shared.Rent(maxLen));
        try
        {
            using (CodedOutputStream cos = new(rentedSer))
                kvm.WriteTo(cos);

            ReadOnlySpan<byte> serialized = rentedSer.AsSpan(0, serializedSize);

            // ~CURRENT key: encode key + CurrentMarker suffix without an intermediate string.
            System.Diagnostics.Debug.Assert(CurrentMarker.Length == CurrentMarkerUtf8.Length,
                "CurrentMarker string and CurrentMarkerUtf8 span have drifted — update the u8 literal");
            Encoding.UTF8.GetBytes(item.Key, buffer);
            CurrentMarkerUtf8.CopyTo(buffer[keyLen..]);
            batch.Put(buffer[..(keyLen + CurrentMarkerUtf8.Length)], serialized, cf: columnFamily);

            // ~<revision> key: reuse the key bytes already at buffer[0..keyLen), overwrite the suffix.
            buffer[keyLen] = (byte)'~';
            bool formatted = Utf8Formatter.TryFormat(item.Revision, buffer[(keyLen + 1)..], out int revLen);
            System.Diagnostics.Debug.Assert(formatted, "Utf8Formatter.TryFormat failed for revision key");
            batch.Put(buffer[..(keyLen + 1 + revLen)], serialized, cf: columnFamily);
        }
        finally
        {
            if (rentedKey is not null)
                ArrayPool<byte>.Shared.Return(rentedKey);
            ArrayPool<byte>.Shared.Return(rentedSer);
        }
    }

    /// <summary>
    /// Stores a collection of key-value pairs and their associated metadata into the persistence backend.
    /// </summary>
    /// <param name="items">A list of <see cref="PersistenceRequestItem"/> objects, each representing key-value pairs and metadata to be stored.</param>
    /// <returns>Returns <c>true</c> if the operation is successfully completed.</returns>
    public bool StoreKeyValues(List<PersistenceRequestItem> items)
    {
        using WriteBatch batch = new();
        
        foreach (PersistenceRequestItem item in items)
        {
            RocksDbKeyValueMessage kvm = new()
            {
                ExpiresPhysical = item.ExpiresPhysical,
                ExpiresCounter = item.ExpiresCounter,
                LastUsedPhysical = item.LastUsedPhysical,
                LastUsedCounter = item.LastUsedCounter,
                LastModifiedPhysical = item.LastModifiedPhysical,
                LastModifiedCounter = item.LastModifiedCounter,
                Revision = item.Revision,
                State = item.State
            };

            if (item.Value is not null)
                kvm.Value = UnsafeByteOperations.UnsafeWrap(item.Value);
            
            PutStoreItems(batch, item, kvm, columnFamilyKeys);
        }

        db.Write(batch, DefaultWriteOptions);

        return true;
    }

    /// <summary>
    /// Adds serialized representation of a key-value message to the WriteBatch in RocksDB,
    /// associating it with specific keys and a column family.
    /// This method handles the creation of indices for current marker and revision-specific keys.
    /// </summary>
    /// <param name="batch">
    /// The WriteBatch used to batch operations for writing into RocksDB.
    /// </param>
    /// <param name="item">
    /// The persistence request item containing the key and revision data used for creating the entry.
    /// </param>
    /// <param name="kvm">
    /// The RocksDbKeyValueMessage object holding metadata and value to be stored.
    /// </param>
    /// <param name="columnFamily">
    /// The RocksDB column family handle where the key-value pair will be stored.
    /// </param>
    private static void PutStoreItems(WriteBatch batch, PersistenceRequestItem item, RocksDbKeyValueMessage kvm, ColumnFamilyHandle columnFamily)
    {
        int serializedSize = kvm.CalculateSize();
        int keyLen = Encoding.UTF8.GetByteCount(item.Key);
        // The ~<revision> key is the larger of the two indices; one bounded buffer serves both.
        // 21 = '~'(1) + 20 digits (long.MinValue = -9223372036854775808 is 20 chars including '-').
        int maxLen = keyLen + 21;

        byte[]? rentedKey = null;
        byte[] rentedSer = ArrayPool<byte>.Shared.Rent(serializedSize);
        Span<byte> buffer = maxLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rentedKey = ArrayPool<byte>.Shared.Rent(maxLen));
        try
        {
            using (CodedOutputStream cos = new(rentedSer))
                kvm.WriteTo(cos);

            ReadOnlySpan<byte> serialized = rentedSer.AsSpan(0, serializedSize);

            // ~CURRENT key: encode key + CurrentMarker suffix without an intermediate string.
            System.Diagnostics.Debug.Assert(CurrentMarker.Length == CurrentMarkerUtf8.Length,
                "CurrentMarker string and CurrentMarkerUtf8 span have drifted — update the u8 literal");
            Encoding.UTF8.GetBytes(item.Key, buffer);
            CurrentMarkerUtf8.CopyTo(buffer[keyLen..]);
            batch.Put(buffer[..(keyLen + CurrentMarkerUtf8.Length)], serialized, cf: columnFamily);

            // ~<revision> key: reuse the key bytes already at buffer[0..keyLen), overwrite the suffix.
            buffer[keyLen] = (byte)'~';
            bool formatted = Utf8Formatter.TryFormat(item.Revision, buffer[(keyLen + 1)..], out int revLen);
            System.Diagnostics.Debug.Assert(formatted, "Utf8Formatter.TryFormat failed for revision key");
            batch.Put(buffer[..(keyLen + 1 + revLen)], serialized, cf: columnFamily);
        }
        finally
        {
            if (rentedKey is not null)
                ArrayPool<byte>.Shared.Return(rentedKey);
            ArrayPool<byte>.Shared.Return(rentedSer);
        }
    }

    /// <summary>
    /// Retrieves the lock context associated with the specified resource.
    /// </summary>
    /// <param name="resource">The unique identifier of the resource for which the lock context is being retrieved.</param>
    /// <returns>
    /// A <see cref="LockEntry"/> instance containing details about the lock if it exists;
    /// otherwise, null if no lock is associated with the specified resource.
    /// </returns>
    public LockEntry? GetLock(string resource)
    {
        int keyLen = Encoding.UTF8.GetByteCount(resource);
        int totalLen = keyLen + CurrentMarkerUtf8.Length;

        byte[]? rented = null;
        Span<byte> buffer = totalLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rented = ArrayPool<byte>.Shared.Rent(totalLen));
        try
        {
            buffer = buffer[..totalLen];
            Encoding.UTF8.GetBytes(resource.AsSpan(), buffer);
            CurrentMarkerUtf8.CopyTo(buffer[keyLen..]);

            byte[]? value = db.Get(buffer, cf: columnFamilyLocks);
            if (value is null)
                return null;

            RocksDbLockMessage message = UnserializeLockMessage(value);

            byte[]? owner;

            if (MemoryMarshal.TryGetArray(message.Owner.Memory, out ArraySegment<byte> segment))
                owner = segment.Array;
            else
                owner = message.Owner.ToByteArray();

            return new()
            {
                Owner = owner,
                FencingToken = message.FencingToken,
                Expires = new(message.ExpiresNode, message.ExpiresPhysical, message.ExpiresCounter),
                LastUsed = new(message.LastUsedNode, message.LastUsedPhysical, message.LastUsedCounter),
                LastModified = new(message.LastModifiedNode, message.LastModifiedPhysical, message.LastModifiedCounter),
                State = (LockState)message.State
            };
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Retrieves the key-value context associated with the specified key name. If the key does not exist,
    /// the method returns null.
    /// </summary>
    /// <param name="keyName">The name of the key to retrieve the associated key-value context.</param>
    /// <returns>
    /// A <see cref="KeyValueEntry"/> object containing the value, revision, expiration details, and other metadata
    /// associated with the key, or null if the key does not exist.
    /// </returns>
    public KeyValueEntry? GetKeyValue(string keyName)
    {
        int keyLen = Encoding.UTF8.GetByteCount(keyName);
        int totalLen = keyLen + CurrentMarkerUtf8.Length;

        byte[]? rented = null;
        Span<byte> buffer = totalLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rented = ArrayPool<byte>.Shared.Rent(totalLen));
        try
        {
            buffer = buffer[..totalLen];
            Encoding.UTF8.GetBytes(keyName.AsSpan(), buffer);
            CurrentMarkerUtf8.CopyTo(buffer[keyLen..]);

            byte[]? value = db.Get(buffer, cf: columnFamilyKeys);
            if (value is null)
                return null;

            RocksDbKeyValueMessage message = UnserializeKeyValueMessage(value);

            byte[]? messageValue;

            if (MemoryMarshal.TryGetArray(message.Value.Memory, out ArraySegment<byte> segment))
                messageValue = segment.Array;
            else
                messageValue = message.Value.ToByteArray();

            return new()
            {
                Value = messageValue,
                Revision = message.Revision,
                Expires = new(message.ExpiresNode, message.ExpiresPhysical, message.ExpiresCounter),
                LastUsed = new(message.LastUsedNode, message.LastUsedPhysical, message.LastUsedCounter),
                LastModified = new(message.LastModifiedNode, message.LastModifiedPhysical, message.LastModifiedCounter),
                State = (KeyValueState)message.State,
            };
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Retrieves a key-value revision based on the specified key name and revision number.
    /// </summary>
    /// <param name="keyName">The name of the key to retrieve.</param>
    /// <param name="revision">The specific revision number of the key to retrieve.</param>
    /// <returns>
    /// A <see cref="KeyValueEntry"/> object containing the key-value pair metadata and value,
    /// or <c>null</c> if the key or revision is not found.
    /// </returns>
    public KeyValueEntry? GetKeyValueRevision(string keyName, long revision)
    {
        int keyLen = Encoding.UTF8.GetByteCount(keyName);
        int maxLen = keyLen + 21; // '~'(1) + up to 20 decimal digits

        byte[]? rented = null;
        Span<byte> buffer = maxLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rented = ArrayPool<byte>.Shared.Rent(maxLen));
        try
        {
            Encoding.UTF8.GetBytes(keyName, buffer);
            buffer[keyLen] = (byte)'~';
            bool formatted = Utf8Formatter.TryFormat(revision, buffer[(keyLen + 1)..], out int revLen);
            System.Diagnostics.Debug.Assert(formatted, "Utf8Formatter.TryFormat failed for revision lookup key");

            byte[]? value = db.Get(buffer[..(keyLen + 1 + revLen)], cf: columnFamilyKeys);
            if (value is null)
                return null;

            RocksDbKeyValueMessage message = UnserializeKeyValueMessage(value);

            byte[]? messageValue;

            if (MemoryMarshal.TryGetArray(message.Value.Memory, out ArraySegment<byte> segment))
                messageValue = segment.Array;
            else
                messageValue = message.Value.ToByteArray();

            return new()
            {
                Value = messageValue,
                Revision = message.Revision,
                Expires = new(message.ExpiresNode, message.ExpiresPhysical, message.ExpiresCounter),
                LastUsed = new(message.LastUsedNode, message.LastUsedPhysical, message.LastUsedCounter),
                LastModified = new(message.LastModifiedNode, message.LastModifiedPhysical, message.LastModifiedCounter),
                State = (KeyValueState)message.State,
            };
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Retrieves a list of key-value pairs that match the specified prefix key.
    /// </summary>
    /// <param name="prefixKeyName">The prefix string used to filter and retrieve matching key-value pairs.</param>
    /// <returns>A list of tuples where each tuple contains a string key and a corresponding <see cref="ReadOnlyKeyValueEntry"/> value.</returns>
    public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName)
    {
        List<(string, ReadOnlyKeyValueEntry)> result = [];

        int prefixLen = Encoding.UTF8.GetByteCount(prefixKeyName);
        byte[]? rented = null;
        Span<byte> prefixBytes = prefixLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rented = ArrayPool<byte>.Shared.Rent(prefixLen));
        try
        {
            prefixBytes = prefixBytes[..prefixLen];
            Encoding.UTF8.GetBytes(prefixKeyName, prefixBytes);

            using Iterator? iterator = db.NewIterator(cf: columnFamilyKeys);
            iterator.Seek(prefixBytes);

            while (iterator.Valid() && result.Count < KeyValueScanLimits.MaxPrefixScanResults)
            {
                // GetKeySpan returns a span directly over native memory — no byte[] copy.
                // Valid only until the next iterator move; consumed entirely before Next().
                ReadOnlySpan<byte> rawKey = iterator.GetKeySpan();

                if (!rawKey.StartsWith(prefixBytes))
                    break;

                if (!rawKey.EndsWith(CurrentMarkerUtf8))
                {
                    iterator.Next();
                    continue;
                }

                // Decode only keys that pass both filters.
                string keyWithoutMarker = Encoding.UTF8.GetString(rawKey[..^CurrentMarkerUtf8.Length]);

                RocksDbKeyValueMessage message = UnserializeKeyValueMessage(iterator.GetValueSpan());

                byte[]? messageValue;
                if (MemoryMarshal.TryGetArray(message.Value.Memory, out ArraySegment<byte> segment))
                    messageValue = segment.Array;
                else
                    messageValue = message.Value.ToByteArray();

                result.Add((keyWithoutMarker, new(
                    messageValue,
                    message.Revision,
                    new(message.ExpiresNode, message.ExpiresPhysical, message.ExpiresCounter),
                    new(message.LastUsedNode, message.LastUsedPhysical, message.LastUsedCounter),
                    new(message.LastModifiedNode, message.LastModifiedPhysical, message.LastModifiedCounter),
                    (KeyValueState)message.State
                )));

                iterator.Next();
            }

            return result;
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Retrieves a bounded, ordered page of key-value pairs whose keys start with <paramref name="prefix"/>,
    /// beginning at <paramref name="startKey"/> (or the prefix start if null), up to <paramref name="limit"/> entries.
    /// </summary>
    public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit)
    {
        List<(string, ReadOnlyKeyValueEntry)> result = [];

        int prefixLen = Encoding.UTF8.GetByteCount(prefix);
        string seekStr = startKey ?? prefix;
        int seekLen = Encoding.UTF8.GetByteCount(seekStr);

        byte[]? rentedPrefix = null;
        Span<byte> prefixBytes = prefixLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rentedPrefix = ArrayPool<byte>.Shared.Rent(prefixLen));

        byte[]? rentedSeek = null;
        Span<byte> seekBytes = seekLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rentedSeek = ArrayPool<byte>.Shared.Rent(seekLen));

        try
        {
            prefixBytes = prefixBytes[..prefixLen];
            Encoding.UTF8.GetBytes(prefix, prefixBytes);

            seekBytes = seekBytes[..seekLen];
            Encoding.UTF8.GetBytes(seekStr, seekBytes);

            using Iterator iterator = db.NewIterator(cf: columnFamilyKeys);
            iterator.Seek(seekBytes);

            while (iterator.Valid() && result.Count < limit)
            {
                // GetKeySpan returns a span directly over native memory — no byte[] copy.
                // Valid only until the next iterator move; consumed entirely before Next().
                ReadOnlySpan<byte> rawKey = iterator.GetKeySpan();

                if (!rawKey.StartsWith(prefixBytes))
                    break;

                if (!rawKey.EndsWith(CurrentMarkerUtf8))
                {
                    iterator.Next();
                    continue;
                }

                // Decode only keys that pass both filters.
                string keyWithoutMarker = Encoding.UTF8.GetString(rawKey[..^CurrentMarkerUtf8.Length]);

                RocksDbKeyValueMessage message = UnserializeKeyValueMessage(iterator.GetValueSpan());

                byte[]? messageValue;
                if (MemoryMarshal.TryGetArray(message.Value.Memory, out ArraySegment<byte> segment))
                    messageValue = segment.Array;
                else
                    messageValue = message.Value.ToByteArray();

                result.Add((keyWithoutMarker, new(
                    messageValue,
                    message.Revision,
                    new(message.ExpiresNode, message.ExpiresPhysical, message.ExpiresCounter),
                    new(message.LastUsedNode, message.LastUsedPhysical, message.LastUsedCounter),
                    new(message.LastModifiedNode, message.LastModifiedPhysical, message.LastModifiedCounter),
                    (KeyValueState)message.State
                )));

                iterator.Next();
            }

            return result;
        }
        finally
        {
            if (rentedPrefix is not null)
                ArrayPool<byte>.Shared.Return(rentedPrefix);
            if (rentedSeek is not null)
                ArrayPool<byte>.Shared.Return(rentedSeek);
        }
    }


    private static RocksDbLockMessage UnserializeLockMessage(ReadOnlySpan<byte> serializedData) =>
        RocksDbLockMessage.Parser.ParseFrom(serializedData);

    private static RocksDbKeyValueMessage UnserializeKeyValueMessage(ReadOnlySpan<byte> serializedData) =>
        RocksDbKeyValueMessage.Parser.ParseFrom(serializedData);

    public bool PruneKeyValueRevisions(
        IReadOnlyCollection<string>? keys,
        int retentionCount,
        TimeSpan retentionAge,
        int batchSize,
        out RevisionPruneResult result)
    {
        int keysVisited = 0;
        int deleted = 0;
        bool batchLimitReached = false;
        List<string>? remaining = null;

        if (keys is not null)
        {
            IList<string> keyList = keys as IList<string> ?? keys.ToList();

            for (int i = 0; i < keyList.Count; i++)
            {
                if (deleted >= batchSize)
                {
                    // Batch full before reaching this key — everything from here on still needs work.
                    batchLimitReached = true;
                    for (int j = i; j < keyList.Count; j++)
                        (remaining ??= []).Add(keyList[j]);
                    break;
                }

                string key = keyList[i];
                PruneRevisionsForKey(key, retentionCount, retentionAge, batchSize, ref deleted, out bool keyLimitReached);
                keysVisited++;

                if (keyLimitReached)
                {
                    // This key was only partially pruned, and the batch is now full.
                    batchLimitReached = true;
                    (remaining ??= []).Add(key);
                    for (int j = i + 1; j < keyList.Count; j++)
                        (remaining ??= []).Add(keyList[j]);
                    break;
                }
            }
        }
        else
        {
            // Backend-wide sweep: visit each logical key via its ~CURRENT entry, resuming from the
            // cursor left by the previous pass so each pass scans only a bounded slice (at most
            // batchSize keys or batchSize deletes) instead of the whole column family.
            int keyBudget = batchSize;
            bool paused = false;

            using Iterator iterator = db.NewIterator(cf: columnFamilyKeys);

            if (sweepCursor is null)
                iterator.SeekToFirst();
            else
                iterator.Seek(sweepCursor);

            while (iterator.Valid())
            {
                if (deleted >= batchSize || keysVisited >= keyBudget)
                {
                    // Pause here; resume from the current (unprocessed) entry next pass.
                    sweepCursor = iterator.GetKeySpan().ToArray();
                    batchLimitReached = true;
                    paused = true;
                    break;
                }

                // Only ~CURRENT rows map to a logical key to prune. Test the suffix on the native
                // span and decode just the logical key for those — revision rows skip the decode.
                ReadOnlySpan<byte> rawKeySpan = iterator.GetKeySpan();

                if (rawKeySpan.EndsWith(CurrentMarkerUtf8))
                {
                    string logicalKey = Encoding.UTF8.GetString(rawKeySpan[..^CurrentMarkerUtf8.Length]);
                    PruneRevisionsForKey(logicalKey, retentionCount, retentionAge, batchSize, ref deleted, out bool keyLimitReached);
                    keysVisited++;

                    if (keyLimitReached)
                    {
                        // Key only partially pruned — resume at this same key next pass.
                        sweepCursor = rawKeySpan.ToArray(); // ToArray only on pause path
                        batchLimitReached = true;
                        paused = true;
                        break;
                    }
                }

                iterator.Next();
            }

            // Reached the end of the column family without pausing: full scan complete, wrap around.
            if (!paused)
                sweepCursor = null;
        }

        result = new(keysVisited, deleted, batchLimitReached, remaining);
        return true;
    }

    /// <summary>
    /// Prunes old revision records for a single logical key, keeping the current revision
    /// and any revisions within the configured count/age retention window.
    /// </summary>
    private void PruneRevisionsForKey(
        string key,
        int retentionCount,
        TimeSpan retentionAge,
        int batchSize,
        ref int deleted,
        out bool batchLimitReached)
    {
        batchLimitReached = false;

        int keyLen = Encoding.UTF8.GetByteCount(key);
        // ~CURRENT is the longer suffix; one buffer serves both the marker lookup and the ~ prefix.
        int maxLen = keyLen + CurrentMarkerUtf8.Length;

        byte[]? rentedKey = null;
        Span<byte> keyBuffer = maxLen <= KeyStackThreshold
            ? stackalloc byte[KeyStackThreshold]
            : (rentedKey = ArrayPool<byte>.Shared.Rent(maxLen));
        try
        {
            // Encode the key once; both RocksDB lookups reuse these bytes.
            Encoding.UTF8.GetBytes(key.AsSpan(), keyBuffer);

            // Determine the current revision so we never delete its historical record.
            CurrentMarkerUtf8.CopyTo(keyBuffer[keyLen..]);
            byte[]? currentData = db.Get(keyBuffer[..(keyLen + CurrentMarkerUtf8.Length)], cf: columnFamilyKeys);
            if (currentData is null)
                return;

            RocksDbKeyValueMessage currentMessage = UnserializeKeyValueMessage(currentData);
            long currentRevision = currentMessage.Revision;

            // Collect all revision entries for this key by seeking to "<key>~".
            // Revision entries have numeric suffixes; ~CURRENT is skipped.
            // Reuse key bytes already in keyBuffer[0..keyLen); overwrite suffix to just "~".
            keyBuffer[keyLen] = (byte)'~';
            ReadOnlySpan<byte> prefixBytes = keyBuffer[..(keyLen + 1)];

            bool needAge = retentionAge > TimeSpan.Zero;
            long cutoffPhysical = needAge
                ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - (long)retentionAge.TotalMilliseconds
                : long.MinValue;

            List<(long Revision, long LastModifiedPhysical, byte[] RawKeyBytes)> revisions = [];

            using (Iterator iterator = db.NewIterator(cf: columnFamilyKeys))
            {
                iterator.Seek(prefixBytes);

                while (iterator.Valid())
                {
                    // Filter on the native key span — no per-row string decode. Only rows we keep are
                    // materialised (their key bytes are retained below for the later batch.Delete).
                    ReadOnlySpan<byte> entryKey = iterator.GetKeySpan();

                    if (!entryKey.StartsWith(prefixBytes))
                        break;

                    // Skip the ~CURRENT sentinel — never a candidate for deletion.
                    if (entryKey.EndsWith(CurrentMarkerUtf8))
                    {
                        iterator.Next();
                        continue;
                    }

                    // The suffix after "<key>~" must be a numeric revision; reject any trailing junk
                    // so "<key>~123abc" is ignored exactly as long.TryParse would.
                    ReadOnlySpan<byte> suffix = entryKey[prefixBytes.Length..];
                    if (!Utf8Parser.TryParse(suffix, out long revisionNum, out int consumed) || consumed != suffix.Length)
                    {
                        iterator.Next();
                        continue;
                    }

                    long lastModifiedPhysical = 0;
                    if (needAge)
                    {
                        RocksDbKeyValueMessage msg = UnserializeKeyValueMessage(iterator.GetValueSpan());
                        lastModifiedPhysical = msg.LastModifiedPhysical;
                    }

                    // Copy the key out of native memory: it must outlive the iterator for batch.Delete.
                    revisions.Add((revisionNum, lastModifiedPhysical, entryKey.ToArray()));
                    iterator.Next();
                }
            }

            if (revisions.Count == 0)
                return;

            // Sort descending so index 0 is the newest revision.
            revisions.Sort(RevisionDescComparer);

            using WriteBatch batch = new();
            int deletedInBatch = 0;

            for (int i = 0; i < revisions.Count; i++)
            {
                (long revNum, long lastModifiedPhysical, byte[] rawKeyBytes) = revisions[i];

                // Always protect the current revision's historical record.
                if (revNum == currentRevision)
                    continue;

                bool deleteByCount = retentionCount > 0 && i >= retentionCount;
                bool deleteByAge = needAge && lastModifiedPhysical < cutoffPhysical;

                if (!deleteByCount && !deleteByAge)
                    continue;

                if (deleted + deletedInBatch >= batchSize)
                {
                    batchLimitReached = true;
                    break;
                }

                batch.Delete(rawKeyBytes, cf: columnFamilyKeys);
                deletedInBatch++;
            }

            if (deletedInBatch > 0)
                db.Write(batch, DefaultWriteOptions);

            deleted += deletedInBatch;
        }
        finally
        {
            if (rentedKey is not null) ArrayPool<byte>.Shared.Return(rentedKey);
        }
    }

    public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime)
    {
        // rocksdb_checkpoint_create requires the leaf to NOT exist — it creates the directory
        // itself. Use a temp sibling so a failure before the rename can never leave a partial
        // checkpoint at destinationPath.
        string? parent = Path.GetDirectoryName(destinationPath);
        if (parent is not null)
            Directory.CreateDirectory(parent);

        string tmpPath = destinationPath + ".tmp_" + Guid.NewGuid().ToString("N")[..8];

        try
        {
            using Checkpoint cp = db.Checkpoint();
            cp.Save(tmpPath, logSizeForFlush: 0); // RocksDB creates tmpPath

            CheckpointManifest manifest = CheckpointManifest.From(appliedIndex, appliedTime);
            manifest.WriteTo(tmpPath);

            Directory.Move(tmpPath, destinationPath);

            return new(destinationPath, manifest);
        }
        catch
        {
            if (Directory.Exists(tmpPath))
                Directory.Delete(tmpPath, recursive: true);
            throw;
        }
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);

        db.Dispose();
    }
}
