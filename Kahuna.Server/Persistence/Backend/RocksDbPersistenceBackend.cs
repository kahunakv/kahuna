
using System.Runtime.InteropServices;
using System.Text;
using Kahuna.Server.Locks;
using Kahuna.Persistence.Protos;
using RocksDbSharp;
using Google.Protobuf;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks.Data;
using Microsoft.IO;

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
    /// Represents the maximum allowable size, in bytes, for a serialized message
    /// within the persistence backend. This limit is applied to both lock and
    /// key-value messages being processed to ensure they do not exceed memory
    /// constraints and to maintain efficient performance.
    /// </summary>
    private const int MaxMessageSize = 1024;

    /// <summary>
    /// Defines a constant string value used as a marker suffix appended to keys
    /// within the database operations. This marker is utilized to represent the
    /// "current" version of an item in RocksDB storage, ensuring unique identification
    /// of the latest state of a record.
    /// </summary>
    private const string CurrentMarker = "~CURRENT";

    /// <summary>
    /// Manages memory streams efficiently by utilizing the RecyclableMemoryStreamManager,
    /// which is designed to reduce memory fragmentation and improve memory allocation
    /// performance for I/O operations. This variable is used to acquire recyclable memory
    /// streams throughout the RocksDbPersistenceBackend implementation to handle operations
    /// like serialization and deserialization efficiently.
    /// </summary>
    private static readonly RecyclableMemoryStreamManager manager = new();

    /// <summary>
    /// Represents the default write options for write operations in the persistence backend.
    /// Configured with synchronization enabled to ensure durability by flushing changes to disk
    /// before returning control to the calling code.
    /// </summary>
    private static readonly WriteOptions DefaultWriteOptions = new WriteOptions().SetSync(true);
    
    private readonly RocksDb db;

    private readonly ColumnFamilyHandle columnFamilyKeys;
    
    private readonly ColumnFamilyHandle columnFamilyLocks;
    
    private readonly string path;
    
    private readonly string dbRevision;

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
            .SetWalRecoveryMode(Recovery.AbsoluteConsistency);
        
        ColumnFamilies columnFamilies = new()
        {
            { "kv", new() },
            { "locks", new() }
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
        byte[] serialized = Serialize(kvm);

        string currentMarker = string.Concat(item.Key, CurrentMarker);
        
        Span<byte> index1 = stackalloc byte[Encoding.UTF8.GetByteCount(currentMarker)];
        Encoding.UTF8.GetBytes(currentMarker.AsSpan(), index1);
        
        batch.Put(index1, serialized, cf: columnFamily);
        
        string keyRevision = string.Concat(item.Key, "~", item.Revision);
        
        Span<byte> index2 = stackalloc byte[Encoding.UTF8.GetByteCount(keyRevision)];
        Encoding.UTF8.GetBytes(keyRevision.AsSpan(), index2);
        
        batch.Put(index2, serialized, cf: columnFamily);
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
        byte[] serialized = Serialize(kvm);

        string currentMarker = string.Concat(item.Key, CurrentMarker);
        
        Span<byte> index1 = stackalloc byte[Encoding.UTF8.GetByteCount(currentMarker)];
        Encoding.UTF8.GetBytes(currentMarker.AsSpan(), index1);
        
        batch.Put(index1, serialized, cf: columnFamily);
        
        string keyRevision = string.Concat(item.Key, "~", item.Revision);
        
        Span<byte> index2 = stackalloc byte[Encoding.UTF8.GetByteCount(keyRevision)];
        Encoding.UTF8.GetBytes(keyRevision.AsSpan(), index2);
        
        batch.Put(index2, serialized, cf: columnFamily);
    }

    /// <summary>
    /// Retrieves the lock context associated with the specified resource.
    /// </summary>
    /// <param name="resource">The unique identifier of the resource for which the lock context is being retrieved.</param>
    /// <returns>
    /// A <see cref="LockContext"/> instance containing details about the lock if it exists;
    /// otherwise, null if no lock is associated with the specified resource.
    /// </returns>
    public LockContext? GetLock(string resource)
    {
        Span<byte> buffer = stackalloc byte[Encoding.UTF8.GetByteCount(resource)];
        Encoding.UTF8.GetBytes(resource.AsSpan(), buffer);
        
        byte[]? value = db.Get(buffer, cf: columnFamilyLocks);
        if (value is null)
            return null;

        RocksDbLockMessage message = UnserializeLockMessage(value);
        
        byte[]? owner;
        
        if (MemoryMarshal.TryGetArray(message.Owner.Memory, out ArraySegment<byte> segment))
            owner = segment.Array;
        else
            owner = message.Owner.ToByteArray();

        LockContext context = new()
        {
            Owner = owner,
            FencingToken = message.FencingToken,
            Expires = new(message.ExpiresNode, message.ExpiresPhysical, message.ExpiresCounter),
            LastUsed = new(message.LastUsedNode, message.LastUsedPhysical, message.LastUsedCounter),
            LastModified = new(message.LastModifiedNode, message.LastModifiedPhysical, message.LastModifiedCounter),
        };

        return context;
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
        string currentKey = keyName + CurrentMarker;
        
        Span<byte> buffer = stackalloc byte[Encoding.UTF8.GetByteCount(currentKey)];
        Encoding.UTF8.GetBytes(currentKey.AsSpan(), buffer);
        
        byte[]? value = db.Get(buffer, cf: columnFamilyKeys);
        if (value is null)
            return null;

        RocksDbKeyValueMessage message = UnserializeKeyValueMessage(value);
        
        byte[]? messageValue;
        
        if (MemoryMarshal.TryGetArray(message.Value.Memory, out ArraySegment<byte> segment))
            messageValue = segment.Array;
        else
            messageValue = message.Value.ToByteArray();

        KeyValueEntry entry = new()
        {
            Value = messageValue,
            Revision = message.Revision,
            Expires = new(message.ExpiresNode, message.ExpiresPhysical, message.ExpiresCounter),
            LastUsed = new(message.LastUsedNode, message.LastUsedPhysical, message.LastUsedCounter),
            LastModified = new(message.LastModifiedNode, message.LastModifiedPhysical, message.LastModifiedCounter),
            State = (KeyValueState)message.State,
        };

        return entry;
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
        string keyRevision = string.Concat(keyName, "~", revision);
        
        Span<byte> buffer = stackalloc byte[Encoding.UTF8.GetByteCount(keyRevision)];
        Encoding.UTF8.GetBytes(keyRevision.AsSpan(), buffer);
        
        byte[]? value = db.Get(buffer, cf: columnFamilyKeys);
        if (value is null)
            return null;

        RocksDbKeyValueMessage message = UnserializeKeyValueMessage(value);
        
        byte[]? messageValue;
        
        if (MemoryMarshal.TryGetArray(message.Value.Memory, out ArraySegment<byte> segment))
            messageValue = segment.Array;
        else
            messageValue = message.Value.ToByteArray();

        KeyValueEntry entry = new()
        {
            Value = messageValue,
            Revision = message.Revision,
            Expires = new(message.ExpiresNode, message.ExpiresPhysical, message.ExpiresCounter),
            LastUsed = new(message.LastUsedNode, message.LastUsedPhysical, message.LastUsedCounter),
            LastModified = new(message.LastModifiedNode, message.LastModifiedPhysical, message.LastModifiedCounter),
            State = (KeyValueState)message.State,
        };

        return entry;
    }

    /// <summary>
    /// Retrieves a list of key-value pairs that match the specified prefix key.
    /// </summary>
    /// <param name="prefixKeyName">The prefix string used to filter and retrieve matching key-value pairs.</param>
    /// <returns>A list of tuples where each tuple contains a string key and a corresponding <see cref="ReadOnlyKeyValueEntry"/> value.</returns>
    public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName)
    {
        List<(string, ReadOnlyKeyValueEntry)> result = [];
        
        Span<byte> buffer = stackalloc byte[Encoding.UTF8.GetByteCount(prefixKeyName)];
        Encoding.UTF8.GetBytes(prefixKeyName.AsSpan(), buffer);
        
        using Iterator? iterator = db.NewIterator(cf: columnFamilyKeys);
        iterator.Seek(buffer);

        while (iterator.Valid())
        {
            string key = Encoding.UTF8.GetString(iterator.Key());
            
            if (!key.StartsWith(prefixKeyName, StringComparison.Ordinal)) 
                break; // Stop when we leave the prefix range
            
            if (!key.EndsWith(CurrentMarker, StringComparison.Ordinal))
            {
                iterator.Next();
                continue;
            }
            
            string keyWithoutMarker = key[..^CurrentMarker.Length];
            
            RocksDbKeyValueMessage message = UnserializeKeyValueMessage(iterator.Value());
            
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

    /// <summary>
    /// Serializes a provided <see cref="RocksDbLockMessage"/> instance into a byte array.
    /// This method ensures that larger messages, in particular those with an Owner
    /// property exceeding the maximum allowed size, are handled efficiently by utilizing
    /// a recyclable memory stream.
    /// </summary>
    /// <param name="message">The <see cref="RocksDbLockMessage"/> instance to be serialized.</param>
    /// <returns>A byte array representation of the serialized <see cref="RocksDbLockMessage"/>.</returns>
    private static byte[] Serialize(RocksDbLockMessage message)
    {
        if (!message.Owner.IsEmpty && message.Owner.Length >= MaxMessageSize)
        {
            using RecyclableMemoryStream recyclableMemoryStream = manager.GetStream();
            message.WriteTo((Stream)recyclableMemoryStream);
            return recyclableMemoryStream.ToArray();    
        }
        
        using MemoryStream memoryStream = manager.GetStream();
        message.WriteTo(memoryStream);
        return memoryStream.ToArray();
    }

    /// <summary>
    /// Serializes a given <see cref="RocksDbKeyValueMessage"/> into a byte array representation.
    /// Allows memory-efficient serialization of message objects, leveraging a memory stream manager
    /// for large payloads exceeding a predefined size.
    /// </summary>
    /// <param name="message">The <see cref="RocksDbKeyValueMessage"/> instance to be serialized. This contains the data to convert into a byte array.</param>
    /// <returns>A byte array representing the serialized form of the given message.</returns>
    private static byte[] Serialize(RocksDbKeyValueMessage message)
    {
        if (!message.Value.IsEmpty && message.Value.Length >= MaxMessageSize)
        {
            using RecyclableMemoryStream recyclableMemoryStream = manager.GetStream();
            message.WriteTo((Stream)recyclableMemoryStream);
            return recyclableMemoryStream.ToArray();
        }
        
        using MemoryStream memoryStream = manager.GetStream();
        message.WriteTo(memoryStream);
        return memoryStream.ToArray();
    }

    /// <summary>
    /// Converts a serialized byte span into a <see cref="RocksDbLockMessage"/> object.
    /// This method deserializes the provided byte data, supporting efficient memory
    /// handling through the use of recyclable memory streams.
    /// </summary>
    /// <param name="serializedData">The serialized data represented as a read-only span
    /// of bytes, which encodes the <see cref="RocksDbLockMessage"/> object.</param>
    /// <returns>Returns a deserialized instance of <see cref="RocksDbLockMessage"/>
    /// based on the provided byte data.</returns>
    private static RocksDbLockMessage UnserializeLockMessage(ReadOnlySpan<byte> serializedData)
    {
        if (serializedData.Length >= MaxMessageSize)
        {
            using RecyclableMemoryStream recycledMemoryStream = manager.GetStream(serializedData);
            return RocksDbLockMessage.Parser.ParseFrom(recycledMemoryStream);
        }
        
        using RecyclableMemoryStream memoryStream = manager.GetStream(serializedData);
        return RocksDbLockMessage.Parser.ParseFrom(memoryStream);
    }

    /// <summary>
    /// Converts a serialized byte span into a <see cref="RocksDbKeyValueMessage"/> object.
    /// This method deserializes the provided binary data into a structured message format
    /// to facilitate further operations on the key-value store.
    /// </summary>
    /// <param name="serializedData">
    /// A read-only span of bytes representing the serialized data of a key-value message.
    /// </param>
    /// <returns>
    /// An instance of <see cref="RocksDbKeyValueMessage"/> representing the deserialized key-value message.
    /// </returns>
    private static RocksDbKeyValueMessage UnserializeKeyValueMessage(ReadOnlySpan<byte> serializedData)
    {
        if (serializedData.Length >= MaxMessageSize)
        {
            using RecyclableMemoryStream recycledMemoryStream = manager.GetStream(serializedData);
            return RocksDbKeyValueMessage.Parser.ParseFrom(recycledMemoryStream);
        }
        
        using MemoryStream memoryStream = manager.GetStream(serializedData);
        return RocksDbKeyValueMessage.Parser.ParseFrom(memoryStream);
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        
        db.Dispose();
    }
}