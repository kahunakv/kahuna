
using System.Runtime.InteropServices;
using System.Text;
using Kahuna.Server.Locks;
using Kahuna.Persistence.Protos;
using RocksDbSharp;
using Google.Protobuf;
using Kahuna.Server.KeyValues;
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
public class RocksDbPersistenceBackend : IPersistenceBackend, IDisposable
{
    private const int MaxMessageSize = 1024;
    
    private const string CurrentMarker = "~CURRENT";
    
    private static readonly RecyclableMemoryStreamManager manager = new();
    
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
            Expires = new(message.ExpiresPhysical, message.ExpiresCounter),
            LastUsed = new(message.LastUsedPhysical, message.LastUsedCounter),
            LastModified = new(message.LastModifiedPhysical, message.LastModifiedCounter),
        };

        return context;
    }

    public KeyValueContext? GetKeyValue(string keyName)
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

        KeyValueContext context = new()
        {
            Value = messageValue,
            Revision = message.Revision,
            Expires = new(message.ExpiresPhysical, message.ExpiresCounter),
            LastUsed = new(message.LastUsedPhysical, message.LastUsedCounter),
            LastModified = new(message.LastModifiedPhysical, message.LastModifiedCounter),
            State = (KeyValueState)message.State,
        };

        return context;
    }

    public KeyValueContext? GetKeyValueRevision(string keyName, long revision)
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

        KeyValueContext context = new()
        {
            Value = messageValue,
            Revision = message.Revision,
            Expires = new(message.ExpiresPhysical, message.ExpiresCounter),
            LastUsed = new(message.LastUsedPhysical, message.LastUsedCounter),
            LastModified = new(message.LastModifiedPhysical, message.LastModifiedCounter),
            State = (KeyValueState)message.State,
        };

        return context;
    }

    public List<(string, ReadOnlyKeyValueContext)> GetKeyValueByPrefix(string prefixKeyName)
    {
        List<(string, ReadOnlyKeyValueContext)> result = [];
        
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
                new(message.ExpiresPhysical, message.ExpiresCounter),
                new(message.LastUsedPhysical, message.LastUsedCounter),
                new(message.LastModifiedPhysical, message.LastModifiedCounter),
                (KeyValueState)message.State
            )));

            iterator.Next();
        }

        return result;
    }

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