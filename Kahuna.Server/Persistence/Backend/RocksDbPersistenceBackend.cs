
using System.Text;
using Kahuna.Server.Locks;
using Kahuna.Persistence.Protos;
using RocksDbSharp;
using Google.Protobuf;
using Kahuna.Server.KeyValues;
using Microsoft.IO;

namespace Kahuna.Server.Persistence.Backend;

public class RocksDbPersistenceBackend : IPersistenceBackend, IDisposable
{
    private const int MaxMessageSize = 1024;
    
    private const string CurrentMarker = "~CURRENT";
    
    private static readonly RecyclableMemoryStreamManager manager = new();
    
    private static readonly WriteOptions DefaultWriteOptions = new WriteOptions().SetSync(true);
    
    private readonly RocksDb db;

    private readonly ColumnFamilyHandle? columnFamilyKeys;
    
    private readonly ColumnFamilyHandle? columnFamilyLocks;
    
    private readonly string path;
    
    private readonly string dbRevision;
    
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

    public bool StoreLocks(List<PersistenceRequestItem> items)
    {
        using WriteBatch batch = new();

        foreach (PersistenceRequestItem item in items)
        {
            RocksDbLockMessage kvm = new()
            {
                ExpiresPhysical = item.ExpiresPhysical,
                ExpiresCounter = item.ExpiresCounter,
                FencingToken = item.Revision,
                State = item.State
            };

            if (item.Value != null)
                kvm.Owner = UnsafeByteOperations.UnsafeWrap(item.Value);

            batch.Put(Encoding.UTF8.GetBytes(item.Key), Serialize(kvm), cf: columnFamilyLocks); 
        }
        
        db.Write(batch, DefaultWriteOptions);

        return true;
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
                Revision = item.Revision,
                State = item.State
            };

            if (item.Value is not null)
                kvm.Value = UnsafeByteOperations.UnsafeWrap(item.Value);

            byte[] serialized = Serialize(kvm);

            byte[] index = Encoding.UTF8.GetBytes(string.Concat(item.Key, CurrentMarker));
            batch.Put(index, serialized, cf: columnFamilyKeys);

            index = Encoding.UTF8.GetBytes(string.Concat(item.Key, "~", item.Revision));
            batch.Put(index, serialized, cf: columnFamilyKeys);
        }

        db.Write(batch, DefaultWriteOptions);

        return true;
    }

    public LockContext? GetLock(string resource)
    {
        Span<byte> buffer = stackalloc byte[Encoding.UTF8.GetByteCount(resource)];
        Encoding.UTF8.GetBytes(resource.AsSpan(), buffer);
        
        byte[]? value = db.Get(buffer, cf: columnFamilyLocks);
        if (value is null)
            return null;

        RocksDbLockMessage message = UnserializeLockMessage(value);

        LockContext context = new()
        {
            Owner = message.Owner?.ToByteArray(),
            FencingToken = message.FencingToken,
            Expires = new(message.ExpiresPhysical, message.ExpiresCounter),
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

        KeyValueContext context = new()
        {
            Value = message.Value?.ToByteArray(),
            Revision = message.Revision,
            Expires = new(message.ExpiresPhysical, message.ExpiresCounter),
            State = (KeyValueState)message.State,
        };

        return context;
    }

    public KeyValueContext? GetKeyValueRevision(string keyName, long revision)
    {
        string keyRevision = string.Concat(keyName, "~", revision);
        
        Span<byte> buffer = stackalloc byte[Encoding.UTF8.GetByteCount(keyRevision)];
        Encoding.UTF8.GetBytes(keyRevision.AsSpan(), buffer);
        
        byte[]? value = db.Get(Encoding.UTF8.GetBytes(keyRevision), cf: columnFamilyKeys);
        if (value is null)
            return null;

        RocksDbKeyValueMessage message = UnserializeKeyValueMessage(value);

        KeyValueContext context = new()
        {
            Value = message.Value?.ToByteArray(),
            Revision = message.Revision,
            Expires = new(message.ExpiresPhysical, message.ExpiresCounter),
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

            result.Add((keyWithoutMarker, new(
                message.Value?.ToByteArray(), 
                message.Revision, 
                new(message.ExpiresPhysical, message.ExpiresCounter),
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