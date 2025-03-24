
using System.Text;
using Kahuna.Server.Locks;
using Kahuna.Persistence.Protos;
using RocksDbSharp;
using Google.Protobuf;
using Kahuna.Server.KeyValues;

namespace Kahuna.Server.Persistence;

public class RocksDbPersistence : IPersistence
{
    private static readonly Task<LockContext?> NullLockContext = Task.FromResult<LockContext?>(null);
    
    private static readonly Task<KeyValueContext?> NullKeyValueContext = Task.FromResult<KeyValueContext?>(null);

    private static readonly WriteOptions DefaultWriteOptions = new WriteOptions().SetSync(true);
    
    private readonly RocksDb db;

    private readonly ColumnFamilyHandle? columnFamilyKeys;
    
    private readonly ColumnFamilyHandle? columnFamilyLocks;
    
    private readonly string path;
    
    private readonly string dbRevision;
    
    public RocksDbPersistence(string path = ".", string dbRevision = "v1")
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

    public Task<bool> StoreLocks(List<PersistenceRequestItem> items)
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

        return Task.FromResult(true);
    }
    
    public Task<bool> StoreKeyValues(List<PersistenceRequestItem> items)
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

            byte[] index = Encoding.UTF8.GetBytes(string.Concat(item.Key, "~CURRENT"));
            batch.Put(index, serialized, cf: columnFamilyKeys);

            index = Encoding.UTF8.GetBytes(string.Concat(item.Key, "~", item.Revision));
            batch.Put(index, serialized, cf: columnFamilyKeys);
        }

        db.Write(batch, DefaultWriteOptions);

        return Task.FromResult(true);
    }

    public Task<LockContext?> GetLock(string resource)
    {
        byte[]? value = db.Get(Encoding.UTF8.GetBytes(resource), cf: columnFamilyLocks);
        if (value is null)
            return NullLockContext;

        RocksDbLockMessage message = UnserializeLockMessage(value);

        LockContext context = new()
        {
            Owner = message.Owner?.ToByteArray(),
            FencingToken = message.FencingToken,
            Expires = new(message.ExpiresPhysical, message.ExpiresCounter),
        };

        return Task.FromResult<LockContext?>(context);
    }

    public Task<KeyValueContext?> GetKeyValue(string keyName)
    {
        byte[]? value = db.Get(Encoding.UTF8.GetBytes(keyName + "~CURRENT"), cf: columnFamilyKeys);
        if (value is null)
            return NullKeyValueContext;

        RocksDbKeyValueMessage message = UnserializeKeyValueMessage(value);

        KeyValueContext context = new()
        {
            Value = message.Value?.ToByteArray(),
            Revision = message.Revision,
            Expires = new(message.ExpiresPhysical, message.ExpiresCounter),
        };

        return Task.FromResult<KeyValueContext?>(context);
    }

    public Task<KeyValueContext?> GetKeyValueRevision(string keyName, long revision)
    {
        byte[]? value = db.Get(Encoding.UTF8.GetBytes(string.Concat(keyName, "~", revision)), cf: columnFamilyKeys);
        if (value is null)
            return NullKeyValueContext;

        RocksDbKeyValueMessage message = UnserializeKeyValueMessage(value);

        KeyValueContext context = new()
        {
            Value = message.Value?.ToByteArray(),
            Revision = message.Revision,
            Expires = new(message.ExpiresPhysical, message.ExpiresCounter),
        };

        return Task.FromResult<KeyValueContext?>(context);
    }

    private static byte[] Serialize(RocksDbLockMessage message)
    {
        using MemoryStream memoryStream = new();
        message.WriteTo(memoryStream);
        return memoryStream.ToArray();
    }
    
    private static byte[] Serialize(RocksDbKeyValueMessage message)
    {
        using MemoryStream memoryStream = new();
        message.WriteTo(memoryStream);
        return memoryStream.ToArray();
    }

    private static RocksDbLockMessage UnserializeLockMessage(byte[] serializedData)
    {
        using MemoryStream memoryStream = new(serializedData);
        return RocksDbLockMessage.Parser.ParseFrom(memoryStream);
    }
    
    private static RocksDbKeyValueMessage UnserializeKeyValueMessage(byte[] serializedData)
    {
        using MemoryStream memoryStream = new(serializedData);
        return RocksDbKeyValueMessage.Parser.ParseFrom(memoryStream);
    }
}