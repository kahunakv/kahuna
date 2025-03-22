
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

    public Task<bool> StoreLock(
        string resource, 
        byte[]? owner, 
        long expiresPhysical, 
        uint expiresCounter, 
        long fencingToken,
        int state
    )
    {
        RocksDbLockMessage kvm = new()
        {
            ExpiresPhysical = expiresPhysical,
            ExpiresCounter = expiresCounter,
            FencingToken = fencingToken,
            State = state
        };
        
        if (owner != null)
            kvm.Owner = UnsafeByteOperations.UnsafeWrap(owner);
        
        db.Put(Encoding.UTF8.GetBytes(resource), Serialize(kvm), cf: columnFamilyLocks);

        return Task.FromResult(true);
    }
    
    public Task<bool> StoreKeyValue(
        string key, 
        byte[]? value, 
        long expiresPhysical, 
        uint expiresCounter, 
        long revision,
        int state
    )
    {
        RocksDbKeyValueMessage kvm = new()
        {
            ExpiresPhysical = expiresPhysical,
            ExpiresCounter = expiresCounter,
            Revision = revision,
            State = state
        };

        if (value is not null)
            kvm.Value = UnsafeByteOperations.UnsafeWrap(value);

        byte[] serialized = Serialize(kvm);

        using WriteBatch batch = new();
        
        byte[] index = Encoding.UTF8.GetBytes(key + "~CURRENT");
        batch.Put(index, serialized, cf: columnFamilyKeys);
        
        index = Encoding.UTF8.GetBytes(key + "~" + revision);
        batch.Put(index, serialized, cf: columnFamilyKeys);

        db.Write(batch);

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
        byte[]? value = db.Get(Encoding.UTF8.GetBytes(keyName + "~" + revision), cf: columnFamilyKeys);
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