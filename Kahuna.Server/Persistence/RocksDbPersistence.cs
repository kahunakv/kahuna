
using System.Text;
using Kahuna.Locks;
using Kahuna.Persistence.Protos;
using RocksDbSharp;
using Google.Protobuf;
using Kahuna.KeyValues;

namespace Kahuna.Persistence;

public class RocksDbPersistence : IPersistence
{
    private const string LockPrefix = "lk-";
    
    private const string KeyValuePrefix = "kv-";
    
    private readonly RocksDb db;
    
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

        this.db = RocksDb.Open(dbOptions, fullPath);
    }

    public Task StoreLock(
        string resource, 
        string owner, 
        long expiresPhysical, 
        uint expiresCounter, 
        long fencingToken,
        int consistency, 
        int state
    )
    {
        db.Put(Encoding.UTF8.GetBytes(LockPrefix + resource), Serialize(new RocksDbLockMessage
        {
            Owner = owner,
            ExpiresPhysical = expiresPhysical,
            ExpiresCounter = expiresCounter,
            FencingToken = fencingToken,
            Consistency = consistency,
            State = state
        }));

        return Task.CompletedTask;
    }
    
    public Task StoreKeyValue(
        string key, 
        string value, 
        long expiresPhysical, 
        uint expiresCounter, 
        long revision,
        int consistency, 
        int state
    )
    {
        db.Put(Encoding.UTF8.GetBytes(LockPrefix + key), Serialize(new RocksDbKeyValueMessage
        {
            Value = value,
            ExpiresPhysical = expiresPhysical,
            ExpiresCounter = expiresCounter,
            Revision = revision,
            Consistency = consistency,
            State = state
        }));

        return Task.CompletedTask;
    }

    public Task<LockContext?> GetLock(string resource)
    {
        byte[]? value = db.Get(Encoding.UTF8.GetBytes(LockPrefix + resource));
        if (value is null)
            return Task.FromResult<LockContext?>(null);

        RocksDbLockMessage message = UnserializeLockMessage(value);

        LockContext context = new()
        {
            Owner = message.Owner,
            FencingToken = message.FencingToken,
            Expires = new(message.ExpiresPhysical, message.ExpiresCounter),
        };

        return Task.FromResult<LockContext?>(context);
    }

    public Task<KeyValueContext?> GetKeyValue(string keyName)
    {
        byte[]? value = db.Get(Encoding.UTF8.GetBytes(KeyValuePrefix + keyName));
        if (value is null)
            return Task.FromResult<KeyValueContext?>(null);

        RocksDbKeyValueMessage message = UnserializeKeyValueMessage(value);

        KeyValueContext context = new()
        {
            Value = message.Value,
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