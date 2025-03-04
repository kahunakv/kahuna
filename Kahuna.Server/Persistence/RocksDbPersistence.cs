
using System.Text;
using Kahuna.Locks;
using Kahuna.Persistence.Protos;
using RocksDbSharp;
using Google.Protobuf;

namespace Kahuna.Persistence;

public class RocksDbPersistence : IPersistence
{
    private readonly RocksDb db;
    
    private readonly string path;
    
    private readonly string revision;
    
    public RocksDbPersistence(string path = ".", string revision = "v1")
    {
        this.path = path;
        this.revision = revision;

        string fullPath = $"{path}/{revision}";
        
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
        long consistency, 
        LockState state
    )
    {
        db.Put(Encoding.UTF8.GetBytes(resource), Serialize(new()
        {
            Owner = owner,
            ExpiresPhysical = expiresPhysical,
            ExpiresCounter = expiresCounter,
            FencingToken = fencingToken,
            Consistency = (int)consistency,
            State = (int)state
        }));

        return Task.CompletedTask;
    }

    public Task<LockContext?> GetLock(string resource)
    {
        byte[]? value = db.Get(Encoding.UTF8.GetBytes(resource));
        if (value is null)
            return Task.FromResult<LockContext?>(null);

        RocksDbLockMessage message = Unserializer(value);

        LockContext context = new()
        {
            Owner = message.Owner,
            FencingToken = message.FencingToken,
            Expires = new(message.ExpiresPhysical, message.ExpiresCounter),
        };

        return Task.FromResult<LockContext?>(context);
    }
    
    private static byte[] Serialize(RocksDbLockMessage message)
    {
        using MemoryStream memoryStream = new();
        message.WriteTo(memoryStream);
        return memoryStream.ToArray();
    }

    private static RocksDbLockMessage Unserializer(byte[] serializedData)
    {
        using MemoryStream memoryStream = new(serializedData);
        return RocksDbLockMessage.Parser.ParseFrom(memoryStream);
    }
    
}