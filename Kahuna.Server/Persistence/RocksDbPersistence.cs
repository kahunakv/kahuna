
using System.Text.Json;
using Kahuna.Locks;
using Kahuna.Shared.Locks;
using RocksDbSharp;

namespace Kahuna.Persistence;

public class RocksDbPersistence : IPersistence
{
    private readonly RocksDb db;
    
    private readonly string path;
    
    private readonly string revision;
    
    public RocksDbPersistence(string path, string revision)
    {
        this.path = path;
        this.revision = revision;
        
        this.db = RocksDb.Open(new DbOptions().SetCreateIfMissing(true), path);
    }

    public async Task StoreLock(
        string resource, 
        string owner, 
        long expiresLogical, 
        uint expiresCounter, 
        long fencingToken,
        long consistency, 
        LockState state
    )
    {
        await Task.CompletedTask;
        
        db.Put(resource, JsonSerializer.Serialize(new PersistenceRequest(
            PersistenceRequestType.Store,
            resource,
            owner,
            fencingToken,
            expiresLogical,
            expiresCounter,
            (LockConsistency) consistency,
            state
        )));
    }

    public Task UpdateLocks(List<PersistenceRequest> items)
    {
        throw new NotImplementedException();
    }

    public Task<LockContext?> GetLock(string resource)
    {
        return Task.FromResult<LockContext?>(null);
    }
}