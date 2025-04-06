
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander.Time;

namespace Kahuna.Server.Communication.Internode;

public interface IInterNodeCommunication
{
    public Task<(LockResponseType, long)> TryLock(string node, string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken);
    
    public Task<(LockResponseType, long)> TryExtendLock(string node, string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken);
    
    public Task<LockResponseType> TryUnlock(string node, string resource, byte[] owner, LockDurability durability, CancellationToken cancellationToken);

    public Task<(LockResponseType, ReadOnlyLockContext?)> GetLock(string node, string resource, LockDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, long)> TrySetKeyValue(string node, HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, long)> TryDeleteKeyValue(string node, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken);

    public Task<(KeyValueResponseType, long)> TryExtendKeyValue(string node, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancelationToken);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryGetValue(string node, HLCTimestamp transactionId, string key, long revision, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueContext?)> TryExistsValue(string node, HLCTimestamp transactionId, string key, long revision, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, string, KeyValueDurability)> TryAcquireExclusiveLock(string node, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancelationToken);

    public Task TryAcquireNodeExclusiveLocks(string node, HLCTimestamp transactionId, List<(string key, int expiresMs, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses, CancellationToken cancelationToken);

    public Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(string node, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken);

    public Task TryReleaseNodeExclusiveLocks(string node, HLCTimestamp transactionId, List<(string key, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(string node, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task TryPrepareNodeMutations(string node, HLCTimestamp transactionId, List<(string key, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> responses, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, long)> TryCommitMutations(string node, HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken);
}