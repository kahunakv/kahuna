
using Kahuna.Shared.Locks;
using Nixie.Routers;
using Standart.Hash.xxHash;

namespace Kahuna.Server.Locks.Data;

/// <summary>
/// Represents a request to perform locking operations on a specific resource.
/// </summary>
internal sealed class LockRequest : IConsistentHashable
{
    /// <summary>
    /// Gets the type of the lock request being issued. The value indicates the operation
    /// the requester wants to perform, such as acquiring, releasing, extending a lock,
    /// or retrieving information about a lock.
    /// </summary>
    /// <remarks>
    /// The possible values are defined by the <see cref="LockRequestType"/> enumeration:
    /// - TryLock: Attempts to acquire a lock on a resource.
    /// - TryExtendLock: Attempts to extend the duration of an existing lock.
    /// - TryUnlock: Attempts to release a lock.
    /// - Get: Retrieves lock information for a specified resource.
    /// </remarks>
    public LockRequestType Type { get; }

    /// <summary>
    /// Gets the identifier of the resource that the lock operation is targeting.
    /// </summary>
    /// <remarks>
    /// This property uniquely identifies the resource for which a lock-related operation
    /// (such as acquire, release, or extend) is being performed. The value is used
    /// to determine which resource the lock request should act upon.
    /// </remarks>
    public string Resource { get; }

    /// <summary>
    /// Gets the identifier of the entity attempting to acquire or manage the lock.
    /// Represents the owner of the lock, which can be used to distinguish between
    /// different lock holders in scenarios where multiple entities compete for the same resource.
    /// </summary>
    /// <remarks>
    /// The value is expected to be a byte array that uniquely identifies the lock owner.
    /// It is used in various lock operations such as checking lock ownership during
    /// extension, release, or validation of a resource lock.
    /// A null value indicates that no specific owner is associated or defined
    /// for the current lock request.
    /// </remarks>
    public byte[]? Owner { get; }

    /// <summary>
    /// Gets the duration, in milliseconds, for which a lock should remain valid. This value
    /// represents the time period after which the lock will automatically expire unless extended.
    /// </summary>
    /// <remarks>
    /// The expiration time is used to manage the lifetime of a lock. It helps ensure locks do
    /// not persist indefinitely in the system. The value is utilized for operations such as
    /// creating or extending locks.
    /// </remarks>
    public int ExpiresMs { get; }

    /// <summary>
    /// Specifies the durability of the lock being requested, indicating whether the lock is ephemeral or persistent.
    /// </summary>
    /// <remarks>
    /// The possible values are determined by the <see cref="LockDurability"/> enumeration:
    /// - Ephemeral: The lock exists transiently and is not maintained across failures or restarts.
    /// - Persistent: The lock is stored persistently and is recoverable after a failure or restart.
    /// </remarks>
    public LockDurability Durability { get; }
    
    /// <summary>
    /// 
    /// </summary>
    public int ProposalId { get; }
    
    /// <summary>
    /// 
    /// </summary>
    public int PartitionId { get; }
    
    /// <summary>
    /// 
    /// </summary>
    public TaskCompletionSource<LockResponse?>? Promise { get; }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="type"></param>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="proposalId"></param>
    /// <param name="messagePromise"></param>
    /// <param name="proposalId"></param>
    /// <param name="partitionId"></param> 
    /// <param name="promise"></param> 
    public LockRequest(
        LockRequestType type, 
        string resource, 
        byte[]? owner, 
        int expiresMs, 
        LockDurability durability, 
        int proposalId, 
        int partitionId,
        TaskCompletionSource<LockResponse?>? promise
    )
    {
        Type = type;
        Resource = resource;
        Owner = owner;
        ExpiresMs = expiresMs;
        Durability = durability;
        ProposalId = proposalId;
        PartitionId = partitionId;
        Promise = promise;
    }

    /// <summary>
    /// Computes a hash value for the resource associated with the lock request.
    /// </summary>
    /// <returns>An integer representing the hash value of the resource.</returns>
    public int GetHash()
    {
        return (int)xxHash64.ComputeHash(Resource);
    }
}