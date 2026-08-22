
using Kahuna.Shared.Locks;
using Kommander;
using Nixie.Routers;

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
    public LockRequestType Type { get; private set; }

    /// <summary>
    /// Gets the identifier of the resource that the lock operation is targeting.
    /// </summary>
    /// <remarks>
    /// This property uniquely identifies the resource for which a lock-related operation
    /// (such as acquire, release, or extend) is being performed. The value is used
    /// to determine which resource the lock request should act upon.
    /// </remarks>
    public string Resource { get; private set; }

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
    public byte[]? Owner { get; private set; }

    /// <summary>
    /// Gets the duration, in milliseconds, for which a lock should remain valid. This value
    /// represents the time period after which the lock will automatically expire unless extended.
    /// </summary>
    /// <remarks>
    /// The expiration time is used to manage the lifetime of a lock. It helps ensure locks do
    /// not persist indefinitely in the system. The value is utilized for operations such as
    /// creating or extending locks.
    /// </remarks>
    public int ExpiresMs { get; private set; }

    /// <summary>
    /// Specifies the durability of the lock being requested, indicating whether the lock is ephemeral or persistent.
    /// </summary>
    /// <remarks>
    /// The possible values are determined by the <see cref="LockDurability"/> enumeration:
    /// - Ephemeral: The lock exists transiently and is not maintained across failures or restarts.
    /// - Persistent: The lock is stored persistently and is recoverable after a failure or restart.
    /// </remarks>
    public LockDurability Durability { get; private set; }
    
    /// <summary>
    /// 
    /// </summary>
    public int ProposalId { get; private set; }
    
    /// <summary>
    /// 
    /// </summary>
    public int PartitionId { get; private set; }
    
    /// <summary>
    ///
    /// </summary>
    public TaskCompletionSource<LockResponse?>? Promise { get; private set; }

    /// <summary>
    /// For CompleteProposal requests: the Raft WAL log index the proposal committed at, so the
    /// apply can stamp it on the background write and the partition's application-durability floor
    /// advances once the flush lands. -1 for every other request type.
    /// </summary>
    public long ProposalLogIndex { get; private set; }

    /// <summary>
    /// Committed lock state carried by an <c>InvalidateOrApply</c> message so the owning actor can
    /// bring a resident cache entry up to date. Non-null only when <see cref="Type"/> is
    /// <see cref="LockRequestType.InvalidateOrApply"/>.
    /// </summary>
    public LockInvalidateOrApplyData? InvalidateOrApplyData { get; private set; }

    /// <summary>
    /// Ownership-transfer marker for pooled fire-and-forget messages: when true, the receiving
    /// <see cref="LockActor"/> returns this request to <see cref="LockRequestPool"/> after
    /// handling it, because the sender kept no reference and cannot recycle it. Ask-style callers
    /// must leave this false and return the request themselves after the reply arrives — setting
    /// it on an asked request would return the object twice.
    /// </summary>
    public bool ReturnToPoolOnReceive { get; internal set; }

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="type"></param>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="proposalId"></param>
    /// <param name="partitionId"></param>
    /// <param name="promise"></param>
    /// <param name="proposalLogIndex"></param>
    public LockRequest(
        LockRequestType type,
        string resource,
        byte[]? owner,
        int expiresMs,
        LockDurability durability,
        int proposalId,
        int partitionId,
        TaskCompletionSource<LockResponse?>? promise,
        long proposalLogIndex = -1,
        LockInvalidateOrApplyData? invalidateOrApplyData = null
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
        ProposalLogIndex = proposalLogIndex;
        InvalidateOrApplyData = invalidateOrApplyData;
    }

    /// <summary>
    /// Repopulates a pooled instance with the same fields the constructor takes. The ownership
    /// marker resets to false; a rent helper that transfers ownership sets it afterwards.
    /// </summary>
    public void Reset(
        LockRequestType type,
        string resource,
        byte[]? owner,
        int expiresMs,
        LockDurability durability,
        int proposalId,
        int partitionId,
        TaskCompletionSource<LockResponse?>? promise,
        long proposalLogIndex = -1,
        LockInvalidateOrApplyData? invalidateOrApplyData = null
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
        ProposalLogIndex = proposalLogIndex;
        InvalidateOrApplyData = invalidateOrApplyData;
        ReturnToPoolOnReceive = false;
    }

    /// <summary>
    /// Drops every reference before the instance parks in the pool, so a pooled request cannot
    /// keep an owner payload, promise, or apply record alive.
    /// </summary>
    public void Clear()
    {
        Type = default;
        Resource = string.Empty;
        Owner = null;
        ExpiresMs = 0;
        Durability = default;
        ProposalId = 0;
        PartitionId = 0;
        Promise = null;
        ProposalLogIndex = -1;
        InvalidateOrApplyData = null;
        ReturnToPoolOnReceive = false;
    }

    /// <summary>
    /// Computes a hash value for the resource associated with the lock request.
    /// </summary>
    /// <returns>An integer representing the hash value of the resource.</returns>
    public int GetHash()
    {
        // Bounded UTF-8 encoding (stack/pooled) over the same xxHash64 algorithm, so the hash value
        // is identical to encoding the resource into a fresh array first.
        return (int)HashUtils.SimpleHash(Resource);
    }
}