
using Kahuna.Shared.Locks;

namespace Kahuna.Server.Locks.Data;

/// <summary>
/// Represents the response of the operation on the locker actor
/// </summary>
internal sealed class LockResponse
{
    /// <summary>
    /// Gets the type of the lock response. This indicates the outcome of a lock-related operation,
    /// such as whether the lock was successfully acquired, extended, released, or other result types.
    /// </summary>
    /// <remarks>
    /// The value is of type <see cref="LockResponseType"/> and corresponds to different possible
    /// states or results of operations involving locks.
    /// </remarks>
    public LockResponseType Type { get; }

    /// <summary>
    /// Gets the fencing token associated with the lock operation. This is a unique, incrementing token
    /// that helps manage and differentiate lock versions to ensure lock consistency and prevent stale updates.
    /// </summary>
    /// <remarks>
    /// The fencing token is particularly useful in distributed systems where it acts as a mechanism
    /// to detect and handle concurrency issues by ensuring operations are performed on the correct
    /// version of the lock.
    /// </remarks>
    public long FencingToken { get; }

    /// <summary>
    /// Gets the context associated with the lock response, if available. The context provides additional
    /// information about the state of the lock, such as its owner, fencing token, and expiration time.
    /// </summary>
    /// <remarks>
    /// The value is of type <see cref="ReadOnlyLockContext"/> and may be null if no context is applicable
    /// or available for the operation result.
    /// </remarks>
    public ReadOnlyLockContext? Context { get; }

    /// <summary>
    /// Represents the response for a lock operation. Used to convey the result
    /// of an action such as acquiring, releasing, or interacting with a lock.
    /// </summary>
    public LockResponse(LockResponseType type)
    {
        Type = type;
    }

    /// <summary>
    /// Represents the response provided by a lock operation, detailing its result and associated metadata,
    /// such as fencing tokens or context information. Used to communicate the outcome of lock-related actions.
    /// </summary>
    public LockResponse(LockResponseType type, long fencingToken)
    {
        Type = type;
        FencingToken = fencingToken;
    }

    /// <summary>
    /// Represents the response for a lock operation, detailing the outcome
    /// and relevant contextual information such as fencing tokens and lock context.
    /// </summary>
    public LockResponse(LockResponseType type, ReadOnlyLockContext? context)
    {
        Type = type;
        Context = context;
    }
}