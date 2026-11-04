
namespace Kahuna.Shared.Locks;

/// <summary>
/// Represents the various response types that can be returned during operations
/// involving locking mechanisms in the system.
/// </summary>
public enum LockResponseType
{
    Locked = 0,
    Busy = 1,
    Extended = 2,
    Unlocked = 3,
    Got = 4,
    WaitingForReplication = 10,
    Errored = 99,
    InvalidInput = 100,
    MustRetry = 101,
    LockDoesNotExist = 102,
    InvalidOwner = 103,
    Aborted = 104,

    /// <summary>
    /// The node's lock store reported damage while answering, or had already fail-stopped on such damage.
    /// Nothing was served from the damaged store, and the node has stopped serving its partitions from it.
    /// Not retryable against this node; see the key-value <c>StorageCorrupt</c> response for the contract.
    /// </summary>
    StorageCorrupt = 105
}
