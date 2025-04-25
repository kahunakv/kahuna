
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
    Errored = 99,
    InvalidInput = 100,
    MustRetry = 101,
    LockDoesNotExist = 102,
    InvalidOwner = 103,
    Aborted = 104
}
