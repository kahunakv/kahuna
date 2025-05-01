
using Kahuna.Shared.Locks;

namespace Kahuna.Server.Locks.Data;

/// <summary>
/// Provides a set of pre-defined static responses for lock-related operations within the locking mechanism.
/// </summary>
/// <remarks>
/// This class contains commonly used <see cref="LockResponse"/> objects to represent specific outcomes,
/// such as when a lock does not exist, is busy, or an operation encounters an error.
/// Utilizing these static responses helps to standardize result handling and reduces memory allocation overhead.
/// </remarks>
internal static class LockStaticResponses
{
    internal static readonly LockResponse DoesNotExistResponse = new(LockResponseType.LockDoesNotExist);
    
    internal static readonly LockResponse BusyResponse = new(LockResponseType.Busy);
    
    internal static readonly LockResponse ErroredResponse = new(LockResponseType.Errored);
    
    internal static readonly LockResponse InvalidOwnerResponse = new(LockResponseType.InvalidOwner);
    
    internal static readonly LockResponse UnlockedResponse = new(LockResponseType.Unlocked);
    
    internal static readonly LockResponse WaitingForReplication = new(LockResponseType.WaitingForReplication);
}