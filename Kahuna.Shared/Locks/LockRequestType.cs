
namespace Kahuna.Shared.Locks;

/// <summary>
/// Represents the types of lock requests that can be issued.
/// </summary>
public enum LockRequestType
{
    TryLock,
    TryExtendLock,
    TryUnlock,
    Get,
    CompleteProposal,
    ReleaseProposal
}