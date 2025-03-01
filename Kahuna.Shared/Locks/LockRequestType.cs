
namespace Kahuna.Shared.Locks;

public enum LockRequestType
{
    TryLock,
    TryExtendLock,
    TryUnlock,
    Get
}