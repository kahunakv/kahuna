
namespace Kahuna.Locks;

public enum LockRequestType
{
    TryLock,
    TryExtendLock,
    TryUnlock,
    Get
}