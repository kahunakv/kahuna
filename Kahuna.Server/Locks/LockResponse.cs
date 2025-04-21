
using Kahuna.Shared.Locks;

namespace Kahuna.Server.Locks;

/// <summary>
/// Represents the response of the operation on the locker actor
/// </summary>
public sealed class LockResponse
{
    public LockResponseType Type { get; }
    
    public long FencingToken { get; }
    
    public ReadOnlyLockContext? Context { get; }
    
    public LockResponse(LockResponseType type)
    {
        Type = type;
    }
    
    public LockResponse(LockResponseType type, long fencingToken)
    {
        Type = type;
        FencingToken = fencingToken;
    }
    
    public LockResponse(LockResponseType type, ReadOnlyLockContext? context)
    {
        Type = type;
        Context = context;
    }
}