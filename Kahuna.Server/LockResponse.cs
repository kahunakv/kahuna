
namespace Kahuna;

/// <summary>
/// Represents the response of the operation on the locker actor
/// </summary>
public readonly struct LockResponse
{
    public LockResponseType Type { get; }
    
    public LockResponse(LockResponseType type)
    {
        Type = type;
    }
}