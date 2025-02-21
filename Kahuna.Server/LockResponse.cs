
namespace Kahuna;

public readonly struct LockResponse
{
    public LockResponseType Type { get; }
    
    public LockResponse(LockResponseType type)
    {
        Type = type;
    }
}