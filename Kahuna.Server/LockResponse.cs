
namespace Kahuna;

public sealed class LockResponse
{
    public LockResponseType Type { get; }
    
    public LockResponse(LockResponseType type)
    {
        Type = type;
    }
}