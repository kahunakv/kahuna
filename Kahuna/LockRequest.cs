
namespace Kahuna;

public sealed class LockRequest
{
    public LockRequestType Type { get; }
    
    public string? Owner { get; }
    
    public LockRequest(LockRequestType type, string? owner)
    {
        Type = type;
        Owner = owner;
    }
}