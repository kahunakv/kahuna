
namespace Kahuna;

public readonly struct LockRequest
{
    public LockRequestType Type { get; }
    
    public string? Owner { get; }
    
    public int ExpiresMs { get; }
    
    public LockRequest(LockRequestType type, string? owner, int expiresMs)
    {
        Type = type;
        Owner = owner;
        ExpiresMs = expiresMs;
    }
}