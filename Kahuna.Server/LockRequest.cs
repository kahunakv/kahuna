
namespace Kahuna;

/// <summary>
/// Represents a request to perform an action on a locker actor
/// </summary>
public readonly struct LockRequest
{
    public LockRequestType Type { get; }
    
    public string? Owner { get; }
    
    public int ExpiresMs { get; }
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="type"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    public LockRequest(LockRequestType type, string? owner, int expiresMs)
    {
        Type = type;
        Owner = owner;
        ExpiresMs = expiresMs;
    }
}