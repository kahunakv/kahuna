
using Nixie.Routers;

namespace Kahuna;

/// <summary>
/// Represents a request to perform an action on a locker actor
/// </summary>
public readonly struct LockRequest : IConsistentHashable
{
    public LockRequestType Type { get; }
    
    public string Resource { get; }
    
    public string Owner { get; }
    
    public int ExpiresMs { get; }
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="type"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    public LockRequest(LockRequestType type, string resource, string owner, int expiresMs)
    {
        Type = type;
        Resource = resource;
        Owner = owner;
        ExpiresMs = expiresMs;
    }

    public int GetHash()
    {
        return Math.Abs(Resource?.GetHashCode() ?? 0);
    }
}