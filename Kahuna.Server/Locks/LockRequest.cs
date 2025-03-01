
using Kahuna.Shared.Locks;
using Nixie.Routers;
using Standart.Hash.xxHash;

namespace Kahuna.Locks;

/// <summary>
/// Represents a request to perform an action on a locker actor
/// </summary>
public readonly struct LockRequest : IConsistentHashable
{
    public LockRequestType Type { get; }
    
    public string Resource { get; }
    
    public string Owner { get; }
    
    public int ExpiresMs { get; }
    
    public LockConsistency Consistency { get; }
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="type"></param>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    public LockRequest(LockRequestType type, string resource, string owner, int expiresMs, LockConsistency consistency)
    {
        Type = type;
        Resource = resource;
        Owner = owner;
        ExpiresMs = expiresMs;
        Consistency = consistency;
    }

    public int GetHash()
    {
        return (int)xxHash32.ComputeHash(Resource ?? "");
    }
}