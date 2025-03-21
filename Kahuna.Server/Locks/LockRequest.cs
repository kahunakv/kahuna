
using Kahuna.Shared.Locks;
using Nixie.Routers;
using Standart.Hash.xxHash;

namespace Kahuna.Server.Locks;

/// <summary>
/// Represents a request to perform an action on a locker actor
/// </summary>
public readonly struct LockRequest : IConsistentHashable
{
    public LockRequestType Type { get; }
    
    public string Resource { get; }
    
    public byte[]? Owner { get; }
    
    public int ExpiresMs { get; }
    
    public LockDurability Durability { get; }
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="type"></param>
    /// <param name="resource"></param>
    /// <param name="owner"></param>
    /// <param name="expiresMs"></param>
    /// <param name="consistency"></param>
    public LockRequest(LockRequestType type, string resource, byte[]? owner, int expiresMs, LockDurability durability)
    {
        Type = type;
        Resource = resource;
        Owner = owner;
        ExpiresMs = expiresMs;
        Durability = durability;
    }

    public int GetHash()
    {
        return (int)xxHash32.ComputeHash(Resource ?? "");
    }
}