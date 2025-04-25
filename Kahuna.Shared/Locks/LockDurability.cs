
namespace Kahuna.Shared.Locks;

/// <summary>
/// Represents the durability level of a lock in the system.
/// </summary>
public enum LockDurability
{
    Ephemeral = 0,
    Persistent = 1
}