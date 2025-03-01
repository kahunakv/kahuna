
namespace Kahuna.Shared.Locks;

public enum LockConsistency
{
    Ephemeral = 0,
    Consistent = 1,
    ReplicationConsistent = 2
}