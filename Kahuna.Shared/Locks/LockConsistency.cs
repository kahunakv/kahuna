
namespace Kahuna.Shared.Locks;

public enum LockConsistency
{
    Ephemeral = 0,
    Linearizable = 1,
    ReplicationConsistent = 2
}