
namespace Kahuna.Shared.KeyValue;

public enum KeyValueConsistency
{
    Ephemeral = 0,
    Linearizable = 1,
    Serializable = 2,
    LinearizableReplication = 3
}