
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// One key of a batched commit-time conflict probe: which key to inspect, in which keyspace, and which
/// conflict classes to answer for it (<see cref="KeyValueConflictChecks"/>).
/// </summary>
/// <param name="Key">The logical key to probe. Routing authority — the probe is served by this key's leader.</param>
/// <param name="Durability">The keyspace the key lives in.</param>
/// <param name="Checks">The conflict classes to answer. <see cref="KeyValueConflictChecks.None"/> answers no conflict.</param>
public readonly record struct KeyValueConflictProbe(
    string Key,
    KeyValueDurability Durability,
    KeyValueConflictChecks Checks);
