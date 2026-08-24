
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// One key of a batched commit-time conflict probe: which key to inspect, in which keyspace, and which
/// conflict classes to answer for it (<see cref="KeyValueConflictChecks"/>).
/// </summary>
/// <param name="Key">The logical key to probe. Routing authority — the probe is served by this key's leader.</param>
/// <param name="Durability">The keyspace the key lives in.</param>
/// <param name="Checks">The conflict classes to answer. <see cref="KeyValueConflictChecks.None"/> answers no conflict.</param>
/// <param name="BaseRevision">For <see cref="KeyValueConflictChecks.StagedBase"/>: the committed base this
/// transaction's read-modify-write of the key was validated against — the revision when the base existed, or
/// <c>-1</c> when the validated base was "key does not exist". Ignored by every other check.</param>
public readonly record struct KeyValueConflictProbe(
    string Key,
    KeyValueDurability Durability,
    KeyValueConflictChecks Checks,
    long BaseRevision = -1);
