
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// The fields the staged-base fence judges for one prepare, carried with the replica verdict ask so a replica
/// that has not applied the prepare yet judges from its committed-head memory instead of answering
/// <see cref="KeyValueStagedBaseVerdict.NotApplied"/>: the validated base revision (<c>long.MinValue</c> when the
/// write carries no validated base) and the state it was observed in (the server's key-value state enum, as an
/// integer like the wire carries it), the staged revision, and whether the write stages no revision at all.
/// Aligned by index with the ask's keys.
/// </summary>
public readonly record struct KeyValueStagedBaseProbe(long BaseRevision, int BaseState, long Revision, bool NoRevision);
