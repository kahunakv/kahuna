namespace Kahuna.Server.Persistence.Backend;

/// <summary>
/// Outcome of a persistent key/value revision prune pass.
/// </summary>
public readonly record struct RevisionPruneResult(
    int KeysVisited,
    int RevisionsDeleted,
    bool BatchLimitReached
);
