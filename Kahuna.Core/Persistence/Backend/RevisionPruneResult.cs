namespace Kahuna.Server.Persistence.Backend;

/// <summary>
/// Outcome of a persistent key/value revision prune pass.
/// </summary>
/// <param name="KeysVisited">Number of logical keys inspected during the pass.</param>
/// <param name="RevisionsDeleted">Number of historical revision records deleted.</param>
/// <param name="BatchLimitReached">
/// <c>true</c> when the pass stopped early because the batch budget was exhausted (or, for a
/// backend-wide sweep, because there is more of the keyspace left to scan).
/// </param>
/// <param name="RemainingKeys">
/// For a targeted pass, the subset of the requested keys that still has prunable revisions or was
/// never visited because the batch filled. <c>null</c> when nothing remains or the backend does not
/// report per-key backlog. Lets the caller requeue only keys with real work instead of all of them.
/// </param>
public readonly record struct RevisionPruneResult(
    int KeysVisited,
    int RevisionsDeleted,
    bool BatchLimitReached,
    IReadOnlyCollection<string>? RemainingKeys = null
);
