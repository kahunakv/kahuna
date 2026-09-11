namespace Kahuna.Server.Persistence.Backend;

/// <summary>
/// Outcome of a persistent key/value revision prune pass.
/// </summary>
/// <param name="KeysVisited">Number of logical keys inspected during the pass.</param>
/// <param name="RevisionsDeleted">Number of historical revision records deleted.</param>
/// <param name="BatchLimitReached">
/// <c>true</c> when the pass stopped early with work remaining: the delete budget was exhausted,
/// the time budget ran out (<see cref="TimeBudgetExhausted"/> says which), or, for a backend-wide
/// sweep, there is more of the keyspace left to scan. The caller re-queues / resumes on this flag
/// alone; the reason flags only refine it.
/// </param>
/// <param name="RemainingKeys">
/// For a targeted pass, the subset of the requested keys that still has prunable revisions or was
/// never visited because the batch filled. <c>null</c> when nothing remains or the backend does not
/// report per-key backlog. Lets the caller requeue only keys with real work instead of all of them.
/// </param>
/// <param name="FloorViolations">
/// Number of floor-protected revisions (revision ≥ the floor-boundary revision) that were deleted
/// despite a non-zero snapshot floor being passed to <c>PruneKeyValueRevisions</c>. The built-in
/// backends compute this by auditing the actual deletions independently of their own floor clamp —
/// SQLite via a before/after count of the protected set, RocksDB by observing the delete site — so a
/// correct clamp always yields 0 and a regression in the clamp yields a positive count. A non-zero
/// value triggers the <c>kahuna.snapshot_floor.missing_protected_version_total</c> metric in
/// <c>BackgroundWriterActor</c> and must never occur in normal operation.
/// </param>
/// <param name="KeysSkipped">
/// Of <paramref name="KeysVisited"/>, the keys answered from the backend's prune memo without a
/// revision walk because the memo proved nothing was deletable yet. Backends without a memo report 0.
/// </param>
/// <param name="TimeBudgetExhausted">
/// <c>true</c> when the pass stopped on the caller's wall-clock budget (the budgeted
/// <c>PruneKeyValueRevisions</c> overload) with keys or keyspace still unvisited. Always accompanied
/// by <see cref="BatchLimitReached"/>. Backends that ignore the budget never set it.
/// </param>
public readonly record struct RevisionPruneResult(
    int KeysVisited,
    int RevisionsDeleted,
    bool BatchLimitReached,
    IReadOnlyCollection<string>? RemainingKeys = null,
    int FloorViolations = 0,
    int KeysSkipped = 0,
    bool TimeBudgetExhausted = false
);
