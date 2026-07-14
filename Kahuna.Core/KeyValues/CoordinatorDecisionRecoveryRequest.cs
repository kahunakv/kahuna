namespace Kahuna.Server.KeyValues;

/// <summary>
/// Wakes the <see cref="CoordinatorDecisionRecoveryActor"/> to sweep outstanding coordinator decision records.
/// Carries no payload: every wake — periodic tick or a local data-partition leadership acquisition — runs the
/// same idempotent sweep over the records anchored to partitions this node currently leads.
/// </summary>
public sealed class CoordinatorDecisionRecoveryRequest
{
}
