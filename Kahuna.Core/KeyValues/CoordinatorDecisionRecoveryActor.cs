using Kahuna.Server.Configuration;
using Nixie;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// One per node: periodically (and on local data-partition leadership acquisition) re-drives outstanding durable
/// coordinator decision records for the anchor partitions this node currently leads — committing not-yet-acked
/// participants, releasing their receipts, marking decisions <c>Completed</c>, and purging completed records
/// after the retention window. Runs off the request and key-value actor mailboxes so a coordinator that died
/// mid-phase-two, or one never colocated with the anchor leader, is completed on the anchor partition's leader.
/// A single serial mailbox means the periodic tick and a leadership-triggered wake never overlap; the underlying
/// stores are idempotent, so a race with a request-path drive is safe.
/// </summary>
internal sealed class CoordinatorDecisionRecoveryActor : IActor<CoordinatorDecisionRecoveryRequest>
{
    private readonly KeyValuesManager manager;
    private readonly TimeSpan retentionTtl;
    private readonly ILogger<IKahuna> logger;

    public CoordinatorDecisionRecoveryActor(
        IActorContext<CoordinatorDecisionRecoveryActor, CoordinatorDecisionRecoveryRequest> context,
        KeyValuesManager manager,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    )
    {
        this.manager = manager;
        this.retentionTtl = configuration.TransactionOutcomeRetentionTtl;
        this.logger = logger;

        context.ActorSystem.StartPeriodicTimer(
            context.Self,
            "recover-coordinator-decisions",
            new(),
            configuration.CollectionInterval,
            configuration.CollectionInterval
        );
    }

    public async Task Receive(CoordinatorDecisionRecoveryRequest message)
    {
        try
        {
            await manager.RecoverOutstandingDecisions(retentionTtl, CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to recover outstanding coordinator decisions");
        }
    }
}
