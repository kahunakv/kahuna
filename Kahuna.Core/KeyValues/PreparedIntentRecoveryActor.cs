using Kahuna.Server.Configuration;
using Nixie;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// One per node: periodically drives participant-side recovery for the durable-intent 2PC path, resolving due
/// unresolved prepared intents on the partitions this node leads to their canonical decision. A single serial
/// mailbox keeps periodic ticks from overlapping; the underlying stores are idempotent, so a race with a
/// request-path finalize is safe. A no-op unless the durable-intent path is enabled.
/// </summary>
internal sealed class PreparedIntentRecoveryActor : IActor<PreparedIntentRecoveryRequest>
{
    private readonly KeyValuesManager manager;
    private readonly ILogger<IKahuna> logger;

    public PreparedIntentRecoveryActor(
        IActorContext<PreparedIntentRecoveryActor, PreparedIntentRecoveryRequest> context,
        KeyValuesManager manager,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    )
    {
        this.manager = manager;
        this.logger = logger;

        context.ActorSystem.StartPeriodicTimer(
            context.Self,
            "recover-prepared-intents",
            new(),
            configuration.CollectionInterval,
            configuration.CollectionInterval
        );
    }

    public async Task Receive(PreparedIntentRecoveryRequest message)
    {
        try
        {
            await manager.RecoverPreparedIntents(CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to recover prepared intents");
        }

        // Same serial tick reclaims durable-2PC metadata whose retention window has elapsed (terminal records +
        // their participants' completion receipts), so both stores return to a steady-state floor instead of
        // growing for the node's lifetime. Isolated from recovery so a GC fault never stalls recovery.
        try
        {
            await manager.CollectDurableTransactionRecords(CancellationToken.None);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to collect durable transaction records");
        }
    }
}
