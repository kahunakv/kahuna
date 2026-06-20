using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Transactions;
using Nixie;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Periodically asks the transaction coordinator to reclaim interactive transaction sessions that
/// were started but never committed or rolled back (client crash, timeout, dropped connection).
/// Without this sweep those sessions — and any locks they hold — leak for the process lifetime.
/// </summary>
internal sealed class TransactionReaperActor : IActor<TransactionReaperRequest>
{
    private readonly KeyValueTransactionCoordinator coordinator;
    private readonly ILogger<IKahuna> logger;

    public TransactionReaperActor(
        IActorContext<TransactionReaperActor, TransactionReaperRequest> context,
        KeyValueTransactionCoordinator coordinator,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    )
    {
        this.coordinator = coordinator;
        this.logger = logger;

        context.ActorSystem.StartPeriodicTimer(
            context.Self,
            "reap-transactions",
            new(),
            configuration.CollectionInterval,
            configuration.CollectionInterval
        );
    }

    public async Task Receive(TransactionReaperRequest message)
    {
        try
        {
            await coordinator.ReapAbandonedSessions();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to reap abandoned transactions");
        }
    }
}
