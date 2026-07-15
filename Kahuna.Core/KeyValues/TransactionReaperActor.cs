using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Transactions;
using Nixie;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Periodic coordinator maintenance sweep. On each tick it renews the range locks of live sessions so
/// they outlive their original acquire TTL without a client heartbeat, then reclaims interactive sessions
/// that were started but never committed or rolled back (client crash, timeout, dropped connection).
/// Without this sweep abandoned sessions — and any locks they hold — leak for the process lifetime, and a
/// long-running transaction's range locks would lapse unless the client re-acquired them on a timer.
/// </summary>
internal sealed class TransactionReaperActor : IActor<TransactionReaperRequest>
{
    private readonly TransactionCoordinator coordinator;
    private readonly ILogger<IKahuna> logger;

    public TransactionReaperActor(
        IActorContext<TransactionReaperActor, TransactionReaperRequest> context,
        TransactionCoordinator coordinator,
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
        // Renew live sessions' range locks before reaping, so a healthy long-running session's locks are kept
        // alive while abandoned sessions past their deadline are reclaimed. The two are independent: a renewal
        // failure must not skip the reap sweep, and vice versa.
        try
        {
            await coordinator.RenewSessionRangeLocks();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to renew session range locks");
        }

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
