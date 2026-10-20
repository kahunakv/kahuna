using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
using Nixie;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Periodically removes snapshot holds and history holds whose lease has expired without an explicit release (and
/// history holds of partitions that no longer exist). Without this sweep, a crashed or unresponsive holder would pin
/// MVCC revision history, or a partition's WAL, for the process lifetime.
/// </summary>
internal sealed class SnapshotFloorReaperActor : IActor<SnapshotFloorReaperRequest>
{
    private readonly SnapshotFloorStore floorStore;

    private readonly HistoryHoldStore historyHoldStore;

    private readonly ILogger<IKahuna> logger;

    public SnapshotFloorReaperActor(
        IActorContext<SnapshotFloorReaperActor, SnapshotFloorReaperRequest> context,
        SnapshotFloorStore floorStore,
        HistoryHoldStore historyHoldStore,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    )
    {
        this.floorStore = floorStore;
        this.historyHoldStore = historyHoldStore;
        this.logger = logger;

        context.ActorSystem.StartPeriodicTimer(
            context.Self,
            "reap-snapshot-holds",
            new(),
            configuration.CollectionInterval,
            configuration.CollectionInterval
        );
    }

    public async Task Receive(SnapshotFloorReaperRequest message)
    {
        try
        {
            await floorStore.PurgeExpiredHoldsAsync();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to purge expired snapshot holds");
        }

        try
        {
            await historyHoldStore.PurgeAsync();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to purge expired history holds");
        }
    }
}
