using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
using Nixie;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Periodically removes snapshot holds whose lease has expired without an explicit release.
/// Without this sweep, a crashed or unresponsive holder would pin MVCC revision history for
/// its held timestamp for the process lifetime.
/// </summary>
internal sealed class SnapshotFloorReaperActor : IActor<SnapshotFloorReaperRequest>
{
    private readonly SnapshotFloorStore floorStore;

    private readonly ILogger<IKahuna> logger;

    public SnapshotFloorReaperActor(
        IActorContext<SnapshotFloorReaperActor, SnapshotFloorReaperRequest> context,
        SnapshotFloorStore floorStore,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    )
    {
        this.floorStore = floorStore;
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
    }
}
