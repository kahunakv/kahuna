using Kahuna.Server.Configuration;
using Kahuna.Shared.KeyValue;
using Nixie;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Periodically fans out cache collection to every key/value actor instance,
/// staggering sends across the collection interval to avoid synchronized sweeps.
/// </summary>
internal sealed class KeyValueCollectorActor : IActor<KeyValueCollectorRequest>
{
    private readonly ActorSystem actorSystem;
    private readonly TimeSpan collectionInterval;
    private readonly IReadOnlyList<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> ephemeralInstances;
    private readonly IReadOnlyList<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> persistentInstances;

    public KeyValueCollectorActor(
        IActorContext<KeyValueCollectorActor, KeyValueCollectorRequest> context,
        IReadOnlyList<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> ephemeralInstances,
        IReadOnlyList<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> persistentInstances,
        KahunaConfiguration configuration,
        ILogger<IKahuna> _
    )
    {
        actorSystem = context.ActorSystem;
        collectionInterval = configuration.CollectionInterval;
        this.ephemeralInstances = ephemeralInstances;
        this.persistentInstances = persistentInstances;

        context.ActorSystem.StartPeriodicTimer(
            context.Self,
            "collect-keyvalues",
            new(),
            collectionInterval,
            collectionInterval
        );
    }

    public Task Receive(KeyValueCollectorRequest message)
    {
        int total = ephemeralInstances.Count + persistentInstances.Count;
        if (total == 0)
            return Task.CompletedTask;

        int index = 0;
        foreach (IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor in ephemeralInstances)
            ScheduleCollect(actor, index++, total);

        foreach (IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor in persistentInstances)
            ScheduleCollect(actor, index++, total);

        return Task.CompletedTask;
    }

    private void ScheduleCollect(
        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor,
        int index,
        int total
    )
    {
        long delayTicks = collectionInterval.Ticks * index / total;
        TimeSpan delay = delayTicks > 0 ? TimeSpan.FromTicks(delayTicks) : TimeSpan.Zero;

        actorSystem.ScheduleOnce<KeyValueActor, KeyValueRequest, KeyValueResponse>(
            actor,
            new KeyValueRequest(KeyValueRequestType.Collect),
            delay
        );
    }
}
