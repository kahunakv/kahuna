
using Nixie;

using Kahuna.Server.Configuration;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Periodically invokes <see cref="RangeSplitTrigger.TriggerAsync"/> to auto-split
/// KeyRange descriptors that exceed the configured size threshold.
///
/// <para>
/// The actor fires on every <see cref="KahunaConfiguration.CollectionInterval"/> tick.
/// <see cref="RangeSplitTrigger.TriggerAsync"/> itself guards against running on non-dual-leader
/// nodes and returns 0 immediately when this node does not hold leadership of both the system
/// partition (0) and the meta partition (1). The actor merely provides the periodic schedule.
/// </para>
/// </summary>
internal sealed class RangeSplitCheckerActor : IActor<RangeSplitCheckerRequest>
{
    private readonly RangeSplitTrigger trigger;
    private readonly ILogger<IKahuna>  logger;

    public RangeSplitCheckerActor(
        IActorContext<RangeSplitCheckerActor, RangeSplitCheckerRequest> context,
        RangeSplitTrigger trigger,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger)
    {
        this.trigger = trigger;
        this.logger  = logger;

        // Stagger the first fire by half the interval so it doesn't overlap with the initial
        // collection sweep that the KeyValueCollectorActor also fires at startup.
        TimeSpan interval = configuration.CollectionInterval;
        TimeSpan initial  = TimeSpan.FromTicks(interval.Ticks / 2);

        context.ActorSystem.StartPeriodicTimer(
            context.Self,
            "range-split-check",
            new RangeSplitCheckerRequest(),
            initial,
            interval
        );
    }

    public async Task Receive(RangeSplitCheckerRequest _)
    {
        try
        {
            int splits = await trigger.TriggerAsync();
            if (splits > 0)
                logger.LogInformation("RangeSplitChecker: performed {Count} split(s).", splits);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "RangeSplitChecker: unhandled exception during trigger pass.");
        }
    }
}

internal sealed class RangeSplitCheckerRequest;
