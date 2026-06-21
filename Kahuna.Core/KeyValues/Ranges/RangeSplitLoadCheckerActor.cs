
using Nixie;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Periodically invokes <see cref="RangeSplitTrigger.LoadCheckAsync"/> to evaluate the K2.2
/// load branch for auto-split. Runs at <see cref="KahunaConfiguration.RangeSplitLoadPollInterval"/>
/// (~5 s) — faster than the slow count-based <see cref="RangeSplitCheckerActor"/> — because the
/// load predicate is cheap (three <c>IRaft</c> accessor reads per descriptor) and the
/// <see cref="KahunaConfiguration.RangeSplitLoadWindow"/> debounce requires sub-CollectionInterval
/// resolution to be measured accurately.
/// </summary>
internal sealed class RangeSplitLoadCheckerActor : IActor<RangeSplitLoadCheckerRequest>
{
    private readonly RangeSplitTrigger trigger;
    private readonly ILogger<IKahuna>  logger;

    public RangeSplitLoadCheckerActor(
        IActorContext<RangeSplitLoadCheckerActor, RangeSplitLoadCheckerRequest> context,
        RangeSplitTrigger trigger,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger)
    {
        this.trigger = trigger;
        this.logger  = logger;

        TimeSpan interval = configuration.RangeSplitLoadPollInterval;
        TimeSpan initial  = TimeSpan.FromTicks(interval.Ticks / 2);

        context.ActorSystem.StartPeriodicTimer(
            context.Self,
            "range-split-load-check",
            new RangeSplitLoadCheckerRequest(),
            initial,
            interval
        );
    }

    public async Task Receive(RangeSplitLoadCheckerRequest _)
    {
        try
        {
            int splits = await trigger.LoadCheckAsync();
            if (splits > 0)
                logger.LogRangeSplitLoadCheckerPerformed(splits);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "RangeSplitLoadChecker: unhandled exception during load-check pass.");
        }
    }
}

internal sealed class RangeSplitLoadCheckerRequest;
