
using Nixie;

using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Periodically invokes <see cref="RangeMergeTrigger.TriggerAsync"/> to auto-merge
/// KeyRange descriptor pairs that are both under the configured minimum size.
///
/// <para>
/// The actor fires on every <see cref="KahunaConfiguration.CollectionInterval"/> tick,
/// staggered by ¾ of the interval so it does not overlap with the split checker (½ interval)
/// or the collection sweep (0 offset). <see cref="RangeMergeTrigger.TriggerAsync"/> guards
/// against running on non-dual-leader nodes and returns 0 immediately when this node does not
/// hold leadership of both the system partition (0) and the meta partition (1).
/// </para>
/// </summary>
internal sealed class RangeMergeCheckerActor : IActor<RangeMergeCheckerRequest>
{
    private readonly RangeMergeTrigger trigger;
    private readonly ILogger<IKahuna>  logger;

    public RangeMergeCheckerActor(
        IActorContext<RangeMergeCheckerActor, RangeMergeCheckerRequest> context,
        RangeMergeTrigger trigger,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger)
    {
        this.trigger = trigger;
        this.logger  = logger;

        // Stagger at ¾ of the interval — different from the split checker (½ interval) so the
        // two background passes do not compete for the dual-leader slot simultaneously.
        TimeSpan interval = configuration.CollectionInterval;
        TimeSpan initial  = TimeSpan.FromTicks(interval.Ticks * 3 / 4);

        context.ActorSystem.StartPeriodicTimer(
            context.Self,
            "range-merge-check",
            new RangeMergeCheckerRequest(),
            initial,
            interval
        );
    }

    public async Task Receive(RangeMergeCheckerRequest _)
    {
        try
        {
            int merges = await trigger.TriggerAsync();
            if (merges > 0)
                logger.LogRangeMergeCheckerPerformed(merges);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "RangeMergeChecker: unhandled exception during trigger pass.");
        }
    }
}

internal sealed class RangeMergeCheckerRequest;
