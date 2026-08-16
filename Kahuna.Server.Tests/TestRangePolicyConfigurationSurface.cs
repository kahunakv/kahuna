using Kahuna.Server.Configuration;

namespace Kahuna.Server.Tests;

/// <summary>
/// Coverage for the key-range split/merge policy configuration surface.
/// <para>
/// The bug this guards against is not a wrong value but a missing assignment: a flag that parses,
/// binds, and is then never copied into the options the node runs on. The server behaves exactly as
/// if the flag had not been passed, and nothing fails — which is how these knobs came to exist on
/// the configuration classes for a long time while being unreachable from a running server. So each
/// flag is followed the whole way: command line → embedded options → the configuration the managers
/// read.
/// </para>
/// </summary>
public sealed class TestRangePolicyConfigurationSurface
{
    /// <summary>Non-default values throughout, so a missed assignment cannot pass by coincidence.</summary>
    private static KahunaCommandLineOptions PolicyOptions() => new()
    {
        RangeSplitThreshold = 20,
        RangeSplitMinRangeSize = 3,
        RangeSplitSettleWindowSeconds = 30,
        RangeMergeMinSize = 7,
        RangeCollectionIntervalSeconds = 11,
        RangeSplitLoadThreshold = 250.5,
        RangeSplitLoadMinQueueDepth = 16,
        RangeSplitLoadWindowSeconds = 45,
        RangeSplitLoadPollIntervalSeconds = 9
    };

    [Fact]
    public void EveryRangePolicyFlag_ReachesTheEmbeddedOptions()
    {
        EmbeddedKahunaOptions options = EmbeddedOptionsFactory.CreateEmbeddedOptions(PolicyOptions());

        Assert.Equal(20, options.RangeSplitThreshold);
        Assert.Equal(3, options.RangeSplitMinRangeSize);
        Assert.Equal(TimeSpan.FromSeconds(30), options.RangeSplitSettleWindow);
        Assert.Equal(7, options.RangeMergeMinSize);
        Assert.Equal(TimeSpan.FromSeconds(11), options.CollectionInterval);
        Assert.Equal(250.5, options.RangeSplitLoadThreshold);
        Assert.Equal(16, options.RangeSplitLoadMinQueueDepth);
        Assert.Equal(TimeSpan.FromSeconds(45), options.RangeSplitLoadWindow);
        Assert.Equal(TimeSpan.FromSeconds(9), options.RangeSplitLoadPollInterval);
    }

    [Fact]
    public void EveryRangePolicyKnob_ReachesTheConfigurationTheManagersRead()
    {
        EmbeddedKahunaOptions options = EmbeddedOptionsFactory.CreateEmbeddedOptions(PolicyOptions());

        KahunaConfiguration configuration =
            EmbeddedKahunaNode.CreateKahunaConfiguration(options, singleProcessRaftGroup: false);

        Assert.Equal(20, configuration.RangeSplitThreshold);
        Assert.Equal(3, configuration.RangeSplitMinRangeSize);
        Assert.Equal(TimeSpan.FromSeconds(30), configuration.RangeSplitSettleWindow);
        Assert.Equal(7, configuration.RangeMergeMinSize);
        Assert.Equal(TimeSpan.FromSeconds(11), configuration.CollectionInterval);
        Assert.Equal(250.5, configuration.RangeSplitLoadThreshold);
        Assert.Equal(16, configuration.RangeSplitLoadMinQueueDepth);
        Assert.Equal(TimeSpan.FromSeconds(45), configuration.RangeSplitLoadWindow);
        Assert.Equal(TimeSpan.FromSeconds(9), configuration.RangeSplitLoadPollInterval);
    }

    /// <summary>
    /// Both constructors must carry the same knobs. They used to build the configuration from two
    /// duplicated blocks, and that is exactly how <c>RangeMergeMinSize</c> came to be settable in a
    /// standalone node and silently fixed at its default everywhere else.
    /// </summary>
    [Fact]
    public void BothConstructionPaths_CarryTheSameRangePolicy()
    {
        EmbeddedKahunaOptions options = EmbeddedOptionsFactory.CreateEmbeddedOptions(PolicyOptions());

        KahunaConfiguration standalone = EmbeddedKahunaNode.CreateKahunaConfiguration(options, singleProcessRaftGroup: true);
        KahunaConfiguration cluster = EmbeddedKahunaNode.CreateKahunaConfiguration(options, singleProcessRaftGroup: false);

        Assert.Equal(standalone.RangeSplitThreshold, cluster.RangeSplitThreshold);
        Assert.Equal(standalone.RangeSplitMinRangeSize, cluster.RangeSplitMinRangeSize);
        Assert.Equal(standalone.RangeSplitSettleWindow, cluster.RangeSplitSettleWindow);
        Assert.Equal(standalone.RangeMergeMinSize, cluster.RangeMergeMinSize);
        Assert.Equal(standalone.CollectionInterval, cluster.CollectionInterval);
        Assert.Equal(standalone.RangeSplitLoadThreshold, cluster.RangeSplitLoadThreshold);
        Assert.Equal(standalone.RangeSplitLoadMinQueueDepth, cluster.RangeSplitLoadMinQueueDepth);
        Assert.Equal(standalone.RangeSplitLoadWindow, cluster.RangeSplitLoadWindow);
        Assert.Equal(standalone.RangeSplitLoadPollInterval, cluster.RangeSplitLoadPollInterval);

        // The one property that legitimately differs, asserted so the shared factory cannot quietly
        // start handing the cluster path the in-process topology guarantee.
        Assert.True(standalone.SingleProcessRaftGroup);
        Assert.False(cluster.SingleProcessRaftGroup);
    }

    /// <summary>
    /// The defaults are what an operator who passes no range flags gets, so they must match the
    /// documented ones rather than whatever the options classes happen to initialise.
    /// </summary>
    [Fact]
    public void RangePolicyDefaults_MatchTheDocumentedValues()
    {
        EmbeddedKahunaOptions options = EmbeddedOptionsFactory.CreateEmbeddedOptions(new KahunaCommandLineOptions());

        Assert.Equal(1_000, options.RangeSplitThreshold);
        Assert.Equal(10, options.RangeSplitMinRangeSize);
        Assert.Equal(TimeSpan.FromSeconds(10), options.RangeSplitSettleWindow);
        Assert.Equal(10, options.RangeMergeMinSize);
        Assert.Equal(TimeSpan.FromSeconds(60), options.CollectionInterval);
        Assert.Equal(0, options.RangeSplitLoadThreshold);
        Assert.Equal(8, options.RangeSplitLoadMinQueueDepth);
        Assert.Equal(TimeSpan.FromSeconds(15), options.RangeSplitLoadWindow);
        Assert.Equal(TimeSpan.FromSeconds(5), options.RangeSplitLoadPollInterval);
    }

    /// <summary>
    /// Zero on a threshold knob is not "no limit": the corresponding background checker is never
    /// spawned, so the feature is off entirely. Pinned here because the help text promises it and a
    /// reader of the configuration alone cannot tell the difference.
    /// </summary>
    [Fact]
    public void ZeroThresholds_SurviveAsZeroRatherThanBeingCoercedToADefault()
    {
        EmbeddedKahunaOptions options = EmbeddedOptionsFactory.CreateEmbeddedOptions(new KahunaCommandLineOptions
        {
            RangeSplitThreshold = 0,
            RangeMergeMinSize = 0,
            RangeSplitLoadThreshold = 0
        });

        KahunaConfiguration configuration =
            EmbeddedKahunaNode.CreateKahunaConfiguration(options, singleProcessRaftGroup: false);

        Assert.Equal(0, configuration.RangeSplitThreshold);
        Assert.Equal(0, configuration.RangeMergeMinSize);
        Assert.Equal(0, configuration.RangeSplitLoadThreshold);
    }

    /// <summary>
    /// The settle window is what stops a freshly split range from being split again before its new
    /// leader has stabilised, so a window shorter than the stability gate defeats it. Now that both
    /// values are operator-settable, the check runs against the values the operator actually passed —
    /// previously the settle window never reached the instance being validated, so the guard could
    /// not fire whatever was configured.
    /// </summary>
    [Fact]
    public void SettleWindowShorterThanLeaderStability_IsRefusedWithBothValuesNamed()
    {
        EmbeddedKahunaOptions options = EmbeddedOptionsFactory.CreateEmbeddedOptions(new KahunaCommandLineOptions
        {
            RangeSplitSettleWindowSeconds = 3,
            RaftMinLeaderStabilityMs = 5_000
        });

        KahunaConfiguration configuration =
            EmbeddedKahunaNode.CreateKahunaConfiguration(options, singleProcessRaftGroup: false);

        KahunaServerException exception = Assert.Throws<KahunaServerException>(() =>
            ConfigurationValidator.ValidateSettleWindow(configuration, options.MinLeaderStability.Ticks / TimeSpan.TicksPerMillisecond));

        Assert.Contains("3000", exception.Message);
        Assert.Contains("5000", exception.Message);

        // The CLI value reaches the embedded options too, so the node's own startup check sees it.
        Assert.Equal(TimeSpan.FromSeconds(3), options.RangeSplitSettleWindow);
        Assert.Equal(TimeSpan.FromMilliseconds(5_000), options.MinLeaderStability);
    }

    /// <summary>
    /// The collection interval is shared with session range-lock renewal — the lease is twice it and
    /// the renewal sweep is bounded by it — so lowering it to make splits fire sooner silently
    /// shortens how long a lock survives a slow participant. The floor refuses that trade instead of
    /// clamping it, because a clamped value is a setting the operator asked for and did not get.
    /// </summary>
    [Fact]
    public void CollectionIntervalBelowTheFloor_IsRefused()
    {
        KahunaConfiguration configuration = new() { CollectionInterval = TimeSpan.FromSeconds(1) };

        KahunaServerException exception =
            Assert.Throws<KahunaServerException>(() => ConfigurationValidator.ValidateCollectionInterval(configuration));

        Assert.Contains("--range-collection-interval", exception.Message);

        // The floor is the phase-two commit timeout: an entire renewal sweep may not be given less
        // time than a single commit round trip is allowed.
        configuration.CollectionInterval = TimeSpan.FromMilliseconds(configuration.Phase2CommitTimeout);
        ConfigurationValidator.ValidateCollectionInterval(configuration);

        // The documented tuning example for reaching auto-split within a short run stays legal.
        ConfigurationValidator.ValidateCollectionInterval(new() { CollectionInterval = TimeSpan.FromSeconds(5) });
    }

    /// <summary>
    /// The embedded API is deliberately exempt: in-process tests drive lease expiry at sub-second
    /// intervals, and the floor is a guard on the operator-facing flag, not on the library.
    /// </summary>
    [Fact]
    public void CollectionIntervalFloor_DoesNotApplyToTheEmbeddedConfiguration()
    {
        KahunaConfiguration configuration = EmbeddedKahunaNode.CreateKahunaConfiguration(
            new EmbeddedKahunaOptions { CollectionInterval = TimeSpan.FromMilliseconds(100) },
            singleProcessRaftGroup: true);

        Assert.Equal(TimeSpan.FromMilliseconds(100), configuration.CollectionInterval);
    }
}
