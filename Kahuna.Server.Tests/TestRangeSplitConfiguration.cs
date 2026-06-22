using Kahuna.Server.Configuration;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit tests for operator-facing range-split configuration validation.
/// No cluster infrastructure required — all cases are pure validation logic.
/// </summary>
public sealed class TestRangeSplitConfiguration
{
    // ── ConfigurationValidator.ValidateSettleWindow ──────────────────────────

    [Fact]
    public void ValidateSettleWindow_NoOp_WhenMinStabilityIsZero()
    {
        // minLeaderStabilityMs == 0 means the feature is disabled; no constraint applies.
        var config = new KahunaConfiguration { RangeSplitSettleWindow = TimeSpan.FromSeconds(1) };
        ConfigurationValidator.ValidateSettleWindow(config, minLeaderStabilityMs: 0);
    }

    [Fact]
    public void ValidateSettleWindow_NoOp_WhenSettleWindowIsZero()
    {
        // RangeSplitSettleWindow == 0 means the settle window is disabled; no constraint applies.
        var config = new KahunaConfiguration { RangeSplitSettleWindow = TimeSpan.Zero };
        ConfigurationValidator.ValidateSettleWindow(config, minLeaderStabilityMs: 5_000);
    }

    [Fact]
    public void ValidateSettleWindow_NoOp_WhenBothAreZero()
    {
        var config = new KahunaConfiguration { RangeSplitSettleWindow = TimeSpan.Zero };
        ConfigurationValidator.ValidateSettleWindow(config, minLeaderStabilityMs: 0);
    }

    [Fact]
    public void ValidateSettleWindow_NoOp_WhenWindowExceedsStability()
    {
        var config = new KahunaConfiguration { RangeSplitSettleWindow = TimeSpan.FromSeconds(10) };
        ConfigurationValidator.ValidateSettleWindow(config, minLeaderStabilityMs: 5_000);
    }

    [Fact]
    public void ValidateSettleWindow_NoOp_WhenWindowEqualsStability()
    {
        // Boundary: equal is acceptable (≥, not >).
        var config = new KahunaConfiguration { RangeSplitSettleWindow = TimeSpan.FromSeconds(5) };
        ConfigurationValidator.ValidateSettleWindow(config, minLeaderStabilityMs: 5_000);
    }

    [Fact]
    public void ValidateSettleWindow_Throws_WhenWindowIsShorterThanStability()
    {
        var config = new KahunaConfiguration { RangeSplitSettleWindow = TimeSpan.FromSeconds(2) };
        Assert.Throws<KahunaServerException>(() =>
            ConfigurationValidator.ValidateSettleWindow(config, minLeaderStabilityMs: 5_000));
    }

    [Fact]
    public void ValidateSettleWindow_Throws_WhenWindowIsByOneMillisecondTooShort()
    {
        // Off-by-one: 4999 ms < 5000 ms — must reject.
        var config = new KahunaConfiguration { RangeSplitSettleWindow = TimeSpan.FromMilliseconds(4_999) };
        Assert.Throws<KahunaServerException>(() =>
            ConfigurationValidator.ValidateSettleWindow(config, minLeaderStabilityMs: 5_000));
    }

    [Fact]
    public void ValidateSettleWindow_Defaults_Satisfy_Constraint()
    {
        // Default settle window (10 s) must be >= default MinLeaderStabilityMs (5 000 ms).
        var config = new KahunaConfiguration();
        ConfigurationValidator.ValidateSettleWindow(config, minLeaderStabilityMs: 5_000);
    }

    // ── EmbeddedKahunaNode.ValidateOptions (embedded path) ───────────────────

    [Fact]
    public void EmbeddedKahunaNode_Throws_WhenSettleWindowShorterThanMinStability()
    {
        // ValidateOptions fires in the constructor before any Raft setup is attempted,
        // so the ArgumentException is raised synchronously with no side-effects.
        var options = new EmbeddedKahunaOptions
        {
            NodeName              = "test",
            Host                  = "localhost",
            RangeSplitSettleWindow = TimeSpan.FromSeconds(2),
            MinLeaderStability    = TimeSpan.FromSeconds(5),
        };

        Assert.Throws<ArgumentException>(() => new EmbeddedKahunaNode(options));
    }

    [Fact]
    public void EmbeddedKahunaNode_Throws_WhenBalancerIntervalConstraintViolated()
    {
        // Existing cross-field constraint: ReportInterval must be < ReportTtl.
        var options = new EmbeddedKahunaOptions
        {
            NodeName                    = "test",
            Host                        = "localhost",
            EnableLeaderBalancer        = true,
            LeaderBalancerReportInterval = TimeSpan.FromSeconds(30),
            LeaderBalancerReportTtl     = TimeSpan.FromSeconds(10), // shorter than ReportInterval
        };

        Assert.Throws<ArgumentException>(() => new EmbeddedKahunaNode(options));
    }
}
