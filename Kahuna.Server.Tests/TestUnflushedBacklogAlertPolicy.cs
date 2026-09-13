using System.Diagnostics;
using Kahuna.Server.Persistence;

namespace Kahuna.Server.Tests;

/// <summary>
/// The unflushed-backlog gauges existed since 1.7.8, but on the 45-minute learned-routing soak a follower's
/// durable-apply lag saw-toothed to two thirds of the item budget without a single log line. The alert policy
/// turns samples into a warning at 75% of a budget, a warning when back-pressure actually engages, reminders
/// while either lasts, and a clear line — with hysteresis so a backlog hovering at the line does not flap.
/// </summary>
public sealed class TestUnflushedBacklogAlertPolicy
{
    private static long Ticks(TimeSpan at) => (long)(at.TotalSeconds * Stopwatch.Frequency);

    [Fact]
    public void Fraction_IsTheTighterOfTheTwoBounds_AndIgnoresDisabledOnes()
    {
        Assert.Equal(0.5, UnflushedBacklogAlertPolicy.Fraction(500, 0, 1000, 0));
        Assert.Equal(0.8, UnflushedBacklogAlertPolicy.Fraction(500, 800, 1000, 1000));
        Assert.Equal(0.5, UnflushedBacklogAlertPolicy.Fraction(500, 800, 1000, 0));
        Assert.Equal(0.0, UnflushedBacklogAlertPolicy.Fraction(500, 800, 0, 0));
        Assert.Equal(1.5, UnflushedBacklogAlertPolicy.Fraction(1500, 0, 1000, 0));
    }

    [Fact]
    public void WarnsAt75Percent_HoldsThroughTheHysteresisBand_ClearsBelow60()
    {
        UnflushedBacklogAlertPolicy policy = new(1000, 0, TimeSpan.FromMinutes(10));
        TimeSpan tick = TimeSpan.FromSeconds(5);
        int i = 0;

        Assert.Equal(UnflushedBacklogAlert.None, policy.Observe(700, 0, false, Ticks(tick * i++)).Alert);
        Assert.Equal(UnflushedBacklogLevel.Normal, policy.Level);

        UnflushedBacklogObservation raised = policy.Observe(760, 0, false, Ticks(tick * i++));
        Assert.Equal(UnflushedBacklogAlert.WarningRaised, raised.Alert);
        Assert.Equal(0.76, raised.Fraction, 3);
        Assert.Equal(UnflushedBacklogLevel.Warning, policy.Level);

        // The saw-tooth: 650 → 720 → 690 sits between the clear and warn lines and must stay quiet and raised.
        Assert.Equal(UnflushedBacklogAlert.None, policy.Observe(650, 0, false, Ticks(tick * i++)).Alert);
        Assert.Equal(UnflushedBacklogAlert.None, policy.Observe(720, 0, false, Ticks(tick * i++)).Alert);
        Assert.Equal(UnflushedBacklogAlert.None, policy.Observe(690, 0, false, Ticks(tick * i++)).Alert);
        Assert.Equal(UnflushedBacklogLevel.Warning, policy.Level);

        UnflushedBacklogObservation cleared = policy.Observe(590, 0, false, Ticks(tick * i++));
        Assert.Equal(UnflushedBacklogAlert.Cleared, cleared.Alert);
        Assert.Equal(UnflushedBacklogLevel.Warning, cleared.From);
        Assert.Equal(UnflushedBacklogLevel.Normal, policy.Level);

        // Under the warn line from normal: nothing, even inside the band.
        Assert.Equal(UnflushedBacklogAlert.None, policy.Observe(700, 0, false, Ticks(tick * i)).Alert);
        Assert.Equal(UnflushedBacklogLevel.Normal, policy.Level);
    }

    [Fact]
    public void GateClosing_LogsOnceThenReminds_OpeningLogsAgain()
    {
        UnflushedBacklogAlertPolicy policy = new(1000, 0, TimeSpan.FromMinutes(10));

        Assert.Equal(UnflushedBacklogAlert.WarningRaised, policy.Observe(800, 0, false, Ticks(TimeSpan.Zero)).Alert);

        UnflushedBacklogObservation closed = policy.Observe(1_050, 0, gateClosed: true, Ticks(TimeSpan.FromSeconds(5)));
        Assert.Equal(UnflushedBacklogAlert.GateClosed, closed.Alert);
        Assert.Equal(UnflushedBacklogLevel.Warning, closed.From);
        Assert.Equal(UnflushedBacklogLevel.Gated, policy.Level);

        // Still gated: quiet until the reminder interval, then one reminder.
        Assert.Equal(UnflushedBacklogAlert.None, policy.Observe(1_200, 0, true, Ticks(TimeSpan.FromMinutes(5))).Alert);
        Assert.Equal(UnflushedBacklogAlert.GateReminder, policy.Observe(1_200, 0, true, Ticks(TimeSpan.FromMinutes(10.1))).Alert);
        Assert.Equal(UnflushedBacklogAlert.None, policy.Observe(1_200, 0, true, Ticks(TimeSpan.FromMinutes(15))).Alert);
        Assert.Equal(TimeSpan.FromMinutes(15) - TimeSpan.FromSeconds(5), policy.LevelDuration(Ticks(TimeSpan.FromMinutes(15))));

        // The gate opens but the backlog is still in the band: "released", level drops to warning, not normal.
        UnflushedBacklogObservation opened = policy.Observe(900, 0, false, Ticks(TimeSpan.FromMinutes(16)));
        Assert.Equal(UnflushedBacklogAlert.GateOpened, opened.Alert);
        Assert.Equal(UnflushedBacklogLevel.Gated, opened.From);
        Assert.Equal(UnflushedBacklogLevel.Warning, policy.Level);

        // Drains fully: cleared.
        Assert.Equal(UnflushedBacklogAlert.Cleared, policy.Observe(100, 0, false, Ticks(TimeSpan.FromMinutes(17))).Alert);
        Assert.Equal(UnflushedBacklogLevel.Normal, policy.Level);
    }

    [Fact]
    public void GateOpeningStraightToNormal_IsOneClearedLineNamingTheGate()
    {
        UnflushedBacklogAlertPolicy policy = new(1000, 0, TimeSpan.FromMinutes(10));

        Assert.Equal(UnflushedBacklogAlert.GateClosed, policy.Observe(1_500, 0, true, Ticks(TimeSpan.Zero)).Alert);

        UnflushedBacklogObservation cleared = policy.Observe(10, 0, false, Ticks(TimeSpan.FromSeconds(5)));
        Assert.Equal(UnflushedBacklogAlert.Cleared, cleared.Alert);
        Assert.Equal(UnflushedBacklogLevel.Gated, cleared.From);
        Assert.Equal(UnflushedBacklogLevel.Normal, cleared.To);
    }

    [Fact]
    public void WarningReminder_OncePerInterval_WhileTheWarningHolds()
    {
        UnflushedBacklogAlertPolicy policy = new(1000, 0, TimeSpan.FromMinutes(10));
        TimeSpan tick = TimeSpan.FromSeconds(5);

        Assert.Equal(UnflushedBacklogAlert.WarningRaised, policy.Observe(800, 0, false, Ticks(TimeSpan.Zero)).Alert);

        int reminders = 0;
        int lines = 0;
        int samples = (int)(TimeSpan.FromMinutes(45) / tick);
        for (int i = 1; i <= samples; i++)
        {
            UnflushedBacklogAlert alert = policy.Observe(800, 0, false, Ticks(tick * i)).Alert;
            if (alert == UnflushedBacklogAlert.WarningReminder)
                reminders++;
            if (alert != UnflushedBacklogAlert.None)
                lines++;
        }

        Assert.Equal(4, reminders);
        Assert.Equal(4, lines);
    }

    [Fact]
    public void ByteBound_AlertsOnItsOwn_WhenItemsAreFine()
    {
        UnflushedBacklogAlertPolicy policy = new(1_000_000, 1_000, TimeSpan.FromMinutes(10));

        UnflushedBacklogObservation raised = policy.Observe(10, 800, false, Ticks(TimeSpan.Zero));
        Assert.Equal(UnflushedBacklogAlert.WarningRaised, raised.Alert);
        Assert.Equal(0.8, raised.Fraction, 3);
    }
}
