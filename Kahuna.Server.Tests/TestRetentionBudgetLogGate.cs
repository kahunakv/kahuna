using System.Diagnostics;
using Kahuna.Server.KeyValues.Transactions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Being over the durable-2PC resident-metadata budget is the steady state under sustained load (the commit
/// rate times the floor exceeds the budget), so the retention sweep must not log it per tick: the 1.7.8
/// soaks carried one warning per minute per node for 45 minutes. The gate allows one line when a streak
/// starts, one reminder per interval while it lasts, and one line when it ends.
/// </summary>
public sealed class TestRetentionBudgetLogGate
{
    private static long Ticks(TimeSpan at) => (long)(at.TotalSeconds * Stopwatch.Frequency);

    [Fact]
    public void OneLineAtStreakStart_OneReminderPerInterval_OneLineAtEnd()
    {
        RetentionBudgetLogGate gate = new(TimeSpan.FromMinutes(10));
        TimeSpan tick = TimeSpan.FromSeconds(15);

        Assert.Equal(RetentionBudgetLogAction.None, gate.Observe(overBudget: false, Ticks(TimeSpan.Zero)));
        Assert.False(gate.Active);

        Assert.Equal(RetentionBudgetLogAction.StreakStarted, gate.Observe(overBudget: true, Ticks(tick)));
        Assert.True(gate.Active);

        // 45 minutes of over-budget sweeps at a 15-second tick: 180 observations, 4 reminders (at 10, 20, 30, 40 min).
        int reminders = 0;
        int lines = 0;
        for (int i = 2; i <= 180; i++)
        {
            RetentionBudgetLogAction action = gate.Observe(overBudget: true, Ticks(tick * i));
            if (action == RetentionBudgetLogAction.Reminder)
                reminders++;
            if (action != RetentionBudgetLogAction.None)
                lines++;
        }

        Assert.Equal(4, reminders);
        Assert.Equal(4, lines);
        Assert.Equal(tick * 179, gate.StreakDuration(Ticks(tick * 180)));

        Assert.Equal(RetentionBudgetLogAction.StreakEnded, gate.Observe(overBudget: false, Ticks(tick * 181)));
        Assert.False(gate.Active);
        Assert.Equal(TimeSpan.Zero, gate.StreakDuration(Ticks(tick * 181)));
        Assert.Equal(RetentionBudgetLogAction.None, gate.Observe(overBudget: false, Ticks(tick * 182)));

        // A new streak starts its own reminder clock.
        Assert.Equal(RetentionBudgetLogAction.StreakStarted, gate.Observe(overBudget: true, Ticks(tick * 183)));
        Assert.Equal(RetentionBudgetLogAction.None, gate.Observe(overBudget: true, Ticks(tick * 184)));
    }

    [Fact]
    public void ReminderIsRelativeToTheLastLine_NotTheStreakStart()
    {
        RetentionBudgetLogGate gate = new(TimeSpan.FromMinutes(10));

        Assert.Equal(RetentionBudgetLogAction.StreakStarted, gate.Observe(true, Ticks(TimeSpan.Zero)));
        Assert.Equal(RetentionBudgetLogAction.None, gate.Observe(true, Ticks(TimeSpan.FromMinutes(9.9))));
        Assert.Equal(RetentionBudgetLogAction.Reminder, gate.Observe(true, Ticks(TimeSpan.FromMinutes(12))));
        Assert.Equal(RetentionBudgetLogAction.None, gate.Observe(true, Ticks(TimeSpan.FromMinutes(20))));
        Assert.Equal(RetentionBudgetLogAction.Reminder, gate.Observe(true, Ticks(TimeSpan.FromMinutes(22))));
    }
}
