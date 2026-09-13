using System.Diagnostics;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>What the retention sweep should log for one observation of the memory budget.</summary>
internal enum RetentionBudgetLogAction
{
    /// <summary>Nothing: either under budget with no streak to close, or inside a streak between reminders.</summary>
    None,

    /// <summary>The first over-budget sweep after being under budget.</summary>
    StreakStarted,

    /// <summary>An over-budget sweep at least the reminder interval after the last line.</summary>
    Reminder,

    /// <summary>The first under-budget sweep after a streak.</summary>
    StreakEnded
}

/// <summary>
/// Decides when a sustained over-budget condition earns a log line. Being over the resident-metadata budget
/// is the steady state at a few thousand commits per second — the commit rate times the floor exceeds the
/// budget, and nothing but a config change alters that — so the sweep must not turn every tick into a
/// warning. The gate allows one line when a streak starts, one reminder per <see cref="ReminderInterval"/>
/// while it continues (carrying current numbers), and one line when it ends; the state in between is left
/// to the gauges. Pure and clock-free (it takes <see cref="Stopwatch"/> timestamps), so the cadence is testable.
/// </summary>
internal sealed class RetentionBudgetLogGate(TimeSpan reminderInterval)
{
    /// <summary>Spacing of reminder lines while a streak continues.</summary>
    public TimeSpan ReminderInterval { get; } = reminderInterval;

    private bool active;

    private long streakStartTicks;

    private long lastLogTicks;

    /// <summary>True while the last observation was over budget.</summary>
    public bool Active => active;

    /// <summary>How long the current streak has lasted at <paramref name="nowTicks"/>; zero outside a streak.</summary>
    public TimeSpan StreakDuration(long nowTicks) => active ? Stopwatch.GetElapsedTime(streakStartTicks, nowTicks) : TimeSpan.Zero;

    public RetentionBudgetLogAction Observe(bool overBudget, long nowTicks)
    {
        if (!overBudget)
        {
            if (!active)
                return RetentionBudgetLogAction.None;

            active = false;
            return RetentionBudgetLogAction.StreakEnded;
        }

        if (!active)
        {
            active = true;
            streakStartTicks = nowTicks;
            lastLogTicks = nowTicks;
            return RetentionBudgetLogAction.StreakStarted;
        }

        if (Stopwatch.GetElapsedTime(lastLogTicks, nowTicks) < ReminderInterval)
            return RetentionBudgetLogAction.None;

        lastLogTicks = nowTicks;
        return RetentionBudgetLogAction.Reminder;
    }
}
