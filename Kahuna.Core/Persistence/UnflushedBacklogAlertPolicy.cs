using System.Diagnostics;

namespace Kahuna.Server.Persistence;

/// <summary>Where the unflushed backlog sits against its budget.</summary>
internal enum UnflushedBacklogLevel
{
    /// <summary>Below the warning line (or back below the clear line after a warning).</summary>
    Normal,

    /// <summary>At or above <see cref="UnflushedBacklogAlertPolicy.WarnFraction"/> of a budget; the gate is still open.</summary>
    Warning,

    /// <summary>Over a budget: the write aggregator refuses ordinary writes on this node.</summary>
    Gated
}

/// <summary>What one observation of the backlog earns in the log.</summary>
internal enum UnflushedBacklogAlert
{
    None,

    /// <summary>Crossed the warning line from normal.</summary>
    WarningRaised,

    /// <summary>Still in warning, a reminder interval after the last line.</summary>
    WarningReminder,

    /// <summary>Back-pressure engaged: a budget is exceeded and ordinary writes are being refused.</summary>
    GateClosed,

    /// <summary>Still gated, a reminder interval after the last line.</summary>
    GateReminder,

    /// <summary>The gate reopened but the backlog is still above the clear line.</summary>
    GateOpened,

    /// <summary>Back below the clear line after a warning or a gate.</summary>
    Cleared
}

/// <summary>One observation's outcome: the alert to log (if any) and the levels it moved between.</summary>
internal readonly record struct UnflushedBacklogObservation(UnflushedBacklogAlert Alert, UnflushedBacklogLevel From, UnflushedBacklogLevel To, double Fraction);

/// <summary>
/// Turns backlog samples into operator alerts with hysteresis and rate limiting. The gauges show the backlog
/// continuously, but nobody watches a gauge: an operator needs a line when the backlog is heading for the
/// budget (<see cref="WarnFraction"/> of either bound), a line when back-pressure actually engages (a bound
/// exceeded, ordinary writes refused), and a line when it clears — not a line per sample while a saw-toothing
/// follower lag hovers near the line. Hysteresis (<see cref="ClearFraction"/>) keeps a backlog oscillating
/// around the warning line from raising and clearing every sample; the reminder interval keeps a sustained
/// condition visible without flooding. Pure and clock-free (it takes <see cref="Stopwatch"/> timestamps).
/// </summary>
internal sealed class UnflushedBacklogAlertPolicy(long maxItems, long maxBytes, TimeSpan reminderInterval)
{
    /// <summary>Fraction of a budget at which the warning is raised.</summary>
    public const double WarnFraction = 0.75;

    /// <summary>Fraction of a budget below which a raised warning (or an opened gate) clears.</summary>
    public const double ClearFraction = 0.60;

    public long MaxItems { get; } = maxItems;

    public long MaxBytes { get; } = maxBytes;

    public TimeSpan ReminderInterval { get; } = reminderInterval;

    public UnflushedBacklogLevel Level { get; private set; }

    private long lastLogTicks;

    private long levelSinceTicks;

    /// <summary>How long the current level has held at <paramref name="nowTicks"/>.</summary>
    public TimeSpan LevelDuration(long nowTicks) => Stopwatch.GetElapsedTime(levelSinceTicks, nowTicks);

    /// <summary>The backlog as a fraction of the tighter budget: the larger of items ÷ max items and bytes ÷ max
    /// bytes, each term ignored when its bound is disabled (≤ 0). 0 when both bounds are disabled.</summary>
    public static double Fraction(long items, long bytes, long maxItems, long maxBytes)
    {
        double byItems = maxItems > 0 ? (double)Math.Max(0, items) / maxItems : 0;
        double byBytes = maxBytes > 0 ? (double)Math.Max(0, bytes) / maxBytes : 0;
        return Math.Max(byItems, byBytes);
    }

    public UnflushedBacklogObservation Observe(long items, long bytes, bool gateClosed, long nowTicks)
    {
        double fraction = Fraction(items, bytes, MaxItems, MaxBytes);
        UnflushedBacklogLevel from = Level;

        UnflushedBacklogLevel to;
        if (gateClosed)
            to = UnflushedBacklogLevel.Gated;
        else if (fraction >= WarnFraction)
            to = UnflushedBacklogLevel.Warning;
        else if (from != UnflushedBacklogLevel.Normal && fraction >= ClearFraction)
            to = UnflushedBacklogLevel.Warning; // inside the hysteresis band: stay raised until it really drains
        else
            to = UnflushedBacklogLevel.Normal;

        UnflushedBacklogAlert alert;

        if (to != from)
        {
            Level = to;
            levelSinceTicks = nowTicks;
            lastLogTicks = nowTicks;

            alert = to switch
            {
                UnflushedBacklogLevel.Gated => UnflushedBacklogAlert.GateClosed,
                UnflushedBacklogLevel.Warning => from == UnflushedBacklogLevel.Gated ? UnflushedBacklogAlert.GateOpened : UnflushedBacklogAlert.WarningRaised,
                _ => UnflushedBacklogAlert.Cleared
            };
        }
        else if (to != UnflushedBacklogLevel.Normal && Stopwatch.GetElapsedTime(lastLogTicks, nowTicks) >= ReminderInterval)
        {
            lastLogTicks = nowTicks;
            alert = to == UnflushedBacklogLevel.Gated ? UnflushedBacklogAlert.GateReminder : UnflushedBacklogAlert.WarningReminder;
        }
        else
            alert = UnflushedBacklogAlert.None;

        return new UnflushedBacklogObservation(alert, from, to, fraction);
    }
}
