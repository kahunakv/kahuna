using System.Diagnostics.Metrics;

using Kommander.Time;

using Kahuna.Server.KeyValues.Watch;

namespace Kahuna.Server.KeyValues;

/// <summary>What the commit admission window decided for one log entry.</summary>
internal enum CommitAdmissionVerdict
{
    Admitted = 0,

    /// <summary>The commit timestamp is at or below the entry time minus the lag bound.</summary>
    BelowFloor = 1,

    /// <summary>The commit timestamp is at or above the entry time plus <see cref="CommitAdmission.MaxClockLeadMs"/>.</summary>
    AboveBound = 2
}

/// <summary>
/// The commit admission window: the apply-time rule that bounds a new commit's timestamp by the time of the log
/// entry that carries it. Kommander stamps every entry with the leader's clock at propose (<c>RaftLog.Time</c>);
/// the value is the same on every replica and never goes down in log order. An entry with time τ that creates a
/// commit at timestamp C (a prepare, or a write made outside a transaction) is admitted only when
/// <c>τ − lag &lt; C &lt; τ + MaxClockLeadMs</c>, where the lag is the bound the proposer wrote into the entry.
///
/// <para><b>Why.</b> A commit timestamp is minted before its entry reaches the log, on the clock of whichever node
/// minted it, so without a bound a low timestamp can land after higher ones and a high one far ahead of the entry.
/// The lower bound makes a watermark sound: after an entry at τ applies, every later entry has a time at or above
/// τ and admits only commits above <c>τ − lag</c>. The upper bound makes a replay start sound: every commit above a
/// timestamp W comes from an entry whose time is above <c>W − MaxClockLeadMs</c>.</para>
///
/// <para><b>Deterministic.</b> The verdict reads only the entry (its time, its commit timestamp and its lag), so
/// every replica, a restart replay and a snapshot catch-up reach the same verdict without memoizing it. An entry
/// whose lag is zero (written with the window off, or by a producer that copies an existing commit, such as a range
/// move) is always admitted.</para>
/// </summary>
internal static class CommitAdmission
{
    /// <summary>
    /// How far ahead of a log entry's time a commit timestamp may be, and how far ahead of the serving node's clock a
    /// snapshot read may be. One constant for both: a commit further ahead than a snapshot read may be served at
    /// would land above reads that already promised nothing commits there. A fixed constant, not a setting, because
    /// the admission verdict is a replicated transition that every replica must judge with the same bound.
    /// </summary>
    public const int MaxClockLeadMs = 5_000;

    private static readonly Counter<long> Refusals =
        KeyValueWatchMetrics.Meter.CreateCounter<long>(
            "kahuna.kv.commit_admission.refused_total",
            description: "Log entries refused at apply by the commit admission window on this node, by entry kind (prepare, write) and bound (floor, lead).");

    /// <summary>Judges one entry. <paramref name="lagMs"/> is the bound written in the entry; zero or less admits.
    /// An entry with no time (applied outside a log) cannot be judged and is admitted.</summary>
    public static CommitAdmissionVerdict Judge(HLCTimestamp entryTime, HLCTimestamp commitTimestamp, long lagMs)
    {
        if (lagMs <= 0 || entryTime == HLCTimestamp.Zero)
            return CommitAdmissionVerdict.Admitted;

        if (commitTimestamp <= Floor(entryTime, lagMs))
            return CommitAdmissionVerdict.BelowFloor;

        if (commitTimestamp >= entryTime + MaxClockLeadMs)
            return CommitAdmissionVerdict.AboveBound;

        return CommitAdmissionVerdict.Admitted;
    }

    /// <summary>The lowest commit timestamp an entry at <paramref name="entryTime"/> can no longer admit: every
    /// admitted commit is strictly above it.</summary>
    public static HLCTimestamp Floor(HLCTimestamp entryTime, long lagMs) =>
        new(entryTime.N, entryTime.L - lagMs, entryTime.C);

    /// <summary>Counts a refusal at apply. <paramref name="kind"/> is <c>prepare</c> or <c>write</c>.</summary>
    public static void CountRefusal(CommitAdmissionVerdict verdict, string kind) =>
        Refusals.Add(1,
            new KeyValuePair<string, object?>("kind", kind),
            new KeyValuePair<string, object?>("bound", verdict == CommitAdmissionVerdict.BelowFloor ? "floor" : "lead"));
}
