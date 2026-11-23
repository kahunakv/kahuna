using System.Diagnostics;
using System.Diagnostics.Metrics;

using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Times the commit calls of one node, from the entry of the call to its answer, and splits that time into stages.
///
/// <para>A call records one sample on <see cref="CallMs"/>. Its stages record no sample: they add to totals that
/// <c>kahuna.transactions.commit_stage_ms</c> reports when a listener collects, so a call costs one listener
/// callback whatever the number of stages. The stages of a call add up to the call, so for one path the rate of
/// the stage totals, summed over <c>stage</c>, equals the rate of the sum of <see cref="CallMs"/>, summed over
/// <c>outcome</c>. A stage total divided by the count of <see cref="CallMs"/> for its path is the mean of the stage.
/// A distribution for each stage is not recorded.</para>
///
/// <para>The totals belong to the node and not to the process, so in an in-process cluster each node has its own.
/// With no listener on either instrument a call reads two fields and nothing else.</para>
/// </summary>
internal sealed class CommitCallTelemetry : IDisposable
{
    /// <summary>The number of <see cref="CommitCallStage"/> values.</summary>
    internal const int StageCount = (int)CommitCallStage.Answer + 1;

    /// <summary>The number of <see cref="CommitCallPath"/> values.</summary>
    internal const int PathCount = (int)CommitCallPath.Forwarded + 1;

    /// <summary>
    /// Wall time of one commit call of an interactive transaction, tagged by <c>path</c> (<c>one_phase</c>,
    /// <c>two_phase</c>, <c>ephemeral</c>, <c>read_only</c>, <c>mirrored</c>, <c>replayed</c>, <c>forwarded</c>,
    /// <c>none</c>) and by <c>outcome</c> (<c>committed</c>, <c>aborted</c>, <c>rolled_back</c>, <c>must_retry</c>,
    /// <c>errored</c>). One sample for each call that enters the node. A call that another node serves is recorded
    /// on both nodes, under <c>path=forwarded</c> on the node that forwarded it. The tag set is fixed. A script
    /// commits inside its own execution and records nothing here.
    /// </summary>
    internal static readonly Histogram<double> CallMs =
        DurableTransactionMetrics.Meter.CreateHistogram<double>(
            "kahuna.transactions.commit_call_ms", unit: "ms",
            description: "Commit call wall time of interactive transactions, from entry to answer, tagged by path and outcome.");

    private static readonly double MsPerTick = 1000.0 / Stopwatch.Frequency;

    private static readonly KeyValuePair<string, object?>[] StageTags =
        Tags<CommitCallStage>("stage", StageCount, StageName);

    private static readonly KeyValuePair<string, object?>[] PathTags =
        Tags<CommitCallPath>("path", PathCount, PathName);

    private static readonly KeyValuePair<string, object?> OutcomeCommitted = new("outcome", "committed");
    private static readonly KeyValuePair<string, object?> OutcomeAborted = new("outcome", "aborted");
    private static readonly KeyValuePair<string, object?> OutcomeRolledBack = new("outcome", "rolled_back");
    private static readonly KeyValuePair<string, object?> OutcomeMustRetry = new("outcome", "must_retry");
    private static readonly KeyValuePair<string, object?> OutcomeErrored = new("outcome", "errored");

    // Instance-owned: the counter's callback captures this node's totals, so the instrument must end with the node.
    private readonly Meter meter = new("Kahuna", "1.0");

    /// <summary>
    /// Time the commit calls spent in each stage since the node started, tagged by <c>stage</c> and <c>path</c>. A
    /// pair that no call has used is not reported.
    /// </summary>
    private readonly ObservableCounter<double> stageMs;

    /// <summary>Test-only: observes each stage a call of this node enters, in order.</summary>
    internal Action<CommitCallStage>? TestStageEntered;

    private readonly long[] calls = new long[PathCount];

    // One row of StageCount totals for each path. A call adds to one row.
    private readonly long[] stageTicks = new long[PathCount * StageCount];

    internal CommitCallTelemetry()
    {
        stageMs = meter.CreateObservableCounter(
            "kahuna.transactions.commit_stage_ms", ObserveStages, unit: "ms",
            description: "Time commit calls of interactive transactions spent in each stage, tagged by stage and path; the stages of a path sum to commit_call_ms.");
    }

    /// <summary>Opens the stage clock of a commit call that enters the node now, or returns null when no listener
    /// reads the commit call metrics.</summary>
    internal CommitCallTimeline? Begin() =>
        CallMs.Enabled || stageMs.Enabled ? new CommitCallTimeline(this, Stopwatch.GetTimestamp()) : null;

    /// <summary>Records one finished call. <paramref name="stages"/> holds its ticks by stage and
    /// <paramref name="totalTicks"/> is their sum.</summary>
    internal void Record(CommitCallPath path, KeyValueResponseType answer, long totalTicks, ReadOnlySpan<long> stages)
    {
        int row = (int)path * StageCount;

        for (int i = 0; i < stages.Length; i++)
        {
            if (stages[i] != 0)
                Interlocked.Add(ref stageTicks[row + i], stages[i]);
        }

        Interlocked.Increment(ref calls[(int)path]);

        CallMs.Record(totalTicks * MsPerTick, PathTags[(int)path], OutcomeTag(answer));
    }

    /// <summary>The calls this node recorded under <paramref name="path"/>.</summary>
    internal long Calls(CommitCallPath path) => Interlocked.Read(ref calls[(int)path]);

    /// <summary>The milliseconds this node's calls spent in <paramref name="stage"/> under <paramref name="path"/>.</summary>
    internal double StageMilliseconds(CommitCallPath path, CommitCallStage stage) =>
        Interlocked.Read(ref stageTicks[(int)path * StageCount + (int)stage]) * MsPerTick;

    /// <summary>The <c>stage</c> tag value of a stage.</summary>
    internal static string StageName(CommitCallStage stage) => stage switch
    {
        CommitCallStage.Route => "route",
        CommitCallStage.Session => "session",
        CommitCallStage.Drain => "drain",
        CommitCallStage.Freeze => "freeze",
        CommitCallStage.Preflight => "preflight",
        CommitCallStage.Encode => "encode",
        CommitCallStage.Attempt => "attempt",
        CommitCallStage.Apply => "apply",
        CommitCallStage.Prepare => "prepare",
        CommitCallStage.Validate => "validate",
        CommitCallStage.Decision => "decision",
        CommitCallStage.Resolution => "resolution",
        CommitCallStage.Ephemeral => "ephemeral",
        CommitCallStage.Release => "release",
        _ => "answer"
    };

    /// <summary>The <c>path</c> tag value of a path.</summary>
    internal static string PathName(CommitCallPath path) => path switch
    {
        CommitCallPath.OnePhase => "one_phase",
        CommitCallPath.TwoPhase => "two_phase",
        CommitCallPath.Ephemeral => "ephemeral",
        CommitCallPath.ReadOnly => "read_only",
        CommitCallPath.Mirrored => "mirrored",
        CommitCallPath.Replayed => "replayed",
        CommitCallPath.Forwarded => "forwarded",
        _ => "none"
    };

    private static KeyValuePair<string, object?> OutcomeTag(KeyValueResponseType answer) => answer switch
    {
        KeyValueResponseType.Committed => OutcomeCommitted,
        KeyValueResponseType.Aborted => OutcomeAborted,
        KeyValueResponseType.RolledBack => OutcomeRolledBack,
        KeyValueResponseType.MustRetry => OutcomeMustRetry,
        _ => OutcomeErrored
    };

    private static KeyValuePair<string, object?>[] Tags<TValue>(string key, int count, Func<TValue, string> name)
        where TValue : struct, Enum
    {
        KeyValuePair<string, object?>[] tags = new KeyValuePair<string, object?>[count];
        TValue[] values = Enum.GetValues<TValue>();

        for (int i = 0; i < count; i++)
            tags[i] = new(key, name(values[i]));

        return tags;
    }

    private List<Measurement<double>> ObserveStages()
    {
        List<Measurement<double>> measurements = [];

        for (int path = 0; path < PathCount; path++)
        {
            for (int stage = 0; stage < StageCount; stage++)
            {
                long ticks = Interlocked.Read(ref stageTicks[path * StageCount + stage]);

                if (ticks != 0)
                    measurements.Add(new(ticks * MsPerTick, StageTags[stage], PathTags[path]));
            }
        }

        return measurements;
    }

    public void Dispose() => meter.Dispose();
}
