using System.Diagnostics;
using System.Diagnostics.Metrics;

using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// The stage clock of a commit call and the totals it feeds: the stages split a call into consecutive parts that
/// add up to it, a call is one histogram sample whatever its stages are, and the stage totals are read only when a
/// listener collects.
/// </summary>
public sealed class TestCommitCallTimeline
{
    private const string CallMetric = "kahuna.transactions.commit_call_ms";

    private const string StageMetric = "kahuna.transactions.commit_stage_ms";

    private static double Ms(long ticks) => ticks * 1000.0 / Stopwatch.Frequency;

    private static double TotalMs(CommitCallTelemetry calls, CommitCallPath path)
    {
        double total = 0;

        foreach (CommitCallStage stage in Enum.GetValues<CommitCallStage>())
            total += calls.StageMilliseconds(path, stage);

        return total;
    }

    /// <summary>The totals are indexed by the enum values, so the two counts must cover every value, and each value
    /// must have a tag of its own.</summary>
    [Fact]
    public void EveryStageAndPath_HasOneIndexAndOneTag()
    {
        CommitCallStage[] stages = Enum.GetValues<CommitCallStage>();
        CommitCallPath[] paths = Enum.GetValues<CommitCallPath>();

        Assert.Equal(CommitCallTelemetry.StageCount, stages.Length);
        Assert.Equal(CommitCallTelemetry.PathCount, paths.Length);

        for (int i = 0; i < stages.Length; i++)
            Assert.Equal(i, (int)stages[i]);

        for (int i = 0; i < paths.Length; i++)
            Assert.Equal(i, (int)paths[i]);

        Assert.Equal(stages.Length, stages.Select(CommitCallTelemetry.StageName).Distinct().Count());
        Assert.Equal(paths.Length, paths.Select(CommitCallTelemetry.PathName).Distinct().Count());
    }

    /// <summary>Each reading of the clock charges the time since the last one to the stage the call was in, and the
    /// parts add up to the call.</summary>
    [Fact]
    public void Stages_AddUpToTheCall()
    {
        using CommitCallTelemetry calls = new();

        CommitCallTimeline timeline = new(calls, startedAt: 1_000) { Path = CommitCallPath.OnePhase };
        timeline.Enter(CommitCallStage.Session, 1_010);
        timeline.Enter(CommitCallStage.Drain, 1_030);
        timeline.Enter(CommitCallStage.Freeze, 1_070);
        timeline.Enter(CommitCallStage.Attempt, 1_150);
        timeline.Enter(CommitCallStage.Answer, 2_150);
        timeline.Complete(KeyValueResponseType.Committed, 2_157);

        Assert.Equal(1, calls.Calls(CommitCallPath.OnePhase));
        Assert.Equal(Ms(10), calls.StageMilliseconds(CommitCallPath.OnePhase, CommitCallStage.Route), 9);
        Assert.Equal(Ms(20), calls.StageMilliseconds(CommitCallPath.OnePhase, CommitCallStage.Session), 9);
        Assert.Equal(Ms(40), calls.StageMilliseconds(CommitCallPath.OnePhase, CommitCallStage.Drain), 9);
        Assert.Equal(Ms(80), calls.StageMilliseconds(CommitCallPath.OnePhase, CommitCallStage.Freeze), 9);
        Assert.Equal(Ms(1_000), calls.StageMilliseconds(CommitCallPath.OnePhase, CommitCallStage.Attempt), 9);
        Assert.Equal(Ms(7), calls.StageMilliseconds(CommitCallPath.OnePhase, CommitCallStage.Answer), 9);
        Assert.Equal(0, calls.StageMilliseconds(CommitCallPath.OnePhase, CommitCallStage.Prepare));
        Assert.Equal(Ms(1_157), TotalMs(calls, CommitCallPath.OnePhase), 9);
    }

    /// <summary>A stage that the call enters again adds to its earlier part, as a finalize that freezes its input a
    /// second time does.</summary>
    [Fact]
    public void AStageEnteredAgain_AddsToItsEarlierPart()
    {
        using CommitCallTelemetry calls = new();

        CommitCallTimeline timeline = new(calls, startedAt: 0) { Path = CommitCallPath.OnePhase };
        timeline.Enter(CommitCallStage.Freeze, 5);
        timeline.Enter(CommitCallStage.Attempt, 15);
        timeline.Enter(CommitCallStage.Freeze, 115);
        timeline.Enter(CommitCallStage.Attempt, 135);
        timeline.Complete(KeyValueResponseType.Committed, 335);

        Assert.Equal(Ms(30), calls.StageMilliseconds(CommitCallPath.OnePhase, CommitCallStage.Freeze), 9);
        Assert.Equal(Ms(300), calls.StageMilliseconds(CommitCallPath.OnePhase, CommitCallStage.Attempt), 9);
        Assert.Equal(Ms(335), TotalMs(calls, CommitCallPath.OnePhase), 9);
    }

    /// <summary>The path of a call is the last one set before it completes, and the whole call is recorded under it.</summary>
    [Fact]
    public void ACallThatChangesPath_IsRecordedUnderItsLastPath()
    {
        using CommitCallTelemetry calls = new();

        CommitCallTimeline timeline = new(calls, startedAt: 0) { Path = CommitCallPath.OnePhase };
        timeline.Enter(CommitCallStage.Attempt, 10);
        timeline.Path = CommitCallPath.TwoPhase;
        timeline.Enter(CommitCallStage.Prepare, 60);
        timeline.Complete(KeyValueResponseType.Committed, 100);

        Assert.Equal(0, calls.Calls(CommitCallPath.OnePhase));
        Assert.Equal(0, TotalMs(calls, CommitCallPath.OnePhase));
        Assert.Equal(1, calls.Calls(CommitCallPath.TwoPhase));
        Assert.Equal(Ms(50), calls.StageMilliseconds(CommitCallPath.TwoPhase, CommitCallStage.Attempt), 9);
        Assert.Equal(Ms(100), TotalMs(calls, CommitCallPath.TwoPhase), 9);
    }

    /// <summary>A clock reading below the last one charges nothing and does not move the clock back, so no stage is
    /// negative and the stages still add up to the call.</summary>
    [Fact]
    public void AClockReadingBelowTheLast_ChargesNothing()
    {
        using CommitCallTelemetry calls = new();

        CommitCallTimeline timeline = new(calls, startedAt: 100) { Path = CommitCallPath.OnePhase };
        timeline.Enter(CommitCallStage.Session, 150);
        timeline.Enter(CommitCallStage.Drain, 120);
        timeline.Complete(KeyValueResponseType.Committed, 160);

        Assert.Equal(Ms(50), calls.StageMilliseconds(CommitCallPath.OnePhase, CommitCallStage.Route), 9);
        Assert.Equal(0, calls.StageMilliseconds(CommitCallPath.OnePhase, CommitCallStage.Session));
        Assert.Equal(Ms(10), calls.StageMilliseconds(CommitCallPath.OnePhase, CommitCallStage.Drain), 9);
        Assert.Equal(Ms(60), TotalMs(calls, CommitCallPath.OnePhase), 9);
    }

    /// <summary>A call is one histogram sample with the path and the outcome it ended with.</summary>
    [Theory]
    [InlineData(KeyValueResponseType.Committed, "committed")]
    [InlineData(KeyValueResponseType.Aborted, "aborted")]
    [InlineData(KeyValueResponseType.RolledBack, "rolled_back")]
    [InlineData(KeyValueResponseType.MustRetry, "must_retry")]
    [InlineData(KeyValueResponseType.Errored, "errored")]
    [InlineData(KeyValueResponseType.InvalidInput, "errored")]
    public void ACall_IsOneSampleTaggedByPathAndOutcome(KeyValueResponseType answer, string expectedOutcome)
    {
        using CommitCallTelemetry calls = new();
        using MetricCapture outcomes = new("outcome", CallMetric);
        using MetricCapture paths = new("path", CallMetric);

        // The histogram is shared by the process, so the sample is told apart by its value.
        long ticks = 7_654_321_000 + (int)answer;
        CommitCallTimeline timeline = new(calls, startedAt: 0) { Path = CommitCallPath.Mirrored };
        timeline.Enter(CommitCallStage.Session, 17);
        timeline.Complete(answer, ticks);

        Assert.Equal(Ms(ticks), Assert.Single(outcomes.Samples(CallMetric, expectedOutcome), v => Math.Abs(v - Ms(ticks)) < 1e-6), 6);
        Assert.Equal(Ms(ticks), Assert.Single(paths.Samples(CallMetric, "mirrored"), v => Math.Abs(v - Ms(ticks)) < 1e-6), 6);
    }

    /// <summary>A collection reports the total of each stage and path that calls have used, and no others.</summary>
    [Fact]
    public void ACollection_ReportsTheStageTotals()
    {
        using CommitCallTelemetry calls = new();

        const long attemptTicks = 8_111_222_333;
        const long releaseTicks = 8_222_333_444;

        for (int i = 0; i < 2; i++)
        {
            CommitCallTimeline timeline = new(calls, startedAt: 0) { Path = CommitCallPath.OnePhase };
            timeline.Enter(CommitCallStage.Attempt, 0);
            timeline.Enter(CommitCallStage.Release, attemptTicks);
            timeline.Complete(KeyValueResponseType.Committed, attemptTicks + releaseTicks);
        }

        // Every node of the process publishes an instrument of this name, so this node's values are told apart by
        // their size.
        List<(string Stage, string Path, double Value)> reported = [];
        using MeterListener listener = new();
        listener.InstrumentPublished = static (instrument, l) =>
        {
            if (instrument.Meter.Name == "Kahuna" && instrument.Name == StageMetric)
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<double>((_, value, tags, _) =>
        {
            string stage = "", path = "";
            foreach (KeyValuePair<string, object?> tag in tags)
            {
                if (tag.Key == "stage")
                    stage = (string)tag.Value!;
                else if (tag.Key == "path")
                    path = (string)tag.Value!;
            }

            reported.Add((stage, path, value));
        });
        listener.Start();
        listener.RecordObservableInstruments();

        Assert.Single(reported, r => r is { Stage: "attempt", Path: "one_phase" } && Math.Abs(r.Value - Ms(2 * attemptTicks)) < 1e-6);
        Assert.Single(reported, r => r is { Stage: "release", Path: "one_phase" } && Math.Abs(r.Value - Ms(2 * releaseTicks)) < 1e-6);
        Assert.All(reported, r => Assert.True(r.Value > 0));

        // A node that ended reports nothing.
        reported.Clear();
        calls.Dispose();
        listener.RecordObservableInstruments();
        Assert.DoesNotContain(reported, r => Math.Abs(r.Value - Ms(2 * attemptTicks)) < 1e-6);
    }

    /// <summary>With a listener on the call histogram, a call that enters the node gets a clock.</summary>
    [Fact]
    public void Begin_WithAListener_OpensAClock()
    {
        using CommitCallTelemetry calls = new();
        using MetricCapture capture = new("path", CallMetric);

        CommitCallTimeline? timeline = calls.Begin();

        Assert.NotNull(timeline);
        timeline.Complete(KeyValueResponseType.Committed);
        Assert.Equal(1, calls.Calls(CommitCallPath.None));
    }

    /// <summary>Calls that end at the same time on several threads lose no tick and no count.</summary>
    [Fact]
    public void ConcurrentCalls_LoseNothing()
    {
        using CommitCallTelemetry calls = new();

        const int threads = 8;
        const int perThread = 20_000;

        Parallel.For(0, threads, new ParallelOptions { MaxDegreeOfParallelism = threads }, _ =>
        {
            for (int i = 0; i < perThread; i++)
            {
                CommitCallTimeline timeline = new(calls, startedAt: 0) { Path = CommitCallPath.TwoPhase };
                timeline.Enter(CommitCallStage.Prepare, 1);
                timeline.Enter(CommitCallStage.Decision, 4);
                timeline.Complete(KeyValueResponseType.Committed, 9);
            }
        });

        const long total = threads * perThread;
        Assert.Equal(total, calls.Calls(CommitCallPath.TwoPhase));
        Assert.Equal(Ms(total * 1), calls.StageMilliseconds(CommitCallPath.TwoPhase, CommitCallStage.Route), 6);
        Assert.Equal(Ms(total * 3), calls.StageMilliseconds(CommitCallPath.TwoPhase, CommitCallStage.Prepare), 6);
        Assert.Equal(Ms(total * 5), calls.StageMilliseconds(CommitCallPath.TwoPhase, CommitCallStage.Decision), 6);
    }
}
