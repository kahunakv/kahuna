using System.Diagnostics;
using System.Diagnostics.Metrics;

using BenchmarkDotNet.Attributes;
using Kahuna.Server.Diagnostics;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// What the stage clock adds to one commit call. Each benchmark walks the stages of a one-phase commit, which
/// enters a stage eleven times.
///
/// <para><b>Shape:</b> <see cref="NotTimed"/> has no listener, so no clock is opened. <see cref="Timed"/> has the
/// collector a node keeps for its dashboard on every instrument of the meter, and records the call as one sample
/// with its stages in totals. <see cref="OneSamplePerStage"/> is the alternative, with the same collector: one
/// histogram sample for each stage and one for the call.</para>
/// </summary>
[MemoryDiagnoser]
public class CommitCallTimelineBenchmark : IDisposable
{
    private static readonly CommitCallStage[] Walk =
    [
        CommitCallStage.Session, CommitCallStage.Drain, CommitCallStage.Freeze, CommitCallStage.Preflight,
        CommitCallStage.Encode, CommitCallStage.Attempt, CommitCallStage.Apply, CommitCallStage.Resolution,
        CommitCallStage.Answer, CommitCallStage.Release, CommitCallStage.Answer
    ];

    private static readonly Meter Alternative = new("Kahuna", "1.0");

    private static readonly Histogram<double> AlternativeMs = Alternative.CreateHistogram<double>("benchmark.commit_stage_ms", unit: "ms");

    private static readonly KeyValuePair<string, object?> PathTag = new("path", "one_phase");

    private KeyValuePair<string, object?>[] stageTags = null!;

    private CommitCallTelemetry calls = null!;

    private EngineMetricsCollector? collector;

    // A commit call keeps its clock on the session, so the clock is a heap object there.
    private CommitCallTimeline? open;

    [GlobalSetup(Target = nameof(NotTimed))]
    public void SetupNotTimed()
    {
        calls = new CommitCallTelemetry();

        if (calls.Begin() is not null)
            throw new InvalidOperationException("a listener is on the commit call metrics");
    }

    [GlobalSetup(Targets = new[] { nameof(Timed), nameof(OneSamplePerStage) })]
    public void SetupTimed()
    {
        calls = new CommitCallTelemetry();
        collector = new EngineMetricsCollector();
        stageTags = [.. Enum.GetValues<CommitCallStage>().Select(s => new KeyValuePair<string, object?>("stage", CommitCallTelemetry.StageName(s)))];

        if (calls.Begin() is null)
            throw new InvalidOperationException("no listener is on the commit call metrics");
    }

    [GlobalCleanup]
    public void Dispose()
    {
        collector?.Dispose();
        calls.Dispose();
        GC.SuppressFinalize(this);
    }

    [Benchmark(Baseline = true)]
    public bool NotTimed() => Call();

    [Benchmark]
    public bool Timed() => Call();

    [Benchmark]
    public long OneSamplePerStage()
    {
        long started = Stopwatch.GetTimestamp();
        long entered = started;

        foreach (CommitCallStage stage in Walk)
        {
            long now = Stopwatch.GetTimestamp();
            AlternativeMs.Record(now - entered, stageTags[(int)stage], PathTag);
            entered = now;
        }

        long ended = Stopwatch.GetTimestamp();
        AlternativeMs.Record(ended - started, PathTag);
        return ended;
    }

    private bool Call()
    {
        open = calls.Begin();

        foreach (CommitCallStage stage in Walk)
            open?.Enter(stage);

        if (open is not { } timeline)
            return false;

        timeline.Path = CommitCallPath.OnePhase;
        timeline.Complete(KeyValueResponseType.Committed);
        return true;
    }
}
