using System.Diagnostics;
using System.Runtime.CompilerServices;

using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// The stage clock of one commit call. Each <see cref="Enter(CommitCallStage)"/> reads the clock once, charges the
/// time since the last reading to the stage the call was in, and moves the call to the next stage. The readings
/// split the call into consecutive parts, so the stages add up to the call exactly. A stage can be entered more
/// than once; its parts add up.
///
/// <para>One call owns a timeline and uses it from one point of execution at a time, so it has no lock. It exists
/// only when a listener reads the commit call metrics (<see cref="CommitCallTelemetry.Begin"/>).</para>
/// </summary>
internal sealed class CommitCallTimeline
{
    [InlineArray(CommitCallTelemetry.StageCount)]
    private struct StageTicks
    {
        private long first;
    }

    private readonly CommitCallTelemetry telemetry;

    private readonly long startedAt;

    private long enteredAt;

    private CommitCallStage current = CommitCallStage.Route;

    private StageTicks stages;

    internal CommitCallTimeline(CommitCallTelemetry telemetry, long startedAt)
    {
        this.telemetry = telemetry;
        this.startedAt = startedAt;
        enteredAt = startedAt;
    }

    /// <summary>How the call is being served. The last value set before <see cref="Complete(KeyValueResponseType)"/>
    /// is the one recorded.</summary>
    internal CommitCallPath Path { get; set; }

    /// <summary>Ends the current stage and moves the call to <paramref name="stage"/>.</summary>
    internal void Enter(CommitCallStage stage) => Enter(stage, Stopwatch.GetTimestamp());

    /// <summary>As <see cref="Enter(CommitCallStage)"/>, at a <see cref="Stopwatch"/> reading the caller took.</summary>
    internal void Enter(CommitCallStage stage, long now)
    {
        Charge(now);
        current = stage;

        telemetry.TestStageEntered?.Invoke(stage);
    }

    /// <summary>Ends the current stage and records the call. The timeline is not used after this.</summary>
    internal void Complete(KeyValueResponseType answer) => Complete(answer, Stopwatch.GetTimestamp());

    /// <summary>As <see cref="Complete(KeyValueResponseType)"/>, at a <see cref="Stopwatch"/> reading the caller took.</summary>
    internal void Complete(KeyValueResponseType answer, long now)
    {
        Charge(now);
        telemetry.Record(Path, answer, enteredAt - startedAt, stages);
    }

    private void Charge(long now)
    {
        // A reading below the last one charges nothing and does not move the clock back.
        if (now <= enteredAt)
            return;

        stages[(int)current] += now - enteredAt;
        enteredAt = now;
    }
}
