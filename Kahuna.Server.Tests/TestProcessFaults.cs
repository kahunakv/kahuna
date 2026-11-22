using Kahuna.Server.Diagnostics;

namespace Kahuna.Server.Tests;

/// <summary>
/// The process-fault policy behind the zombie-follower fix: an out-of-memory raised inside the WAL/apply
/// pipeline must terminate the process (or, with fail-fast disabled, propagate and mark the node unhealthy),
/// never be logged and swallowed like a malformed entry. The decision is tested through its pure form — a real
/// <see cref="OutOfMemoryException"/> must never be thrown in a test host that has the first-chance policy
/// installed, which every embedded node in this suite installs.
/// </summary>
public sealed class TestProcessFaults
{
    [Fact]
    public void OrdinaryExceptions_AreSurvivable()
    {
        Assert.False(ProcessFaults.IsFatal(new InvalidOperationException()));
        Assert.Equal(ProcessFaultDisposition.Survivable, ProcessFaults.Classify(new InvalidOperationException(), failFast: true));
        Assert.Equal(ProcessFaultDisposition.Survivable, ProcessFaults.Classify(new IOException(), failFast: false));

        // The apply-site filter admits them to the catch, whatever the fail-fast setting.
        Assert.True(ProcessFaults.Survivable(new InvalidOperationException(), "test"));
        Assert.Null(ProcessFaults.FatalFaultObserved);
    }

    [Fact]
    public void OutOfMemory_IsFatal_TerminatesWhenFailFastIsOn_PropagatesAndMarksUnhealthyWhenOff()
    {
        OutOfMemoryException oom = new();
        InsufficientMemoryException insufficient = new();

        Assert.True(ProcessFaults.IsFatal(oom));
        Assert.True(ProcessFaults.IsFatal(insufficient)); // derives from OutOfMemoryException

        Assert.Equal(ProcessFaultDisposition.FailFast, ProcessFaults.Classify(oom, failFast: true));
        Assert.Equal(ProcessFaultDisposition.PropagateAndMarkUnhealthy, ProcessFaults.Classify(oom, failFast: false));
        Assert.Equal(ProcessFaultDisposition.PropagateAndMarkUnhealthy, ProcessFaults.Classify(insufficient, failFast: false));
    }

    [Fact]
    public void OutOfMemory_InsideRecoverableWork_FailsThatWorkOnly()
    {
        Assert.Equal(ProcessFaultDisposition.RecoverableWork, ProcessFaults.Classify(new OutOfMemoryException(), failFast: true, recoverableWork: true));
        Assert.Equal(ProcessFaultDisposition.RecoverableWork, ProcessFaults.Classify(new OutOfMemoryException(), failFast: false, recoverableWork: true));
        Assert.Equal(ProcessFaultDisposition.Survivable, ProcessFaults.Classify(new IOException(), failFast: true, recoverableWork: true));

        Assert.False(ProcessFaults.InRecoverableWork);

        // The work completes and reports its own result when nothing ran out of memory.
        Assert.True(ProcessFaults.RunRecoverable(0, static _ => ProcessFaults.InRecoverableWork, out bool outOfMemory));
        Assert.False(outOfMemory);
        Assert.False(ProcessFaults.RunRecoverable(0, static _ => false, out outOfMemory));
        Assert.False(outOfMemory);

        // The first-chance observer sees an out-of-memory raised inside the work before any catch does. It
        // notes the fault for the caller and does not escalate; the work is reported as failed although its
        // own catch handled the exception and it returned true. Observed through the form that cannot end
        // the test host.
        ProcessFaultDisposition observed = default;
        bool completed = ProcessFaults.RunRecoverable(
            0,
            _ =>
            {
                observed = ProcessFaults.Observe(new OutOfMemoryException(), terminate: false);
                return true;
            },
            out outOfMemory);

        Assert.Equal(ProcessFaultDisposition.RecoverableWork, observed);
        Assert.True(outOfMemory);
        Assert.False(completed);

        // The exemption ends with the work: the same observation outside it is fatal again, and nothing of
        // the last call leaks into the next one.
        Assert.False(ProcessFaults.InRecoverableWork);
        Assert.NotEqual(ProcessFaultDisposition.RecoverableWork, ProcessFaults.Observe(new OutOfMemoryException(), terminate: false));
        Assert.Equal(ProcessFaultDisposition.Survivable, ProcessFaults.Observe(new IOException(), terminate: false));

        Assert.True(ProcessFaults.RunRecoverable(0, static _ => true, out outOfMemory));
        Assert.False(outOfMemory);
    }

    [Fact]
    public void RecoverableWork_Nested_ReportsAnInnerFaultToTheOuterCall()
    {
        bool innerOutOfMemory = false;
        bool innerCompleted = true;

        bool outerCompleted = ProcessFaults.RunRecoverable(
            0,
            _ =>
            {
                innerCompleted = ProcessFaults.RunRecoverable(
                    0,
                    static _ =>
                    {
                        ProcessFaults.Observe(new OutOfMemoryException(), terminate: false);
                        return true;
                    },
                    out innerOutOfMemory);

                // Still inside the outer work after the inner one returned.
                return ProcessFaults.InRecoverableWork;
            },
            out bool outerOutOfMemory);

        Assert.True(innerOutOfMemory);
        Assert.False(innerCompleted);
        Assert.True(outerOutOfMemory);
        Assert.False(outerCompleted);
        Assert.False(ProcessFaults.InRecoverableWork);
    }

    [Fact]
    public void FirstChancePolicy_InstallsOnce()
    {
        // Idempotent: the second and later installs are no-ops, so every node constructed in a process shares
        // one handler rather than stacking one per node.
        ProcessFaults.InstallFirstChancePolicy();
        ProcessFaults.InstallFirstChancePolicy();
    }
}
