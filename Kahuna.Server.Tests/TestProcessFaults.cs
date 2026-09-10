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
    public void FirstChancePolicy_InstallsOnce()
    {
        // Idempotent: the second and later installs are no-ops, so every node constructed in a process shares
        // one handler rather than stacking one per node.
        ProcessFaults.InstallFirstChancePolicy();
        ProcessFaults.InstallFirstChancePolicy();
    }
}
