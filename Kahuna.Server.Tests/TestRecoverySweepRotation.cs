using Kahuna.Server.KeyValues.Transactions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The rotation that keeps the capped prepared-intent recovery sweep fair across passes. Without
/// it, the sweep took the first cap partitions in store-enumeration order on every pass, so a
/// persistent backlog on the early partitions starved the later ones out of recovery entirely —
/// and an orphan intent on a starved partition crossed the record-retention horizon un-aborted,
/// after which nothing could ever resolve it.
/// </summary>
public sealed class TestRecoverySweepRotation
{
    [Fact]
    public void UnderTheCapEverythingIsTaken()
    {
        List<int> all = [1, 2, 3];
        Assert.Equal(all, DurableMaintenanceService.SelectRotated(all, resumeAfter: -1, cap: 5));
        Assert.Equal(all, DurableMaintenanceService.SelectRotated(all, resumeAfter: 2, cap: 0));
    }

    [Fact]
    public void ConsecutivePassesCoverEveryPartition()
    {
        List<int> sorted = [1, 2, 3, 4, 5];

        List<int> first = DurableMaintenanceService.SelectRotated(sorted, resumeAfter: -1, cap: 2);
        Assert.Equal([1, 2], first);

        List<int> second = DurableMaintenanceService.SelectRotated(sorted, resumeAfter: first[^1], cap: 2);
        Assert.Equal([3, 4], second);

        List<int> third = DurableMaintenanceService.SelectRotated(sorted, resumeAfter: second[^1], cap: 2);
        Assert.Equal([5, 1], third);
    }

    [Fact]
    public void ResumePastTheEndWrapsToTheFront()
    {
        Assert.Equal([1, 2], DurableMaintenanceService.SelectRotated([1, 2, 3], resumeAfter: 3, cap: 2));
        Assert.Equal([1, 2], DurableMaintenanceService.SelectRotated([1, 2, 3], resumeAfter: 99, cap: 2));
    }

    [Fact]
    public void ADisappearedResumePartitionResumesAtItsSuccessor()
    {
        // The previously swept partition may no longer be due (or led); the rotation resumes at the
        // next higher id rather than resetting to the front, so no range gets skipped.
        Assert.Equal([4, 6], DurableMaintenanceService.SelectRotated([2, 4, 6], resumeAfter: 3, cap: 2));
    }
}
