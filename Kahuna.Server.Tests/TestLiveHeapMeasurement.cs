using System.Runtime.CompilerServices;

namespace Kahuna.Server.Tests;

/// <summary>
/// The memory tests of the install and of the store load are only as good as the number they subtract. These
/// check that number against a known amount of live memory.
/// </summary>
[Collection("ExclusiveAllocationMeasurement")]
public sealed class TestLiveHeapMeasurement
{
    private const int HeldBytes = 32 * 1024 * 1024;

    // A quarter of the held amount: far above the drift of an idle process, far below the amount itself.
    private const int Tolerance = HeldBytes / 4;

    private byte[]? held;

    // The array is allocated and dropped in methods of their own: an unoptimised build keeps a reference in the
    // frame that allocated it for as long as that frame lives, whatever is assigned to the field afterwards.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Hold() => held = new byte[HeldBytes];

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Release() => held = null;

    [Fact]
    public void Measure_FollowsLiveObjects_UpAndDown()
    {
        long before = LiveHeapMeasurement.Measure();

        Hold();
        long holding = LiveHeapMeasurement.Measure();

        Release();
        long released = LiveHeapMeasurement.Measure();

        Assert.InRange(holding - before, HeldBytes - Tolerance, HeldBytes + Tolerance);
        Assert.InRange(holding - released, HeldBytes - Tolerance, HeldBytes + Tolerance);
    }

    [Fact]
    public void Measure_OfAnUnchangedHeap_IsRepeatable()
    {
        long first = LiveHeapMeasurement.Measure();

        for (int i = 0; i < 10; i++)
            Assert.InRange(LiveHeapMeasurement.Measure() - first, -Tolerance / 8, Tolerance / 8);
    }

    [Fact]
    public void Sampler_SeesMemoryHeldOnlyWhileItRuns()
    {
        long before = LiveHeapMeasurement.Measure();
        long peak;

        using (LiveHeapPeakSampler sampler = new("test-heap-sampler"))
        {
            Hold();
            Thread.Sleep(200);
            Release();

            peak = sampler.Stop();
        }

        long after = LiveHeapMeasurement.Measure();

        Assert.InRange(peak - Math.Max(before, after), HeldBytes - Tolerance, HeldBytes + Tolerance);
    }
}
